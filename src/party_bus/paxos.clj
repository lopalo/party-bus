(ns party-bus.paxos
  (:require [clojure.set :refer [intersection]]
            [manifold.deferred :as md]
            [party-bus.core :refer [flow => let> if>]]
            [party-bus.cluster
             [process :as p]
             [util :as u]]))

(def acceptor-group "paxos-acceptors")
(def proposer-group "paxos-proposers")
(def leader-group "paxos-leaders")

(defn gr [group suffix]
  (str group ":" suffix))

(defn- acceptor-handler
  [p
   {:keys [group-suffix]}
   {:keys [accepted-proposal proposal-number] :as state}
   [_ body :as msg]]
  (case (u/msg-type msg)
    :prepare
    (let [proposal-number' body]
      (if (> proposal-number' proposal-number)
        (do
          (u/response p msg {:status :promise
                             :accepted-proposal accepted-proposal})
          (assoc state :proposal-number proposal-number'))
        (do
          (u/response p msg {:status :refusal
                             :proposal-number proposal-number})
          state)))
    :accept
    (let [{proposal-number' :proposal-number pid :pid} body
          proposers (p/get-group-members p (gr proposer-group group-suffix))]
      (if (and (= proposal-number' proposal-number)
               (contains? proposers pid))
        (do
          (u/response p msg {:status :accepted})
          (assoc state :accepted-proposal {:number proposal-number'
                                           :pid pid}))
        (do
          (u/response p msg {:status :refusal})
          state)))
    :group-change
    (let [{:keys [pid group deleted?]} body]
      (if (and (= group (gr proposer-group group-suffix))
               deleted?
               (= pid (get accepted-proposal :pid)))
        (assoc state :accepted-proposal nil)
        state))
    :get-state
    (do
      (u/response p msg state)
      state)))

(defn acceptor [p {:keys [group-suffix] :as params}]
  (p/add-to-group p (gr acceptor-group group-suffix))
  (p/watch-group p (gr proposer-group group-suffix))
  (let [state {:accepted-proposal nil
               :proposal-number 0}]
    (u/receive-loop acceptor-handler p params state)))

(defn proposer
  [p
   {:keys [group-suffix
           acceptor-pids
           proposal-number
           election-pause
           leader-process]
    [min-round-pause max-round-pause] :round-pause
    rt :request-timeout
    :as params}]
  (flow
    (let> [pid (p/get-pid p)
           acceptor-gr (gr acceptor-group group-suffix)
           leader-gr (gr leader-group group-suffix)
           get-acceptors #(intersection (p/get-group-members p acceptor-gr)
                                        acceptor-pids)
           quorum (-> acceptor-pids count (/ 2) int inc)
           valid? #(>= (count (intersection acceptor-pids %)) quorum)
           restart
           (fn [proposal-number]
             (let [params (assoc params :proposal-number (inc proposal-number))]
               (p/spawn p #(proposer % params))
               (p/terminate p)))])
    (p/add-to-group p (gr proposer-group group-suffix))
    (=> (u/sleep p election-pause))
    (=> (md/loop []
          (when (or (seq (p/get-group-members p leader-gr))
                    (not (valid? (get-acceptors))))
            (u/sleep-recur p election-pause))))
    (=> (md/loop [proposal-number proposal-number]
          (flow
            (when-not (valid? (get-acceptors))
              (restart proposal-number))
            (=> (u/drop-responses p))
            (=> (u/multicall p acceptor-pids "prepare" proposal-number rt) rs)
            (let> [values (vals rs)
                   promises (filter #(= (:status %) :promise) values)
                   {{accepted-pid :pid} :accepted-proposal}
                   (when (seq promises)
                     (apply max-key
                            #(get-in % [:accepted-proposal :number] 0)
                            promises))
                   proposal-pid (or accepted-pid pid)
                   accept-body {:proposal-number proposal-number
                                :pid proposal-pid}
                   next-proposal-number (->> values
                                             (filter #(= (:status %) :refusal))
                                             (map :proposal-number)
                                             (reduce max proposal-number)
                                             inc)
                   round-pause (+ min-round-pause
                                  (rand-int (- max-round-pause
                                               min-round-pause)))])
            (if> (>= (count promises) quorum)
                 :else (u/sleep-recur p round-pause next-proposal-number))
            (=> (u/multicall p acceptor-pids "accept" accept-body rt) rs)
            (let> [acceptances (filter #(= (-> % second :status) :accepted) rs)
                   elector-pids (->> acceptances (map first) set)])
            (if> (>= (count acceptances) quorum)
                 :else (u/sleep-recur p round-pause next-proposal-number))
            (when (not= proposal-pid pid)
              (restart next-proposal-number))
            [elector-pids proposal-number]))
        [elector-pids proposal-number])
    (let> [acceptor-pids' (p/watch-group p acceptor-gr)
           elector-pids (intersection elector-pids acceptor-pids')])
    (when-not (valid? elector-pids)
      (restart proposal-number))
    (p/add-to-group p leader-gr)
    (p/spawn p #(leader-process % proposal-number) {:bound? true})
    (=> (md/loop [elector-pids elector-pids]
          (flow
            (=> (p/receive-with-header p (str "group-change:" acceptor-gr))
                [_ {:keys [pid deleted?]}])
            (let> [elector-pids' (if deleted?
                                   (disj elector-pids pid)
                                   elector-pids)])
            (when (valid? elector-pids')
              (md/recur elector-pids')))))
    (restart proposal-number)))
