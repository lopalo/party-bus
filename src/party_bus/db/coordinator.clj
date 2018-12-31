(ns party-bus.db.coordinator
  (:require [manifold.deferred :as md]
            [party-bus.core :refer [flow => let>]]
            [party-bus.cluster
             [process :as p]
             [util :as u]]
            [party-bus.paxos :as paxos]
            [party-bus.db.storage.core :as sc]
            [party-bus.db.storage.replicated :as sr]))

(def coordinator-group sc/coordinator-group)

(def acceptor-group (paxos/gr paxos/acceptor-group coordinator-group))
(def proposer-group (paxos/gr paxos/proposer-group coordinator-group))

(defn acceptor [p]
  (paxos/acceptor p {:group-suffix coordinator-group}))

(defn- configure-replication [p error-logger epoch pids]
  (flow
    (=> (u/multicall p pids "get-meta" nil 1000) res)
    (let> [configure
           (fn [source replica]
             (when (< (:replication-lag source)
                      (:max-replication-lag source))
               (p/send-to p (:pid source) "configure"
                          {:epoch epoch :source-pid nil})
               (when (:pid replica)
                 (p/send-to p (:pid replica) "configure"
                            {:epoch epoch :source-pid (:pid source)}))))])
    (doseq [[label pair] (->> res
                              (map #(assoc (second %) :pid (first %)))
                              (group-by :label))]
      (case (count pair)
        0 nil
        1 (configure (first pair) nil)
        2 (let [[s s'] pair
                rl (:replication-lag s)
                rl' (:replication-lag s')]
            (cond
              (= rl rl')
              (configure (assoc s :replication-lag 0) s')
              (< rl rl')
              (configure s s')
              :else
              (configure s' s)))
        (error-logger (format "Too many storages with label '%s'" label))))))

(defn- coordinator* [p epoch error-logger]
  (p/add-to-group p coordinator-group)
  (flow
    (=> (configure-replication p error-logger epoch
                               (p/watch-group p sr/group)))
    (md/loop []
      (flow
        (=> (p/receive p) msg)
        (=> (case (u/msg-type msg)
              :group-change
              (configure-replication p error-logger epoch
                                     (p/get-group-members p sr/group))))
        (md/recur)))))

(defn coordinator [p params error-logger]
  (paxos/proposer p (merge
                     params
                     {:group-suffix coordinator-group
                      :leader-process #(coordinator* %1 %2 error-logger)
                      :proposal-number 0})))
