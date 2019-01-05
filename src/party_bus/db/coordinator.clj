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

(defn- configure [p error-logger epoch pids]
  (flow
    (=> (u/multicall p pids "get-meta" nil 1000) res)
    (let> [master->replica
           (->> res
                (map #(assoc (second %) :pid (first %)))
                (group-by :label)
                (map (fn [[label pair]]
                       (case (count pair)
                         1 nil ;;ignore to prevent promotion of old master,
                               ;;since there is no replica to compare
                         2 (let [[s s'] pair
                                 p (:promotion s)
                                 p' (:promotion s')
                                 next-p (inc (max p p'))]
                             (if (>= p p')
                               [(:pid s) {:pid (:pid s') :promotion next-p}]
                               [(:pid s') {:pid (:pid s) :promotion next-p}]))
                         (->> label
                              (format "Too many storages with label '%s'")
                              error-logger))))
                (into {}))])
    (doseq [[master-pid r] master->replica]
      (p/send-to p master-pid "promote" {:epoch epoch
                                         :promotion (:promotion r)})
      (p/send-to p (:pid r) "setup-replication" {:epoch epoch
                                                 :source-pid master-pid}))
    master->replica))

(defn- failover [p error-logger epoch master->replica pid]
  (if-let [r (master->replica pid)]
    (do
      (p/send-to p (:pid r) "promote" {:epoch epoch
                                       :promotion (-> r :promotion inc)})
      (dissoc master->replica pid))
    master->replica))

(defn- handler
  [p
   {:keys [epoch error-logger]}
   master->replica
   [_ body :as msg]]
  (case (u/msg-type msg)
    :configure
    (configure p error-logger epoch (p/watch-group p sr/group))
    :group-change
    (let [{:keys [pid deleted?]} body]
      (if deleted?
        (failover p error-logger epoch master->replica pid)
        master->replica))))

(defn- coordinator* [p epoch error-logger]
  (p/add-to-group p coordinator-group)
  (p/send-to p (p/get-pid p) "configure" nil)
  (u/receive-loop handler p {:epoch epoch :error-logger error-logger} {}))

(defn coordinator [p params error-logger]
  (paxos/proposer p (merge
                     params
                     {:group-suffix coordinator-group
                      :leader-process #(coordinator* %1 %2 error-logger)
                      :proposal-number 0})))
