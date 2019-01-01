(ns party-bus.db.storage.replicated
  (:require [clojure.string :refer [blank?]]
            [amalloy.ring-buffer :refer [ring-buffer]]
            [manifold [deferred :as md]]
            [party-bus.core :as c :refer [flow => if> let>]]
            [party-bus.cluster
             [process :as p]
             [util :as u]]
            [party-bus.db.storage
             [core :as sc]
             [persistent-in-memory :as pims]]))

(def group "paired-storages")

(def ^:private meta-key "*meta*")

(defn- update-meta [tx f & args]
  (let [m (sc/get-val' tx meta-key)
        m' (apply f m args)]
    (sc/set-val' tx meta-key m')))

(defn- replicator
  [p pims *source-pid
   {:keys [initial-pull-timeout pull-timeout replication-period]}]
  (md/loop [timeout initial-pull-timeout
            ts (c/now-ms)
            position nil]
    (flow
      (=> (u/drop-responses p))
      (let> [source-pid @*source-pid])
      (if> source-pid
           :else (u/sleep-recur p replication-period
                                initial-pull-timeout (c/now-ms) nil))
      (=> (u/call p source-pid "pull-changes" position timeout) response)
      (let> [ts' (c/now-ms)
             lag (- ts' ts)])
      (if> (not= response ::skip)
           :else (do (u/sleep-recur p replication-period
                                    pull-timeout ts' position)))
      (if> (not= response :timeout)
           :else (do (sc/run-transaction' pims update-meta update
                                          :replication-lag + lag)
                     (u/sleep-recur p replication-period
                                    initial-pull-timeout ts' position)))
      (=> (sc/run-transaction'
           pims
           (fn [tx]
             (update-meta tx assoc :replication-lag lag)
             (doseq [m (:mutations response)
                     [k v] m]
               (if (some? v)
                 (sc/set-val' tx k v)
                 (sc/del-val' tx k))))))
      (u/sleep-recur p replication-period
                     pull-timeout ts' (:position response)))))

(defn storage
  [{:keys [label max-queue-size max-replication-lag]
    :as options}]
  {:pre [(not (blank? label))]}
  (let [pims (pims/storage options)
        coordinator-pid (atom nil)
        *epoch (atom 0)
        *source-pid (atom nil)
        position (ref 0)
        replication-queue (ref (ring-buffer max-queue-size))
        writable? #(and @coordinator-pid (not @*source-pid))]
    (reify sc/Storage
      (initialize [this source create?]
        (sc/initialize pims source create?)
        (sc/run-transaction' pims update-meta
                             (fn [m]
                               (if-not m
                                 {:label label
                                  :replication-lag Long/MAX_VALUE}
                                 (do
                                   (assert (= label (:label m)) label)
                                   m)))))
      (get-value [this key options]
        (sc/get-value pims key options))

      (set-value [this key value options]
        (assert (not= key meta-key) "Setting meta is forbidden")
        (assert (writable?) "Read-only")
        (sc/set-value pims key value options))

      (del-value [this key]
        (assert (not= key meta-key) "Deleting meta is forbidden")
        (assert (writable?) "Read-only")
        (sc/del-value pims key))

      (get-key-range [this test key options]
        (sc/get-key-range pims test key options))
      (get-key-range [this start-test start-key end-test end-key options]
        (sc/get-key-range pims start-test start-key end-test end-key options))

      (end-transaction [this changed-keys]
        (let [d (md/deferred)
              mutations (->> changed-keys
                             (map
                              (fn [k]
                                [k (sc/get-value pims k nil)]))
                             (into {}))]
          (commute position inc)
          (commute replication-queue conj [mutations d])
          (md/chain' (sc/end-transaction pims changed-keys) (constantly d))))

      (snapshot [this]
        (sc/snapshot pims))

      (controller [this p]
        (p/spawn p (partial sc/controller pims) {:bound? true})
        (p/spawn p #(replicator % pims *source-pid options) {:bound? true})
        (p/watch-group p sc/coordinator-group)
        (p/add-to-group p group)
        (md/loop []
          (flow
            (=> (p/receive p) [_ body sender-pid :as msg])
            (case (u/msg-type msg)
              :get-meta
              (u/response p msg (assoc (sc/get-value pims meta-key nil)
                                       :max-replication-lag
                                       max-replication-lag))
              :configure
              (let [{:keys [epoch source-pid]} body]
                (when (>= epoch @*epoch)
                  (reset! *epoch epoch)
                  (reset! coordinator-pid sender-pid)
                  (reset! *source-pid source-pid)
                  (when-not source-pid
                    (sc/run-transaction' pims update-meta
                                         assoc :replication-lag 0))))
              :group-change
              (let [{:keys [pid deleted?]} body]
                (when (and deleted? (= pid @coordinator-pid))
                  (reset! coordinator-pid nil)))
              :pull-changes
              (if (writable?)
                (let [pos body
                      [ack-mutations response]
                      (dosync
                      ;;holds read locks during transaction
                       (let [pos' (ensure position)
                             rqueue (ensure replication-queue)
                             ack-amount (- (count rqueue) (- pos' (or pos 0)))
                             full-sync? (or (nil? pos) (neg? ack-amount))]
                         (if full-sync?
                           [()
                            {:mutations [(dissoc (sc/snapshot pims) meta-key)]
                             :position pos'}]
                           (do
                             (ref-set replication-queue
                                      (reduce (fn [q _] (pop q))
                                              rqueue
                                              (range ack-amount)))
                             [(take ack-amount rqueue)
                              {:mutations (->> rqueue (drop ack-amount)
                                               (map first) vec)
                               :position pos'}]))))]
                  (u/response p msg response)
                  (run! #(md/success! % true) (map second ack-mutations)))
                (u/response p msg ::skip)))
            (md/recur)))))))
