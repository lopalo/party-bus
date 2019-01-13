(ns party-bus.db.storage.replicated
  (:require [clojure.string :refer [blank?]]
            [clojure.set :refer [difference]]
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
  [p pims *source-pid *replication-lag
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
           :else (do (swap! *replication-lag + lag)
                     (u/sleep-recur p replication-period
                                    initial-pull-timeout ts' position)))
      (reset! *replication-lag lag)
      (=> (sc/run-transaction'
           pims
           (fn [tx]
             (when-let [snapshot (:snapshot response)]
               (let [ks (set (sc/get-keys' tx >= "" {:ensure-keys? true}))
                     ks' (set (keys snapshot))]
                 (doseq [k (difference ks ks')
                         :when (not= k meta-key)]
                   (sc/del-val' tx k))
                 (doseq [[k v] snapshot]
                   (sc/set-val' tx k v))))
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
        *coordinator-pid (atom nil)
        *epoch (atom 0)
        *replicator-pid (atom nil)
        *source-pid (atom nil)
        *replication-lag (atom 0)
        position (ref 0)
        replication-queue (ref (ring-buffer max-queue-size))
        writable? #(and @*coordinator-pid (not @*source-pid))]
    (reify sc/Storage
      (initialize [this source create?]
        (if (sc/initialize pims source create?)
          (do
            (sc/run-transaction' pims update-meta
                                 (fn [m]
                                   (if-not m
                                     {:label label
                                      :promotion 0}
                                     (do
                                       (assert (= label (:label m)) label)
                                       m))))
            true)
          false))
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

      (end-transaction [this modified-keys]
        (let [d (md/deferred)
              mutations (->> modified-keys
                             (map
                              (fn [k]
                                [k (sc/get-value pims k nil)]))
                             (into {}))]
          (commute position inc)
          (commute replication-queue conj [mutations d])
          (md/chain' (sc/end-transaction pims modified-keys) (constantly d))))

      (snapshot [this]
        (sc/snapshot pims))

      (controller [this p]
        (p/spawn p (partial sc/controller pims) {:bound? true})
        (p/spawn p
                 #(replicator % pims *source-pid *replication-lag options)
                 {:bound? true})
        (p/watch-group p sc/coordinator-group)
        (p/add-to-group p group)
        (md/loop []
          (flow
            (=> (p/receive p) [_ body sender-pid :as msg])
            (case (u/msg-type msg)
              :get-meta
              (u/response p msg (sc/get-value pims meta-key nil))
              :promote
              (let [{:keys [epoch promotion]} body]
                (when (and (>= epoch @*epoch)
                           (<= @*replication-lag max-replication-lag))
                  (reset! *epoch epoch)
                  (reset! *coordinator-pid sender-pid)
                  (reset! *replication-lag 0)
                  (reset! *source-pid nil)
                  (sc/run-transaction' pims update-meta
                                       assoc :promotion promotion)))
              :setup-replication
              (let [{:keys [epoch source-pid]} body]
                (when (>= epoch @*epoch)
                  (reset! *epoch epoch)
                  (reset! *coordinator-pid sender-pid)
                  (reset! *replication-lag (* 2 max-replication-lag))
                  (reset! *source-pid source-pid)))
              :group-change
              (let [{:keys [pid deleted?]} body]
                (when (and deleted? (= pid @*coordinator-pid))
                  (reset! *coordinator-pid nil)))
              :pull-changes
              (if (writable?)
                (let [pos body
                      [ack-mutations response]
                      (dosync
                      ;;holds read locks during transaction
                       (let [pos' (ensure position)
                             rqueue (ensure replication-queue)
                             ack-amount (- (count rqueue) (- pos' (or pos 0)))
                             full-sync? (or (not= @*replicator-pid sender-pid)
                                            (nil? pos)
                                            (neg? ack-amount))]
                         (if full-sync?
                           (do
                             (reset! *replicator-pid sender-pid)
                             [()
                              {:snapshot (dissoc (sc/snapshot pims) meta-key)
                               :position pos'}])
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
