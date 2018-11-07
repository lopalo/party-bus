(ns party-bus.db.storage.persistent-in-memory
  (:require [clojure.java.io :as io]
            [clojure.string :refer [split]]
            [medley.core :refer [map-vals remove-vals]]
            [manifold [deferred :as md]]
            [taoensso.nippy :as n]
            [party-bus.cluster
             [process :as p]
             [util :as u]]
            [party-bus.db.storage
             [core :as sc]
             [in-memory :as ims]])
  (:import [java.io
            File
            FileOutputStream]
           [java.nio.channels
            FileLock]))

(defn- get-snapshot+logs [directory create?]
  (let [files (sc/collect-files directory)
        [snapshot-num snapshot-file]
        (if-let [snapshot (-> files :snapshot first)]
          snapshot
          (if create?
            (let [f (io/file directory "0.snapshot")]
              (n/freeze-to-file f {})
              [0 f])
            (throw (IllegalStateException. "No snapshot file"))))
        logs (subseq (:log files (sorted-map)) >= snapshot-num)
        log-num
        (if-let [[last-num] (last logs)]
          (inc last-num)
          (if create?
            snapshot-num
            (throw (IllegalStateException. "No log file"))))
        log-file (doto (io/file directory (str log-num ".log"))
                   .createNewFile)
        log-files (concat (map second logs) (list log-file))]
    [snapshot-file log-files]))

(defn storage
  [{:keys [queue-history
           log-write-period
           log-compaction-size]
    :or {queue-history 10
         log-write-period 100
         log-compaction-size 104857600}
    :as options}]
  (let [ims (ims/storage options)
        log-compaction-size 300 ;;TODO: delete
        dir-lock (atom nil)
        initial-log-file (atom nil)
        write-queue (ref []
                         :min-history queue-history
                         :max-history queue-history)]
    (reify sc/Storage
      (initialize [this source create?]
        (when create?
          (.mkdirs (io/file source)))
        (if (reset! dir-lock (sc/lock-dir source))
          (let [[snapshot-file log-files] (get-snapshot+logs source create?)
                data (persistent!
                      (reduce
                       (fn [data [k v]]
                         (if (some? v)
                           (assoc! data k v)
                           (dissoc! data k)))
                       (->> snapshot-file n/thaw-from-file (into {}) transient)
                       (->> log-files
                            (mapcat sc/read-mutations)
                            (apply concat))))]
            (reset! initial-log-file (last log-files))
            (sc/initialize ims data true))
          false))
      (get-value [this key ensure-key? ensure-value?]
        (sc/get-value ims key ensure-key? ensure-value?))
      (set-value [this key value]
        (sc/set-value ims key value))
      (del-value [this key]
        (sc/del-value ims key))
      (get-key-range [this test key ensure-keys?]
        (sc/get-key-range ims test key ensure-keys?))
      (get-key-range [this start-test start-key end-test end-key ensure-keys?]
        (sc/get-key-range ims start-test start-key end-test end-key ensure-keys?))
      (end-transaction [this keys]
        (let [d (md/deferred)
              mutations (->> keys
                             (map
                              (fn [k]
                                [k (sc/get-value ims k false false)]))
                             (into {}))]
          (commute write-queue conj [mutations d])
          d))
      (controller [this p]
        (md/finally'
         (md/loop [^File
                   log-file @initial-log-file
                   ^FileOutputStream
                   log-stream (sc/make-log-stream @initial-log-file)]
           (let [d (u/sleep p log-write-period)
                 compaction? (>= (.length log-file) log-compaction-size)
                 [snapshot wqueue]
                 (dosync
                   ;;holds read lock of write-queue during creation a snapshot
                  (let [wqueue (ensure write-queue)
                        snapshot (when compaction?
                                   (sc/controller ims nil))]
                    (ref-set write-queue [])
                    [snapshot wqueue]))]

             (sc/write-mutations log-stream (map first wqueue))
             (sc/fsync log-stream)
             (run! #(md/success! % true) (map second wqueue))
             (let [[log-file' log-stream']
                   (if compaction?
                     (let [snapshot (remove-vals nil? snapshot)
                           number (-> log-file sc/file-type+number second inc)
                           dir (.getParent log-file)
                           log-file' (io/file dir (str number ".log"))
                           log-stream' (sc/make-log-stream log-file')]
                       (.close log-stream)
                       (p/spawn
                        p
                        (fn [_]
                          (let [path (str dir "/" number ".snapshot")
                                ^FileOutputStream
                                stream (FileOutputStream. path)]
                            (.write stream ^bytes (n/freeze snapshot))
                            (sc/fsync stream)
                            (let [files (sc/collect-files dir)
                                  sm (sorted-map)
                                  old-files #(subseq (% files sm) < number)]
                              (doseq [[_ f] (old-files :snapshot)]
                                (.delete ^File f))
                              (doseq [[_ f] (old-files :log)]
                                (.delete ^File f))))))
                       [log-file' log-stream'])
                     [log-file log-stream])]

               (md/chain d (fn [_] (md/recur log-file' log-stream'))))))
         #(.release ^FileLock @dir-lock))))))
