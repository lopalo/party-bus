(ns party-bus.db.storage
  (:require [clojure.java.io :as io]
            [clojure.string :refer [split]]
            [medley.core :refer [map-vals remove-vals]]
            [manifold [deferred :as md]]
            [gloss.data.primitives :refer [long->byte-array]]
            [gloss.io :refer [decode]]
            [taoensso.nippy :as n]
            [party-bus.core :as c]
            [party-bus.cluster
             [process :as p]
             [util :as u]])
  (:import [java.io
            File
            FileDescriptor
            FileOutputStream
            FileInputStream]
           [java.nio.channels
            FileChannel
            FileLock
            OverlappingFileLockException]))

(defprotocol Storage
  (initialize [this source create?])
  (get-value [this key ensure-key? ensure-value?])
  (set-value [this key value])
  (del-value [this key])
  (get-key-range
    [this test key ensure-keys?]
    [this start-test start-key end-test end-key ensure-keys?])
  (end-transaction [this keys])
  (controller [this p]))

(defprotocol Transaction
  (get-val
    [this key-space key]
    [this key-space key ensure-key?]
    [this key-space key ensure-key? ensure-value?])
  (get-keys
    [this key-space test key]
    [this key-space test key ensure-keys?]
    [this key-space start-test start-key end-test end-key]
    [this key-space start-test start-key end-test end-key ensure-keys?])
  (set-val [this key-space key value])
  (del-val [this key-space key]))

(defn run-transaction [key-spaces f & args]
  (let [modified-keys (atom {})
        tx
        (reify Transaction
          (get-val [this key-space key]
            (get-val this key-space key true))
          (get-val [this key-space key ensure-key?]
            (get-val this key-space key ensure-key? true))
          (get-val [this key-space key ensure-key? ensure-value?]
            (get-value (key-spaces key-space) key ensure-key? ensure-value?))

          (get-keys [this key-space test key]
            (get-keys this key-space test key true))
          (get-keys [this key-space test key ensure-keys?]
            (get-key-range (key-spaces key-space) test key ensure-keys?))
          (get-keys [this key-space start-test start-key end-test end-key]
            (get-keys this key-space
                      start-test start-key end-test end-key
                      true))
          (get-keys
            [this key-space start-test start-key end-test end-key ensure-keys?]
            (get-key-range (key-spaces key-space)
                           start-test start-key end-test end-key
                           ensure-keys?))

          (set-val [this key-space key value]
            (swap! modified-keys update key-space c/set-conj key)
            (set-value (key-spaces key-space) key value))

          (del-val [this key-space key]
            (swap! modified-keys update key-space c/set-conj key)
            (del-value (key-spaces key-space) key)))
        [result deferreds]
        (dosync
         (reset! modified-keys {})
         [(apply f tx args)
          (doall
           (for [[key-space ks] @modified-keys]
             (end-transaction (key-spaces key-space) ks)))])]
    (md/chain'
     (apply md/zip' deferreds)
     (constantly result))))

(defn in-memory [{:keys [keys-history value-history]
                  :or {keys-history 100
                       value-history 10}}]
  (let [storage (ref (sorted-map)
                     :min-history keys-history
                     :max-history keys-history)
        vref #(ref %
                   :min-history value-history
                   :max-history value-history)]
    (reify Storage
      (initialize [this source _]
        (let [data (map-vals vref source)]
          (dosync
           (alter storage into data)))
        true)

      (get-value [this key ensure-key? ensure-value?]
        (let [strg ((if ensure-key? ensure deref) storage)]
          (when-let [r (strg key)]
            ((if ensure-value? ensure deref) r))))

      (set-value [this key value]
        (if-let [r (@storage key)]
          (ref-set r value)
          (alter storage assoc key (vref value)))
        nil)

      (del-value [this key]
        (when-let [r (@storage key)]
          (ref-set r nil))
        (alter storage dissoc key)
        nil)

      (get-key-range [this test key ensure-keys?]
        (let [strg ((if ensure-keys? ensure deref) storage)]
          (map first (subseq strg test key))))
      (get-key-range [this start-test start-key end-test end-key ensure-keys?]
        (let [strg ((if ensure-keys? ensure deref) storage)]
          (map first (subseq strg start-test start-key end-test end-key))))

      (end-transaction [this keys]
        (md/success-deferred true))

      (controller [this _]
        (map-vals deref @storage)))))

(defn- fsync [^FileOutputStream stream]
  (.flush stream)
  (-> stream ^FileDescriptor .getFD .sync))

(defn- file-num+type [^File file]
  (let [[nm tp] (-> file .getName (split #"\."))
        number (try (Long/parseLong nm)
                    (catch NumberFormatException _))]
    [number (keyword tp)]))

(defn- find-snapshots+logs [directory]
  (reduce
   (fn [[snapshots logs] file]
     (let [[number tp] (file-num+type file)]
       (if (integer? number)
         (case tp
           :snapshot [(assoc snapshots number file) logs]
           :log [snapshots (assoc logs number file)]
           [snapshots logs])
         [snapshots logs])))
   [(sorted-map) (sorted-map)]
   (file-seq (io/file directory))))

(defn- get-snapshot+logs [directory create?]
  (let [[snapshots logs] (find-snapshots+logs directory)
        [snapshot-num snapshot-file]
        (if (seq snapshots)
          (first snapshots)
          (if create?
            (let [f (io/file directory "0.snapshot")]
              (n/freeze-to-file f {})
              [0 f])
            (throw (IllegalStateException. "No snapshot file"))))
        log-files (map second (subseq logs >= snapshot-num))
        log-files
        (if (seq log-files)
          log-files
          (if create?
            (let [file (io/file directory "0.log")]
              (.createNewFile file)
              (list file))
            (throw (IllegalStateException. "No log file"))))]
    [snapshot-file log-files]))

(defn- read-mutations [^File log-file]
  (let [^FileInputStream
        stream (FileInputStream. log-file)]
    ((fn self []
       (lazy-seq
        (let [long-length 8
              length-bs (byte-array long-length)
              read-count (.read stream length-bs)
              mutation
              (when (= read-count long-length)
                (let [length (decode :int64 length-bs)
                      data-bs (byte-array length)
                      read-count (.read stream data-bs)]
                  (when (= read-count length)
                    (n/thaw data-bs))))]
          (if mutation
            (cons mutation (self))
            (.close stream))))))))

(defn- write-mutations [^FileOutputStream log-stream mutations]
  (doseq [m mutations
          :let [^bytes bs (n/freeze m)]]
    (->> bs count ^bytes (long->byte-array) (.write log-stream))
    (.write log-stream bs)))

(defn- make-log-stream [^File log-file]
  (FileOutputStream. ^File log-file true))

(defn- lock-dir [directory]
  (let [file (io/file directory "lock")]
    (try
      (-> file FileOutputStream. ^FileChannel .getChannel .tryLock)
      (catch OverlappingFileLockException _))))

(defn in-memory+disk [{:keys [queue-history
                              log-write-period
                              log-compaction-size]
                       :or {queue-history 10
                            log-write-period 100
                            log-compaction-size 104857600}
                       :as options}]
  (let [ims (in-memory options)
        dir-lock (atom nil)
        initial-log-file (atom nil)
        write-queue (ref []
                         :min-history queue-history
                         :max-history queue-history)]
    (reify Storage
      (initialize [this source create?]
        (when create?
          (.mkdirs (io/file source)))
        (if (reset! dir-lock (lock-dir source))
          (let [[snapshot-file log-files] (get-snapshot+logs source create?)
                data (persistent!
                      (reduce
                       (fn [data [k v]]
                         (if (some? v)
                           (assoc! data k v)
                           (dissoc! data k)))
                       (->> snapshot-file n/thaw-from-file (into {}) transient)
                       (->> log-files
                            (mapcat read-mutations)
                            (apply concat))))]
            (reset! initial-log-file (last log-files))
            (initialize ims data true))
          false))
      (get-value [this key ensure-key? ensure-value?]
        (get-value ims key ensure-key? ensure-value?))
      (set-value [this key value]
        (set-value ims key value))
      (del-value [this key]
        (del-value ims key))
      (get-key-range [this test key ensure-keys?]
        (get-key-range ims test key ensure-keys?))
      (get-key-range [this start-test start-key end-test end-key ensure-keys?]
        (get-key-range ims start-test start-key end-test end-key ensure-keys?))
      (end-transaction [this keys]
        (let [d (md/deferred)
              mutations (->> keys
                             (map
                              (fn [k]
                                [k (get-value ims k false false)]))
                             (into {}))]
          (commute write-queue conj [mutations d])
          d))
      (controller [this p]
        (md/finally'
         (md/loop [^File
                   log-file @initial-log-file
                   ^FileOutputStream
                   log-stream (make-log-stream @initial-log-file)]
           (let [d (u/sleep p log-write-period)
                 compaction? (>= (.length log-file) log-compaction-size)
                 [snapshot wqueue]
                 (dosync
                   ;;holds read lock of write-queue during creation a snapshot
                  (let [wqueue (ensure write-queue)
                        snapshot (when compaction?
                                   (controller ims nil))]
                    (ref-set write-queue [])
                    [snapshot wqueue]))]

             (write-mutations log-stream (map first wqueue))
             (fsync log-stream)
             (run! #(md/success! % true) (map second wqueue))
             (let [[log-file' log-stream']
                   (if compaction?
                     (let [snapshot (remove-vals nil? snapshot)
                           number (-> log-file file-num+type first inc)
                           dir (.getParent log-file)
                           log-file' (io/file dir (str number ".log"))
                           log-stream' (make-log-stream log-file')]
                       (.close log-stream)
                       (p/spawn
                        p
                        (fn [_]
                          (let [path (str dir "/" number ".snapshot")
                                ^FileOutputStream
                                stream (FileOutputStream. path)]
                            (.write stream ^bytes (n/freeze snapshot))
                            (fsync stream)
                            (let [[snapshots logs] (find-snapshots+logs dir)]
                              (doseq [[_ f] (subseq snapshots < number)]
                                (.delete ^File f))
                              (doseq [[_ f] (subseq logs < number)]
                                (.delete ^File f))))))
                       [log-file' log-stream'])
                     [log-file log-stream])]

               (md/chain d (fn [_] (md/recur log-file' log-stream'))))))
         #(.release ^FileLock @dir-lock))))))
