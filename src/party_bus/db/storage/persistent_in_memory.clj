(ns party-bus.db.storage.persistent-in-memory
  (:require [clojure.java.io :as io]
            [clojure.string :refer [split]]
            [medley.core :refer [remove-vals]]
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
            FileDescriptor
            FileOutputStream
            FileInputStream
            DataOutputStream
            DataInputStream
            EOFException]
           [java.nio.channels
            FileLock]))

(defn- file-type+number [^File file]
  (let [[nm tp] (-> file .getName (split #"\."))
        number (try (Long/parseLong nm)
                    (catch NumberFormatException _))]
    [(keyword tp) number]))

(defn- collect-files [directory]
  (reduce
   (fn [collection file]
     (let [[tp number] (file-type+number file)
           sm-assoc (fnil assoc (sorted-map))]
       (if (integer? number)
         (update collection tp sm-assoc number file)
         collection)))
   {}
   (file-seq (io/file directory))))

(defn- get-snapshot+logs [directory create?]
  (let [files (collect-files directory)
        [snapshot-num snapshot-file]
        (if-let [snapshot (-> files :snapshot first)]
          snapshot
          (if create?
            (let [f (io/file directory "0.snapshot")]
              (n/freeze-to-file f {} sc/nippy-opts)
              [0 f])
            (sc/illegal-state! "No snapshot file")))
        logs (subseq (:log files (sorted-map)) >= snapshot-num)
        log-num
        (if-let [[last-num] (last logs)]
          (inc last-num)
          (if create?
            snapshot-num
            (sc/illegal-state! "No log file")))
        log-file (doto (io/file directory (str log-num ".log"))
                   .createNewFile)
        log-files (concat (map second logs) (list log-file))]
    [snapshot-file log-files]))

(defn- ^FileOutputStream make-log-stream [^File log-file]
  (FileOutputStream. ^File log-file true))

(defn- fsync [^FileOutputStream stream]
  (.flush stream)
  (-> stream ^FileDescriptor .getFD .sync))

(defn- read-mutations [^File log-file]
  (let [^DataInputStream
        stream (-> log-file FileInputStream. DataInputStream.)]
    ((fn self []
       (lazy-seq
        (try
          (let [length (.readInt stream)
                bs (byte-array length)]
            (.readFully stream bs)
            (cons (n/thaw bs) (self)))
          (catch EOFException _
            (.close stream))))))))

(defn- write-mutations [^FileOutputStream log-stream mutations]
  (let [stream (DataOutputStream. log-stream)]
    (doseq [m mutations
            :let [^bytes bs (n/freeze m sc/nippy-opts)]]
      (.writeInt stream (alength bs))
      (.write stream bs))))

(defn storage
  [{:keys [log-write-period
           log-compaction-size]
    :or {log-write-period 100
         log-compaction-size 104857600}
    :as options}]
  (let [ims (ims/storage options)
        dir-lock (atom nil)
        initial-log-file (atom nil)
        write-queue (ref [])]
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
                       (->> snapshot-file n/thaw-from-file transient)
                       (->> log-files
                            (mapcat read-mutations)
                            (apply concat))))]
            (reset! initial-log-file (last log-files))
            (sc/initialize ims data true))
          false))

      (get-value [this key options]
        (sc/get-value ims key options))

      (set-value [this key value options]
        (sc/set-value ims key value options))

      (del-value [this key]
        (sc/del-value ims key))

      (get-key-range [this test key options]
        (sc/get-key-range ims test key options))
      (get-key-range [this start-test start-key end-test end-key options]
        (sc/get-key-range ims start-test start-key end-test end-key options))

      (end-transaction [this changed-keys]
        (let [d (md/deferred)
              mutations (->> changed-keys
                             (map
                              (fn [k]
                                [k (sc/get-value ims k nil)]))
                             (into {}))]
          (commute write-queue conj [mutations d])
          d))

      (snapshot [this]
        (sc/snapshot ims))

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
                                   (sc/snapshot ims))]
                    (ref-set write-queue [])
                    [snapshot wqueue]))]

             (write-mutations log-stream (map first wqueue))
             (fsync log-stream)
             (run! #(md/success! % true) (map second wqueue))
             (let [[log-file' log-stream']
                   (if compaction?
                     (let [number (-> log-file file-type+number second inc)
                           dir (.getParent log-file)
                           log-file' (io/file dir (str number ".log"))
                           log-stream' (make-log-stream log-file')]
                       (.close log-stream)
                       (p/spawn
                        p
                        (fn [_]
                          (let [path (str dir "/" number ".snapshot")
                                ^FileOutputStream
                                stream (FileOutputStream. path)
                                snapshot (->> snapshot
                                              (into {})
                                              (remove-vals nil?))]
                            (.write stream ^bytes (n/freeze snapshot
                                                            sc/nippy-opts))
                            (fsync stream)
                            (let [files (collect-files dir)
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
