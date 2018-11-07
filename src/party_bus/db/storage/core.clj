(ns party-bus.db.storage.core
  (:require [clojure.java.io :as io]
            [clojure.string :refer [split]]
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

(defn fsync [^FileOutputStream stream]
  (.flush stream)
  (-> stream ^FileDescriptor .getFD .sync))

(defn file-type+number [^File file]
  (let [[nm tp] (-> file .getName (split #"\."))
        number (try (Long/parseLong nm)
                    (catch NumberFormatException _))]
    [(keyword tp) number]))

(defn collect-files [directory]
  (reduce
   (fn [collection file]
     (let [[tp number] (file-type+number file)
           sm-assoc (fnil assoc (sorted-map))]
       (if (integer? number)
         (update collection tp sm-assoc number file)
         collection)))
   {}
   (file-seq (io/file directory))))

(defn read-mutations [^File log-file]
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

(defn write-mutations [^FileOutputStream log-stream mutations]
  (doseq [m mutations
          :let [^bytes bs (n/freeze m)]]
    (->> bs count ^bytes (long->byte-array) (.write log-stream))
    (.write log-stream bs)))

(defn make-log-stream [^File log-file]
  (FileOutputStream. ^File log-file true))

(defn lock-dir [directory]
  (let [file (io/file directory "lock")]
    (try
      (-> file FileOutputStream. ^FileChannel .getChannel .tryLock)
      (catch OverlappingFileLockException _))))
