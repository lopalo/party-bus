(ns party-bus.db.storage.core
  (:require [clojure.java.io :as io]
            [manifold [deferred :as md]]
            [party-bus.core :as c])
  (:import [java.io
            FileOutputStream]
           [java.nio.channels
            FileChannel
            OverlappingFileLockException]))

(defprotocol Storage
  (initialize [this source create?])
  (get-value [this key options])
  (set-value [this key value options])
  (del-value [this key])
  (get-key-range
    [this test key options]
    [this start-test start-key end-test end-key options])
  (end-transaction [this modified-keys])
  (snapshot [this])
  (controller [this p]))

(defprotocol Transaction
  (get-val
    [this key-space key]
    [this key-space key options])
  (get-keys
    [this key-space test key]
    [this key-space test key options]
    [this key-space start-test start-key end-test end-key]
    [this key-space start-test start-key end-test end-key options])
  (set-val
    [this key-space key value]
    [this key-space key value options])
  (del-val [this key-space key]))

(defn run-transaction [key-spaces f & args]
  (let [modified-keys (ref {})
        tx
        (reify Transaction
          (get-val [this key-space key]
            (get-val this key-space key nil))
          (get-val [this key-space key options]
            (get-value (key-spaces key-space) key options))

          (get-keys [this key-space test key]
            (get-keys this key-space test key nil))
          (get-keys [this key-space test key options]
            (get-key-range (key-spaces key-space) test key options))
          (get-keys [this key-space start-test start-key end-test end-key]
            (get-keys this key-space start-test start-key end-test end-key nil))
          (get-keys
            [this key-space start-test start-key end-test end-key options]
            (get-key-range (key-spaces key-space)
                           start-test start-key end-test end-key
                           options))

          (set-val [this key-space key value]
            (set-val this key-space key value nil))
          (set-val [this key-space key value options]
            (alter modified-keys update key-space c/set-conj key)
            (set-value (key-spaces key-space) key value options))

          (del-val [this key-space key]
            (alter modified-keys update key-space c/set-conj key)
            (del-value (key-spaces key-space) key)))
        [result deferreds]
        (dosync
         [(apply f tx args)
          (doall
           (for [[key-space ks] @modified-keys]
             (end-transaction (key-spaces key-space) ks)))])]
    (md/chain'
     (apply md/zip' deferreds)
     (constantly result))))

(defprotocol SingleStorageTransaction
  (get-val'
    [this key]
    [this key options])
  (get-keys'
    [this test key]
    [this test key options]
    [this start-test start-key end-test end-key]
    [this start-test start-key end-test end-key options])
  (set-val'
    [this key value]
    [this key value options])
  (del-val' [this key]))

(defn run-transaction' [storage f & args]
  (let [modified-keys (ref #{})
        tx
        (reify SingleStorageTransaction
          (get-val' [this key]
            (get-val' this key nil))
          (get-val' [this key options]
            (get-value storage key options))

          (get-keys' [this test key]
            (get-keys' this test key nil))
          (get-keys' [this test key options]
            (get-key-range storage test key options))
          (get-keys' [this start-test start-key end-test end-key]
            (get-keys' this start-test start-key end-test end-key nil))
          (get-keys'
            [this start-test start-key end-test end-key options]
            (get-key-range storage
                           start-test start-key end-test end-key
                           options))

          (set-val' [this key value]
            (set-val' this key value nil))
          (set-val' [this key value options]
            (alter modified-keys conj key)
            (set-value storage key value options))

          (del-val' [this key]
            (alter modified-keys conj key)
            (del-value storage key)))
        [result deferred]
        (dosync
         [(apply f tx args)
          (if (seq @modified-keys)
            (end-transaction storage @modified-keys)
            (md/success-deferred true))])]
    (md/chain' deferred (constantly result))))

(defn lock-dir [directory]
  (let [file (io/file directory "lock")]
    (try
      (-> file FileOutputStream. ^FileChannel .getChannel .tryLock)
      (catch OverlappingFileLockException _))))

(def nippy-opts {:compressor :auto
                 :encryptor nil
                 :password nil})

(defn illegal-state! [^String msg]
  (throw (IllegalStateException. msg)))

(def coordinator-group "coordinator")
