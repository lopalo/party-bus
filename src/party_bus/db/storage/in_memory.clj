(ns party-bus.db.storage.in-memory
  (:require [clojure.java.io :as io]
            [clojure.string :refer [split]]
            [medley.core :refer [map-vals remove-vals]]
            [manifold [deferred :as md]]
            [party-bus.cluster
             [process :as p]
             [util :as u]]
            [party-bus.db.storage.core :as sc])
  (:import [java.io
            File
            FileDescriptor
            FileOutputStream
            FileInputStream]
           [java.nio.channels
            FileChannel
            OverlappingFileLockException]))

(defn storage [{:keys [keys-history value-history]
                :or {keys-history 100
                     value-history 10}}]
  (let [storage (ref (sorted-map)
                     :min-history keys-history
                     :max-history keys-history)
        vref #(ref %
                   :min-history value-history
                   :max-history value-history)]
    (reify sc/Storage
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
