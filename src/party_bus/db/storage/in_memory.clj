(ns party-bus.db.storage.in-memory
  (:require [clojure.data.avl :as avl]
            [medley.core :refer [map-vals]]
            [manifold [deferred :as md]]
            [party-bus.db.storage.core :as sc]))

(defn storage [{:keys [keys-history value-history]
                :or {keys-history 100
                     value-history 10}}]
  (let [storage (ref (avl/sorted-map)
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

      (get-value [this key options]
        (let [strg ((if (:ensure-key? options) ensure deref) storage)]
          (when-let [r (strg key)]
            ((if (:ensure-value? options) ensure deref) r))))

      (set-value [this key value _]
        (if-let [r (@storage key)]
          (ref-set r value)
          (alter storage assoc key (vref value)))
        nil)

      (del-value [this key]
        (when-let [r (@storage key)]
          (ref-set r nil))
        (alter storage dissoc key)
        nil)

      (get-key-range [this test key options]
        (let [strg ((if (:ensure-keys? options) ensure deref) storage)]
          (map first (subseq strg test key))))
      (get-key-range [this start-test start-key end-test end-key options]
        (let [strg ((if (:ensure-keys? options) ensure deref) storage)]
          (map first (subseq strg start-test start-key end-test end-key))))

      (end-transaction [this changed-keys]
        (md/success-deferred true))

      (controller [this _]
        (map-vals deref @storage)))))
