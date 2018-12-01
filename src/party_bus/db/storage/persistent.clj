(ns party-bus.db.storage.persistent
  (:require [clojure.java.io :as io]
            [clojure.data.avl :as avl]
            [medley.core :refer [map-vals filter-vals]]
            [manifold [deferred :as md]]
            [taoensso.nippy :as n]
            [party-bus.core :as c]
            [party-bus.cluster
             [process :as p]
             [util :as u]]
            [party-bus.db.storage
             [core :as sc]
             [in-memory :as ims]])
  (:import [java.io
            File
            FileOutputStream
            RandomAccessFile
            EOFException]
           [java.nio.channels
            FileLock]))

(defrecord Files [keys values mutations])

(defrecord Pages [occupancy
                  key-pages value-pages
                  key-freelist value-freelist])

(defrecord Occupancy [key-page value-page amount])

(defrecord PendingMutations [keys values deferreds])

(def ^:private pending-mutations* (PendingMutations. {} {} []))

(def ^:private default-storage-options
  {:key-page-size 128
   :value-page-size 512
   :write-period 3000
   :working-set-capactity 1000
   :eviction-period 1000})

(def ^:private header-size 2)

(defn- ^RandomAccessFile make-file [^File file]
  (RandomAccessFile. file "rw"))

(defn- get-files [directory create?]
  (apply ->Files
         (for [fname '("keys" "values" "mutations")]
           (let [file (io/file directory fname)]
             (when-not (.exists file)
               (if create?
                 (.createNewFile file)
                 (->> fname (format "No %s file") sc/illegal-state!)))
             (make-file file)))))

(defn- fsync [^RandomAccessFile file]
  (-> file ^FileDescriptor .getFD .sync))

(defn- fsync-kv [{:keys [keys values]}]
  (fsync keys)
  (fsync values))

(defn- truncate [^RandomAccessFile file]
  (.setLength file 0))

(defn- init-files
  [{:keys [^RandomAccessFile keys ^RandomAccessFile values]}
   {:keys [key-page-size value-page-size]}]
  (.seek keys 0)
  (.seek values 0)
  (when (= (.length keys) 0)
    (.writeShort keys key-page-size))
  (when (= (.length values) 0)
    (.writeShort values value-page-size)))

(defn- check-files
  [{:keys [^RandomAccessFile keys
           ^RandomAccessFile values]}
   {:keys [key-page-size value-page-size]}]
  (.seek keys 0)
  (.seek values 0)
  (when-not (= key-page-size (.readUnsignedShort keys))
    (sc/illegal-state! "Key page size doesn't match"))
  (when-not (= value-page-size (.readUnsignedShort values))
    (sc/illegal-state! "Value page size doesn't match")))

(defn- count-pages [^RandomAccessFile file page-size]
  (-> file .length (- header-size) (/ page-size) Math/ceil int))

(defn- peek* [v]
  (->> v count dec (nth v)))

(defn- empty?* [v]
  (zero? (count v)))

(def ^:private ss-conj (fnil conj (avl/sorted-set)))

(defn- group-pages [page-numbers]
  (->>
   (loop [[p & pages] (sort page-numbers)
          clusters (transient [])
          cluster (transient [])]
     (if p
       (if (or (empty?* cluster) (= (-> cluster peek* inc) p))
         (recur pages clusters (conj! cluster p))
         (recur pages
                (conj! clusters (persistent! cluster))
                (transient [p])))
       (if (empty?* cluster)
         clusters
         (conj! clusters (persistent! cluster)))))
   persistent!
   (group-by count)
   (into (avl/sorted-map))
   (map-vals #(->> % (map first) (into (avl/sorted-set))))))

(defn- read-pages
  [{:keys [^RandomAccessFile keys
           ^RandomAccessFile values]}
   {:keys [key-page-size value-page-size]}]
  (.seek keys header-size)
  (.seek values header-size)
  (let [key-pages (count-pages keys key-page-size)
        value-pages (count-pages values value-page-size)
        [occupancy key-freelist val-freelist]
        (loop [key-page 0
               occupancy (transient {})
               key-freelist (transient (avl/sorted-set))
               val-freelist (-> value-pages range set transient)]
          (let [key-bs (byte-array key-page-size)
                read-amount  (.read keys key-bs)
                end? (= read-amount -1)]
            (if end?
              (map persistent! [occupancy key-freelist val-freelist])
              (if (zero? (first key-bs))
                (recur (inc key-page)
                       occupancy
                       (conj! key-freelist key-page)
                       val-freelist)
                (let [[key value-page amount] (n/thaw key-bs)
                      o (Occupancy. key-page value-page amount)
                      vpages (->> value-pages (range value-page) (take amount))]
                  (recur
                   (inc key-page)
                   (assoc! occupancy key o)
                   key-freelist
                   (reduce disj! val-freelist vpages)))))))
        value-freelist (group-pages val-freelist)]
    (Pages. occupancy key-pages value-pages key-freelist value-freelist)))

(defn- allocate-pages
  [{:keys [occupancy key-pages value-pages key-freelist value-freelist]
    :as pages}
   key
   amount]
  (if-let [o (occupancy key)]
    (do
      (when-not (= (:amount o) amount)
        (sc/illegal-state! "Cannot reallocate pages"))
      pages)
    (let [[key-page key-pages key-freelist]
          (if-let [page (first key-freelist)]
            [page key-pages (disj key-freelist page)]
            [key-pages (inc key-pages) key-freelist])
          [value-page value-pages value-freelist]
          (if-let [[size [page]] (-> value-freelist (subseq >= amount) first)]
            (let [value-freelist (c/disj-dissoc value-freelist size page)
                  size' (- size amount)
                  page' (+ page amount)
                  value-freelist (if (pos? size')
                                   (update value-freelist size' ss-conj page')
                                   value-freelist)]
              [page value-pages value-freelist])
            [value-pages (+ value-pages amount) value-freelist])
          o (Occupancy. key-page value-page amount)
          occupancy (assoc occupancy key o)]
      (Pages. occupancy key-pages value-pages key-freelist value-freelist))))

(defn- free-pages
  [{:keys [occupancy key-freelist value-freelist]
    :as pages}
   key]
  (if-let [{:keys [key-page value-page amount]} (occupancy key)]
    (let [occupancy (dissoc occupancy key)
          key-freelist (conj key-freelist key-page)
          value-freelist (update value-freelist amount ss-conj value-page)]
      (assoc pages
             :occupancy occupancy
             :key-freelist key-freelist
             :value-freelist value-freelist))
    pages))

(defn- read-mutations [^RandomAccessFile file]
  (try
    (let [length (.readInt file)
          bs (byte-array length)]
      (.readFully file bs)
      (n/thaw bs))
    (catch EOFException _)))

(defn- write-mutations [^RandomAccessFile file mutations]
  (when (-> mutations :keys empty? not)
    (let [^bytes bs (n/freeze mutations sc/nippy-opts)]
      (.writeInt file (alength bs))
      (.write file bs))))

(defn- apply-mutations
  [{:keys [^RandomAccessFile keys ^RandomAccessFile values]}
   {:keys [key-page-size value-page-size]}
   mutations]
  (when mutations
    (doseq [[page ^bytes bs] (:keys mutations)]
      (.seek keys (+ header-size (* page key-page-size)))
      (if bs
        (.write keys bs)
        (.writeByte keys 0)))
    (doseq [[page ^bytes bs] (:values mutations)]
      (.seek values (+ header-size (* page value-page-size)))
      (.write values bs))))

(defn- read-value [^RandomAccessFile values occupancy value-page-size key]
  (when-let [{:keys [value-page amount]} (occupancy key)]
    (let [bs (byte-array (* amount value-page-size))]
      (.seek values (+ header-size (* value-page value-page-size)))
      (.read values bs)
      (n/thaw bs))))

(defn storage
  [options]
  (let [{:keys [key-page-size
                value-page-size
                write-period
                working-set-capactity
                eviction-period]}
        (merge default-storage-options options)
        ims (ims/storage options)
        dir-lock (atom nil)
        *files (atom nil)
        *pages (ref nil)
        pending-mutations (ref pending-mutations*)
        access-time (ref c/index)
        touch #(commute access-time c/idx-assoc % (c/now-ms))]
    (reify sc/Storage
      (initialize [this source create?]
        (when create?
          (.mkdirs (io/file source)))
        (if (reset! dir-lock (sc/lock-dir source))
          (let [files (reset! *files (get-files source create?))]
            (init-files files options)
            (check-files files options)
            (->> files
                 :mutations
                 read-mutations
                 (apply-mutations files options))
            (fsync-kv files)
            (doto (:mutations files)
              truncate
              fsync)
            (let [pages (read-pages files options)]
              (dosync
               (ref-set *pages pages)))
            (sc/initialize ims {} true))
          false))

      (get-value [this key options]
        (let [v (sc/get-value ims key options)]
          (if (some? v)
            (do
              (touch key)
              v)
            (let [valf (:values @*files)
                  occupancy (:occupancy @*pages)
                  v (read-value valf occupancy value-page-size key)]
              (when (some? v)
                (sc/set-value ims key v options)
                (touch key)
                v)))))

      (set-value [this key value options]
        (sc/set-value ims key value options)
        (touch key)
        (let [amount (:pages options 1)
              pages @*pages ;;ensure in end-transaction prevents write skew
              pages' (allocate-pages pages key amount)]
          (when-not (= pages pages')
            (ref-set *pages pages'))))

      (del-value [this key]
        (commute access-time c/idx-dissoc key)
        (sc/del-value ims key))

      (get-key-range [this test key options]
        (let [{o :occupancy} ((if (:ensure-keys? options) ensure deref) *files)]
          (map first (subseq o test key))))
      (get-key-range [this start-test start-key end-test end-key options]
        (let [{o :occupancy} ((if (:ensure-keys? options) ensure deref) *files)]
          (map first (subseq o start-test start-key end-test end-key))))

      (end-transaction [this changed-keys]
        (let [d (md/deferred)
              mutations (->> changed-keys
                             (map
                              (fn [k]
                                [k (sc/get-value ims k nil)]))
                             (into {}))
              pages (ensure *pages)
              occupancy (:occupancy pages)
              keys-mutations
              (->> changed-keys
                   (keep
                    (fn [k]
                      (when-let [o (occupancy k)]
                        (let [data [k (:value-page o) (:amount o)]
                              ^bytes
                              bs (when (some? (mutations k))
                                   (n/freeze data sc/nippy-opts))]
                          (when (and (some? bs) (> (alength bs) key-page-size))
                            (sc/illegal-state!
                             (format "Key %s doesn't fit in page size" k)))
                          [(:key-page o) bs]))))
                   (into {}))
              vals-mutations
              (->> changed-keys
                   (keep
                    (fn [k]
                      (when-let [o (occupancy k)]
                        (let [v (mutations k)
                              ^bytes bs (n/freeze v sc/nippy-opts)]
                          (when (> (alength bs)
                                   (* (:amount o) value-page-size))
                            (sc/illegal-state!
                             (format
                              "Value for key %s doesn't fit in allocated size"
                              k)))
                          [(:value-page o) bs]))))
                   (into {}))
              del-keys (->> mutations (filter-vals nil?) keys)
              pages' (reduce free-pages pages del-keys)]
          (when-not (= pages pages')
            (ref-set *pages pages'))
          (commute pending-mutations
                   #(-> %
                        (update :keys merge keys-mutations)
                        (update :values merge vals-mutations)
                        (update :deferreds conj d)))
          d))

      (controller [this p]
        (p/spawn
         p
         (fn [p]
           (md/loop []
             (dosync
              (let [at @access-time
                    n (-> at :direct count (- working-set-capactity))
                    ks (take n (c/idx-search at < (c/now-ms)))]
                (doseq [k ks]
                  (commute access-time c/idx-dissoc k)
                  (sc/del-value ims k))))
             (u/sleep-recur p eviction-period)))
         {:bound? true})

        (md/finally'
         (md/loop []
           (let [d (u/sleep p write-period)
                 files @*files
                 mutations (dosync
                            (let [mutations @pending-mutations]
                              (ref-set pending-mutations pending-mutations*)
                              mutations))
                 kv-mutations (select-keys mutations [:keys :values])]
             (write-mutations (:mutations files) kv-mutations)
             (fsync (:mutations files))
             (apply-mutations files options kv-mutations)
             (fsync-kv files)
             (doto (:mutations files)
               truncate
               fsync)
             (run! #(md/success! % true) (:deferreds mutations))
             (md/chain d (fn [_] (md/recur)))))
         #(.release ^FileLock @dir-lock))))))
