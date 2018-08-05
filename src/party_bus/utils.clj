(ns party-bus.utils
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn]
            [clojure.string :refer [split join]]
            [gloss.core :as g]
            [manifold.deferred :as md])
  (:import [java.net InetSocketAddress]
           [java.io PushbackReader]))

(defn now-ms []
  ;Should it be monotonic?
  (System/currentTimeMillis))

(defn socket-address
  ([^long port] (socket-address "127.0.0.1" port))
  ([^String host ^long port] (InetSocketAddress. host port)))

(defn host-port [^InetSocketAddress address]
  [(.getHostString address) (.getPort address)])

(defn str->host-port [string]
  (let [[host port] (split string #":")]
    [host (Integer/parseInt port)]))

(defn str->socket-address [string]
  (apply socket-address (str->host-port string)))

(g/defcodec ipv4-c
  (-> 4 (repeat :ubyte) vec)
  (fn [string]
    (map #(Integer/parseInt %)
         (split string #"\.")))
  (fn [parts]
    (join "." parts)))

(g/defcodec address-c
  [ipv4-c :uint16]
  host-port
  (fn [[host port]]
    (socket-address host port)))

(defrecord Index [direct inverse])

(declare idx-dissoc)

(defn idx-assoc [idx k v]
  (let [idx (if (= (get-in idx [:direct k] ::no-val) ::no-val)
              idx
              (idx-dissoc idx k))]
    (-> idx
        (assoc-in [:direct k] v)
        (update-in [:inverse v] (fnil conj #{}) k))))

(defn idx-dissoc [idx k]
  (let [v (get-in idx [:direct k] ::no-val)
        size (count (get-in idx [:inverse v]))
        idx (if (> size 1) (update-in idx [:inverse v] disj k)
                (update idx :inverse dissoc v))]
    (update idx :direct dissoc k)))

(defn idx-search [idx & args]
  (mapcat second (apply subseq (:inverse idx) args)))

(def index (Index. (hash-map) (sorted-map)))

(defmacro let<
  "Similar to Manifold's let-flow, but with less magic."
  [bindings & body]
  ((fn self [name+forms]
     (if (seq name+forms)
       (let [[[n form] & name+forms'] name+forms]
         `(md/chain' ~form (fn [~n] ~(self name+forms'))))
       `(do ~@body)))
   (partition 2 bindings)))

(defn load-edn [source]
  (with-open [r (io/reader source)]
    (edn/read (PushbackReader. r))))

