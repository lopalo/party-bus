(ns party-bus.core
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

(def set-conj (fnil conj #{}))

(defn disj-dissoc [m k v]
  (let [s (-> m (get k) (disj v))]
    (if (empty? s)
      (dissoc m k)
      (assoc m k s))))

(defn dissoc-empty [m k]
  (let [v (get m k)]
    (if (empty? v)
      (dissoc m k)
      m)))

(defrecord Index [direct inverse])

(declare idx-dissoc)

(defn idx-assoc [idx k v]
  (let [idx (if (= (get-in idx [:direct k] ::no-val) ::no-val)
              idx
              (idx-dissoc idx k))]
    (-> idx
        (assoc-in [:direct k] v)
        (update-in [:inverse v] set-conj k))))

(defn idx-dissoc [idx k]
  (let [v (get-in idx [:direct k] ::no-val)]
    (-> idx
        (update :direct dissoc k)
        (update :inverse disj-dissoc v k))))

(defn idx-search [idx & args]
  (mapcat second (apply subseq (:inverse idx) args)))

(def index (Index. (hash-map) (sorted-map)))

(defn =>
  ([expr]
   (=> expr '_))
  ([expr name]
   (assert nil "=> used not in (flow ...) block")))

(defn let> [bindings]
  (assert nil "let> used not in (flow ...) block"))

(defn when> [test]
  (assert nil "when> used not in (flow ...) block"))

(defn- form= [form v]
  (and (list? form) (= (resolve (first form)) v)))

(defmacro flow [& body]
  `(md/chain'
    (do
      ~@((fn self [[form & forms]]
           (cond
             (nil? form)
             ()
             (form= form #'=>)
             (let [[_ form' name] form
                   name (or name '_)]
               (list `(md/chain' ~form' (fn [~name] ~@(self forms)))))
             (form= form #'let>)
             (list `(let ~(second form) ~@(self forms)))
             (form= form #'when>)
             (list `(when ~(second form) ~@(self forms)))
             :default
             (cons form (self forms))))
         body))))

(defn load-edn [source]
  (with-open [r (io/reader source)]
    (edn/read (PushbackReader. r))))

(defn prefix [s]
  (subs s 0 (dec (count s))))

(defn next-string [s]
  (->> s last int inc char (str (prefix s))))

