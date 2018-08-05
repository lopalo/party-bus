(ns party-bus.dht.peer.core
  (:require [clojure.string :refer [join]]
            [gloss.io :refer [encode decode]]
            [manifold.deferred :as md]
            aleph.udp
            [digest :refer [sha1]]
            [party-bus.utils :as u]
            [party-bus.dht.peer-interface :as p :refer [get-address
                                                        get-state
                                                        update-state-in
                                                        create-deferred]]
            [party-bus.dht.peer.codec :as codec])
  (:import [java.net InetSocketAddress]
           [aleph.udp UdpPacket]
           [party_bus.dht.core Period ControlCommand Init Terminate]))

(def ^:const N 160)

(def ^:const exponents (range N))

(defn config [p & path]
  (-> p get-state :config deref (get-in path)))

(defn send-to [p receiver msg]
  (p/send-to p receiver (encode codec/message msg)))

(defn hash- [x]
  (-> (if (instance? InetSocketAddress x)
        (join ":" (u/host-port x))
        x)
      ^String sha1
      (BigInteger. 16)))

(def ^:const max-hash
  (as-> "f" $ (repeat 40 $) (apply str $) (BigInteger. ^String $ 16)))

(defn distance [^BigInteger h ^BigInteger h']
  (.abs (.subtract h' h)))

(defn nearest-address [p hash-val]
  (let [state (get-state p)
        h (:hash state)
        pointers (get-in state [:contacts :pointers])
        [a] (first (rsubseq pointers <= hash-val))
        [b] (first (subseq pointers > hash-val))
        point (min-key (partial distance hash-val) h (or a h) (or b h))]
    (if (= point h)
      (get-address p)
      (first (pointers point)))))

(defn create-request
  ([p]
   (create-request p (config p :request-timeout)))
  ([p timeout]
   (let [d (create-deferred p)
         req-id (update-state-in p [:request-count] inc)]
     (update-state-in p [:requests] assoc req-id d)
     (md/finally' d #(update-state-in p [:requests] dissoc req-id))
     (md/timeout! d timeout {:timeout? true})
     [req-id d])))

(defn resolve-request [p message]
  (when-some [d (get-in (get-state p) [:requests (:request-id message)])]
    (md/success! d message)))

(defn forward-lookup [p {h :hash {trace? :trace-route} :flags :as msg}]
  (let [address (get-address p)
        nearest-addr (nearest-address p h)]
    (if (= nearest-addr address)
      false
      (do
        (send-to p nearest-addr
                 (update msg :route #(if trace? (conj % address) [])))
        true))))

(defn respond-lookup
  [p req-msg response-type data]
  (when (get-in req-msg [:flags :respond])
    (send-to p (:response-address req-msg)
             {:type response-type
              :request-id (:request-id req-msg)
              :data data
              :route (if (get-in req-msg [:flags :trace-route])
                       (conj (:route req-msg) (get-address p))
                       [])})))

(declare period-handler packet-handler cmd-handler)

(defmulti ^:private handler (fn [_ _ msg] (class msg)))

(defmethod handler Init [p hooks _]
  (update-state-in p [:hash] (constantly (-> p get-address hash-)))
  ((:init hooks) p))

(defmethod handler Period [p _ {:keys [id]}]
  (period-handler p id))

(defmethod handler UdpPacket [p _ {:keys [sender message]}]
  (packet-handler p sender (decode codec/message message)))

(defmethod handler ControlCommand [p _ {:keys [cmd args]}]
  (cmd-handler p cmd args))

(defmethod handler Terminate [p hooks _]
  ((:terminate hooks) p))

(defmulti period-handler (fn [p id] id))

(defmulti packet-handler (fn [p sender message] (:type message)))

(defmulti cmd-handler (fn [p cmd args] cmd))

(def peer-handler handler)
