(ns party-bus.dht.peer.core
  (:require [clojure.string :refer [join]]
            [medley.core :refer [abs]]
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

(def N 160)

(def options
  {:contacts {:pointers-step 1
              :ping {:period 4000
                     :timeout 3000}
              :stabilization {:period 6000
                              :step 1
                              :range N
                              :timeout 5000}}
   :request-timeout 2000
   :storage {:max-ttl 3600000
             :default-ttl 600000
             :expired-cleanup-period 1000}
   :trie {:upcast-period 4000
          :node-ttl 15000
          :expired-cleanup-period 1000}})

(defn send-to [p receiver msg]
  (p/send-to p receiver (encode codec/message msg)))

(defn hash- [x]
  (-> (if (instance? InetSocketAddress x)
        (join ":" (u/host-port x))
        x)
      ^String sha1
      (BigInteger. 16)
      bigint))

;TODO: optimization

(def ^:private max-hash
  (as-> "f" $ (repeat 40 $) (apply str $) (BigInteger. ^String $ 16)))

(def ^:private ^BigInteger two (biginteger 2))

(defn pointers [origin n step]
  (for [sign [- +]
        n (range 0 n step)
        :let [v (sign origin (.pow two n))]
        :when (< 0 v max-hash)]
    v))

(defn distance [h h']
  (abs (- h' h)))

(defn nearest-address [p hash-val]
  (let [state (get-state p)
        h (:hash state)
        contacts (get-in state [:contacts :pointers])
        pointers' (conj (keys contacts) h)
        pointer (apply min-key (partial distance hash-val) pointers')]
    (if (= pointer h)
      (get-address p)
      (first (contacts pointer)))))

(defn create-request
  ([p]
   (create-request p (:request-timeout options)))
  ([p timeout]
   (let [d (create-deferred p)
         req-id (update-state-in p [:request-count] inc)]
     (update-state-in p [:requests] assoc req-id d)
     (md/finally d #(update-state-in p [:requests] dissoc req-id))
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
