(ns party-bus.dht.peer
  (:require [medley.core :refer [abs]]
            [gloss.io :refer [encode decode]]
            [manifold.deferred :as md :refer [let-flow]]
            [party-bus.utils :as u]
            [party-bus.dht
             [peer-interface :as p]
             [curator :as curator]
             [codec :as codec]])
  (:import [aleph.udp UdpPacket]
           [party_bus.dht.core Period ControlCommand Init Terminate]))

(def options
  {:contacts {:ping-period 200
              :expired-cleanup-period 1000
              :ttl 5000}
   :storage {:max-ttl 3600000
             :default-ttl 600000
             ;TODO: deleting expired keys from data and expiration maps
             :expired-cleanup-period 1000}
   :lookup-timeout 2000})

(defn- send-to [p receiver msg]
  (p/send-to p receiver (encode codec/message msg)))

(defn- contact-expiration-ms []
  (+ (u/now-ms) (get-in options [:contacts :ttl])))

(defn- alive-contacts [p]
  (-> (p/get-state p)
      :contacts
      (u/idx-search >= (u/now-ms))))

(defn- -hash [x]
  (hash x)) ;TODO: SHA-1 to avoid collisions

(defn- distance [hash-val hashable]
  (abs (- hash-val (-hash hashable))))

(defn- nearest-address [p hash-val]
  (apply min-key
         (partial distance hash-val)
         (-> p alive-contacts (conj (p/get-address p)))))

(defn- create-request [p]
  (let [d (p/create-deferred p)
        req-id (p/update-state-in p [:request-count] inc)]
    (p/update-state-in p [:requests] assoc req-id d)
    (md/finally d #(p/update-state-in p [:requests] dissoc req-id))
    [req-id d]))

(defn- resolve-request [p message]
  (when-some [d (get-in (p/get-state p) [:requests (:request-id message)])]
    (md/success! d message)))

(defn- insert-kv-fn [k v ttl]
  (fn [storage]
    (-> storage
        (assoc-in [:data k] v)
        (update-in [:expiration] u/idx-assoc k (+ (u/now-ms) ttl)))))

(defn- forward-lookup [p {h :hash {trace? :trace-route} :flags :as msg}]
  (let [address (p/get-address p)
        nearest-addr (nearest-address p h)]
    (if (= nearest-addr address)
      false
      (do
        (send-to p nearest-addr
                 (update msg :route #(if trace? (conj % address))))
        true))))

(defn- respond-lookup
  [p req-msg response-type data]
  (send-to p (:response-address req-msg)
             {:type response-type
              :request-id (:request-id req-msg)
              :data data
              :route (if (get-in req-msg [:flags :trace-route])
                       (conj (:route req-msg) (p/get-address p))
                       [])}))

(declare period-handler packet-handler cmd-handler)

(defmulti handler (fn [_ msg] (class msg)))

(defmethod handler Init [p _]
  (p/create-period p :ping (get-in options [:contacts :ping-period]))
  (p/create-period p :expired-cleanup
                   (get-in options [:contacts :expired-cleanup-period])))

(defmethod handler Period [p {:keys [id]}]
  (period-handler p id))

(defmethod handler UdpPacket [p {:keys [sender message]}]
  (packet-handler p sender (decode codec/message message)))

(defmethod handler ControlCommand [p {:keys [cmd args]}]
  (cmd-handler p cmd args))

(defmethod handler Terminate [p _])

(defmulti period-handler (fn [p id] id))

(defmethod period-handler :ping [p _]
  (let [contacts (-> (p/get-state p) :contacts :direct keys set)]
    (doseq [contact (disj contacts (p/get-address p))]
      (send-to p contact {:type :ping}))))

(defmethod period-handler :expired-cleanup [p _]
  (p/update-state-in
   p [:contacts]
   (fn [cts]
     (reduce u/idx-dissoc cts
             (u/idx-search cts < (u/now-ms))))))

(defmulti packet-handler (fn [p sender message] (:type message)))

(defmethod packet-handler :ping [p sender message]
  (p/update-state-in p [:contacts]
                     u/idx-assoc
                     sender (contact-expiration-ms))
  (send-to p sender {:type :pong
                     :contacts (-> (alive-contacts p)
                                   set
                                   (disj sender)
                                   vec)}))
(defmethod packet-handler :store [p _ {k :key v :value ttl :ttl :as msg}]
  (when-not (forward-lookup p msg)
    (p/update-state-in p [:storage] (insert-kv-fn k v ttl))
    (respond-lookup p msg :store-response (p/get-address p))))

(defmethod packet-handler :store-response [p _ msg]
  (resolve-request p msg))

(defmethod packet-handler :find-value [p _ msg])
  ;TODO: if value is present in storage, return it

(defmethod packet-handler :pong [p sender {:keys [contacts]}]
  (p/update-state-in
   p [:contacts]
   (fn [cts]
     (as-> contacts $
           (zipmap $ (repeat (u/now-ms)))
           (assoc $ sender (contact-expiration-ms))
           (map (fn [[c exp]] [c (max (get-in cts [:direct c] 0) exp)]) $)
           (reduce (partial apply u/idx-assoc) cts $)))))

(defmulti cmd-handler (fn [p cmd args] cmd))

(defmethod cmd-handler :put [p _ {k :key v :value ttl :ttl trace? :trace?}]
  (let [key-hash (-hash k)
        address (p/get-address p)
        nearest-addr (nearest-address p key-hash)
        sopts (:storage options)
        ttl (-> ttl (or (:default-ttl sopts)) (min (:max-ttl sopts)))
        route (if trace? [address] [])]
    (if (= nearest-addr address)
      (do
        (p/update-state-in p [:storage] (insert-kv-fn k v ttl))
        (doto (p/create-deferred p)
          (md/success! {:ttl ttl
                        :address address
                        :route route})))
      (let [[req-id d] (create-request p)]
        (send-to p nearest-addr
                 {:type :store
                  :hash key-hash
                  :flags {:trace-route trace?
                          :empty 0}
                  :response-address address
                  :request-id req-id
                  :route route
                  :key k
                  :value v
                  :ttl ttl})
        (md/timeout! d (:lookup-timeout options) {:timeout? true})
        (let-flow [{:keys [timeout? data route]} d]
                  (if timeout?
                    ::timeout
                    {:ttl ttl
                     :address data
                     :route route}))))))

(defmethod cmd-handler :get [p _ {k :key}])

(defn create-peer [curator host port contacts]
  (curator/create-peer curator host port handler
                       {:contacts
                        (reduce #(u/idx-assoc %1 %2 (u/now-ms))
                                u/index contacts)
                        :storage
                        {:data {}
                         :expiration u/index}
                        :request-count 0
                        :requests {}}))
