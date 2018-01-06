(ns party-bus.dht.peer
  (:require [clojure.string :refer [join]]
            [medley.core :refer [abs]]
            [gloss.io :refer [encode decode]]
            [manifold.deferred :as md :refer [let-flow]]
            [party-bus.utils :as u]
            [party-bus.dht
             [peer-interface :as p :refer [get-address
                                           get-state
                                           update-state-in
                                           create-period
                                           create-deferred]]
             [curator :as curator]
             [codec :as codec]])
  (:import [java.net InetSocketAddress]
           [aleph.udp UdpPacket]
           [party_bus.dht.core Period ControlCommand Init Terminate]))

(def options
  {:contacts {:ping-period 200
              :expired-cleanup-period 1000
              :ttl 5000}
   :storage {:max-ttl 3600000
             :default-ttl 600000
             :expired-cleanup-period 1000}
   :request-timeout 2000})

(defn- send-to [p receiver msg]
  (p/send-to p receiver (encode codec/message msg)))

(defn- contact-expiration-ms []
  (+ (u/now-ms) (get-in options [:contacts :ttl])))

(defn- alive-contacts [p]
  (-> (get-state p)
      :contacts
      (u/idx-search >= (u/now-ms))))

(defn- -hash [x]
  ;TODO: SHA-1 to avoid collisions
  (hash (if (instance? InetSocketAddress x)
          (join ":" (u/host-port x))
          x)))

(defn- distance [hash-val hashable]
  (abs (- hash-val (-hash hashable))))

(defn- nearest-address [p hash-val]
  (apply min-key
         (partial distance hash-val)
         (-> p alive-contacts (conj (get-address p)))))

(defn- create-request [p]
  (let [d (create-deferred p)
        req-id (update-state-in p [:request-count] inc)]
    (update-state-in p [:requests] assoc req-id d)
    (md/finally d #(update-state-in p [:requests] dissoc req-id))
    (md/timeout! d (:request-timeout options) {:timeout? true})
    [req-id d]))

(defn- resolve-request [p message]
  (when-some [d (get-in (get-state p) [:requests (:request-id message)])]
    (md/success! d message)))

(defn- forward-lookup [p {h :hash {trace? :trace-route} :flags :as msg}]
  (let [address (get-address p)
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
                     (conj (:route req-msg) (get-address p))
                     [])}))

(defn- insert-kv-fn [k v ttl]
  (fn [storage]
    (-> storage
        (assoc-in [:data k] v)
        (update :expiration u/idx-assoc k (+ (u/now-ms) ttl)))))

(defn- key-ttl [storage k]
  (if-some [exp (get-in storage [:expiration :direct k])]
    (- exp (u/now-ms))
    0))

(declare period-handler packet-handler cmd-handler)

(defmulti handler (fn [_ msg] (class msg)))

(defmethod handler Init [p _]
  (create-period p :ping (get-in options [:contacts :ping-period]))
  (create-period p :expired-contacts-cleanup
                 (get-in options [:contacts :expired-cleanup-period]))
  (create-period p :expired-kv-cleanup
                 (get-in options [:storage :expired-cleanup-period])))

(defmethod handler Period [p {:keys [id]}]
  (period-handler p id))

(defmethod handler UdpPacket [p {:keys [sender message]}]
  (packet-handler p sender (decode codec/message message)))

(defmethod handler ControlCommand [p {:keys [cmd args]}]
  (cmd-handler p cmd args))

(defmethod handler Terminate [p _])

(defmulti period-handler (fn [p id] id))

(defmethod period-handler :ping [p _]
  (let [contacts (-> (get-state p) :contacts :direct keys set)]
    (doseq [contact (disj contacts (get-address p))]
      (send-to p contact {:type :ping}))))

(defmethod period-handler :expired-contacts-cleanup [p _]
  (update-state-in
   p [:contacts]
   (fn [cts]
     (reduce u/idx-dissoc cts
             (u/idx-search cts < (u/now-ms))))))

(defmethod period-handler :expired-kv-cleanup [p _]
  (update-state-in
   p [:storage]
   (fn [storage]
     (let [expired-keys (u/idx-search (:expiration storage) < (u/now-ms))]
       (-> storage
           (update :data #(reduce dissoc % expired-keys))
           (update :expiration #(reduce u/idx-dissoc % expired-keys)))))))

(defmulti packet-handler (fn [p sender message] (:type message)))

(defmethod packet-handler :ping [p sender message]
  (update-state-in p [:contacts]
                   u/idx-assoc
                   sender (contact-expiration-ms))
  (send-to p sender {:type :pong
                     :contacts (-> (alive-contacts p)
                                   set
                                   (disj sender)
                                   vec)}))
(defmethod packet-handler :store [p _ {k :key v :value ttl :ttl :as msg}]
  (when-not (forward-lookup p msg)
    (update-state-in p [:storage] (insert-kv-fn k v ttl))
    (respond-lookup p msg :store-response (get-address p))))

(defmethod packet-handler :store-response [p _ msg]
  (resolve-request p msg))

(defmethod packet-handler :find-value [p _ {k :key :as msg}]
  (let [storage (-> p get-state :storage)
        v (get-in storage [:data k])
        data {:value (or v "")
              :ttl (key-ttl storage k)}]
    (if v
      (respond-lookup p msg :find-value-response data)
      (when-not (forward-lookup p msg)
        (respond-lookup p msg :find-value-response data)))))

(defmethod packet-handler :find-value-response [p _ msg]
  (resolve-request p msg))

(defmethod packet-handler :pong [p sender {:keys [contacts]}]
  (update-state-in
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
        address (get-address p)
        nearest-addr (nearest-address p key-hash)
        sopts (:storage options)
        ttl (-> ttl (or (:default-ttl sopts)) (min (:max-ttl sopts)))
        route (if trace? [address] [])]
    (if (= nearest-addr address)
      (do
        (update-state-in p [:storage] (insert-kv-fn k v ttl))
        (doto (create-deferred p)
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
        (let-flow [{:keys [timeout? data route]} d]
                  (if timeout?
                    ::timeout
                    {:ttl ttl
                     :address data
                     :route route}))))))

(defmethod cmd-handler :get [p _ {k :key trace? :trace?}]
  (let [key-hash (-hash k)
        address (get-address p)
        storage (-> p get-state :storage)
        v (get-in storage [:data k])
        nearest-addr (nearest-address p key-hash)
        route (if trace? [address] [])]
    (if (or v (= nearest-addr address))
      (doto (create-deferred p)
        (md/success! {:value v
                      :ttl (key-ttl storage k)
                      :route route}))
      (let [[req-id d] (create-request p)]
        (send-to p nearest-addr
                 {:type :find-value
                  :hash key-hash
                  :flags {:trace-route trace?
                          :empty 0}
                  :response-address address
                  :request-id req-id
                  :route route
                  :key k})
        (let-flow
         [{timeout? :timeout? {:keys [ttl value]} :data route :route} d]
         (if timeout?
           ::timeout
           {:ttl ttl
            :value value
            :route route}))))))

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
