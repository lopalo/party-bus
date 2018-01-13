(ns party-bus.dht.peer
  (:require [clojure.string :refer [join]]
            [medley.core :refer [abs]]
            [gloss.io :refer [encode decode]]
            [manifold.deferred :as md :refer [let-flow]]
            [digest :refer [sha1]]
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

(set! *warn-on-reflection* true)

(def options
  {:stabilization-period 3000
   :storage {:max-ttl 3600000
             :default-ttl 600000
             :expired-cleanup-period 1000}
   :request-timeout 2000})

(defn- send-to [p receiver msg]
  (p/send-to p receiver (encode codec/message msg)))

(defn- hash- [x]
  (-> (if (instance? InetSocketAddress x)
        (join ":" (u/host-port x))
        x)
      ^String sha1
      (BigInteger. 16)
      bigint))

(def ^:private N 160)

(def ^:private max-hash
  (as-> "f" $ (repeat 40 $) (apply str $) (BigInteger. ^String $ 16)))

(def ^:private ^BigInteger two (biginteger 2))

(defn- direction [origin hash-val]
  (if (< hash-val origin) :left :right))

(defn- pointers [origin sign]
  (for [n (range N)
        :let [v (sign origin (.pow two n))]
        :when (< 0 v max-hash)]
    v))

(def ^:private distances (into (sorted-set) (pointers 0 +)))

(defn- distance [h h']
  (abs (- h' h)))

(defn- nearest-address [p hash-val]
  (let [state (get-state p)
        h (:hash state)
        contacts (get-in state [:contacts (direction h hash-val)])
        [_ address] (first (rsubseq contacts <= (distance h hash-val)))]
    address))

(defn- insert-contact [p address]
  (let [h (-> p get-state :hash)
        h' (hash- address)
        dist (first (rsubseq distances <= (distance h h')))]
    (when-not (= address (get-address p))
      (update-state-in p [:contacts (direction h h') dist]
                       #(if % % address)))))

(defn- insert-seeds [p]
  (run! (partial insert-contact p)
        (get-in (get-state p) [:contacts :seeds])))

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
    (insert-contact p (:response-address msg))
    (if (= nearest-addr address)
      false
      (do
        (send-to p nearest-addr
                 (update msg :route #(if trace? (conj % address) [])))
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
  (let [address (get-address p)
        h (hash- address)
        contacts (sorted-map 0 address)]
    (update-state-in p [:hash] (constantly h))
    (update-state-in p [:contacts :left] (constantly contacts))
    (update-state-in p [:contacts :right] (constantly contacts))
    (insert-seeds p)
    (create-period p :stabilization (:stabilization-period options))
    (create-period p :expired-kv-cleanup
                   (get-in options [:storage :expired-cleanup-period]))))

(defmethod handler Period [p {:keys [id]}]
  (period-handler p id))

(defmethod handler UdpPacket [p {:keys [sender message]}]
  (packet-handler p sender (decode codec/message message)))

(defmethod handler ControlCommand [p {:keys [cmd args]}]
  (cmd-handler p cmd args))

(defmethod handler Terminate [p _])

(defmulti period-handler (fn [p id] id))

(defmethod period-handler :stabilization [p _]
  (let [address (get-address p)
        state (get-state p)
        cs (:contacts state)
        empty-contacts? (-> (:left cs) (merge (:right cs)) (dissoc 0) empty?)
        h (:hash state)
        stabilize
        (fn [dir]
          (apply
           md/zip
           (for [pointer (pointers h (if (= dir :left) - +))
                 :let [nearest-addr (nearest-address p pointer)]
                 :when (not= nearest-address address)]
             (let [[req-id d] (create-request p)]
               (send-to p nearest-addr
                        {:type :find-peer
                         :hash pointer
                         :flags {:trace-route false
                                 :empty 0}
                         :response-address address
                         :request-id req-id
                         :route []})
               (let-flow
                [dist (distance h pointer)
                 {timeout? :timeout? address' :data} d
                 dir' (if address' (direction h (hash- address')))]
                (if timeout?
                  (update-state-in p [:contacts dir] dissoc dist)
                  (when (and (not= address' address) (= dir dir'))
                    (update-state-in p [:contacts dir dist]
                                     (constantly address')))))))))]
    (when empty-contacts?
      (insert-seeds p))
    (md/zip
     (stabilize :left)
     (stabilize :right))))

(defmethod period-handler :expired-kv-cleanup [p _]
  (update-state-in
   p [:storage]
   (fn [storage]
     (let [expired-keys (u/idx-search (:expiration storage) < (u/now-ms))]
       (-> storage
           (update :data #(reduce dissoc % expired-keys))
           (update :expiration #(reduce u/idx-dissoc % expired-keys)))))))

(defmulti packet-handler (fn [p sender message] (:type message)))

(defmethod packet-handler :find-peer [p _ {k :key :as msg}]
  (when-not (forward-lookup p msg)
    (respond-lookup p msg :find-peer-response (get-address p))))

(defmethod packet-handler :find-peer-response [p _ msg]
  (resolve-request p msg))

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

(defmulti cmd-handler (fn [p cmd args] cmd))

(defmethod cmd-handler :put [p _ {k :key v :value ttl :ttl trace? :trace?}]
  (let [key-hash (hash- k)
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
  (let [key-hash (hash- k)
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
                       {:hash nil
                        :contacts
                        {:seeds (set contacts)
                         :left nil
                         :right nil}
                        :storage
                        {:data {}
                         :expiration u/index}
                        :request-count 0
                        :requests {}}))
