(ns party-bus.dht.peer
  (:require [clojure.set :refer [union]]
            [gloss.io :refer [encode decode]]
            [party-bus.utils :as u]
            [party-bus.dht
             [peer-interface :as p]
             [curator :as curator]
             [codec :as codec]])
  (:import [aleph.udp UdpPacket]
           [party_bus.dht.core Period ControlCommand Init Terminate]))

(def options
  {:ping-period 200
   :contact-ttl 5000
   :ttl-check-period 1000})

(defn- send-to [p receiver msg]
  (p/send-to p receiver (encode codec/message msg)))

(defn- distance [hash-val hashable]
  (bit-xor hash-val (hash hashable)))

(defn- forward-lookup [p message]
  (let [forwarded? false]
    forwarded?))

(defn- respond-lookup [p message])

(declare period-handler packet-handler cmd-handler)

(defmulti handler (fn [_ msg] (class msg)))

(defmethod handler Init [p _]
  (p/create-period p :ping (:ping-period options))
  (p/create-period p :ttl-check (:ttl-check-period options)))

(defmethod handler Period [p {:keys [id]}]
  (period-handler p id))

(defmethod handler UdpPacket [p {:keys [sender message]}]
  (packet-handler p sender (decode codec/message message)))

(defmethod handler ControlCommand [p {:keys [cmd args]}]
  (cmd-handler p cmd args))

(defmethod handler Terminate [p _])

(defmulti period-handler (fn [p id] id))

(defmethod period-handler :ping [p _]
  (let [contacts (get-in (p/get-state p) [:contacts :all])]
    (p/update-state-in p [:contacts :all] empty)
    (doseq [contact (disj contacts (p/get-address p))]
      (send-to p contact {:type :ping}))))

(defmethod period-handler :ttl-check [p _]
  (let [alive (get-in (p/get-state p) [:contacts :alive])
        dead (u/idx-search alive < (- (u/now-ms) (:contact-ttl options)))]
    (p/update-state-in
     p [:contacts :alive] (partial reduce u/idx-dissoc) dead)))

(defmulti packet-handler (fn [p sender message] (:type message)))

(defmethod packet-handler :ping [p sender message]
  (p/update-state-in p [:contacts :all] conj sender)
  (send-to p sender {:type :pong
                     :contacts (-> (p/get-state p)
                                   :contacts
                                   :alive
                                   :direct
                                   keys
                                   set
                                   (disj sender)
                                   vec)}))

(defmethod packet-handler :pong [p sender {:keys [contacts]}]
  (p/update-state-in
   p [:contacts]
   (fn [cts]
     (-> cts
         (update :all union (set contacts))
         (update :all conj sender)
         (update :alive u/idx-assoc sender (u/now-ms))))))

(defmulti cmd-handler (fn [p cmd args] cmd))

(defmethod cmd-handler :put [p _ {k :key v :value}])

(defmethod cmd-handler :get [p _ {k :key}])

(defn create-peer [curator host port contacts]
  (curator/create-peer curator host port handler
                       {:contacts
                        {:all (set contacts)
                         :alive u/index}}))
