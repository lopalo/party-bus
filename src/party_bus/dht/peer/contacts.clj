(ns party-bus.dht.peer.contacts
  (:require [manifold.deferred :as md]
            [party-bus.utils :as u :refer [let<]]
            [party-bus.dht.peer-interface :refer [get-address
                                                  get-state
                                                  update-state-in
                                                  create-period]]
            [party-bus.dht.peer.core :as core :refer [options]]))

(defn- insert-contact [p address]
  (let [h (-> p get-state :hash)
        hash-val (core/hash- address)
        step (get-in options [:contacts :pointers-step])
        pointers (conj (core/pointers h core/N step) h)
        pointer (apply min-key (partial core/distance hash-val) pointers)]
    (when-not (= pointer h)
      (update-state-in
       p [:contacts :pointers]
       (fn [contacts]
         (let [[_ hash-val'] (contacts pointer)]
           (if (or (nil? hash-val')
                   (< (core/distance hash-val pointer)
                      (core/distance hash-val' pointer)))
             (assoc contacts pointer [address hash-val])
             contacts)))))))

(defn- insert-seeds [p]
  (run! (partial insert-contact p)
        (get-in (get-state p) [:contacts :seeds])))

(defn init [p]
  (insert-seeds p)
  (create-period p :ping (get-in options [:contacts :ping :period]))
  (create-period p :stabilization
                 (get-in options [:contacts :stabilization :period])))

(defn terminate [p])

(defmethod core/period-handler :ping [p _]
  (apply
   md/zip
   (for [[pointer [address]] (get-in (get-state p) [:contacts :pointers])]
     (let [timeout (get-in options [:contacts :ping :timeout])
           [req-id d] (core/create-request p timeout)]
       (core/send-to p address {:type :ping :request-id req-id})
       (let< [{timeout? :timeout?} d]
         (when timeout?
           (update-state-in
            p [:contacts :pointers]
            (fn [contacts]
              (let [[address'] (contacts pointer)]
                (if (= address address')
                  (dissoc contacts pointer)
                  contacts))))))))))

(defmethod core/period-handler :stabilization [p _]
  (let [address (get-address p)
        state (get-state p)
        opts (get-in options [:contacts :stabilization])]
    (when (empty? (:contacts state))
      (insert-seeds p))
    (apply
     md/zip
     (for [pointer (core/pointers (:hash state) (:range opts) (:step opts))
           :let [nearest-addr (core/nearest-address p pointer)]
           :when (not= nearest-addr address)]
       (let [[req-id d] (core/create-request p (:timeout opts))]
         (core/send-to p nearest-addr
                       {:type :find-peer
                        :hash pointer
                        :flags {:trace-route false
                                :respond true
                                :empty 0}
                        :response-address address
                        :request-id req-id
                        :route []})
         (let< [{timeout? :timeout? address' :data} d]
           (when-not timeout?
             (insert-contact p address'))))))))

(defmethod core/packet-handler :ping [p sender {:keys [request-id]}]
  (insert-contact p sender)
  (core/send-to p sender {:type :pong :request-id request-id}))

(defmethod core/packet-handler :pong [p _ msg]
  (core/resolve-request p msg))

(defmethod core/packet-handler :find-peer [p _ {k :key :as msg}]
  (insert-contact p (:response-address msg))
  (when-not (core/forward-lookup p msg)
    (core/respond-lookup p msg :find-peer-response (get-address p))))

(defmethod core/packet-handler :find-peer-response [p _ msg]
  (core/resolve-request p msg))
