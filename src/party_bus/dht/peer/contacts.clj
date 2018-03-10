(ns party-bus.dht.peer.contacts
  (:require [manifold.deferred :as md]
            [party-bus.utils :as u :refer [let<]]
            [party-bus.dht.peer-interface :refer [get-address
                                                  get-state
                                                  update-state-in
                                                  create-period]]
            [party-bus.dht.peer.core :as core :refer [options N max-hash]]))

(def ^:private ^BigInteger two (biginteger 2))

(defn- points
  ([origin]
   (points origin core/exponents))
  ([origin exponents]
   (for [sign [- +]
         exp exponents
         :when (and (>= exp 0) (< exp N))
         :let [v (sign origin (.pow two exp))
               v (cond
                   (< v 0) (+ v max-hash)
                   (> v max-hash) (- v max-hash)
                   :else v)]]
     (biginteger v))))

(defn- insert-contact [p address]
  (let [state (get-state p)
        h (:hash state)
        points (get-in state [:contacts :points])
        hash-val (core/hash- address)
        a (first (rsubseq points <= hash-val))
        b (first (subseq points > hash-val))
        point (min-key (partial core/distance hash-val) h (or a h) (or b h))]
    (when-not (= point h)
      (update-state-in
       p [:contacts :pointers]
       (fn [pointers]
         (let [[_ hash-val'] (pointers point)]
           (if (or (nil? hash-val')
                   (< (core/distance hash-val point)
                      (core/distance hash-val' point)))
             (assoc pointers point [address hash-val])
             pointers)))))))

(defn- insert-seeds [p]
  (run! (partial insert-contact p)
        (get-in (get-state p) [:contacts :seeds])))

(defn init [p]
  (let [points (points (-> p get-state :hash))]
    (update-state-in p [:contacts :points] into points))
  (insert-seeds p)
  (create-period p :ping (get-in options [:contacts :ping :period]))
  (create-period p :stabilization
                 (get-in options [:contacts :stabilization :period])))

(defn terminate [p])

(defmethod core/period-handler :ping [p _]
  (apply
   md/zip'
   (for [[point [address]] (get-in (get-state p) [:contacts :pointers])]
     (let [timeout (get-in options [:contacts :ping :timeout])
           [req-id d] (core/create-request p timeout)]
       (core/send-to p address {:type :ping :request-id req-id})
       (let< [{timeout? :timeout?} d]
         (when timeout?
           (update-state-in
            p [:contacts :pointers]
            (fn [pointers]
              (let [[address'] (pointers point)]
                (if (= address address')
                  (dissoc pointers point)
                  pointers))))))))))

(defmethod core/period-handler :stabilization [p _]
  (let [address (get-address p)
        state (get-state p)
        pointers (get-in state [:contacts :pointers])
        contacts (->> pointers vals (map first) shuffle)
        opts (get-in options [:contacts :stabilization])
        points (points (:hash state) (:exponents opts))]
    (when (empty? contacts)
      (insert-seeds p))
    (doseq [[point contact] (map vector points (cycle contacts))
            :let [nearest-addr (core/nearest-address p point)]]
      (core/send-to p contact
                    {:type :find-peer
                     :hash point
                     :flags {:trace-route false
                             :respond true
                             :empty 0}
                     :response-address address
                     :request-id 0
                     :route []}))))

(defmethod core/packet-handler :ping [p sender {:keys [request-id]}]
  (insert-contact p sender)
  (core/send-to p sender {:type :pong :request-id request-id}))

(defmethod core/packet-handler :pong [p _ msg]
  (core/resolve-request p msg))

(defmethod core/packet-handler :find-peer [p _ {k :key :as msg}]
  (insert-contact p (:response-address msg))
  (when-not (core/forward-lookup p msg)
    (core/respond-lookup p msg :find-peer-response 0)))

(defmethod core/packet-handler :find-peer-response [p sender msg]
  (insert-contact p sender))
