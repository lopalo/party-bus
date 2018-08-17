(ns party-bus.dht.storage
  (:require [clojure.set :refer [difference]]
            [medley.core :refer [map-vals map-kv]]
            [party-bus.utils :as u :refer [let<]]
            [party-bus.peer.interface :refer [get-address
                                              get-state
                                              update-state-in
                                              create-period]]
            [party-bus.dht.core :as core :refer [config]]))

(defn- kv-inserter [k v ttl groups]
  (fn [storage]
    (-> storage
        (assoc-in [:data k] v)
        (update :groups
                (partial map-kv (fn [group ks]
                                  (if (groups group)
                                    [group (conj ks k)]
                                    [group ks]))))
        (update :expiration u/idx-assoc k (+ (u/now-ms) ttl)))))

(defn- key-ttl [storage k]
  (if-some [exp (get-in storage [:expiration :direct k])]
    (- exp (u/now-ms))
    0))

(defn init [p]
  (create-period p :expired-kv-cleanup
                 (config p :storage :expired-cleanup-period)))

(defn terminate [p])

(defmethod core/period-handler :expired-kv-cleanup [p _]
  (update-state-in
   p [:storage]
   (fn [storage]
     (let [expired-keys (u/idx-search (:expiration storage) < (u/now-ms))
           expired-keys' (set expired-keys)]
       (-> storage
           (update :data #(reduce dissoc % expired-keys))
           (update :groups
                   (partial map-vals (fn [ks]
                                       (difference ks expired-keys'))))
           (update :expiration #(reduce u/idx-dissoc % expired-keys)))))))

(defmethod core/packet-handler :store
  [p _ {k :key :keys [value ttl key-groups] :as msg}]
  (when-not (core/forward-lookup p msg)
    (update-state-in p [:storage] (kv-inserter k value ttl key-groups))
    (core/respond-lookup p msg :store-response (get-address p))))

(defmethod core/packet-handler :store-response [p _ msg]
  (core/resolve-request p msg))

(defmethod core/packet-handler :find-value [p _ {k :key :as msg}]
  (let [storage (-> p get-state :storage)
        v (get-in storage [:data k])
        data {:value (or v "")
              :ttl (key-ttl storage k)}]
    (if v
      (core/respond-lookup p msg :find-value-response data)
      (when-not (core/forward-lookup p msg)
        (core/respond-lookup p msg :find-value-response data)))))

(defmethod core/packet-handler :find-value-response [p _ msg]
  (core/resolve-request p msg))

(defmethod core/cmd-handler :put
  [p _ {k :key
        :keys [value ttl trace? response? trie?]
        :or {trace? false response? true trie? false}}]
  (let [key-hash (core/hash- k)
        address (get-address p)
        nearest-addr (core/nearest-address p key-hash)
        sopts (config p :storage)
        ttl (-> ttl (or (:default-ttl sopts)) (min (:max-ttl sopts)))
        route (if trace? [address] [])
        key-groups {:trie-leaf trie? :empty 0}]
    (if (= nearest-addr address)
      (do
        (update-state-in p [:storage] (kv-inserter k value ttl key-groups))
        {:ttl ttl
         :address address
         :route route})
      (if-not response?
        (do
          (core/send-to p nearest-addr
                        {:type :store
                         :hash key-hash
                         :flags {:trace-route false
                                 :respond false
                                 :empty 0}
                         :response-address address
                         :request-id 0
                         :hops 1
                         :route []
                         :key k
                         :key-groups key-groups
                         :value value
                         :ttl ttl})
          {:ttl ttl})
        (let [[req-id d] (core/create-request p)]
          (core/send-to p nearest-addr
                        {:type :store
                         :hash key-hash
                         :flags {:trace-route trace?
                                 :respond true
                                 :empty 0}
                         :response-address address
                         :request-id req-id
                         :hops 1
                         :route route
                         :key k
                         :key-groups key-groups
                         :value value
                         :ttl ttl})
          (let< [{:keys [timeout? data route]} d]
            (if timeout?
              ::timeout
              {:ttl ttl
               :address data
               :route route})))))))

(defmethod core/cmd-handler :get [p _ {k :key trace? :trace?}]
  (let [key-hash (core/hash- k)
        address (get-address p)
        storage (-> p get-state :storage)
        v (get-in storage [:data k])
        nearest-addr (core/nearest-address p key-hash)
        route (if trace? [address] [])]
    (if (or v (= nearest-addr address))
      {:value v
       :ttl (key-ttl storage k)
       :route route}
      (let [[req-id d] (core/create-request p)]
        (core/send-to p nearest-addr
                      {:type :find-value
                       :hash key-hash
                       :flags {:trace-route trace?
                               :respond true
                               :empty 0}
                       :response-address address
                       :request-id req-id
                       :hops 1
                       :route route
                       :key k})
        (let< [{timeout? :timeout? {:keys [ttl value]} :data route :route} d]
          (if timeout?
            ::timeout
            {:ttl ttl
             :value value
             :route route}))))))
