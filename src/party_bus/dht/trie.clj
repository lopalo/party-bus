(ns party-bus.dht.trie
  (:require [party-bus.utils :as u :refer [flow =>]]
            [party-bus.peer.interface :refer [get-address
                                              get-state
                                              update-state-in
                                              create-period]]
            [party-bus.dht.core :as core :refer [config]]))

;; IMPLEMENATION FEATURE
;; A non-leaf node cannot contain values, so
;; a leaf node is discarded in favor of non-leaf node.

(defn- prefix [s]
  (subs s 0 (dec (count s))))

(defn- next-string [s]
  (->> s last int inc char (str (prefix s))))

(defn- node-inserter [k amount ttl]
  (fn [trie]
    (-> trie
        ;; Discards a leaf node if there is a non-leaf node
        (update-in [:nodes k]
                   #(if (and (integer? %) (pos? %) (zero? amount)) % amount))
        (update :expiration u/idx-assoc k (+ (u/now-ms) ttl)))))

(defn- subnodes [nodes prfx]
  (if (= prfx "")
    nodes
    (into (sorted-map) (subseq nodes >= prfx < (next-string prfx)))))

(defn- upcast [p k amount]
  (when-not (= "" k)
    (let [key-hash (core/hash- (prefix k))
          address (get-address p)
          nearest-addr (core/nearest-address p key-hash)
          ttl (config p :trie :node-ttl)]
      (if (= nearest-addr address)
        (update-state-in p [:trie] (node-inserter k amount ttl))
        (core/send-to p nearest-addr
                      {:type :store-trie
                       :hash key-hash
                       :flags {:trace-route false
                               :respond false
                               :empty 0}
                       :response-address address
                       :request-id 0
                       :hops 1
                       :route []
                       :key k
                       :amount amount}))))) ; 0 means a leaf

(defn- upcast-leaves [p]
  (doseq [k (-> p get-state :storage :groups :trie-leaf)]
    (upcast p k 0)))

(defn- upcast-nodes [p]
  (let [nodes (-> p get-state :trie :nodes)]
    (doseq [[prfx key+amounts] (group-by (comp prefix first) nodes)
            :let [amount (->> key+amounts
                              (map (comp (partial max 1) second))
                              (reduce +))]]
      (upcast p prfx amount))))

(defn init [p]
  (create-period p :expired-trie-cleanup
                 (config p :trie :expired-cleanup-period))
  (create-period p :trie-upcast (config p :trie :upcast-period)))

(defn terminate [p])

(defmethod core/period-handler :expired-trie-cleanup [p _]
  (update-state-in
   p [:trie]
   (fn [trie]
     (let [expired-keys (u/idx-search (:expiration trie) < (u/now-ms))]
       (-> trie
           (update :nodes #(reduce dissoc % expired-keys))
           (update :expiration #(reduce u/idx-dissoc % expired-keys)))))))

(defmethod core/period-handler :trie-upcast [p _]
  (upcast-leaves p)
  (upcast-nodes p))

(defmethod core/packet-handler :store-trie
  [p _ {k :key amount :amount :as msg}]
  (when-not (core/forward-lookup p msg)
    (update-state-in p [:trie]
                     (node-inserter k amount (config p :trie :node-ttl)))))

(defmethod core/packet-handler :find-trie [p _ {prfx :prefix :as msg}]
  (when-not (core/forward-lookup p msg)
    (let [data (-> p get-state :trie :nodes (subnodes prfx) vec)]
      (core/respond-lookup p msg :find-trie-response data))))

(defmethod core/packet-handler :find-trie-response [p _ msg]
  (core/resolve-request p msg))

(defmethod core/cmd-handler :get-trie [p _ {prfx :prefix trace? :trace?}]
  (let [prefix-hash (core/hash- prfx)
        address (get-address p)
        nodes (-> p get-state :trie :nodes)
        nearest-addr (core/nearest-address p prefix-hash)
        route (if trace? [address] [])]
    (if (= nearest-addr address)
      {:trie (subnodes nodes prfx)
       :route route}
      (let [[req-id d] (core/create-request p)]
        (core/send-to p nearest-addr
                      {:type :find-trie
                       :hash prefix-hash
                       :flags {:trace-route trace?
                               :respond true
                               :empty 0}
                       :response-address address
                       :request-id req-id
                       :hops 1
                       :route route
                       :prefix prfx})
        (flow
          (=> d {:keys [timeout? data route]})
          (if timeout?
            ::timeout
            {:trie (into (sorted-map) data)
             :route route}))))))
