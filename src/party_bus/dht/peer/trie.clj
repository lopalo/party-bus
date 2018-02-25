(ns party-bus.dht.peer.trie
  (:require [manifold.deferred :as md]
            [party-bus.utils :as u :refer [let<]]
            [party-bus.dht.peer-interface :refer [get-address
                                                  get-state
                                                  update-state-in
                                                  create-period]]
            [party-bus.dht.peer.core :as core :refer [options]]))

;; IMPLEMENATION FEATURE
;; A non-leaf node cannot contain values, so
;; a leaf node is discarded in favor of non-leaf node.

(defn- prefix [s]
  (subs s 0 (dec (count s))))

(defn- next-string [s]
  (->> s last int inc char (str (prefix s))))

(defn- parse-int [^String s]
  (Long. s))

(defn- insert-node-fn [k amount]
  (fn [trie]
    (-> trie
        ;; Discards a leaf node if there is a non-leaf node
        (update-in [:nodes k]
                   #(if (and (integer? %) (pos? %) (zero? amount)) % amount))
        (update :expiration u/idx-assoc k
                (+ (u/now-ms) (get-in options [:trie :node-ttl]))))))

(defn- subnodes [nodes prfx]
  (into (sorted-map) (subseq nodes >= prfx < (next-string prfx))))

(defn- upcast [p k amount]
  (when-not (= "" k)
    (let [key-hash (core/hash- (prefix k))
          address (get-address p)
          nearest-addr (core/nearest-address p key-hash)]
      (if (= nearest-addr address)
        (update-state-in p [:trie] (insert-node-fn k amount))
        (core/send-to p nearest-addr
                      {:type :store-trie
                       :hash key-hash
                       :flags {:trace-route false
                               :respond false
                               :empty 0}
                       :response-address address
                       :request-id 0
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
                 (get-in options [:trie :expired-cleanup-period]))
  (create-period p :trie-upcast (get-in options [:trie :upcast-period])))

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
    (update-state-in p [:trie] (insert-node-fn k amount))))

(defmethod core/packet-handler :find-trie [p _ {prfx :prefix :as msg}]
  (when-not (core/forward-lookup p msg)
    (let [data (-> p get-state :trie :nodes {:trie (subnodes prfx)})]
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
      {:trie (subnodes nodes prfx)}
      (let [[req-id d] (core/create-request p)]
        (core/send-to p nearest-addr
                      {:type :find-trie
                       :hash prefix-hash
                       :flags {:trace-route trace?
                               :respond true
                               :empty 0}
                       :response-address address
                       :request-id req-id
                       :route route
                       :prefix prfx})
        (let< [{:keys [timeout? data route]} d]
          (if timeout? ::timeout data))))))
