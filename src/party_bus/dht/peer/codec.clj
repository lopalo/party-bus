(ns party-bus.dht.peer.codec
  (:require [gloss.core :as g]
            [party-bus.utils :refer [address-c]]))

(def zeros (repeat 0))

;SHA-1 hash of key or peer's address
(g/defcodec -hash
  (-> 20 (repeat :ubyte) vec)
  (fn [^clojure.lang.BigInt x]
    (let [b (-> x .toBigInteger .toByteArray reverse (concat zeros))]
      (->> b (take 20) reverse vec)))
  (fn [bs]
    (->> bs (cons 0) byte-array BigInteger. bigint)))

(def -key (g/finite-frame :byte (g/string :utf-8)))

(def -value (g/finite-frame :int16 (g/string :utf-8)))

(g/defcodec msg-type
  (g/enum :byte
          :ping :pong
          :find-peer :find-peer-response
          :find-value :find-value-response
          :store :store-response
          :find-trie :find-trie-response
          :store-trie))

(g/defcodec ping
  {:type :ping
   :request-id :int64})

(g/defcodec pong
  {:type :pong
   :request-id :int64})

(def lookup-frame
  {:hash -hash
   :flags (g/bit-map
           :trace-route 1
           :respond 1
           :empty 6)
   :response-address address-c
   :request-id :int64
   :route (g/repeated address-c)})

(def lookup-response-frame
  {:request-id :int64
   :data nil
   :route (g/repeated address-c)})

(g/defcodec find-peer
  (merge
   lookup-frame
   {:type :find-peer}))

(g/defcodec find-peer-response
  (merge
   lookup-response-frame
   {:type :find-peer-response
    :data address-c}))

(g/defcodec find-value
  (merge
   lookup-frame
   {:type :find-value
    :key -key}))

(g/defcodec find-value-response
  (merge
   lookup-response-frame
   {:type :find-value-response
    :data {:value -value
           :ttl :int32}}))

(g/defcodec store
  (merge
   lookup-frame
   {:type :store
    :key -key
    :key-groups (g/bit-map
                 :trie-leaf 1
                 :empty 7)
    :value -value
    :ttl :int32}))

(g/defcodec store-response
  (merge
   lookup-response-frame
   {:type :store-response
    :data address-c}))

(g/defcodec find-trie
  (merge
   lookup-frame
   {:type :find-trie
    :prefix -key}))

(g/defcodec find-trie-response
  (merge
   lookup-response-frame
   {:type :find-trie-response
    :data (g/repeated [-key :int64])}))

(g/defcodec store-trie
  (merge
   lookup-frame
   {:type :store-trie
    :key -key
    :amount :int64})) ; 0 means a leaf

(g/defcodec message
  (g/header
   msg-type
   {:ping ping
    :pong pong
    :find-peer find-peer
    :find-peer-response find-peer-response
    :find-value find-value
    :find-value-response find-value-response
    :store store
    :store-response store-response
    :find-trie find-trie
    :find-trie-response find-trie-response
    :store-trie store-trie}
   :type))
