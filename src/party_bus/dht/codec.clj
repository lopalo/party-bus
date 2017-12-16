(ns party-bus.dht.codec
  (:require [gloss.core :as g]
            [party-bus.utils :refer [address-c]]))

(def -hash :int32) ;hash of key or peer's address

(def -key (g/finite-frame :byte (g/string :utf-8)))

(def -value (g/finite-frame :int16 (g/string :utf-8)))

(g/defcodec msg-type
  (g/enum :byte
          :ping :pong
          :find-peer :find-peer-response
          :find-value :find-value-response
          :store))

(g/defcodec ping
  {:type :ping})

(g/defcodec pong
  {:type :pong
   :contacts (g/repeated address-c)})

(def lookup-frame
  {:hash -hash
   :flags (g/bit-map
           {:trace-route 1
            :empty 7})
   :route (g/repeated address-c)})

(g/defcodec find-peer
  (merge
   lookup-frame
   {:type :find-peer
    :response-address address-c}))

(g/defcodec find-peer-responce
  {:type :find-peer-response
   :route (g/repeated address-c)})

(g/defcodec find-value
  (merge
   lookup-frame
   {:type :find-value
    :key -key
    :response-address address-c}))

(g/defcodec find-value-responce
  {:type :find-value-response
   :value -value
   :route (g/repeated address-c)})

(g/defcodec store
  (merge
   lookup-frame
   {:type :store
    :key -key
    :value -value}))

(g/defcodec message
  (g/header
   msg-type
   {:ping ping
    :pong pong
    :find-peer find-peer
    :find-peer-response find-peer-responce
    :find-value find-value
    :find-value-response find-value-responce
    :store store}
   :type))
