(ns party-bus.cluster.codec
  (:require [gloss.core :as g]
            [party-bus.utils :refer [address-c]]))

(def string (g/finite-frame :uint32 (g/string :utf-8)))

(def proc-number :int64)

(g/defcodec msg-type
  (g/enum :byte
          :init-connection
          :ping
          :letter))

(g/defcodec init-connection
  {:type :init-connection
   :endpoint address-c})

(g/defcodec ping
  {:type :ping})

(g/defcodec letter
  {:type :letter
   :sender proc-number
   :receiver proc-number
   :body string})

(g/defcodec message
  (g/header
   msg-type
   {:init-connection init-connection
    :ping ping
    :letter letter}
   :type))
