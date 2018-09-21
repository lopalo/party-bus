(ns party-bus.cluster.codec
  (:require [gloss.core :as g]
            [party-bus.cluster.transport :refer [init-connection-c ping-c]]))

(def string (g/finite-frame :uint32 (g/string :utf-8)))

(def process-number :int64)

(g/defcodec msg-type
  (g/enum :byte
          :init-connection
          :ping
          :letter
          :merge-groups
          :add-to-groups
          :delete-from-groups
          :delete-from-all-groups
          :kill))

(g/defcodec letter
  {:type :letter
   :sender-number process-number
   :receiver-number process-number
   :header string
   :body string})

(g/defcodec merge-groups
  {:type :merge-groups
   :number->groups (g/repeated [process-number (g/repeated string)])})

(g/defcodec add-to-groups
  {:type :add-to-groups
   :process-number process-number
   :groups (g/repeated string)})

(g/defcodec delete-from-groups
  {:type :delete-from-groups
   :process-number process-number
   :groups (g/repeated string)})

(g/defcodec delete-from-all-groups
  {:type :delete-from-all-groups
   :process-number process-number})

(g/defcodec kill
  {:type :kill
   :process-number process-number})

(g/defcodec message
  (g/header
   msg-type
   {:init-connection init-connection-c
    :ping ping-c
    :letter letter
    :merge-groups merge-groups
    :add-to-groups add-to-groups
    :delete-from-groups delete-from-groups
    :delete-from-all-groups delete-from-all-groups
    :kill kill}
   :type))
