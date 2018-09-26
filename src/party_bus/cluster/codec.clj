(ns party-bus.cluster.codec
  (:require [gloss.core :as g]
            [gloss.core.protocols :refer [Reader Writer]]
            [gloss.data.bytes.core :refer [dup-bytes]]
            [byte-streams :refer [to-byte-array]]
            [taoensso.nippy :refer [freeze thaw]]
            [party-bus.cluster.transport :refer [init-connection-c ping-c]])
  (:import [java.nio ByteBuffer]))

(def string (g/finite-frame :uint32 (g/string :utf-8)))

(g/defcodec edn
  (g/finite-frame
   :uint32
   (reify
     Reader
     (read-bytes [this buf-seq]
       (let [byte-arr (-> buf-seq dup-bytes to-byte-array)]
         [true byte-arr nil]))
     Writer
     (sizeof [x]
       nil)
     (write-bytes [_ _ byte-arr]
       [(ByteBuffer/wrap byte-arr)])))
  freeze
  thaw)

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
          :kill
          :cork
          :uncork))

(g/defcodec letter
  {:type :letter
   :sender-number process-number
   :receiver-numbers (g/repeated process-number)
   :header string
   :body edn})

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

(g/defcodec cork
  {:type :cork
   :process-number process-number})

(g/defcodec uncork
  {:type :uncork
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
    :kill kill
    :cork cork
    :uncork uncork}
   :type))
