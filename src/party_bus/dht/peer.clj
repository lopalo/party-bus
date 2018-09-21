(ns party-bus.dht.peer
  (:require [party-bus.core :as c]
            [party-bus.peer.curator :as curator]
            [party-bus.dht
             [core :refer [peer-handler]]
             [contacts :as contacts]
             [storage :as storage]
             [trie :as trie]]))

(defn- combine [& hooks]
  (fn [p]
    (doseq [h hooks] (h p))))

(def ^:private hooks
  {:init (combine contacts/init
                  storage/init
                  trie/init)
   :terminate (combine contacts/terminate
                       storage/terminate
                       trie/terminate)})

(defn- handler [p msg]
  (peer-handler p hooks msg))

(defn create-peer [curator config host port contacts]
  (curator/create-peer curator host port handler
                       {:config config
                        :hash nil
                        :contacts
                        {:seeds (set contacts)
                         :points (sorted-set)
                         :pointers (sorted-map)}
                        :storage
                        {:data {}
                         :groups {:trie-leaf #{}}
                         :expiration c/index}
                        :trie
                        {:nodes (sorted-map)
                         :expiration c/index}
                        :request-count 0
                        :requests {}}))
