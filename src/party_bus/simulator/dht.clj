(ns party-bus.simulator.dht
  (:require [clojure.set :refer [difference]]
            [compojure
             [core :refer [routes GET PUT POST DELETE]]
             [coercions :refer [as-int]]]
            [manifold.deferred :as md]
            [party-bus.dht.curator :as c]
            [party-bus.dht.peer :as p]
            [party-bus.utils :as u]
            [party-bus.simulator.core :refer [as-bool
                                              connect-ws
                                              edn-response
                                              edn-body]]))

(defrecord State [dht-ips curator])

(defn- listen-to-addresses [curator req max-total]
  (connect-ws
   (c/listen-to-addresses curator) req
   (fn [[old-peers new-peers]]
     (let [total (count new-peers)]
       (if old-peers
         (if (> total (count old-peers))
           [:add
            (if (<= total max-total) (difference new-peers old-peers) #{})
            total]
           [:delete (difference old-peers new-peers) total])
         [:initial (->> new-peers shuffle (take max-total) set) total])))))

(defn- create-peer [curator ip port contacts]
  (md/chain
   (p/create-peer curator ip port (map u/str->socket-address contacts))
   (constantly (edn-response :ok))))

(defn- terminate-peer [curator ip port]
  (c/terminate-peer curator (u/socket-address ip port))
  (edn-response :ok))

(defn- listen-to-peer [curator req ip port]
  (connect-ws
   (c/listen-to-peer curator (u/socket-address ip port)) req
   (fn [[old-st new-st]]
     (let [contacts-view (comp set (partial map first) vals :pointers)
           view #(-> %
                     (dissoc :request-count)
                     (dissoc :requests)
                     (update :contacts contacts-view)
                     (update-in [:storage :expiration] :direct)
                     (update :trie :nodes))
           new-v (view new-st)]
       (if (or (nil? old-st)
               (not= (view old-st) new-v))
         new-v)))))

(defn- put-value [curator ip port args]
  (md/chain
   (c/control-command curator (u/socket-address ip port) :put args)
   edn-response))

(defn- get-value [curator ip port key trace?]
  (md/chain
   (c/control-command curator (u/socket-address ip port)
                      :get {:key key :trace? trace?})
   edn-response))

(defn- get-trie [curator ip port prefix trace?]
  (md/chain
   (c/control-command curator (u/socket-address ip port)
                      :get-trie {:prefix prefix :trace? trace?})
   edn-response))

(defn make-state [dht-ips]
  (when (seq dht-ips)
    (->State dht-ips (c/create-curator 8 nil println))))

(defn make-handler [{:keys [dht-ips curator]}]
  (routes
   (GET "/ip-addresses" []
     (edn-response dht-ips))
   (GET "/peer-addresses" [max-total :<< as-int :as req]
     (listen-to-addresses curator req max-total))
   (GET "/peer/:ip/:port" [ip port :<< as-int :as req]
     (listen-to-peer curator req ip port))
   (POST "/peer/:ip" [ip :as req]
     (create-peer curator ip 0 (edn-body req)))
   (PUT "/peer/:ip/:port" [ip port :<< as-int :as req]
     (create-peer curator ip port (edn-body req)))
   (DELETE "/peer/:ip/:port" [ip port :<< as-int]
     (terminate-peer curator ip port))
   (PUT "/put/:ip/:port" [ip port :<< as-int :as req]
     (put-value curator ip port (edn-body req)))
   (GET "/get/:ip/:port" [ip port :<< as-int key trace :<< as-bool]
     (get-value curator ip port key trace))
   (GET "/get-trie/:ip/:port" [ip port :<< as-int prefix trace :<< as-bool]
     (get-trie curator ip port prefix trace))))
