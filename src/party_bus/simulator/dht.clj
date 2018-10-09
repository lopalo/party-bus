(ns party-bus.simulator.dht
  (:require [clojure.set :refer [difference]]
            [compojure
             [core :refer [routes GET PUT POST DELETE]]
             [coercions :refer [as-int]]]
            [manifold.deferred :as md]
            [party-bus.peer.curator :as c]
            [party-bus.dht.peer :as p]
            [party-bus.core :refer [socket-address str->socket-address]]
            [party-bus.simulator.core
             :refer [not-blank? as-bool connect-ws edn-response edn-body]]))

(defrecord State [config dht-ips curator])

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

(defn- create-peer [curator config ip port contacts]
  (md/chain'
   (p/create-peer curator config ip port (map str->socket-address contacts))
   (constantly (edn-response true))))

(defn- terminate-peer [curator ip port]
  (c/terminate-peer curator (socket-address ip port))
  (edn-response true))

(defn- listen-to-peer [curator req ip port]
  (connect-ws
   (c/listen-to-peer curator (socket-address ip port)) req
   (fn [[old-st new-st]]
     (let [contacts-view (comp set (partial map first) vals :pointers)
           view #(-> %
                     (dissoc :config :request-count :requests)
                     (update :contacts contacts-view)
                     (update-in [:storage :expiration] :direct)
                     (update :trie :nodes))
           new-v (view new-st)]
       (when (or (nil? old-st)
                 (not= (view old-st) new-v))
         new-v)))))

(defn- put-value [curator ip port {:keys [key value] :as args}]
  {:pre [(not-blank? key) (not-blank? value)]}
  (md/chain'
   (c/control-command curator (socket-address ip port) :put args)
   edn-response))

(defn- get-value [curator ip port key trace?]
  {:pre [(not-blank? key)]}
  (md/chain'
   (c/control-command curator (socket-address ip port)
                      :get {:key key :trace? trace?})
   edn-response))

(defn- get-trie [curator ip port prefix trace?]
  {:pre [(not-blank? prefix)]}
  (md/chain'
   (c/control-command curator (socket-address ip port)
                      :get-trie {:prefix prefix :trace? trace?})
   edn-response))

(defn init-state [config dht-ips]
  (when (seq dht-ips)
    (let [cur-conf (:curator @config)
          num-threads (:num-threads cur-conf)
          exec-options (:executor cur-conf)]
      (->State config
               dht-ips
               (c/create-curator num-threads exec-options println)))))

(defn destroy-state [{:keys [curator]}]
  (c/terminate-all-peers curator)
  (c/shutdown curator true))

(defn make-handler [{:keys [config dht-ips curator]}]
  (routes
   (GET "/ip-addresses" []
     (edn-response dht-ips))
   (GET "/peer-addresses" [max-total :<< as-int :as req]
     (listen-to-addresses curator req max-total))
   (GET "/peer/:ip/:port" [ip port :<< as-int :as req]
     (listen-to-peer curator req ip port))
   (POST "/peer/:ip" [ip :as req]
     (create-peer curator config ip 0 (edn-body req)))
   (PUT "/peer/:ip/:port" [ip port :<< as-int :as req]
     (create-peer curator config ip port (edn-body req)))
   (DELETE "/peer/:ip/:port" [ip port :<< as-int]
     (terminate-peer curator ip port))
   (PUT "/put/:ip/:port" [ip port :<< as-int :as req]
     (put-value curator ip port (edn-body req)))
   (GET "/get/:ip/:port" [ip port :<< as-int key trace :<< as-bool]
     (get-value curator ip port key trace))
   (GET "/get-trie/:ip/:port" [ip port :<< as-int prefix trace :<< as-bool]
     (get-trie curator ip port prefix trace))))
