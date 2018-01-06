(ns party-bus.simulator.dht
  (:require [compojure
             [core :refer [routes GET PUT POST DELETE]]
             [coercions :refer [as-int]]]
            [manifold.deferred :as md]
            [party-bus.dht.curator :as c]
            [party-bus.dht.peer :as p]
            [party-bus.utils :as u]
            [party-bus.simulator.core :refer [connect-ws
                                              edn-response
                                              edn-body]]))

(defrecord State [dht-ips curator])

(defn- listen-to-addresses [curator req]
  (connect-ws (c/listen-to-addresses curator) req))

(defn- create-peer [curator ip port contacts]
  (md/chain
   (p/create-peer curator ip port (map u/str->socket-address contacts))
   (constantly (edn-response :ok))))

(defn- terminate-peer [curator ip port]
  (c/terminate-peer curator (u/socket-address ip port))
  (edn-response :ok))

(defn- listen-to-peer [curator req ip port]
  (connect-ws (c/listen-to-peer curator (u/socket-address ip port))
              req
              (fn [s]
                (-> s
                    (dissoc :requests)
                    (update :contacts :direct)
                    (update-in [:storage :expiration] :direct)))))

(defn- put-value [curator ip port args]
  (md/chain
   (c/control-command curator (u/socket-address ip port) :put args)
   edn-response))

(defn- get-value [curator ip port args]
  (md/chain
   (c/control-command curator (u/socket-address ip port) :get args)
   edn-response))

(defn make-state [dht-ips]
  (when (seq dht-ips)
    (->State dht-ips (c/create-curator 8 nil prn))))

(defn make-handler [{:keys [dht-ips curator]}]
  (routes
   (GET "/ip-addresses" []
     (edn-response dht-ips))
   (GET "/peer-addresses" req
     (listen-to-addresses curator req))
   (GET "/peer/:ip/:port" [ip port :<< as-int :as req]
     (listen-to-peer curator req ip port))
   (POST "/peer/:ip" [ip :as req]
     (create-peer curator ip 0 (edn-body req)))
   (PUT "/peer/:ip/:port" [ip port :<< as-int :as req]
     (create-peer curator ip port (edn-body req)))
   (DELETE "/peer/:ip/:port" [ip port :<< as-int]
     (terminate-peer curator ip port))
   (POST "/put/:ip/:port" [ip port :<< as-int :as req]
     (put-value curator ip port (edn-body req)))
   (POST "/get/:ip/:port" [ip port :<< as-int :as req]
     (get-value curator ip port (edn-body req)))))
