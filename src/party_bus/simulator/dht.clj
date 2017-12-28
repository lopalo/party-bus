(ns party-bus.simulator.dht
  (:require [compojure
             [core :refer [routes GET POST DELETE]]
             [coercions :refer [as-int]]]
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
  @(p/create-peer curator ip port (map u/str->socket-address contacts))
  (edn-response :ok))

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
   (POST "/peer/:ip/:port" [ip port :<< as-int :as req]
     (create-peer curator ip port (edn-body req)))
   (DELETE "/peer/:ip/:port" [ip port :<< as-int]
     (terminate-peer curator ip port))))
