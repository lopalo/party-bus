(ns party-bus.simulator.server
  (:require [ring.middleware.defaults :refer [wrap-defaults site-defaults]]
            [compojure.core :refer [routes context GET]]
            [aleph.http :as http]
            [party-bus.utils :as u]
            [party-bus.simulator.dht :as dht]
            [party-bus.simulator.core :refer [edn-response]]))

(defn- make-handler [all-addresses dht-state]
  (routes
   (GET "/all-addresses" []
     (edn-response all-addresses))
   (context "/dht" []
     (dht/make-handler dht-state))))

(defn start-server [options]
  (let [dht-state (dht/make-state (:dht-ips options))
        address (-> options :listen-address u/str->socket-address)]
    (-> (make-handler (:connect-addresses options) dht-state)
        (wrap-defaults (assoc-in site-defaults
                                 [:security :anti-forgery] false))
        (http/start-server {:socket-address address}))))
