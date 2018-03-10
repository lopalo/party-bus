(ns party-bus.simulator.server
  (:require [clojure.string :refer [starts-with?]]
            [ring.middleware.defaults :refer [wrap-defaults site-defaults]]
            [ring.middleware.resource :refer [wrap-resource]]
            [ring.middleware.cors :as cors]
            [ring.util.response :as rr]
            [compojure.core :refer [routes context GET]]
            [aleph.http :as http]
            [manifold.deferred :as md]
            [party-bus.utils :as u]
            [party-bus.simulator.dht :as dht]
            [party-bus.simulator.core :refer [edn-response]]))

(defn- make-handler [all-addresses dht-state]
  (routes
   (GET "/all-addresses" []
     (edn-response all-addresses))
   (context "/dht" []
     (dht/make-handler dht-state))))

(defn- wrap-deferred [handler]
  (fn [request respond raise]
    (let [response (handler request)]
      (if (md/deferred? response)
        (md/on-realized response respond raise)
        (respond response)))))

(defn- wrap-asynchronous [handler]
  (fn [request]
    (let [d (md/deferred)]
      (handler request (partial md/success! d) (partial md/error! d))
      d)))

(defn- wrap-cors [handler & access-control]
  (let [access-control (cors/normalize-config access-control)]
    (fn [request respond raise]
      (if (and (cors/preflight? request)
               (cors/allow-request? request access-control))
        (-> request
            (cors/add-access-control access-control
                                     (rr/response "preflight complete"))
            respond)
        (if (and (cors/origin request)
                 (cors/allow-request? request access-control))
          (handler request
                   #(respond
                     (if %
                       (cors/add-access-control request access-control %)))
                   raise)
          (handler request respond raise))))))

(defn start-server [options]
  (let [dht-state (dht/make-state (:dht-ips options))
        address (-> options :listen-address u/str->socket-address)]
    (-> (make-handler (:connect-addresses options) dht-state)
        wrap-deferred
        (wrap-defaults (-> site-defaults
                           (assoc-in [:security :anti-forgery] false)
                           (assoc-in [:static :resources] ["cljsjs" "public"])))
        (wrap-cors :access-control-allow-origin [#".*"]
                   :access-control-allow-methods [:get :put :post :delete])
        wrap-asynchronous
        (http/start-server {:socket-address address
                            :epoll? true}))))

;; for embedding into figwheel's server
(def cljsjs-handler (wrap-resource identity "cljsjs"))
