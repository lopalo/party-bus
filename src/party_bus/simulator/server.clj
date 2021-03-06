(ns party-bus.simulator.server
  (:require [clojure.string :refer [starts-with?]]
            [clojure.java.io :as io]
            [ring.middleware.defaults :refer [wrap-defaults site-defaults]]
            [ring.middleware.resource :refer [wrap-resource]]
            [ring.middleware.cors :as cors]
            [ring.util.response :as rr]
            [compojure.core :refer [routes context GET POST]]
            [aleph.http :as http]
            [manifold.deferred :as md]
            [rum.derived-atom :refer [derived-atom]]
            [party-bus.core :as c]
            [party-bus.simulator.core :refer [edn-response]]
            [party-bus.simulator.dht :as dht]
            [party-bus.simulator.cluster :as cluster]
            [party-bus.simulator.paxos :as paxos]
            [party-bus.simulator.db :as db])
  (:import [java.io File Closeable]))

(defrecord State [config connect-addresses dht cluster paxos db])

(defn- watch-config [config-src config]
  (let [^File file (io/file config-src)]
    (loop [lm (.lastModified file)]
      (Thread/sleep 1000)
      (when @config
        (let [lm' (.lastModified file)]
          (when-not (= lm lm')
            (let [cnf (c/load-edn config-src)]
              (swap! config #(when % cnf))))
          (recur lm'))))))

(defn- init-state [options]
  (let [config-src (:config options)
        config (-> config-src c/load-edn atom)
        dht (dht/init-state (derived-atom [config] :dht :dht)
                            (:dht-ips options))
        service-specs (merge
                       {}
                       paxos/service-specs
                       db/service-specs)
        cluster (cluster/init-state (derived-atom [config] :node :node)
                                    (:local-node-addresses options)
                                    (:remote-node-addresses options)
                                    service-specs)
        paxos (paxos/init-state (derived-atom [config] :paxos :paxos))
        db (db/init-state (derived-atom [config] :db :db) cluster)]
    (future (watch-config config-src config))
    (->State config (:connect-addresses options) dht cluster paxos db)))

(defn- destroy-state [{:keys [dht cluster paxos db config]}]
  (db/destroy-state db)
  (paxos/destroy-state paxos)
  (cluster/destroy-state cluster)
  (dht/destroy-state dht)
  (reset! config nil))

(defn- make-handler [{:keys [connect-addresses dht cluster paxos db]}]
  (routes
   (GET "/all-addresses" []
     (edn-response connect-addresses))
   (context "/dht" []
     (dht/make-handler dht))
   (context "/cluster" []
     (cluster/make-handler cluster))
   (context "/paxos" []
     (paxos/make-handler paxos cluster))
   (context "/db" []
     (db/make-handler db cluster))))

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
                     (when %
                       (cors/add-access-control request access-control %)))
                   raise)
          (handler request respond raise))))))

(defn- start-http [address state]
  (-> state
      make-handler
      wrap-deferred
      (wrap-defaults (-> site-defaults
                         (assoc-in [:security :anti-forgery] false)
                         (assoc-in [:static :resources] ["cljsjs" "public"])))
      (wrap-cors :access-control-allow-origin [#".*"]
                 :access-control-allow-methods [:get :put :post :delete])
      wrap-asynchronous
      (http/start-server {:socket-address address
                          :epoll? true})))

(defn start-server [options]
  (let [address (-> options :listen-address c/str->socket-address)
        state (init-state options)
        ^Closeable
        http-server (start-http address state)]
    (reify Closeable
      (close [this]
        (.close http-server)
        (destroy-state state)))))

;; for embedding into figwheel's server
(def cljsjs-handler (wrap-resource identity "cljsjs"))
