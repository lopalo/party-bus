(ns party-bus.simulator.db
  (:require [compojure
             [core :refer [routes GET POST]]
             [coercions :refer [as-int]]]
            [party-bus.cluster
             [node :as n]
             [node-agent :as na]
             [process :as p]
             [util :as u]]
            [party-bus.db.controller :refer [controller basic-handlers]]
            [party-bus.simulator.core :refer [edn-response edn-body]]
            [party-bus.simulator.cluster :as sc]))

(def ^:private group "db")

(def ^:private worker-group (str group "-workers"))

(defn- command [node ip port number command-name params]
  (let [pid (sc/->pid ip port number)]
    (sc/exec node p
             (u/call p pid command-name params 1000))))

(def command-handlers
  (merge
   basic-handlers
   {}))

(defn- db [p params]
  (controller p (assoc params
                       :handlers command-handlers
                       :worker-groups [worker-group])))

(def service-specs
  {:db db})

(defn init-state [config]
  config)

(defn destroy-state [config])

(defn make-handler [config [node]]
  (routes
   (GET "/key-spaces" _
     ;;TODO: return storage type
     (-> @config :key-spaces keys set edn-response))
   (GET "/workers" req
     (sc/members node worker-group req))
   (POST "/spawn/:ip/:port" [ip port :<< as-int]
     (sc/spawn-service node ip port
                       {:service :db
                        :parameters @config
                        :groups [group]}))
   (POST "/command/:ip/:port/:number/:command-name"
     [ip port :<< as-int number :<< as-int command-name :as req]
     (let [params (edn-body req)]
       (command node ip port number command-name params)))))
