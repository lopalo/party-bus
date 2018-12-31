(ns party-bus.simulator.db
  (:require [medley.core :refer [map-vals]]
            [compojure
             [core :refer [routes GET POST]]
             [coercions :refer [as-int]]]
            [party-bus.cluster
             [node :as n]
             [node-agent :as na]
             [process :as p]
             [util :as u]]
            [party-bus.db.controller :refer [controller basic-handlers]]
            [party-bus.db.coordinator :as dc]
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
  {:db db
   :coordinator #(dc/coordinator %1 %2 println)})

(defn init-state [config nodes]
  (doseq [node nodes]
    (p/spawn-process node dc/acceptor))
  config)

(defn destroy-state [config])

(defn make-handler [config [node]]
  (routes
   (GET "/key-spaces" _
     (->> @config :key-spaces (map-vals :storage) edn-response))
   (GET "/workers" req
     (sc/members node worker-group req))
   (GET "/coordinator-acceptors" req
     (sc/members node dc/acceptor-group req))
   (GET "/coordinators" req
     (sc/members node dc/proposer-group req))
   (POST "/spawn/coordinators" req
     (let [acceptor-pids (->> req edn-body (map #(apply sc/->pid %)) set)
           params (assoc (:coordinator @config) :acceptor-pids acceptor-pids)
           body {:service :coordinator
                 :parameters params}]
       (sc/exec node p
                (doseq [pid (p/get-group-members p na/group)]
                  (p/send-to p pid "spawn-service" body)))))
   (POST "/spawn/:ip/:port" [ip port :<< as-int]
     (sc/spawn-service node ip port
                       {:service :db
                        :parameters @config
                        :groups [group]}))
   (POST "/command/:ip/:port/:number/:command-name"
     [ip port :<< as-int number :<< as-int command-name :as req]
     (let [params (edn-body req)]
       (command node ip port number command-name params)))))
