
(ns party-bus.simulator.cluster
  (:require [clojure.set :refer [union]]
            [compojure
             [core :refer [routes GET POST DELETE]]
             [coercions :refer [as-int]]]
            [manifold
             [deferred :as md]
             [stream :as ms]]
            [party-bus.core :as c :refer [flow =>]]
            [party-bus.cluster
             [node :as n]
             [node-agent :as na]
             [process :as p]
             [util :as u]]
            [party-bus.simulator.core
             :refer [edn-response edn-body edn-transform connect-ws]])
  (:import [party_bus.cluster.core ProcessId]))

(defmethod edn-transform ProcessId [pid]
  (conj (-> pid :endpoint c/host-port) (:number pid)))

(def polling-period 1000)

(defn ->pid [ip port number]
  (ProcessId. (c/socket-address ip port) number))

(defn- agent-pid [p ip port]
  (let [address (c/socket-address ip port)
        agent-pids (p/get-group-members p na/group)]
    (some #(when (= (:endpoint %) address) %) agent-pids)))

(defmacro exec [node process-binding & body]
  `(md/chain'
    (p/spawn-process
     ~node
     (fn [~process-binding]
       ~@body)
     {:exception-handler md/error-deferred})
    edn-response))

(defn poll [f req]
  (let [s (ms/stream 1 nil)]
    (connect-ws s req)
    (md/finally'
     (md/loop []
       (when-not (ms/closed? s)
         (flow
           (=> (f) res)
           (=> (ms/put! s res))
           (=> (md/timeout! (md/deferred) polling-period nil))
           (md/recur))))
     #(ms/close! s))))

(defn- connectivity [node]
  (exec node p
        (let [agent-pids (p/get-group-members p na/group)]
          (u/multicall p agent-pids "get-node-agents" nil 1000))))

(defn- groups [node ip port req]
  (exec node p
        (poll
         #(let [agent-pid (agent-pid p ip port)]
            (u/call p agent-pid "get-groups" nil 1000))
         req)))

(defn members
  ([node group req]
   (exec node p
         (poll #(or (p/get-group-members p group) #{}) req)))
  ([node ip port group req]
   (exec node p
         (poll
          #(let [agent-pid (agent-pid p ip port)]
             (md/chain'
              (u/call p agent-pid "get-group-members" group 1000)
              (fn [m] (or m #{}))))
          req))))

(defn spawn-service [node ip port body]
  (exec node p
        (let [agent-pid (agent-pid p ip port)]
          (p/send-to p agent-pid "spawn-service" body))))

(defn- kill [node ip port number]
  (exec node p
        (p/kill p (->pid ip port number))))

(defn init-state [config local-addresses remote-addresses service-specs]
  (doall
   (for [address local-addresses
         :let [[ip port] (c/str->host-port address)
               conf (update @config :transport assoc :host ip :port port)
               node (n/create conf)]]
     (do
       (p/spawn-process node #(na/process % service-specs))
       (doseq [address' (-> local-addresses
                            (union remote-addresses)
                            (disj address))
               :let [[ip' port'] (c/str->host-port address')]]
         (n/connect-to node ip' port'))
       node))))

(defn destroy-state [nodes]
  (run! n/shutdown nodes))

(defn make-handler [[node]]
  (routes
   (GET "/connectivity" []
     (connectivity node))
   (GET "/groups/:ip/:port" [ip port :<< as-int :as req]
     (groups node ip port req))
   (GET "/members/:ip/:port/:group" [ip port :<< as-int group :as req]
     (members node ip port group req))
   (GET "/members/:group" [group :as req]
     (members node group req))
   (POST "/spawn-service/:ip/:port" [ip port :<< as-int :as req]
     (spawn-service node ip port (edn-body req)))
   (DELETE "/kill/:ip/:port/:number" [ip port :<< as-int number :<< as-int]
     (kill node ip port number))))
