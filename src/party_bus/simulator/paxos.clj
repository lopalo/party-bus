(ns party-bus.simulator.paxos
  (:require [compojure
             [core :refer [routes GET POST]]
             [coercions :refer [as-int]]]
            [party-bus.cluster
             [process :as p]
             [util :as u]]
            [party-bus.paxos :as paxos]
            [party-bus.simulator.core :refer [edn-response edn-body]]
            [party-bus.simulator.cluster :as sc]))

(def ^:private paxos-group "sim")

(defn- acceptor [p _]
  (paxos/acceptor p {:group-suffix paxos-group}))

(defn- proposer [p params]
  (let [leader-process (fn [p epoch]
                         (p/add-to-group p (str paxos-group ":epoch:" epoch))
                         (p/receive p))]
    (paxos/proposer p (merge
                       params
                       {:group-suffix paxos-group
                        :leader-process leader-process
                        :proposal-number 0}))))

(def service-specs
  {:paxos-acceptor acceptor
   :paxos-proposer proposer})

(defn- acceptors [node req]
  (sc/exec node p
           (sc/poll
            #(let [pids (->> paxos-group
                             (paxos/gr paxos/acceptor-group)
                             (p/get-group-members p))]
               (u/multicall p pids "get-state" nil 1000))
            req)))

(defn init-state [config]
  config)

(defn destroy-state [config])

(defn make-handler [config [node]]
  (routes
   (GET "/acceptors" req
     (acceptors node req))
   (GET "/proposers" req
     (sc/members node (paxos/gr paxos/proposer-group paxos-group) req))
   (GET "/leaders" req
     (sc/members node (paxos/gr paxos/leader-group paxos-group) req))
   (POST "/spawn/acceptor/:ip/:port" [ip port :<< as-int]
     (sc/spawn-service node ip port {:service :paxos-acceptor}))
   (POST "/spawn/proposer/:ip/:port" [ip port :<< as-int :as req]
     (let [acceptor-pids (->> req edn-body (map #(apply sc/->pid %)) set)
           params (assoc @config :acceptor-pids acceptor-pids)]
       (sc/spawn-service node ip port
                         {:service :paxos-proposer
                          :parameters params})))))

