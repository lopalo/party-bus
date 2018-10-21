(ns party-bus.cluster.node-agent
  (:require [manifold.deferred :as md]
            [party-bus.core :refer [flow => let>]]
            [party-bus.cluster
             [process :as p]
             [util :as u]]))

(defn- spawn-service
  [p service-specs {:keys [service parameters groups process-options]}]
  (p/spawn p
           (fn [p]
             (p/add-to-groups p groups)
             ((service-specs service) p parameters))
           process-options))

(def group "nodes")

(defn process [p service-specs]
  (p/add-to-group p group)
  (md/loop []
    (flow
      (=> (p/receive p) [_ body :as msg])
      (case (u/msg-type msg)
        :get-node-agents
        (u/response p msg (p/get-group-members p group))
        :get-groups
        (u/response p msg (p/get-all-groups p))
        :get-group-members
        (u/response p msg (p/get-group-members p body))
        :spawn-service
        (spawn-service p service-specs body))
      (md/recur))))

