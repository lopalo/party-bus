(ns party-bus.cluster.node
  (:require [manifold
             [executor :refer [fixed-thread-executor]]
             [deferred :as md]
             [stream :as ms]]
            [party-bus.utils :as u]
            [party-bus.cluster
             [core :refer [->Node]]
             [codec :as codec]
             [transport :as t]]))

(defn create-node
  [{:keys [num-threads executor-options]
    :as options}]
  (let [executor (fixed-thread-executor num-threads executor-options)
        transport (t/create-transport executor codec/message options)]
    (->Node executor transport)))

(defn destroy-node [node])

