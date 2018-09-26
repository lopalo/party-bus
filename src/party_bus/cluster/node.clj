(ns party-bus.cluster.node
  (:require [clojure.set :refer [difference]]
            [medley.core :refer [deref-reset!]]
            [manifold
             [executor :refer [fixed-thread-executor]]
             [deferred :as md]
             [stream :as ms]]
            [party-bus.core :as c]
            [party-bus.cluster
             [core :as cc]
             [codec :as codec]
             [transport :as t]])
  (:import [party_bus.cluster.core
            Node
            Groups
            ProcessId]
           [party_bus.cluster.transport
            EndpointConnected
            EndpointDisconnected
            Received]))

(defn create-node [options]
  (let [executor (fixed-thread-executor (:num-threads options)
                                        (:executor options))
        transport (t/create-transport executor codec/message options)
        endpoint (t/endpoint transport)
        node (Node. options
                    executor
                    transport
                    (atom 0)
                    (atom {})
                    (atom (Groups. {} (sorted-map) {}))
                    (atom (sorted-set)))]
    (ms/consume
     (fn [event]
       (condp instance? event
         EndpointConnected
         (t/send-to
          transport
          (.endpoint ^EndpointConnected event)
          {:type :merge-groups
           :number->groups
           (map
            (fn [[^ProcessId pid groups]]
              [(.number pid) (seq groups)])
            (subseq (cc/member->groups node)
                    >= (cc/min-pid endpoint)
                    <= (cc/max-pid endpoint)))})
         EndpointDisconnected
         (let [ep (.endpoint ^EndpointDisconnected event)]
           (doseq [[pid] (subseq (cc/member->groups node)
                                 >= (cc/min-pid ep) <= (cc/max-pid ep))]
             (cc/delete-member node pid))
           (swap! (.corks node)
                  #(as-> % $
                     (subseq $ >= (cc/min-pid ep) <= (cc/max-pid ep))
                     (set $)
                     (difference % $))))
         Received
         (let [ep (.endpoint ^Received event)
               msg (.msg ^Received event)]
           (case (:type msg)
             :letter
             (let [{:keys [sender-number receiver-numbers header body]} msg
                   sender (ProcessId. ep sender-number)]
               (doseq [receiver-number receiver-numbers]
                 (cc/mailbox-put node receiver-number header body sender)
                 (when (cc/mailbox-full? node receiver-number)
                   (let [corked-nodes
                         (cc/modify-corked-nodes node receiver-number
                                                 #(when % (conj % ep)))]
                     (when (contains? corked-nodes ep)
                       (t/send-to transport ep
                                  {:type :cork
                                   :process-number receiver-number}))))))
             :merge-groups
             (doseq [[number groups] (:number->groups msg)]
               (cc/add-member node (ProcessId. ep number) groups))
             :add-to-groups
             (let [{:keys [process-number groups]} msg]
               (cc/add-member node (ProcessId. ep process-number) groups))
             :delete-from-groups
             (let [{:keys [process-number groups]} msg]
               (cc/delete-member node (ProcessId. ep process-number) groups))
             :delete-from-all-groups
             (cc/delete-member node (ProcessId. ep (:process-number msg)))
             :kill
             (cc/kill node (:process-number msg))
             :cork
             (swap! (.corks node) conj (ProcessId. ep (:process-number msg)))
             :uncork
             (swap! (.corks node) disj (ProcessId. ep (:process-number msg)))))))
     (ms/onto executor (t/events transport)))
    node
    node))

(defn connect-to [^Node node host port]
  (t/connect-to (.transport node) (c/socket-address host port)))

(defn destroy-node [^Node node]
  (t/destroy (.transport node))
  (run! cc/kill* (vals (deref-reset! (.processes node) nil))))
