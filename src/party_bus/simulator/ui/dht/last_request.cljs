(ns party-bus.simulator.ui.dht.last-request
  (:require [rum.core :as rum]
            [antizer.rum :as ant]
            [party-bus.simulator.ui.core :refer [hash-]]))

(rum/defc last-request-info [{:keys [last-request show-route *selected-peer]}]
  (ant/card
   {:title "Last request"}
   [:div
    {:key "content"}
    (if (map? last-request)
      (let [{:keys [key method route value trie ttl]} last-request]
        [:div
         (let [[ip port :as address] (first route)]
           [:div "Coordinator: "
            (ant/button {:size :small
                         :on-click #(reset! *selected-peer address)}
                        ip ":" port)])
         (let [[ip port :as address] (peek route)]
           [:div "Target: "
            (ant/button {:size :small
                         :on-click #(reset! *selected-peer address)}
                        ip ":" port)])
         [:div "Method: " (name method)]
         [:div "Key hash: " (hash- key)]
         [:div "Hops: " (count route) " "
          (ant/button {:size :small
                       :on-click show-route} "Show route")]
         (when ttl [:div "TTL: " ttl])
         [:div "Key: " key]
         (when value [:div "Value: " value])
         (when trie [:div "Trie: " (str trie)])])
      [:div [:strong (str last-request)]])]))
