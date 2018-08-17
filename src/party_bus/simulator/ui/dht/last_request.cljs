(ns party-bus.simulator.ui.dht.last-request
  (:require [rum.core :as rum]
            [antizer.rum :as ant]
            [party-bus.simulator.ui.core :refer [hash- format-ts]]))

(rum/defc last-request-info [{:keys [last-request show-route *selected-peer]}]
  (ant/card
   {:title "Last request"}
   [:.last-request
    {:key "content"}
    (let [{:keys [key method ts error route value trie ttl]} last-request]
      [:div
       [:div "Key: " key]
       [:div "Key hash: " (hash- key)]
       [:div "Method: " (name method)]
       [:div "Timestamp: " (format-ts ts)]
       (if-not error
         [:div
          (let [[ip port :as address] (first route)]
            [:div "Coordinator: "
             (ant/button {:size :small
                          :class :contact
                          :on-click #(reset! *selected-peer address)}
                         ip ":" port)])
          (let [[ip port :as address] (peek route)]
            [:div "Target: "
             (ant/button {:size :small
                          :class :contact
                          :on-click #(reset! *selected-peer address)}
                         ip ":" port)])
          [:div "Hops: " (count route) " "
           (ant/button {:size :small
                        :on-click show-route} "Show route")]
          (when value [:div "TTL: " ttl])
          (when value [:div "Value: " value])
          (when trie [:div "Trie: " (str trie)])]
         [:div "Error: " [:strong (str error)]])])]))
