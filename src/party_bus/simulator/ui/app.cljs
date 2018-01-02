(ns party-bus.simulator.ui.app
  (:require [clojure.set :refer [union difference]]
            [cljs.core.async :as async :refer [<!]]
            [rum.core :as rum]
            [party-bus.simulator.ui.core :refer [request INITIAL-ADDRESS]]
            [party-bus.simulator.ui.dht :refer [dht]])
  (:require-macros [cljs.core.async.macros :refer [go go-loop]]))

(enable-console-print!)

(rum/defcs app
  < (rum/local #{} ::simulators)
  < {:did-mount
     (fn [state]
       (go
         (reset!
          (::simulators state)
          (<! (go-loop [addresses #{}
                        next-addresses #{INITIAL-ADDRESS}]
                (if (seq next-addresses)
                  (let [addresses (union addresses next-addresses)
                        next-addresses
                        (<! (async/map
                             #(->> %& (map (comp set :body)) (apply union))
                             (map #(request :get % "/all-addresses")
                                  next-addresses)))
                        next-addresses (difference next-addresses addresses)]
                    (recur addresses next-addresses))
                  addresses)))))
       state)}
  [state]
  (let [sims @(::simulators state)]
    [:div
     [:h1 "Simulator"]
     [:div (for [s (sort sims)] [:span {:key s} s " "])]
     (dht sims)]))

(defn mount []
  (rum/mount (app)
             (. js/document (getElementById "app"))))

(mount)

(defn on-js-reload []
  (mount))
