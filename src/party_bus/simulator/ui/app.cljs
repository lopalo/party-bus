(ns party-bus.simulator.ui.app
  (:require [clojure.set :refer [union difference]]
            [cljs.core.async :as async :refer [<!]]
            [sablono.core :refer-macros [html]]
            [rum.core :as rum :refer [react]]
            [antizer.rum :as ant]
            [party-bus.simulator.ui.core :as core :refer [store]]
            [party-bus.simulator.ui.dht :refer [dht]])
  (:require-macros [cljs.core.async.macros :refer [go go-loop]]))

(enable-console-print!)

(defn- menu-item [key text icon]
  (ant/menu-item
   {:key key}
   (ant/icon {:type icon})
   (html [:span text])))

(rum/defcs app
  < rum/reactive
  < (store #{} ::simulators)
  < (store "dht" ::content)
  < {:did-mount
     (fn [state]
       (go
         (reset!
          (::simulators state)
          (<! (go-loop [addresses #{}
                        next-addresses #{core/INITIAL-ADDRESS}]
                (if (seq next-addresses)
                  (let [addresses (union addresses next-addresses)
                        next-addresses
                        (<! (async/map
                             #(->> %& (map (comp set :body)) (apply union))
                             (map #(core/request :get % "/all-addresses")
                                  next-addresses)))
                        next-addresses (difference next-addresses addresses)]
                    (recur addresses next-addresses))
                  addresses)))))
       state)}
  [state]
  (let [sims (-> state ::simulators react)
        *content (::content state)
        content (react *content)]
    (ant/layout
     (ant/layout-header
      {:class :banner}
      (ant/row
       (ant/col
        {:span 2}
        [:h2.banner-header {:key "header"} "Simulator"])
       (ant/col
        (for [s (sort sims)]
          (ant/tag {:key s :color :blue} s)))))
     (ant/layout
      (ant/layout-sider
       {:collapsible true}
       (ant/menu
        {:theme :dark
         :selected-keys [content]
         :on-click (fn [e] (reset! *content (.-key e)))}
        (menu-item "cluster" "Cluster" :cloud)
        (menu-item "dht" "DHT" :api)))
      (ant/layout-content
       {:class :content-area}
       (case content
         "dht" (dht sims)
         "Unknown content"))))))

(defn mount []
  (rum/mount (app)
             (. js/document (getElementById "app"))))

(mount)

(defn on-js-reload []
  (mount))
