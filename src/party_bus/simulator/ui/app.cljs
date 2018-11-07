(ns party-bus.simulator.ui.app
  (:require [clojure.set :refer [union difference]]
            [cljs.core.async :as async :refer [<!]]
            [sablono.core :refer-macros [html]]
            [rum.core :as rum :refer [react cursor]]
            [antizer.rum :as ant]
            [party-bus.simulator.ui.core
             :as core
             :refer [store init-arg-atom]]
            [party-bus.simulator.ui.dht :refer [dht]]
            [party-bus.simulator.ui.cluster :refer [cluster]]
            [party-bus.simulator.ui.paxos :refer [paxos]]
            [party-bus.simulator.ui.db :refer [db]])
  (:require-macros [cljs.core.async.macros :refer [go-loop]]))

(enable-console-print!)

(defn- menu-item [key text icon]
  (ant/menu-item
   {:key key}
   (ant/icon {:type icon})
   (html [:span text])))

(rum/defcs app
  < rum/reactive
  < (store #{} ::simulators)
  < (init-arg-atom
     first
     {:content "database"
      :dht nil
      :cluster nil
      :paxos nil
      :db nil})
  < {:did-mount
     (fn [state]
       (go-loop [addresses #{core/INITIAL-ADDRESS}]
         (when (seq addresses)
           (let [addresses
                 (<! (async/map
                      #(->> %& (map (comp set :body)) (apply union))
                      (map #(core/request :get % "/all-addresses")
                           addresses)))
                 addresses (difference addresses @(::simulators state))]
             (swap! (::simulators state) union addresses)
             (recur addresses))))
       state)}
  [state *local]
  (let [curs (partial cursor *local)
        sims (-> state ::simulators react)
        *content (curs :content)
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
        (menu-item "cluster" "Cluster" :cloud-o)
        (menu-item "paxos" "Paxos" :like-o)
        (menu-item "database" "Database" :database)
        (menu-item "dht" "DHT" :api)))
      (ant/layout-content
       {:class :content-area}
       (case content
         "cluster" (cluster (curs :cluster) {:simulators sims})
         "paxos" (paxos (curs :paxos) {:simulators sims})
         "database" (db (curs :db) {:simulators sims})
         "dht" (dht (curs :dht) {:simulators sims})
         "Unknown content"))))))

(defonce *app-state (atom nil))

(comment
  (js/console.log @*app-state))

(defn mount []
  (rum/mount (app *app-state)
             (. js/document (getElementById "app"))))

(mount)

(defn on-js-reload []
  (mount))
