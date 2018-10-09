(ns party-bus.simulator.ui.cluster
  (:require [goog.object :as o]
            [clojure.set :refer [union]]
            [clojure.string :as s]
            [cljs.core.async :refer [<!]]
            [medley.core :refer [map-kv]]
            [sablono.core :refer-macros [html]]
            [rum.core :as rum :refer [react cursor]]
            [antizer.rum :as ant]
            [party-bus.simulator.ui.core :as c])
  (:require-macros [clojure.core.strint :refer [<<]]
                   [cljs.core.async.macros :refer [go]]))

(defn node->str [[ip port]]
  (str ip ":" port))

(defn str->node [s]
  (s/split s #":"))

(defn load-connectivity [state]
  (go
    (let [sim (-> state :rum/args first :simulator)
          {res :body} (<! (c/request :get sim "/cluster/connectivity"))]
      (reset! (::connectivity state)
              (map-kv (fn [k v]
                        [(node->str k) (set (map node->str v))])
                      res))))
  state)

(rum/defcs connectivity
  < rum/reactive
  < (c/store {} ::connectivity)
  < {:did-mount load-connectivity}
  [state {:keys [*selected-node]}]
  (let [connectivity (->> state ::connectivity react)
        nodes (->> connectivity vals (reduce union) sort)
        reload #(load-connectivity state)
        selected-node (react *selected-node)]
    (ant/table
     {:pagination false
      :bordered true
      :scroll {:x 870}
      :row-selection {:type :radio
                      :selected-row-keys (when selected-node [selected-node])
                      :on-change #(reset! *selected-node (first %))}
      :title (constantly (html [:h3
                                "Connectivity "
                                (ant/button {:shape :circle
                                             :icon :sync
                                             :on-click reload})]))
      :columns
      (cons
       {:title "Node" :data-index :node}
       (for [n nodes]
         {:title n :data-index n}))
      :data-source
      (for [n nodes
            :let [connections (connectivity n)]]
        (into {:key n
               :node n}
              (for [n' nodes]
                [n' (c/bool-icon (contains? connections n'))])))})))

(rum/defcs node-groups
  < rum/reactive
  < (c/store {} ::groups)
  < (c/store {} ::members)
  < (c/init-arg-atom
     first
     {:selected-group nil})
  [state *local {:keys [node simulator]}]
  (let [[ip port] (str->node node)
        *selected-group (cursor *local :selected-group)
        selected-group (react *selected-group)
        *groups (::groups state)
        *members (::members state)
        members (react *members)
        kill (fn [[ip port n]]
               (c/request :delete
                          simulator
                          (<< "/cluster/kill/~{ip}/~{port}/~{n}")))]
    (ant/card
     {:title node}
     [:div
      {:key "content"}
      (c/ws-listener
       {:value node
        :on-value-change #(reset! *groups nil)
        :connect
        #(c/connect-ws simulator (<< "/cluster/groups/~{ip}/~{port}"))
        :on-message #(when (set? %) (reset! *groups %))})
      (when selected-group
        (c/ws-listener
         {:value [node selected-group]
          :on-value-change #(reset! *members nil)
          :connect
          #(c/connect-ws
            simulator
            (<< "/cluster/members/~{ip}/~{port}/~{selected-group}"))
          :on-message #(when (set? %) (reset! *members %))}))
      (ant/collapse
       {:accordion true
        :active-key selected-group
        :on-change (partial reset! *selected-group)}
       (for [g (-> *groups react sort)]
         (ant/collapse-panel
          {:key g
           :header g}
          (ant/table
           {:pagination false
            :columns [{:title "PID" :data-index :pid}
                      {:title "Kill"
                       :data-index :kill
                       :render
                       (fn [pid]
                         (ant/button {:shape :circle
                                      :type :danger
                                      :icon :close
                                      :on-click #(kill pid)}))}]
            :data-source (for [[ip port n :as pid] (sort members)
                               :let [s (str ip ":" port "|" n)]]
                           {:key s
                            :pid s
                            :kill pid})}))))])))

(rum/defcs cluster
  < rum/reactive
  < (c/init-arg-atom
     first
     {:selected-node nil
      :node-groups nil})
  [state *local {:keys [simulators]}]
  (when-first [sim simulators]
    (let [curs (partial cursor *local)
          *selected-node (curs :selected-node)
          selected-node (react *selected-node)]
      (ant/row
       {:class :cluster
        :gutter 8
        :justify :center}
       (ant/col
        {:span 16}
        (connectivity {:simulator sim
                       :*selected-node *selected-node}))
       (ant/col
        {:span 8}
        (when selected-node
          (node-groups (curs :node-groups)
                       {:simulator sim
                        :node selected-node})))))))

