(ns party-bus.simulator.ui.paxos
  (:require [goog.object :as o]
            [clojure.set :refer [union]]
            [clojure.string :as s]
            [cljs.core.async :refer [<!]]
            [medley.core :refer [map-kv]]
            [sablono.core :refer-macros [html]]
            [rum.core :as rum :refer [react cursor]]
            [antizer.rum :as ant]
            [party-bus.simulator.ui.core :as c]
            [party-bus.simulator.ui.cluster
             :refer [node->str str->node pid->str]])
  (:require-macros [clojure.core.strint :refer [<<]]))

(rum/defcs acceptors
  < rum/reactive
  < (c/init-arg-atom
     first
     {:new-acceptors {}
      :counter 0
      :selected-node nil})
  [state *local {:keys [simulator *acceptors *nodes]}]
  (let [curs (partial cursor *local)
        nodes (react *nodes)
        acceptors (react *acceptors)
        *new-acceptors (curs :new-acceptors)
        new-acceptors (react *new-acceptors)
        *selected-node (curs :selected-node)
        *counter (curs :counter)
        records (if (empty? acceptors) new-acceptors acceptors)]
    [:div
     (ant/table
      {:class :row
       :pagination false
       :bordered true
       :title (constantly (html [:h3 "Acceptors"]))
       :columns [{:title "PID" :data-index :pid}
                 {:title "Proposal number" :data-index :pn}
                 {:title "Accepted number" :data-index :an}
                 {:title "Accepted PID" :data-index :apid}]
       :data-source (for [[pid {pn :proposal-number
                                {an :number apid :pid} :accepted-proposal}]
                          (sort-by first records)
                          :let [s (pid->str pid)]]
                      {:key s :pid s :pn pn :an an :apid (pid->str apid)})})
     (when (and (seq nodes) (empty? acceptors))
       (let [selected-node (react *selected-node)
             add-acceptor
             #(let [new-pid (-> selected-node str->node (conj @*counter))]
                (swap! *new-acceptors assoc new-pid nil)
                (swap! *counter inc))
             create-acceptors
             #(doseq [[[ip port]] new-acceptors]
                (c/request :post simulator
                           (<< "/paxos/spawn/acceptor/~{ip}/~{port}")))]
         [:div
          [:.row
           (ant/select
            {:style {:width 200}
             :value selected-node
             :on-change (partial reset! *selected-node)}
            (for [node (sort nodes)
                  :let [n (node->str node)]]
              (ant/select-option {:value n} n)))
           " "
           (when selected-node
             (ant/button {:icon :plus
                          :on-click add-acceptor}))]
          [:.row
           (when (seq new-acceptors)
             (ant/button {:type :danger
                          :on-click #(reset! *new-acceptors {})}
                         "Reset"))
           " "
           (when (seq new-acceptors)
             (ant/button {:type :primary
                          :on-click create-acceptors}
                         "Submit"))]]))]))

(rum/defcs proposers
  < rum/reactive
  < (c/store {} ::proposers)
  < (c/store {} ::leaders)
  < (c/init-arg-atom
     first
     {:selected-node nil})
  [state *local {:keys [simulator *acceptors *nodes]}]
  (let [*selected-node (cursor *local :selected-node)
        nodes (react *nodes)
        acceptors (react *acceptors)
        *proposers (::proposers state)
        proposers (react *proposers)
        *leaders (::leaders state)
        leaders (react *leaders)
        kill (fn [[ip port n]]
               (c/request :delete
                          simulator
                          (<< "/cluster/kill/~{ip}/~{port}/~{n}")))]
    [:div
     (c/ws-listener
      {:value "proposers"
       :on-value-change #(reset! *proposers nil)
       :connect
       #(c/connect-ws simulator (<< "/paxos/proposers"))
       :on-message #(reset! *proposers %)})
     (c/ws-listener
      {:value "leaders"
       :on-value-change #(reset! *leaders nil)
       :connect
       #(c/connect-ws simulator (<< "/paxos/leaders"))
       :on-message #(reset! *leaders %)})
     (ant/table
      {:class :row
       :pagination false
       :bordered true
       :title (constantly (html [:h3 "Proposers"]))
       :columns [{:title "PID" :data-index :pid}
                 {:title "Leader?"
                  :data-index :leader?
                  :render c/bool-icon}
                 {:title "kill"
                  :data-index :kill
                  :render
                  (fn [pid]
                    (ant/button {:shape :circle
                                 :type :danger
                                 :icon :close
                                 :on-click #(kill pid)}))}]
       :data-source (for [pid (sort proposers)
                          :let [s (pid->str pid)]]
                      {:key s
                       :pid s
                       :leader? (contains? leaders pid)
                       :kill pid})})
     (when (and (seq nodes) (seq acceptors))
       (let [selected-node (react *selected-node)
             create-proposer
             #(let [[ip port] (-> selected-node str->node)]
                (c/request :post simulator
                           (<< "/paxos/spawn/proposer/~{ip}/~{port}")
                           :edn-params (keys acceptors)))]
         [:div
          (ant/select
           {:style {:width 200}
            :value selected-node
            :on-change (partial reset! *selected-node)}
           (for [node (sort nodes)
                 :let [n (node->str node)]]
             (ant/select-option {:value n} n)))
          " "
          (when selected-node
            (ant/button {:icon :plus
                         :on-click create-proposer}))]))]))

(rum/defcs paxos
  < rum/reactive
  < (c/store [] ::nodes)
  < (c/store {} ::acceptors)
  < (c/init-arg-atom
     first
     {:acceptors nil})
  [state *local {:keys [simulators]}]
  (when-first [sim simulators]
    (let [*nodes (::nodes state)
          *acceptors (::acceptors state)]
      (ant/row
       {:class :cluster
        :gutter 8
        :justify :center}
       (c/ws-listener
        {:value "nodes"
         :on-value-change #(reset! *nodes nil)
         :connect
         #(c/connect-ws sim (<< "/cluster/members/nodes"))
         :on-message #(reset! *nodes %)})
       (c/ws-listener
        {:value "acceptors"
         :on-value-change #(reset! *acceptors nil)
         :connect
         #(c/connect-ws sim (<< "/paxos/acceptors"))
         :on-message #(reset! *acceptors %)})
       (ant/col
        {:span 12}
        (acceptors (cursor *local :acceptors)
                   {:simulator sim
                    :*nodes *nodes
                    :*acceptors *acceptors}))
       (ant/col
        {:span 12}
        (proposers (cursor *local :proposers)
                   {:simulator sim
                    :*nodes *nodes
                    :*acceptors *acceptors}))))))

