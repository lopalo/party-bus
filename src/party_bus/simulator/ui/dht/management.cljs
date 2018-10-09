(ns party-bus.simulator.ui.dht.management
  (:require [clojure.string :refer [join]]
            [rum.core :as rum :refer [react cursor]]
            [antizer.rum :as ant]
            [party-bus.simulator.ui.core :refer [init-arg-atom request]])
  (:require-macros [clojure.core.strint :refer [<<]]))

(rum/defcs management
  < rum/reactive
  < (init-arg-atom
     first
     {:create-amount 3
      :terminate-amount 3})
  [state *local {:keys [reload ip->sim sim->total peers]}]
  (let [ips (-> ip->sim keys vec)
        curs (partial cursor *local)
        *create-amount (curs :create-amount)
        *terminate-amount (curs :terminate-amount)
        create-peers
        (fn []
          (let [peers' (vec peers)]
            (doseq [_ (range @*create-amount)
                    :let [ip (rand-nth ips)
                          p-contacts
                          (if (seq peers')
                            (map #(join ":" %)
                                 (repeatedly 4 #(rand-nth peers')))
                            [])]]
              (request :post (ip->sim ip)
                       (str "/dht/peer/" ip)
                       :edn-params p-contacts))))
        terminate-peers
        (fn []
          (doseq [[ip port] (take @*terminate-amount (shuffle peers))]
            (request :delete (ip->sim ip) (<< "/dht/peer/~{ip}/~{port}"))))]
    (ant/card
     {:title "Distributed Hash Table"
      :class :row}
     [:div
      {:key "content"}
      (ant/table
       {:class :row
        :pagination false
        :size :middle
        :columns
        [{:title "Simulator" :data-index :sim :width 120}
         {:title "IP addresses" :data-index :ips}]
        :data-source
        (for [[sim sim-ips] (group-by second ip->sim)]
          {:key sim
           :sim sim
           :ips (for [s (->> sim-ips (map first) sort)]
                  (ant/tag {:key s :color :blue} s))})})

      [:.row
       "Peers: "
       (reduce + (vals sim->total))
       " "
       (ant/button {:shape :circle
                    :size :small
                    :icon :sync
                    :on-click reload})]
      (ant/form
       {:class :row :layout :inline}
       (ant/form-item
        (ant/input-number {:value (react *create-amount)
                           :on-change (partial reset! *create-amount)
                           :min 1
                           :max 50}))
       (ant/form-item
        (ant/button {:on-click create-peers}
                    "Create peers")))
      (if (seq peers)
        (ant/form
         {:class :row :layout :inline}
         (ant/form-item
          (ant/input-number {:value (react *terminate-amount)
                             :on-change (partial reset! *terminate-amount)
                             :min 1
                             :max 200}))
         (ant/form-item
          (ant/button {:type :danger :on-click terminate-peers}
                      "Terminate peers"))))])))
