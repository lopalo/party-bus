(ns party-bus.simulator.ui.dht.management
  (:require [clojure.string :refer [join]]
            [rum.core :as rum :refer [react]]
            [antizer.rum :as ant]
            [party-bus.simulator.ui.core :refer [store request]])
  (:require-macros [clojure.core.strint :refer [<<]]))

(rum/defcs management
  < rum/reactive
  < (store 3 ::create-amount)
  < (store 3 ::terminate-amount)
  [state reload ip->sim sim->total peers]
  (let [ips (-> ip->sim keys vec)
        *create-amount (::create-amount state)
        *terminate-amount (::terminate-amount state)
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

    (ant/collapse
     {:class :management :default-active-key ["1"]}
     (ant/collapse-panel
      {:header "Management" :key "1"}
      [:div
       {:key "content"}
       [:.row
        (ant/button {:type :primary :on-click reload} "Reload")]
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

       [:.row "Peers: " (reduce + (vals sim->total))]
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
                       "Terminate peers"))))]))))
