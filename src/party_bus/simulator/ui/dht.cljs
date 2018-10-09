(ns party-bus.simulator.ui.dht
  (:require [clojure.set :refer [union difference]]
            [cljs.core.async :as async :refer [<! timeout]]
            [medley.core :refer [remove-vals]]
            [rum.core :as rum :refer [react cursor]]
            [antizer.rum :as ant]
            [party-bus.simulator.ui.core :as c]
            [party-bus.simulator.ui.dht.management :refer [management]]
            [party-bus.simulator.ui.dht.graph :refer [graph]]
            [party-bus.simulator.ui.dht.peer :refer [peer]]
            [party-bus.simulator.ui.dht.last-request
             :refer [last-request-info]])
  (:require-macros [cljs.core.async.macros :refer [go]]))

(def simulators (comp :simulators second :rum/args))

(def max-total 120)

(defn load-ips [state]
  (go
    (let [sims (simulators state)
          res (<! (async/map
                   #(map :body %&)
                   (map #(c/request :get % "/dht/ip-addresses") sims)))]
      (reset! (::ip->sim state)
              (into {}
                    (for [[sim ips] (map vector sims res)
                          ip ips]
                      [ip sim])))
      (swap! (::sim->total state) select-keys sims)))
  state)

(rum/defcs dht
  < rum/reactive
  < (c/store {} ::ip->sim)
  < (c/store {} ::sim->total)
  < (c/store #{} ::peers)
  < (c/store false ::reloading)
  < (c/init-arg-atom
     first
     {:selected-peer nil
      :selected-contacts []
      :management nil
      :peer nil
      :last-request nil})
  < {:did-mount load-ips
     :did-remount
     (fn [old-state state]
       (when (not= (simulators old-state) (simulators state))
         (load-ips state))
       state)}
  [state *local {sims :simulators}]
  (let [curs (partial cursor *local)
        ip->sim (-> state ::ip->sim react)
        *sim->total (::sim->total state)
        sim->total (react *sim->total)
        peers (-> state ::peers react)
        *selected-peer (curs :selected-peer)
        selected-peer (react *selected-peer)
        *contacts (curs :selected-contacts)
        *last-request (curs :last-request)
        *reloading (::reloading state)
        show-route #(reset! *contacts
                            (partition 2 1 (-> @*last-request :route)))

        max-total' (->> *sim->total deref
                        count (max 1) (/ max-total) int)
        on-ws-message
        (fn [sim [header peers total]]
          (swap! (::peers state)
                 (case header
                   :initial union
                   :add union
                   :delete difference)
                 peers)
          (swap! *sim->total assoc sim total)
          (when (and (= header :delete) (peers @*selected-peer))
            (reset! *selected-peer nil)
            (reset! *contacts [])))
        reload
        (fn []
          (go
            (reset! *reloading true)
            (<! (timeout 200))
            (reset! *reloading false)))]
    (ant/row
     {:class :dht
      :gutter 8
      :justify :center}
     (ant/col
      {:span 5}
      (management (curs :management)
                  {:reload reload
                   :ip->sim ip->sim
                   :sim->total sim->total
                   :peers peers})
      (when-not (react *reloading)
        (for [sim sims]
          (c/ws-listener {:value sim
                          :connect
                          #(c/connect-ws sim "/dht/peer-addresses"
                                         {:max-total max-total'})
                          :on-message (partial on-ws-message sim)})))
      (when-let [last-request (react *last-request)]
        (last-request-info {:last-request last-request
                            :show-route show-route
                            :*selected-peer *selected-peer})))
     (ant/col
      {:span 12}
      (graph {:peers peers
              :*selected-peer *selected-peer
              :*contacts *contacts}))
     (ant/col
      {:span 7}
      (when (ip->sim (first selected-peer))
        (peer (curs :peer)
              {:address selected-peer
               :*contacts *contacts
               :*last-request *last-request
               :show-route show-route
               :ip->sim ip->sim}))))))
