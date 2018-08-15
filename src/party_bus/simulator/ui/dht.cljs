(ns party-bus.simulator.ui.dht
  (:require [clojure.set :refer [union difference]]
            [cljs.core.async :as async :refer [<!]]
            [medley.core :refer [remove-vals]]
            [rum.core :as rum :refer [react cursor]]
            [antizer.rum :as ant]
            [party-bus.simulator.ui.core
             :refer [zip! request connect-ws init-arg-atom store]]
            [party-bus.simulator.ui.dht.management :refer [management]]
            [party-bus.simulator.ui.dht.graph :refer [graph]]
            [party-bus.simulator.ui.dht.peer :refer [peer]]
            [party-bus.simulator.ui.dht.last-request
             :refer [last-request-info]])
  (:require-macros [cljs.core.async.macros :refer [go go-loop]]))

(def hooks
  (let [max-total 120
        sync-simulators
        (fn sync-simulators [state old-sims sims]
          (let [*local (-> state :rum/args first)
                curs (partial cursor *local)
                *ip->sim (::ip->sim state)
                *sim->ws (::sim->ws state)
                *sim->total (::sim->total state)
                del-sims (difference old-sims sims)
                del-wss (map @*sim->ws del-sims)]
            (swap! *ip->sim #(remove-vals del-sims %))
            (swap! *sim->ws #(apply dissoc % del-sims))
            (swap! *sim->total #(apply dissoc % del-sims))
            (run! async/close! del-wss)
            (go
              (let [add-sims (difference sims old-sims)
                    res (<! (async/map
                             #(map :body %&)
                             (map #(request :get % "/dht/ip-addresses")
                                  add-sims)))
                    add-ip->sim (for [[sim ips] (map vector add-sims res)
                                      ip ips]
                                  [ip sim])
                    max-total' (->> *sim->total deref
                                    count (max 1) (/ max-total) int)
                    connect
                    (fn [sim]
                      (connect-ws sim
                                  "/dht/peer-addresses"
                                  {:max-total max-total'}))
                    add-wss (<! (apply zip! (map connect add-sims)))
                    add-sim->ws (map vector add-sims add-wss)
                    *selected-peer (curs :selected-peer)
                    *contacts (curs :selected-contacts)]
                (swap! *ip->sim #(apply conj % add-ip->sim))
                (swap! *sim->ws #(apply conj % add-sim->ws))
                (doseq [[sim ws-c] add-sim->ws]
                  (go-loop [{[header peers total] :message} (<! ws-c)]
                    (when header
                      (swap! (::peers state)
                             (case header
                               :initial union
                               :add union
                               :delete difference)
                             peers)
                      (swap! *sim->total assoc sim total)
                      (when (and (= header :delete) (peers @*selected-peer))
                        (reset! *selected-peer nil)
                        (reset! *contacts []))
                      (recur (<! ws-c))))))))
          (assoc state ::reload
                 (fn []
                   (sync-simulators state sims #{})
                   (sync-simulators state #{} sims))))
        simulators (comp :simulators second :rum/args)]
    {:did-mount
     (fn [state]
       (sync-simulators state #{} (simulators state)))
     :did-remount
     (fn [old-state state]
       (sync-simulators state (simulators old-state) (simulators state)))
     :will-unmount
     (fn [state]
       (sync-simulators state (simulators state) #{}))}))

(rum/defcs dht
  < rum/reactive
  < (store {} ::ip->sim)
  < (store {} ::sim->ws)
  < (store {} ::sim->total)
  < (store #{} ::peers)
  < (init-arg-atom
     first
     {:selected-peer nil
      :selected-contacts []
      :management nil
      :peer nil
      :last-request nil})
  < hooks
  [state *local]
  (let [curs (partial cursor *local)
        ip->sim (-> state ::ip->sim react)
        sim->total (-> state ::sim->total react)
        peers (-> state ::peers react)
        *selected-peer (curs :selected-peer)
        selected-peer (react *selected-peer)
        *contacts (curs :selected-contacts)
        *last-request (curs :last-request)
        show-route #(reset! *contacts
                            (partition 2 1 (-> @*last-request :route)))]
    (ant/row
     {:class :dht
      :gutter 8
      :justify :center}
     (ant/col
      {:span 5}
      (management (curs :management)
                  {:reload (::reload state)
                   :ip->sim ip->sim
                   :sim->total sim->total
                   :peers peers})
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
              {:selected-peer selected-peer
               :*contacts *contacts
               :*last-request *last-request
               :show-route show-route
               :ip->sim ip->sim}))))))
