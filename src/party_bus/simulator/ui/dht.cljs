(ns party-bus.simulator.ui.dht
  (:require [clojure.set :refer [union difference]]
            [cljs.core.async :as async :refer [<!]]
            [medley.core :refer [remove-vals]]
            [rum.core :as rum :refer [react]]
            [antizer.rum :as ant]
            [party-bus.simulator.ui.core
             :refer [zip! request connect-ws store]]
            [party-bus.simulator.ui.dht.management :refer [management]]
            [party-bus.simulator.ui.dht.graph :refer [graph]]
            [party-bus.simulator.ui.dht.peer :refer [peer]])
  (:require-macros [cljs.core.async.macros :refer [go go-loop]]))

(def hooks
  (let [simulators (comp first :rum/args)
        max-total 120
        sync-simulators
        (fn sync-simulators [state old-sims sims]
          (let [del-sims (difference old-sims sims)
                del-wss (map @(::sim->ws state) del-sims)]
            (swap! (::ip->sim state) #(remove-vals del-sims %))
            (swap! (::sim->ws state) #(apply dissoc % del-sims))
            (swap! (::sim->total state) #(apply dissoc % del-sims))
            (run! async/close! del-wss))
          (go
            (let [add-sims (difference sims old-sims)
                  res (<! (async/map
                           #(map :body %&)
                           (map #(request :get % "/dht/ip-addresses")
                                add-sims)))
                  add-ip->sim (for [[sim ips] (map vector add-sims res)
                                    ip ips]
                                [ip sim])
                  max-total' (->> state ::sim->total deref
                                  count (max 1) (/ max-total) int)
                  connect
                  (fn [sim]
                    (connect-ws sim
                                "/dht/peer-addresses"
                                {:max-total max-total'}))
                  add-wss (<! (apply zip! (map connect add-sims)))
                  add-sim->ws (map vector add-sims add-wss)
                  *selected-peer (::selected-peer state)
                  *contacts (::selected-contacts state)]
              (swap! (::ip->sim state) #(apply conj % add-ip->sim))
              (swap! (::sim->ws state) #(apply conj % add-sim->ws))
              (doseq [[sim ws-c] add-sim->ws]
                (go-loop [{[header peers total] :message} (<! ws-c)]
                  (when header
                    (swap! (::peers state)
                           (case header
                             :initial union
                             :add union
                             :delete difference)
                           peers)
                    (swap! (::sim->total state) assoc sim total)
                    (when (and (= header :delete) (peers @*selected-peer))
                      (reset! *selected-peer nil)
                      (reset! *contacts []))
                    (recur (<! ws-c)))))))
          (assoc state ::reload
                 (fn []
                   (sync-simulators state sims #{})
                   (sync-simulators state #{} sims))))]
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
  < (store nil ::selected-peer)
  < (store [] ::selected-contacts)
  < hooks
  [state simulators]
  (let [ip->sim (-> state ::ip->sim react)
        sim->total (-> state ::sim->total react)
        peers (-> state ::peers react)
        *selected-peer (::selected-peer state)
        selected-peer (react *selected-peer)
        *contacts (::selected-contacts state)]
    (ant/row
     {:class :dht :gutter 8}
     (ant/col
      {:span 8}
      (management (::reload state) ip->sim sim->total peers)
      (when selected-peer
        (peer selected-peer *contacts ip->sim)))
     (ant/col
      {:span 16}
      (graph peers *selected-peer *contacts)))))

