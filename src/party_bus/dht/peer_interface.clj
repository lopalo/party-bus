(ns party-bus.dht.peer-interface
  (:require [manifold
             [deferred :as md]
             [stream :as ms]]
            [party-bus.dht.core :refer [terminated terminated-error]])
  (:import [party_bus.dht.core
            PeerContainer
            Curator
            Period]))

(set! *warn-on-reflection* true)

(defprotocol PeerInterface
  (get-address [this])
  (get-state [this])
  (update-state [this f])
  (send-to [this receiver msg])
  (create-period [this id msec])
  (cancel-period [this id])
  (create-deferred [this])
  (terminate [this]))

(defn- terminated-error! []
  (throw terminated-error))

(defn peer-interface
  [^Curator curator address ^PeerContainer peer]
  (reify PeerInterface
    (get-address [this]
      address)

    (get-state [this]
      (let [state @(.state peer)]
        (if (= state terminated)
          (terminated-error!)
          state)))

    (update-state [this f]
      (let [new-state (swap! (.state peer)
                             #(if (= % terminated) % (f %)))]
        (if (= new-state terminated)
          (terminated-error!)
          new-state)))

    (send-to [this receiver msg]
      (ms/put! (.sock-stream peer)
               {:socket-address receiver
                :message msg}))

    (create-period [this id msec]
      (let [{:keys [period-streams handler]} peer
            period (Period. id)
            ps (ms/periodically msec msec (constantly period))]
        (if (contains? (swap! period-streams #(when % (assoc % id ps))) id)
          (do (ms/on-drained ps #(swap! period-streams dissoc id))
              (ms/consume handler (ms/onto (.executor curator) ps))
              ps)
          (terminated-error!))))

    (cancel-period [this id]
      (some-> peer .period-streams deref (get id) ms/close!))

    (create-deferred [this]
      (let [deferreds (.deferreds peer)
            d (-> curator .executor md/deferred)]
        (if (contains? (swap! deferreds #(when % (conj % d))) d)
          (do (md/finally d #(swap! deferreds disj d))
              d)
          (terminated-error!))))

    (terminate [this]
      (-> peer .sock-stream ms/close!))))

(defn update-state-in [p path f & args]
  (get-in (update-state p #(apply update-in % path f args)) path))
