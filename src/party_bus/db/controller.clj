(ns party-bus.db.controller
  (:require [medley.core :refer [map-vals]]
            [manifold.deferred :as md]
            [party-bus.cluster
             [process :as p]
             [util :as u]]
            [party-bus.db.storage
             [core :as sc]
             [in-memory :as ims]
             [persistent-in-memory :as pims]
             [persistent :as ps]
             [replicated :as rs]]))

(def basic-handlers
  {:get
   (fn [tx {:keys [key-space key]}]
     (sc/get-val tx key-space key))
   :get-keys
   (fn [tx {:keys [key-space range]}]
     (let [r (replace {:< < :<= <= :> > :>= >=} range)]
       (apply sc/get-keys tx key-space r)))
   :set
   (fn [tx {:keys [key-space key value pages]}]
     (sc/set-val tx key-space key value {:pages (or pages 1)})
     true)
   :del
   (fn [tx {:keys [key-space key]}]
     (sc/del-val tx key-space key)
     true)
   :inc
   (fn [tx {:keys [key-space key value]
            :or {value 1}}]
     (let [v (or (sc/get-val tx key-space key) 0)
           v' (+ v value)]
       (sc/set-val tx key-space key v')
       v'))
   :swap
   (fn [tx {:keys [key-space key key']}]
     (let [v (sc/get-val tx key-space key)
           v' (sc/get-val tx key-space key')]
       (sc/set-val tx key-space key v')
       (sc/set-val tx key-space key' v)
       true))})

(defn- worker [p {:keys [handlers key-spaces]} _ [_ body :as msg]]
  (md/chain'
   (sc/run-transaction key-spaces (handlers (u/msg-type msg)) body)
   (partial u/response p msg)))

(defn controller
  [p {:keys [handlers key-spaces worker-amount worker-groups]}]
  (let [key-spaces
        (map-vals (fn [{:keys [storage source create? options]}]
                    (let [s ((case storage
                               :in-memory ims/storage
                               :persistent-in-memory pims/storage
                               :persistent ps/storage
                               :replicated rs/storage)
                             options)
                          initialized? (sc/initialize s source create?)]
                      (when-not initialized?
                        (p/terminate p))
                      (p/spawn p (partial sc/controller s) {:bound? true})
                      s))
                  key-spaces)
        worker-params {:key-spaces key-spaces
                       :handlers handlers}]
    (dotimes [_ worker-amount]
      (p/spawn p
               (fn [p]
                 (p/add-to-groups p worker-groups)
                 (u/receive-loop worker p worker-params nil))
               {:bound? true}))
    (p/receive p)))
