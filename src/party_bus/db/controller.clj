(ns party-bus.db.controller
  (:require [medley.core :refer [map-vals]]
            [manifold.deferred :as md]
            [party-bus.cluster
             [process :as p]
             [util :as u]]
            [party-bus.db.storage :as s]))

(def basic-handlers
  {:get
   (fn [tx {:keys [key-space key]}]
     (s/get-val tx key-space key false false))
   :get-keys
   (fn [tx {:keys [key-space range]}]
     (let [r (replace {:< < :<= <= :> > :>= >=} range)]
       (apply s/get-keys tx key-space r)))
   :set
   (fn [tx {:keys [key-space key value]}]
     (s/set-val tx key-space key value))
   :del
   (fn [tx {:keys [key-space key]}]
     (s/del-val tx key-space key))
   :inc
   (fn [tx {:keys [key-space key value]
            :or {value 1}}]
     (let [v (or (s/get-val tx key-space key false false) 0)]
       (s/set-val tx key-space key (+ v value))))
   :swap
   (fn [tx {:keys [key-space key key']}]
     (let [v (s/get-val tx key-space key false false)
           v' (s/get-val tx key-space key' false false)]
       (s/set-val tx key-space key v')
       (s/set-val tx key-space key' v)))})

(defn- worker [p {:keys [handlers key-spaces]} _ [_ body :as msg]]
  (md/chain'
   (s/run-transaction key-spaces (handlers (u/msg-type msg)) body)
   (partial u/response p msg)))

(defn controller
  [p {:keys [handlers key-spaces worker-amount worker-groups]}]
  (let [key-spaces
        (map-vals (fn [{:keys [storage source create? options]}]
                    (let [s ((case storage
                               :in-memory s/in-memory
                               :in-memory+disk s/in-memory+disk)
                             options)
                          initialized? (s/initialize s source create?)]
                      (when-not initialized?
                        (p/terminate p))
                      (p/spawn p (partial s/controller s) {:bound? true})
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
