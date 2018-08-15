(ns party-bus.simulator.ui.core
  (:require [clojure.string :refer [join]]
            [cljs.core.async :as async :refer [chan <!]]
            [rum.core :as rum]
            [cljs-http.client :as http]
            [chord.client :refer [ws-ch]]
            [cljs-hash.sha1 :refer [sha1]])
  (:require-macros [cljs.core.async.macros :refer [go]]))

(goog-define INITIAL-ADDRESS "")

(defn zip! [& chs]
  (async/map vector chs 1))

(defn request [method address path & {:as opts}]
  (http/request (merge {:method method
                        :url (str "http://" address path)
                        :with-credentials? false}
                       opts)))

(defn connect-ws
  ([address path]
   (connect-ws address path nil))
  ([address path params]
   (go
     (let [query-str (if (map? params)
                       (str "?" (http/generate-query-string params))
                       "")
           url (str "ws://" address path query-str)
           {:keys [ws-channel]} (<! (ws-ch url {:format :edn}))]
       ws-channel))))

(defn store
  ([initial]
   (store initial :rum/store))
  ([initial key]
   {:init
    (fn [state]
      (assoc state key (atom initial)))}))

(defn init-arg-atom [selector data]
  (let [init
        (fn [state]
          (-> state :rum/args selector (swap! #(if (some? %) % data)))
          state)]
    {:init init
     :did-remount #(init %2)}))

(defn setter [*ref]
  #(reset! *ref (.. % -target -value)))

(defn- react-prop [*ref prop-key f]
  (rum/react
   (reify
     IDeref
     (-deref [_])

     IWatchable
     (-add-watch [_ key callback]
       (add-watch *ref (list prop-key key)
                  (fn [_ _ oldv newv]
                    (let [oldv' (f oldv)
                          newv' (f newv)]
                      (when (not= oldv' newv')
                        (callback key nil oldv' newv')))))
       nil)

     (-remove-watch [_ key]
       (remove-watch *ref (list prop-key key))
       nil))))

(defn react-vec [*vec]
  (react-prop *vec ::count count)
  (for [idx (-> *vec deref count range)]
    (rum/cursor *vec idx)))

(defn react-map [*map]
  (react-prop *map ::keys (comp set keys))
  (for [k (keys @*map)]
    [k (rum/cursor *map k)]))

(defn hash- [x]
  (sha1 (if (and (vector? x) (= (count x) 2))
          (join ":" x)
          x)))

