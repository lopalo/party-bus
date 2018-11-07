(ns party-bus.simulator.ui.core
  (:require [clojure.string :refer [join]]
            [cljs.core.async :as async :refer [chan <!]]
            [rum.core :as rum]
            [antizer.rum :as ant]
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
     :before-render init}))

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

(defn format-ts [ts]
  (.toLocaleString (js/Date. ts) "en-GB"))

(defn bool-icon [value]
  (ant/icon (if value
              {:type :check
               :class :green}
              {:type :close
               :class :red})))

(rum/defc ws-listener
  < (store nil ::value)
  < (store nil ::ws-c)
  < {:key-fn (fn [{v :value}] (str v))
     :after-render
     (fn [state]
       (let [[{:keys [value
                      connect
                      on-value-change
                      on-message]}] (:rum/args state)
             *value (::value state)
             *ws-c (::ws-c state)
             active? #(= value @*value)]
         (when (not= value @*value)
           (when-let [ws-c @*ws-c]
             (async/close! ws-c))
           (reset! *value value)
           (when on-value-change
             (on-value-change))
           (go
             (let [ws-c (<! (connect))]
               (when ws-c
                 (when (active?)
                   (reset! *ws-c ws-c)
                   (loop [{msg :message} (<! ws-c)]
                     (when (and msg (active?))
                       (on-message msg)
                       (recur (<! ws-c)))))
                 (async/close! ws-c))))))
       state)
     :will-unmount
     (fn [state]
       (some-> state ::ws-c deref async/close!)
       state)}
  [])

(defn create-form
  [{:keys [form *state args *state] :or {*state (atom nil) args []}}]
  (let [options {:on-values-change #(reset! *state (js->clj %3))}
        props #js {":rum/initial-state" {:rum/args args}}]
    (ant/create-form form :options options :props props)))

(defn form-item-maker [{:keys [form form-style *state]}]
  (fn [field-name label options component]
    (ant/form-item
     (assoc form-style :label label)
     (ant/decorate-field
      form
      field-name
      (assoc options :initial-value
             (some-> *state deref (get field-name)))
      component))))
