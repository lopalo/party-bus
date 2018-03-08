(ns party-bus.simulator.ui.dht.peer
  (:require [clojure.string :as s]
            [cljs.core.async :as async :refer [<!]]
            [rum.core :as rum :refer [react]]
            [antizer.rum :as ant]
            [party-bus.simulator.ui.core
             :refer [store request connect-ws hash-]])
  (:require-macros [clojure.core.strint :refer [<<]]
                   [cljs.core.async.macros :refer [go]]))

(declare put-form last-request-info data-info)

(def ui-state
  {:address nil
   :ws-c nil
   :peer-state nil
   :last-request nil})

(def hooks
  {:after-render
   (fn [state]
     (let [[selected-peer *contacts ip->sim] (:rum/args state)
           [ip port] selected-peer
           *local (::local state)
           {:keys [address ws-c]} @*local
           active? #(= selected-peer (:address @*local))]
       (when (not= selected-peer address)
         (when ws-c (async/close! ws-c))
         (reset! *local (assoc ui-state :address selected-peer))
         (when selected-peer
           (go
             (let [ws-c (<! (connect-ws (ip->sim ip)
                                        (<< "/dht/peer/~{ip}/~{port}")))]
               (when (active?)
                 (swap! *local assoc :ws-c ws-c)
                 (loop [{peer-state :message} (<! ws-c)]
                   (when (and peer-state (active?))
                     (let [p-contacts (:contacts peer-state)]
                       (if-not (:peer-state @*local)
                         (reset! *contacts
                                 (for [c p-contacts] [selected-peer c]))
                         (swap! *contacts
                                (partial remove
                                         (fn [[p p']]
                                           (and (= p selected-peer)
                                                (not (p-contacts p'))))))))
                     (swap! *local assoc :peer-state peer-state)
                     (recur (<! ws-c)))))
               (async/close! ws-c))))))
     state)
   :will-unmount
   (fn [state]
     (some-> state ::local deref :ws-c async/close!))})

(rum/defcs peer
  < (rum/local ui-state ::local)
  < hooks
  [state _ *contacts ip->sim]
  (let [*local (::local state)
        {:keys [address peer-state]} @*local
        [ip port] address
        sim (ip->sim ip)
        input-val #(.-value (rum/ref-node state %))
        show-route #(reset! *contacts
                            (partition 2 1 (-> @*local :last-request :route)))
        set-last-request
        (fn [k method response]
          (when (= address (:address @*local))
            (if (map? response)
              (do
                (swap! *local assoc :last-request
                       (assoc response :key k :method method))
                (show-route))
              (swap! *local assoc :last-request response))))

        do-get
        (fn []
          (go
            (let [k (input-val "get-key")
                  params {:key k
                          :trace true}
                  res (<! (request :get sim
                                   (<< "/dht/get/~{ip}/~{port}")
                                   :query-params params))]
              (set-last-request k :get (:body res)))))
        do-get-trie
        (fn []
          (go
            (let [prefix (input-val "get-trie")
                  params {:prefix prefix
                          :trace true}
                  res (<! (request :get sim
                                   (<< "/dht/get-trie/~{ip}/~{port}")
                                   :query-params params))]
              (set-last-request prefix :get-trie (:body res)))))]
    (ant/card
     {:title (str ip ":" port)}
     [:.peer
      {:key "content"}
      (if address
        [:div
         [:.row "Hash: " (hash- address)]
         (ant/button
          {:class :row
           :type :danger
           :on-click #(request :delete sim (<< "/dht/peer/~{ip}/~{port}"))}
          "Terminate")])
      (if-let [{storage :storage trie :trie p-contacts :contacts} peer-state]
        [:div
         (put-form sim ip port set-last-request)
         (ant/form
          {:class :row :layout :inline}
          (ant/form-item
           (ant/input {:ref "get-key" :placeholder "Key"}))
          (ant/form-item
           (ant/button {:on-click do-get} "Get")))
         (ant/form
          {:class :row :layout :inline}
          (ant/form-item
           (ant/input {:ref "get-trie" :placeholder "Prefix"}))
          (ant/form-item
           (ant/button {:on-click do-get-trie} "Get Trie")))
         (if-let [last-request (:last-request @*local)]
           (last-request-info last-request))
         (data-info address storage trie p-contacts *contacts)])])))

(rum/defcs put-form
  < rum/reactive
  < (store "" ::key)
  < (store "" ::value)
  < (store 600 ::ttl-sec)
  < (store true ::trie?)
  [state sim ip port set-last-request]
  (let [*key (::key state)
        *value (::value state)
        *ttl-sec (::ttl-sec state)
        *trie? (::trie? state)
        set-val
        (fn [*ref]
          #(reset! *ref (.. % -target -value)))
        do-put
        (fn []
          (go
            (let [k @*key
                  params {:key k
                          :value @*value
                          :ttl (* @*ttl-sec 1000)
                          :trie? @*trie?
                          :trace? true}
                  res (<! (request :put sim
                                   (<< "/dht/put/~{ip}/~{port}")
                                   :edn-params params))]
              (set-last-request k :put (:body res)))))
        shuffle-key
        (fn []
          (swap! *key #(->> % vec shuffle (apply str))))]
    (ant/form
     {:class :row :layout :inline}
     (ant/form-item
      (ant/input {:value (react *key)
                  :on-change (set-val *key)
                  :placeholder "Key"}))
     (ant/form-item
      (ant/button {:shape :circle
                   :icon :sync
                   :on-click shuffle-key}))
     (ant/form-item
      (ant/input {:value (react *value)
                  :on-change (set-val *value)
                  :placeholder "Value"}))
     (ant/form-item
      (ant/input-number {:step 60
                         :min 0
                         :value (react *ttl-sec)
                         :on-change (partial reset! *ttl-sec)
                         :formatter #(str % "sec")
                         :parser #(s/replace % #"sec" "")}))
     (ant/form-item
      (ant/switch {:checked-children "Trie leaf"
                   :un-checked-children "Trie leaf"
                   :checked (react *trie?)
                   :on-change (partial reset! *trie?)}))

     (ant/form-item
      (ant/button {:on-click do-put} "Put")))))

(rum/defc last-request-info [last-request]
  (ant/card
   {:title "Last request"
    :class :row}
   [:div
    {:key "content"}
    (if (map? last-request)
      (let [{:keys [key method route value trie ttl]} last-request]
        [:div
         [:div "Method: " (name method)]
         [:div "Key hash: " (hash- key)]
         [:div "Hops: " (count route)]
         (if ttl [:div "TTL: " ttl])
         [:div "Key: " key]
         (if value [:div "Value: " value])
         (if trie [:div "Trie: " (str trie)])])
      [:div [:strong (str last-request)]])]))

(rum/defc data-info [address storage trie p-contacts *contacts]
  (ant/collapse
   (ant/collapse-panel
    {:header "Contacts"}
    [:div
     {:key "contacts"}
     (ant/button
      {:class :contact
       :type :primary
       :size :small
       :on-click #(reset! *contacts (for [c p-contacts] [address c]))}
      "All")
     (for [[ip port :as c] (sort p-contacts)]
       (ant/button
        {:key c
         :class :contact
         :size :small
         :on-click #(reset! *contacts [[address c]])}
        ip ":" port))])
   (ant/collapse-panel
    {:header "Storage"}
    (let [{:keys [data expiration]} storage]
      [:.storage
       {:key "content"}
       (for [[k v] data
             :let [exp (-> k expiration js/Date.
                           (.toLocaleString "en-GB"))]]
         [[:.record {:key k} (<< "~{k} (~{exp}): ~{v}")]])]))
   (ant/collapse-panel
    {:header "Trie"}
    [:div
     {:key "trie"}
     (for [[k n] (sort trie)]
       [[:div {:key k} (<< "~{k}: ~{n}")]])])))

