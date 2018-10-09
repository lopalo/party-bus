(ns party-bus.simulator.ui.dht.peer
  (:require [clojure.string :as s]
            [cljs.core.async :as async :refer [<!]]
            [rum.core :as rum :refer [react cursor]]
            [antizer.rum :as ant]
            [party-bus.simulator.ui.core :as c])
  (:require-macros [clojure.core.strint :refer [<<]]
                   [cljs.core.async.macros :refer [go]]))

(declare put-form data-info)

(rum/defcs peer
  < rum/reactive
  < (c/store nil ::peer-state)
  < (c/init-arg-atom
     first
     {:get-key ""
      :get-trie-prefix ""
      :put-form nil
      :data-info nil})
  [state *local {:keys [address *contacts *last-request show-route ip->sim]}]
  (let [curs (partial cursor *local)
        [ip port] address
        *peer-state (-> state ::peer-state)
        peer-state (react *peer-state)
        sim (ip->sim ip)
        *get-key (curs :get-key)
        *get-trie-pr (curs :get-trie-prefix)
        set-last-request
        (fn [k method response]
          (let [basic {:key k
                       :method method
                       :ts (js/Date.now)}]
            (if (map? response)
              (do
                (reset! *last-request (merge basic response))
                (show-route))
              (reset! *last-request (assoc basic :error response)))))

        do-get
        (fn []
          (go
            (let [k  @*get-key
                  params {:key k
                          :trace true}
                  res (<! (c/request :get sim
                                     (<< "/dht/get/~{ip}/~{port}")
                                     :query-params params))]
              (set-last-request k :get (:body res)))))
        do-get-trie
        (fn []
          (go
            (let [prefix @*get-trie-pr
                  params {:prefix prefix
                          :trace true}
                  res (<! (c/request :get sim
                                     (<< "/dht/get-trie/~{ip}/~{port}")
                                     :query-params params))]
              (set-last-request prefix :get-trie (:body res)))))
        on-ws-message
        (fn [peer-state]
          (let [p-contacts (:contacts peer-state)]
            (if-not @*peer-state
              (reset! *contacts
                      (for [c p-contacts] [address c]))
              (swap! *contacts
                     (partial remove
                              (fn [[p p']]
                                (and (= p address)
                                     (not (p-contacts p'))))))))
          (reset! *peer-state peer-state))]
    (ant/card
     {:title (str ip ":" port)}
     [:.peer
      {:key "content"}
      (when address
        [:div
         (c/ws-listener {:value address
                         :on-value-change #(reset! *peer-state nil)
                         :connect
                         #(c/connect-ws (ip->sim ip)
                                        (<< "/dht/peer/~{ip}/~{port}"))
                         :on-message on-ws-message})
         [:.row "Hash: " (c/hash- address)]
         (ant/button
          {:class :row
           :type :danger
           :on-click #(c/request :delete sim (<< "/dht/peer/~{ip}/~{port}"))}
          "Terminate")])
      (when-let [{storage :storage trie :trie p-contacts :contacts} peer-state]
        [:div
         (put-form (curs :put-form)
                   {:simulator sim
                    :ip ip
                    :port port
                    :set-last-request set-last-request})
         (ant/form
          {:class :row :layout :inline}
          (ant/form-item
           (ant/input {:value (react *get-key)
                       :on-change (c/setter *get-key)
                       :placeholder "Key"}))
          (ant/form-item
           (ant/button {:on-click do-get} "Get")))
         (ant/form
          {:class :row :layout :inline}
          (ant/form-item
           (ant/input {:value (react *get-trie-pr)
                       :on-change (c/setter *get-trie-pr)
                       :placeholder "Prefix"}))
          (ant/form-item
           (ant/button {:on-click do-get-trie} "Get Trie")))
         (data-info (curs :data-info)
                    {:address address
                     :storage storage
                     :trie trie
                     :p-contacts p-contacts
                     :*contacts *contacts})])])))

(rum/defcs put-form
  < rum/reactive
  < (c/init-arg-atom
     first
     {:key ""
      :value ""
      :ttl-sec 600
      :trie? true})
  [state *local {:keys [simulator ip port set-last-request]}]
  (let [curs (partial cursor *local)
        *key (curs :key)
        *value (curs :value)
        *ttl-sec (curs :ttl-sec)
        *trie? (curs :trie?)
        do-put
        (fn []
          (go
            (let [k @*key
                  params {:key k
                          :value @*value
                          :ttl (* @*ttl-sec 1000)
                          :trie? @*trie?
                          :trace? true}
                  res (<! (c/request :put simulator
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
                  :on-change (c/setter *key)
                  :placeholder "Key"}))
     (ant/form-item
      (ant/button {:shape :circle
                   :icon :sync
                   :on-click shuffle-key}))
     (ant/form-item
      (ant/input {:value (react *value)
                  :on-change (c/setter *value)
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

(rum/defc data-info
  < rum/reactive
  < (c/init-arg-atom first #js [])
  [*local {:keys [address storage trie p-contacts *contacts]}]
  (ant/collapse
   {:active-key (react *local)
    :on-change (partial reset! *local)}
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
             :let [exp (-> k expiration c/format-ts)]]
         [[:.record {:key k} (<< "~{k} (~{exp}): ~{v}")]])]))
   (ant/collapse-panel
    {:header "Trie"}
    [:div
     {:key "trie"}
     (for [[k n] (sort trie)]
       [[:div {:key k} (<< "~{k}: ~{n}")]])])))

