(ns party-bus.simulator.ui.dht
  (:require [clojure.set :refer [union difference]]
            [clojure.string :refer [join]]
            [cljs.core.async :as async :refer [<!]]
            [medley.core :refer [remove-vals]]
            [rum.core :as rum :refer [react]]
            [party-bus.simulator.ui.core :refer [request connect-ws]])
  (:require-macros [clojure.core.strint :refer [<<]]
                   [cljs.core.async.macros :refer [go go-loop]]))

(def min-val -2147483648)
(def max-val 2147483647)
(def space-size (- max-val min-val))

(defn- hash- [x]
;TODO: SHA-1 to avoid collisions
  (hash (if (and (vector? x) (= (count x) 2))
          (hash (join ":" x))
          x)))

(defn- position [center radius address]
  (let [h (hash- address)
        [cx cy] center
        t (+ (* (/ h space-size) 2 Math/PI) (/ Math/PI 2))]
    [(+ cx (* radius (Math/cos t)))
     (+ cy (* radius (Math/sin t)))]))

(rum/defc graph
  < rum/reactive
  [peers selected-peer contacts]
  (let [width 840
        height 800
        radius 350
        center [(/ width 2) (/ height 2)]
        [cx cy] center
        selected-peer' (react selected-peer)
        contact-peers (->> contacts (apply concat) set)
        selected-peers (if selected-peer' #{selected-peer'})
        peer
        (fn [address color]
          (let [h (hash- address)
                [x y] (position center radius address)]
            [:g
             {:key h}
             [:circle.peer
              {:cx x
               :cy y
               :r 10
               :stroke :black
               :stroke-width 1
               :fill color
               :on-click #(reset! selected-peer address)}]]))]
    [:svg.graph
     {:width width
      :height height}
     (concat
      (for [[p p'] contacts
            :let [[x y] (position center radius p)
                  [x' y'] (position center radius p')]]
        [:path {:key (str (hash- p) "-" (hash- p'))
                :d (<< "M~{x} ~{y} Q ~{cx} ~{cy} ~{x'} ~{y'}")
                :stroke :black
                :stroke-width 1
                :fill :transparent}])
      (map #(peer % :lightgrey) (difference peers contact-peers selected-peers))
      (map #(peer % :lightgreen) (difference contact-peers selected-peers))
      (map #(peer % :orange) selected-peers))]))

(def peer-ui-state
  {:address nil
   :ws-c nil
   :peer-state nil
   :last-request nil})

(rum/defcs peer
  < rum/reactive
  < (rum/local peer-ui-state ::local)
  < {:after-render
     (fn [state]
       (let [[selected-peer contacts ips] (:rum/args state)
             [ip port :as selected-peer'] @selected-peer
             local (::local state)
             {:keys [address ws-c]} @local
             active? #(= selected-peer' (:address @local))]
         (when (not= selected-peer' address)
           (when ws-c (async/close! ws-c))
           (reset! local (assoc peer-ui-state :address selected-peer'))
           (go
             (let [ws-c (<! (connect-ws (ips ip)
                                        (<< "/dht/peer/~{ip}/~{port}")))]
               (when (active?)
                 (swap! local assoc :ws-c ws-c)
                 (loop [{peer-state :message} (<! ws-c)]
                   (when (and peer-state (active?))
                     (let [p-contacts (-> peer-state :contacts keys set)]
                       (if-not (:peer-state @local)
                         (reset! contacts
                                 (for [c p-contacts] [selected-peer' c]))
                         (swap! contacts
                                (partial remove
                                         (fn [[p p']]
                                           (and (= p selected-peer')
                                                (not (p-contacts p'))))))))
                     (swap! local assoc :peer-state peer-state)
                     (recur (<! ws-c)))))
               (async/close! ws-c)))))

       state)
     :will-unmount
     (fn [state]
       (some-> state ::local deref :ws-c async/close!))}
  [state _ contacts ips]
  (let [local (::local state)
        {:keys [address peer-state]} @local
        [ip port] address
        peer-contacts (-> peer-state :contacts keys)
        input-val #(.-value (rum/ref state %))
        show-route #(reset! contacts
                            (partition 2 1 (-> @local
                                               :last-request
                                               :route
                                               (conj address))))
        set-last-request
        (fn [k method response]
          (when (= address (:address @local))
            (swap! local assoc :last-request
                   (assoc response :key k :method method))
            (show-route)))
        do-put
        (fn []
          (go
            (let [ttl (-> "put-ttl" input-val js/parseInt)
                  k (input-val "put-key")
                  params {:key k
                          :value (input-val "put-value")
                          :ttl (if (js/isNaN ttl) 0 ttl)
                          :trace? true}
                  res (<! (request :post (ips ip)
                                   (<< "/dht/put/~{ip}/~{port}")
                                   :edn-params params))]
              (set-last-request k :put (:body res)))))

        do-get
        (fn []
          (go
            (let [k (input-val "get-key")
                  params {:key k
                          :trace? true}
                  res (<! (request :post (ips ip)
                                   (<< "/dht/get/~{ip}/~{port}")
                                   :edn-params params))]
              (set-last-request k :get (:body res)))))]
    [:.peer
     (if address
       [:div
        [:strong ip ":" port]
        [:div "Hash: " (hash- address)]
        [:button
         {:on-click #(request :delete (ips ip)
                              (<< "/dht/peer/~{ip}/~{port}"))}
         "Terminate"]])
     (if-let [{:keys [storage request-count]} peer-state]
       [:div
        [:div "Request count: " request-count]
        [:div
         [:input {:ref "put-key" :placeholder "Key"}]
         [:input {:ref "put-value" :placeholder "Value"}]
         [:input {:ref "put-ttl" :default-value 10000 :placeholder "TTL"}]
         [:button {:on-click do-put} "Put"]]
        [:div
         [:input {:ref "get-key" :placeholder "Key"}]
         [:button {:on-click do-get} "Get"]]
        (if-let [{:keys [key method route value ttl]} (:last-request @local)]
          [:div
           [:div "Last request"]
           [:ul
            [:li "method: " (name method)]
            [:li "key: " key]
            [:li "key hash: " (hash- key)]
            [:li "hops: " (count route)]
            (if value [:li "value: " value])
            (if ttl [:li "ttl " ttl])]])
        [:div "Data"
         (let [{:keys [data expiration]} storage]
           [:ul (for [[k v] data
                      :let [exp (-> k expiration js/Date.
                                    (.toLocaleString "en-GB"))]]
                  [[:li {:key k} (<< "~{k} (~{exp}): ~{v}")]])])]
        [:div.contact
         {:on-click
          #(reset! contacts (for [c peer-contacts] [address c]))}
         "Contacts"]
        [:ul.contact
         (for [[ip port :as c] (sort peer-contacts)]
           [:li
            {:key c
             :on-click #(reset! contacts [[address c]])}
            ip ":" port])]])]))

(rum/defcs dht
  < (rum/local {} ::ips)
  < (rum/local {} ::curator-ws)
  < (rum/local #{} ::peers)
  < (rum/local nil ::selected-peer)
  < (rum/local [] ::selected-contacts)
  < (let [simulators (comp first :rum/args)
          sync-simulators
          (fn [state old-sims sims]
            (let [del-sims (difference old-sims sims)
                  channels (map @(::curator-ws state) del-sims)]
              (swap! (::ips state) #(remove-vals del-sims %))
              (swap! (::curator-ws state) #(apply dissoc % del-sims))
              (run! async/close! channels))
            (go
              (let [add-sims (difference sims old-sims)
                    res (<! (async/map
                             #(map :body %&)
                             (map #(request :get % "/dht/ip-addresses")
                                  add-sims)))
                    add-ips (for [[sim ips] (map vector add-sims res)
                                  ip ips]
                              [ip sim])
                    channels (<! (async/map
                                  vector
                                  (map #(connect-ws % "/dht/peer-addresses")
                                       add-sims)))]
                (swap! (::ips state) #(apply conj % add-ips))
                (swap! (::curator-ws state)
                       #(apply conj % (map vector add-sims channels)))
                (doseq [ws-c channels]
                  (go-loop [{[header peers] :message} (<! ws-c)]
                    (when header
                      (swap! (::peers state)
                             (case header
                               :initial union
                               :add union
                               :delete difference)
                             peers)
                      (recur (<! ws-c)))))))
            state)]
      {:did-mount
       (fn [state]
         (sync-simulators state #{} (simulators state)))
       :did-remount
       (fn [old-state state]
         (sync-simulators state (simulators old-state) (simulators state)))
       :will-unmount
       (fn [state]
         (sync-simulators state (simulators state) #{}))})
  [state simulators]
  (let [ips @(::ips state)
        ips' (-> ips keys vec)
        peers @(::peers state)
        selected-peer (::selected-peer state)
        contacts (::selected-contacts state)
        op-repeat #(->> % (rum/ref state) .-value js/parseInt range)
        create-peers
        (fn []
          (let [peers' (vec peers)]
            (doseq [_ (op-repeat "create-count")
                    :let [ip (rand-nth ips')
                          p-contacts
                          (if (seq peers')
                            (map #(join ":" %)
                                 (repeatedly 3 #(rand-nth peers')))
                            [])]]
              (request :post (ips ip)
                       (str "/dht/peer/" ip)
                       :edn-params p-contacts))))
        terminate-peers
        (fn []
          (let [peers' (vec peers)]
            (doseq [_ (op-repeat "terminate-count")
                    :let [[ip port] (rand-nth peers')]]
              (request :delete (ips ip) (<< "/dht/peer/~{ip}/~{port}")))))]
    [:div
     [:h3 "DHT"]
     [:div (for [[sim ips] (group-by second @(::ips state))]
             [:div {:key sim} sim ": " (join ", " (map first ips))])]
     [:.dht
      [:.peers
       [:div.horizontal
        [:div "Peers: " (count peers)]
        [:div
         [:input {:ref "create-count" :default-value 3}]
         [:button {:on-click create-peers} "Create peers"]]
        (if (seq peers)
          [:div
           [:input {:ref "terminate-count" :default-value 3}]
           [:button {:on-click terminate-peers} "Terminate peers"]])]
       (graph peers selected-peer @contacts)]
      (peer selected-peer contacts ips)]]))
