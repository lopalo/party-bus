(ns party-bus.simulator.ui.dht
  (:require [clojure.set :refer [union difference]]
            [clojure.string :refer [join]]
            [cljs.core.async :as async :refer [<!]]
            [medley.core :refer [remove-vals]]
            [rum.core :as rum]
            [party-bus.simulator.ui.core :refer [request connect-ws]])
  (:require-macros [clojure.core.strint :refer [<<]]
                   [cljs.core.async.macros :refer [go go-loop]]))

(def min-val -2147483648)
(def max-val 2147483647)
(def space-size (- max-val min-val))

(defn- hash- [address]
  (hash (join ":" address))) ;TODO: SHA-1 to avoid collisions

(defn- position [center radius address]
  (let [h (hash- address)
        [cx cy] center
        t (+ (* (/ h space-size) 2 Math/PI) (/ Math/PI 2))]
    [(+ cx (* radius (Math/cos t)))
     (+ cy (* radius (Math/sin t)))]))

(rum/defc graph [peers contacts]
  (let [width 840
        height 800
        radius 350
        center [(/ width 2) (/ height 2)]
        [cx cy] center]
    [:svg
     {:width width
      :height height
      :style {:border "1px solid black"}}
     (concat
      (for [[p p'] contacts
            :let [[x y] (position center radius p)
                  [x' y'] (position center radius p')]]
        [:path {:key (str (hash- p) "-" (hash- p'))
                :d (<< "M~{x} ~{y} Q ~{cx} ~{cy} ~{x'} ~{y'}")
                :stroke :black
                :stroke-width 1
                :fill :transparent}])
      (for [peer peers
            :let [h (hash- peer)
                  [x y] (position center radius peer)]]
        [:g
         {:key h}
         [:circle {:cx x
                   :cy y
                   :r 10
                   :stroke :black
                   :stroke-width 1
                   :fill :lightgrey}]]))]))

(rum/defcs dht
  < (rum/local {} ::ips)
  < (rum/local {} ::curator-ws)
  < (rum/local #{} ::peers)
  < (rum/local [] ::requests)
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
        ;TODO: show selected peer's contacts
        contacts (if (> (count peers) 1)
                   [[(first peers) (second peers)]]
                   [])
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
     [:div "Peers: " (count peers)]
     [:div
      [:button {:on-click create-peers} "Create peers"]
      [:input {:ref "create-count" :default-value 3}]]
     (if (seq peers)
       [:div
        [:button {:on-click terminate-peers} "Terminate peers"]
        [:input {:ref "terminate-count" :default-value 3}]])
     (graph peers contacts)]))
