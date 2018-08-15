(ns party-bus.simulator.ui.dht.graph
  (:require [clojure.set :refer [difference]]
            [rum.core :as rum :refer [react]]
            [party-bus.simulator.ui.core :refer [hash-]])
  (:require-macros [clojure.core.strint :refer [<<]]))

(def max-hash (as-> "f" $ (repeat 40 $) (apply str $) (js/parseInt $ 16)))

(defn- position [center radius address]
  (let [h (-> address hash- (js/parseInt 16))
        [cx cy] center
        t (- (* (/ h max-hash) 2 Math/PI) (/ Math/PI 2))]
    [(+ cx (* radius (Math/cos t)))
     (+ cy (* radius (Math/sin t)))]))

(rum/defc graph
  < rum/reactive
  [{:keys [peers *selected-peer *contacts]}]
  (let [width 840
        height 800
        radius 350
        center [(/ width 2) (/ height 2)]
        [cx cy] center
        selected-peer (react *selected-peer)
        contacts (react *contacts)
        contact-peers (->> contacts (apply concat) set)
        selected-peers (when selected-peer #{selected-peer})
        peer
        (fn [address color]
          (let [h (hash- address)
                [x y] (position center radius address)]
            [:circle.peer
             {:key h
              :cx x
              :cy y
              :r 10
              :fill color
              :stroke :white
              :stroke-width 1
              :on-click #(reset! *selected-peer address)}]))]
    [:svg.graph
     {:width width
      :height height}
     [:circle.peer
      {:cx cx
       :cy (- cy radius 27)
       :r 15
       :fill "#92c7ec"
       :on-click (fn []
                   (reset! *selected-peer nil)
                   (reset! *contacts []))}]
     (concat
      (for [[p p'] contacts
            :let [[x y] (position center radius p)
                  [x' y'] (position center radius p')]]
        [:path {:key (str (hash- p) "-" (hash- p'))
                :d (<< "M~{x} ~{y} Q ~{cx} ~{cy} ~{x'} ~{y'}")
                :stroke "#108ee9"
                :stroke-width 1
                :fill :transparent}])
      (map #(peer % "#108ee9") (difference peers contact-peers selected-peers))
      (map #(peer % "#87d068") (difference contact-peers selected-peers))
      (map #(peer % "#ffbf00") selected-peers))]))
