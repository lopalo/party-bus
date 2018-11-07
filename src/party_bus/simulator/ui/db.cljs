(ns party-bus.simulator.ui.db
  (:require [cljs.core.async :refer [<!]]
            [rum.core :as rum :refer [react cursor]]
            [antizer.rum :as ant]
            [party-bus.simulator.ui.core :as c]
            [party-bus.simulator.ui.cluster
             :refer [node->str str->node pid->str str->pid]])
  (:require-macros [clojure.core.strint :refer [<<]]
                   [cljs.core.async.macros :refer [go]]))

(rum/defcs management
  < rum/reactive
  < (c/store [] ::nodes)
  < (c/init-arg-atom
     first
     {:selected-node nil})
  [state *local {:keys [simulator]}]
  (let [*nodes (::nodes state)
        nodes (react *nodes)
        *selected-node (cursor *local :selected-node)
        selected-node (react *selected-node)
        spawn
        #(let [[ip port] (-> selected-node str->node)]
           (c/request :post simulator
                      (<< "/db/spawn/~{ip}/~{port}")))]
    (ant/card
     {:title "Management"}
     [:div
      {:key "content"}
      (c/ws-listener
       {:value "nodes"
        :on-value-change #(reset! *nodes nil)
        :connect
        #(c/connect-ws simulator (<< "/cluster/members/nodes"))
        :on-message #(reset! *nodes %)})
      (when (seq nodes)
        [:div
         (ant/select
          {:style {:width 200}
           :value selected-node
           :on-change (partial reset! *selected-node)}
          (for [node (sort nodes)
                :let [n (node->str node)]]
            (ant/select-option {:value n} n)))
         " "
         (ant/button {:type :primary
                      :disabled (nil? selected-node)
                      :on-click spawn}
                     "Spawn controller")])])))

(rum/defcs request-form
  < rum/reactive
  [state workers *state on-submit]
  (let [form (ant/get-form state)
        form-style {:label-col {:span 8}
                    :wrapper-col {:span 16}}
        form-item (c/form-item-maker {:form form
                                      :form-style form-style
                                      :*state *state})
        commands ["get" "get-keys" "set" "del" "inc" "swap"]]
    (ant/form
     {:layout :horizontal}
     (form-item
      "worker" "Worker" {:rules [{:required true}]}
      (ant/select
       (for [worker (sort workers)
             :let [w (pid->str worker)]]
         (ant/select-option {:value w} w))))
     (form-item
      "command" "Command" {}
      (ant/select
       (for [command commands
             :let [c (name command)]]
         (ant/select-option {:value c} c))))
     (case (ant/get-field-value form "command")
       "get"
       (form-item
        "key" "Key" {:rules [{:required true
                              :whitespace true}]}
        (ant/input))
       "set"
       (list
        (form-item
         "key" "Key" {:rules [{:required true
                               :whitespace true}]}
         (ant/input))
        (form-item
         "value" "Value" {:rules [{:required true
                                   :whitespace true}]}
         (ant/input)))
       "del"
       (form-item
        "key" "Key" {:rules [{:required true
                              :whitespace true}]}
        (ant/input))
       "inc"
       (list
        (form-item
         "key" "Key" {:rules [{:required true
                               :whitespace true}]}
         (ant/input))
        (form-item
         "value" "Value" {:rules [{:required true
                                   :type :integer}]}
         (ant/input-number)))
       "get-keys"
       (list
        (form-item
         "start-key" "Start key" {:rules [{:required true
                                           :whitespace true}]}
         (ant/input))
        (form-item
         "end-key" "End key" {:rules [{:required true
                                       :whitespace true}]}
         (ant/input)))
       "swap"
       (list
        (form-item
         "key" "Key" {:rules [{:required true
                               :whitespace true}]}
         (ant/input))
        (form-item
         "key'" "Key'" {:rules [{:required true
                                 :whitespace true}]}
         (ant/input))))

     (when-not (:hide-buttons? state)
       (ant/form-item
        {:wrapper-col {:span 16 :offset 8}}
        (ant/button
         {:type :primary
          :on-click #(ant/validate-fields form on-submit)}
         "Submit"))))))

(rum/defcs request
  < rum/reactive
  < (c/store {} ::workers)
  < (c/init-arg-atom
     first
     {"command" "set"})
  [state *local {:keys [simulator *response]}]
  (let [*workers (::workers state)
        workers (react *workers)
        on-submit
        (fn [errors values]
          (when (nil? errors)
            (go
              (let [values (js->clj values :keywordize-keys true)
                    {:keys [command worker]} values
                    [ip port number] (str->pid worker)
                    url (<< "/db/command/~{ip}/~{port}/~{number}/~{command}")
                    params (dissoc values :command :worker)
                    params (if (= command "get-keys")
                             (-> params
                                 (dissoc :start-key :end-key)
                                 (assoc :range [:>= (:start-key params)
                                                :< (:end-key params)]))

                             params)
                    response (<! (c/request :post simulator url
                                            :edn-params params))]
                (reset! *response {:worker worker
                                   :command command
                                   :parameters params
                                   :response (:body response)
                                   :ts (js/Date.now)})))))]
    (ant/card
     {:title "Request"}
     [:div
      {:key "content"}
      (c/ws-listener
       {:value "workers"
        :on-value-change #(reset! *workers nil)
        :connect
        #(c/connect-ws simulator (<< "/db/workers"))
        :on-message #(when (not= @*workers %)
                       (reset! *workers %))})
      (when (seq workers)
        (c/create-form {:form request-form
                        :*state *local
                        :args [workers *local on-submit]}))])))

(rum/defcs response
  < rum/reactive
  [state *local {:keys [*response]}]
  (when-let [{:keys [worker command parameters response ts]} (react *response)]
    (ant/card
     {:title "Response"}
     [:div
      {:key "content"}
      [:div "Worker: " worker]
      [:div "Command: " command]
      [:div "Parameters: " (str parameters)]
      [:div "Response: " (str response)]
      [:div "Timestamp: " (c/format-ts ts)]])))

(rum/defcs db
  < rum/reactive
  < (c/store nil ::response)
  < (c/init-arg-atom
     first
     {:management nil
      :request nil})
  [state *local {:keys [simulators]}]
  (when-first [sim simulators]
    (let [*response (::response state)]
      (ant/row
       {:class :cluster
        :gutter 8
        :justify :center}
       (ant/col
        {:span 8}
        (management (cursor *local :management)
                    {:simulator sim}))
       (ant/col
        {:span 8}
        (request (cursor *local :request)
                 {:simulator sim
                  :*response *response}))
       (ant/col
        {:span 8}
        (response (cursor *local :response)
                  {:simulator sim
                   :*response *response}))))))

