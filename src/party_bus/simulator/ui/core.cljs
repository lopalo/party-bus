(ns party-bus.simulator.ui.core
  (:require [cljs.core.async :refer [chan <!]]
            [cljs-http.client :as http]
            [chord.client :refer [ws-ch]])
  (:require-macros [cljs.core.async.macros :refer [go]]))

(goog-define INITIAL-ADDRESS "")

(defn request [method address path & {:as opts}]
  (http/request (merge {:method method
                        :url (str "http://" address path)
                        :with-credentials? false}
                       opts)))

(defn connect-ws [address path]
  (go
    (let [{:keys [ws-channel]} (<! (ws-ch (str "ws://" address path)
                                          {:format :edn}))]
      ws-channel)))

