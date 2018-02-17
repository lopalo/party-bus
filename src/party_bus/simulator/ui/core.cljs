(ns party-bus.simulator.ui.core
  (:require [cljs.core.async :as async :refer [chan <!]]
            [cljs-http.client :as http]
            [chord.client :refer [ws-ch]])
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

