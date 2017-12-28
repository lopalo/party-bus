(ns party-bus.simulator.core
  (:require [clojure.walk :refer [prewalk]]
            [clojure.edn :as edn]
            [ring.util.response :refer [response content-type]]
            [ring.util.request :refer [body-string]]
            [manifold.stream :as ms]
            [aleph.http :as http]
            [party-bus.utils :as u])
  (:import [java.net InetSocketAddress]))

(defn translate-addresses [form]
  (prewalk #(if (instance? InetSocketAddress %) (u/host-port %) %) form))

(defn edn-response [x]
  (-> x
      translate-addresses
      pr-str
      response
      (content-type "application/edn")))

(defn edn-body [req]
  (-> req
      body-string
      edn/read-string))

(defn connect-ws
  ([stream request]
   (connect-ws stream request identity))
  ([stream request f]
   (let [ws @(http/websocket-connection request)]
     (ms/on-closed ws #(ms/close! stream))
     (ms/connect (ms/map (comp pr-str translate-addresses f) stream) ws))))
