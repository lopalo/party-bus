(ns party-bus.simulator.core
  (:require [clojure.walk :refer [prewalk]]
            [clojure.string :refer [blank?]]
            [clojure.edn :as edn]
            [ring.util
             [response :refer [response content-type]]
             [request :refer [body-string]]]
            [compojure.response :refer [Renderable]]
            manifold.deferred
            [manifold.stream :as ms]
            [aleph.http :as http]
            [party-bus.core :as c])
  (:import [java.net InetSocketAddress]
           [manifold.deferred IDeferred]))

(defn as-bool [s]
  (= s "true"))

(extend-protocol Renderable
  IDeferred
  (render [d _] d))

(defmulti edn-transform type)

(defmethod edn-transform :default [x] x)

(defmethod edn-transform InetSocketAddress [a]
  (c/host-port a))

(defn- edn-transformation [form]
  (prewalk edn-transform form))

(defn edn-response [x]
  (-> x
      edn-transformation
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
     (ms/connect (->> stream
                      (ms/map f)
                      (ms/filter some?)
                      (ms/map (comp pr-str edn-transformation)))
                 ws))))

(def not-blank? (complement blank?))
