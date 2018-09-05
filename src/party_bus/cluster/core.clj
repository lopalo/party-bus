(ns party-bus.cluster.core)

(defrecord Node [executor transport])

(defrecord EndpointConnected [endpoint])

(defrecord EndpointDisconnected [endpoint])

(defrecord Received [endpoint msg])

(defrecord Process' [node proc-id])

(def default-stream-buffer-size 100)

