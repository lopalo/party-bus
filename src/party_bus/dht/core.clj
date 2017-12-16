(ns party-bus.dht.core)

(defrecord Curator [executor peers exception-logger])

(defrecord PeerContainer [sock-stream
                          period-streams ;atom
                          deferreds ;atom
                          handler
                          state ;atom
                          interface])

(defrecord Period [id])
(defrecord ControlCommand [cmd args])
(defrecord Init [])
(defrecord Terminate [])

(def terminated ::terminated)

(def terminated-error
  (ex-info "Peer is terminated" {:reason terminated}))
