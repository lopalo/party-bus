(ns party-bus.peer.core)

(defrecord Curator [executor peers exception-logger])

(defrecord PeerContainer [sock-stream
                          period-streams
                          deferreds
                          handler
                          state
                          interface])

(defrecord Period [id])
(defrecord ControlCommand [cmd args])
(defrecord Init [])
(defrecord Terminate [])

(def terminated ::terminated)

(def terminated-error
  (ex-info "Peer is terminated" {:reason terminated}))
