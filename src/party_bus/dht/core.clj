(ns party-bus.dht.core)

(defrecord Curator [executor peers exception-logger])

(defrecord PeerContainer [sock-stream
                          period-streams ;atom
                          deferreds ;atom
                          handler
                          state ;atom
                          interface])

(defrecord Period [id])
(defrecord Init [])
(defrecord Terminate [])

