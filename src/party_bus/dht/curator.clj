(ns party-bus.dht.curator
  (:require [medley.core :refer [deref-reset!]]
            [manifold
             [executor :refer [fixed-thread-executor]]
             [deferred :as md :refer [let-flow]]
             [stream :as ms]]
            [aleph.udp :refer [socket]]
            party-bus.dht.core
            [party-bus.dht.peer-interface :refer [peer-interface]])
  (:import [java.net InetSocketAddress]
           [io.netty.channel.epoll EpollDatagramChannel]
           [party_bus.dht.core
            Curator
            PeerContainer
            Init
            Terminate]))

(set! *warn-on-reflection* true)

(declare terminate-peer*)

(defn create-curator [num-threads executor-options exception-logger]
  (Curator. (fixed-thread-executor num-threads executor-options)
            (atom {})
            exception-logger))

(defn socket-address
  ([^long port] (socket-address "127.0.0.1" port))
  ([^String host ^long port] (InetSocketAddress. host port)))

(defn host-port [^InetSocketAddress address]
  [(.getHostString address) (.getPort address)])

(defn create-peer [^Curator curator host port handler initial-state]
  (let-flow [{:keys [executor peers exception-logger]} curator
             opts {:epoll? true
                   :broadcast? false
                   :raw-stream? false
                   :socket-address (socket-address host port)}
             sock-stream (socket opts)
             ;if a port is 0, OS allocates one from the ephemeral port range
             address (-> sock-stream
                         meta
                         ^EpollDatagramChannel (:aleph/channel)
                         .localAddress)
             period-streams (atom {})
             deferreds (atom #{})
             state (atom initial-state)
             ex-handler (fn [e]
                          (when-not (= (-> e ex-data ::reason) ::terminated)
                            (exception-logger e)
                            (ms/close! sock-stream)))
             handler (fn [x]
                       (when-some [^PeerContainer peer (@peers address)]
                         (try
                           (md/catch
                            (md/chain (handler (.interface peer) x))
                            ex-handler)
                           (catch Throwable e
                             (ex-handler e)))))
             peer (PeerContainer. sock-stream
                                  period-streams
                                  deferreds
                                  handler
                                  state
                                  nil)
             peer-if (peer-interface curator address peer)
             peer (assoc peer :interface peer-if)]
            (swap! peers assoc address peer)
            (ms/on-closed sock-stream #(terminate-peer* curator address))
            (md/chain
             (md/future-with executor (handler (Init.)))
             (fn [_] (ms/consume handler (ms/onto executor sock-stream))))
            address))

(defn- terminate-peer* [^Curator curator address]
  (let [peers (.peers curator)]
    (when-some [{:keys [period-streams deferreds handler state]}
                (@peers address)]
      (md/chain
       (md/future-with (.executor curator) (handler (Terminate.)))
       (fn [_]
         (swap! peers dissoc address)
         (doseq [ps (-> period-streams (deref-reset! nil) vals)]
           (ms/close! ps))
         (doseq [d (deref-reset! deferreds nil)]
           (md/error! d ::terminated))
         (reset! state ::terminated))))))

(defn terminate-peer [^Curator curator address]
  (some-> curator
          .peers
          deref
          ^PeerContainer (get address)
          .sock-stream
          ms/close!))

(defn terminate-all-peers [^Curator curator]
  (run! (partial terminate-peer curator)
        (-> curator .peers deref keys)))
