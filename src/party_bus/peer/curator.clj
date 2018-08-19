(ns party-bus.peer.curator
  (:require [medley.core :refer [deref-reset!]]
            [manifold
             [executor :refer [fixed-thread-executor]]
             [deferred :as md]
             [stream :as ms]]
            [aleph.udp :refer [socket]]
            [party-bus.utils :refer [flow => let> socket-address]]
            [party-bus.peer.core :refer [terminated terminated-error]]
            [party-bus.peer.interface :refer [peer-interface]])
  (:import [io.netty.channel.epoll EpollDatagramChannel]
           [io.aleph.dirigiste Executor]
           [party_bus.peer.core
            Curator
            PeerContainer
            ControlCommand
            Init
            Terminate]))

(declare terminate-peer*)

(defn create-curator [num-threads executor-options exception-logger]
  (Curator. (fixed-thread-executor num-threads executor-options)
            (atom {})
            exception-logger))

(defn create-peer [^Curator curator host port handler initial-state]
  (flow
    (let> [{:keys [executor peers exception-logger]} curator
           opts {:epoll? true
                 :broadcast? false
                 :raw-stream? false
                 :socket-address (socket-address host port)}])
    ;if a port is 0, OS allocates one from the ephemeral port range
    (=> (socket opts) sock-stream)
    (let> [address (-> sock-stream
                       meta
                       ^EpollDatagramChannel (:aleph/channel)
                       .localAddress)
           period-streams (atom {})
           deferreds (atom #{})
           state (atom initial-state)
           ex-handler (fn [e consume-ex?]
                        (when-not (= (-> e ex-data :reason) terminated)
                          (exception-logger e)
                          (ms/close! sock-stream))
                        (when-not consume-ex?
                          (md/error-deferred e executor)))
           handler (fn h
                     ([x]
                      (h x true))
                     ([x consume-ex?]
                      (when-some [^PeerContainer peer (@peers address)]
                        (try
                          (md/catch'
                           (md/chain' (handler (.interface peer) x))
                           #(ex-handler % consume-ex?))
                          (catch Throwable e
                            (ex-handler e consume-ex?))))))
           peer (PeerContainer. sock-stream
                                period-streams
                                deferreds
                                handler
                                state
                                nil)
           peer-if (peer-interface curator address peer)
           peer (assoc peer :interface peer-if)])
    (swap! peers assoc address peer)
    (ms/on-closed sock-stream #(terminate-peer* curator address))
    (md/chain'
     (md/future-with executor (handler (Init.)))
     (fn [_] (ms/consume handler (ms/onto executor sock-stream))))
    address))

(defn- terminate-peer* [^Curator curator address]
  (let [peers (.peers curator)]
    (when-some [{:keys [period-streams deferreds handler state]}
                (@peers address)]
      (md/chain'
       (md/future-with (.executor curator) (handler (Terminate.)))
       (fn [_]
         (swap! peers dissoc address)
         (doseq [ps (-> period-streams (deref-reset! nil) vals)]
           (ms/close! ps))
         (doseq [d (deref-reset! deferreds nil)]
           (md/error! d terminated-error))
         (reset! state terminated))))))

(defn- get-peer ^PeerContainer [^Curator curator address]
  (some-> curator .peers deref (get address)))

(defn control-command [curator address cmd args]
  (some-> (get-peer curator address)
          .handler
          (apply [(ControlCommand. cmd args) false])))

(defn terminate-peer [curator address]
  (some-> (get-peer curator address)
          .sock-stream
          ms/close!))

(defn terminate-all-peers [^Curator curator]
  (run! (partial terminate-peer curator)
        (-> curator .peers deref keys)))

(defn shutdown [^Curator curator now?]
  (let [^Executor executor (.executor curator)]
    (if now?
      (.shutdownNow executor)
      (.shutdown executor))))

(defn listen-to-addresses [^Curator curator]
  (let [s (ms/stream 1 nil (.executor curator))
        peers (.peers curator)]
    (ms/put! s [nil (-> peers deref keys set)])
    (add-watch
     peers s
     (fn [_ _ old-peers new-peers]
       (if new-peers
         (ms/put! s [(-> old-peers keys set) (-> new-peers keys set)])
         (ms/close! s))))
    (ms/on-closed s #(remove-watch peers s))
    s))

(defn listen-to-peer [^Curator curator address]
  (let [s (ms/stream 1 nil (.executor curator))]
    (if-some [peer-state (some-> (get-peer curator address) .state)]
      (do
        (ms/put! s [nil @peer-state])
        (add-watch
         peer-state s
         (fn [_ _ old-st new-st]
           (if (not= new-st terminated)
             (ms/put! s [old-st new-st])
             (ms/close! s))))
        (ms/on-closed s #(remove-watch peer-state s)))
      (ms/close! s))
    s))
