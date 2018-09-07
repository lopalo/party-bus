(ns party-bus.cluster.transport
  (:require [manifold
             [executor :as ex]
             [deferred :as md]
             [stream :as ms]]
            [gloss.io :refer [encode decode-stream]]
            [aleph.tcp :refer [start-server client]]
            [party-bus.utils :refer [flow => let> when>] :as u]
            [party-bus.cluster.core
             :refer [->EndpointConnected
                     ->EndpointDisconnected
                     ->Received
                     default-stream-buffer-size]])
  (:import [java.io Closeable]
           [io.netty.channel.epoll EpollChannelOption]
           [io.netty.bootstrap Bootstrap ServerBootstrap]))

(declare server-bootstrap  client-bootstrap spawn-pinger)

(defprotocol Transport
  (endpoint [this])
  (events [this])
  (connect-to [this remote-endpoint])
  (send-to [this remote-endpoint msg])
  (destroy [this]))

(defn create-transport
  [executor codec {:keys [host port tcp ping-period]
                   :or {ping-period 1000}}]
  (let [opts {:epoll? true
              :raw-stream? false}
        endpoint (u/socket-address host port)
        connections (atom {})
        swap-connection! (partial swap! connections update)
        del-connection!
        (fn [remote-endpoint connection]
          (swap! connections
                 #(if (= (% remote-endpoint) connection)
                    (dissoc % remote-endpoint)
                    %)))
        events (ms/stream default-stream-buffer-size)
        events-source (ms/source-only events)
        transport (atom nil)
        wrap-connection
        (fn [connection]
          (ex/with-executor executor
            (let [out (ms/stream default-stream-buffer-size)]
              (ms/connect (ms/map (partial encode codec) out) connection)
              (ms/splice out (decode-stream connection codec)))))
        init-connection
        (fn [remote-endpoint connection]
          (flow
            (=> (ms/put! events (->EndpointConnected remote-endpoint)))
            (ms/on-closed
             connection
             (fn []
               (del-connection! remote-endpoint connection)
               (ms/put! events (->EndpointDisconnected remote-endpoint))))
            (ms/connect-via
             connection
             (fn [msg]
               (if (= (:type msg) :ping)
                 (do
                   (when-let [tr @transport]
                     (doseq [ep (:other-endpoints msg)]
                       (connect-to tr ep)))
                   (md/success-deferred true))
                 (ms/put! events (->Received remote-endpoint msg))))
             events
             {:downstream? false
              :upstream? true})
            (spawn-pinger ping-period remote-endpoint connection connections)
            (when-not @transport
              (ms/close! connection))
            true))
        server
        (start-server
         (fn [connection _]
           (flow
             (let> [connection (wrap-connection connection)])
             (=> (ms/take! connection) {remote-endpoint :endpoint})
             (let> [superior? (pos? (compare (str remote-endpoint)
                                             (str endpoint)))
                    cs (swap-connection!
                        remote-endpoint
                        #(if (or (not %)
                                ;is a sentinel
                                 (and (not (ms/stream? %)) superior?))
                           connection
                           %))
                    accepted? (= (cs remote-endpoint) connection)])
             (when-not accepted?
               (ms/close! connection))
             (when> accepted?)
             (=> (ms/put! connection {:type :ping :other-endpoints []}))
             (init-connection remote-endpoint connection)))

         (assoc opts
                :socket-address endpoint
                :bootstrap-transform (partial server-bootstrap tcp)))]
    (reset!
     transport
     (reify Transport
       (endpoint [this] endpoint)

       (events [this] events-source)

       (connect-to [this remote-endpoint]
         (assert (not= endpoint remote-endpoint) remote-endpoint)
         (when @transport
           (let [opts (assoc opts
                             :remote-address remote-endpoint
                             :bootstrap-transform (partial client-bootstrap
                                                           tcp))
                 sentinel (Object.)
                 cs (swap-connection! remote-endpoint
                                      #(if-not % sentinel %))
                 watched? (= (cs remote-endpoint) sentinel)]
             (md/finally'
              (flow
                (when> watched?)
                (=> (md/catch' (client opts)
                               (constantly nil))
                    connection)
                (when> connection)
                (let> [connection (wrap-connection connection)])
                (=> (ms/put! connection {:type :init-connection
                                         :endpoint endpoint}))
                (=> (ms/take! connection) response)
                (when> response)
                (let> [cs (swap-connection!
                           remote-endpoint
                           #(if (= % sentinel) connection %))
                       accepted? (= (cs remote-endpoint) connection)])
                (if accepted?
                  (init-connection remote-endpoint connection)
                  (ms/close! connection)))
              #(del-connection! remote-endpoint sentinel)))))

       (send-to [this remote-endpoint msg]
         (some-> @connections (get remote-endpoint) (ms/put! msg)))

       (destroy [this]
         (reset! transport nil)
         (.close ^Closeable server)
         (run! #(when (ms/stream? %) (ms/close! %)) (vals @connections))
         (ms/close! events))))))

(defn- server-bootstrap
  [{:keys [user-timeout no-delay]
    :or {user-timeout 0
         no-delay false}}
   bootstrap]
  (doto ^ServerBootstrap bootstrap
    (.childOption EpollChannelOption/TCP_USER_TIMEOUT (int user-timeout))
    (.childOption EpollChannelOption/TCP_NODELAY no-delay)))

(defn- client-bootstrap
  [{:keys [user-timeout no-delay]
    :or {user-timeout 0
         no-delay false}}
   bootstrap]
  (doto ^Bootstrap bootstrap
    (.option EpollChannelOption/TCP_USER_TIMEOUT (int user-timeout))
    (.option EpollChannelOption/TCP_NODELAY no-delay)))

(defn- spawn-pinger [period remote-endpoint connection connections]
  (md/loop []
    (when-not (ms/closed? connection)
      (flow
        (=> (md/timeout! (md/deferred) period nil))
        (let> [cs (dissoc @connections remote-endpoint)
               endpoints (some->> cs keys shuffle (take 3))])
        (=> (ms/put! connection {:type :ping
                                 :other-endpoints (or endpoints [])}))
        (md/recur)))))

(comment
  (require '[party-bus.cluster.codec :as codec])
  (def ex (ex/fixed-thread-executor 4 {}))
  (do
    (def t1 (create-transport ex codec/message {:host "127.0.0.1" :port 9001}))
    (def t2 (create-transport ex codec/message {:host "127.0.0.1" :port 9002}))
    (def t3 (create-transport ex codec/message {:host "127.0.0.1" :port 9003}))
    (ms/consume (partial println "T1") (events t1))
    (ms/consume (partial println "T2") (events t2))
    (ms/consume (partial println "T3") (events t3)))
  (do
    (connect-to t1 (u/socket-address "127.0.0.1" 9002))
    (connect-to t2 (u/socket-address "127.0.0.1" 9001))
    (connect-to t3 (u/socket-address "127.0.0.1" 9002)))
  (do
    (send-to t2 (u/socket-address "127.0.0.1" 9001)
             {:type :letter
              :sender 666
              :receiver 777
              :body "Foo Bar Baz"}))
  (do
    (destroy t1)
    (destroy t2)
    (destroy t3)))
