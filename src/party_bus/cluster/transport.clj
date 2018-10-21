(ns party-bus.cluster.transport
  (:require [manifold
             [executor :as ex]
             [deferred :as md]
             [stream :as ms]]
            [gloss
             [core :as g]
             [io :refer [encode decode-stream]]]
            [aleph.tcp :refer [start-server client]]
            [party-bus.core :refer [flow => let> if>] :as c])
  (:import [java.io Closeable]
           [io.netty.channel.epoll EpollChannelOption]
           [io.netty.bootstrap Bootstrap ServerBootstrap]))

(declare server-bootstrap  client-bootstrap spawn-pinger)

(defrecord EndpointConnected [endpoint])

(defrecord EndpointDisconnected [endpoint])

(defrecord Received [endpoint msg])

(g/defcodec init-connection-c
  {:type :init-connection
   :endpoint c/address-c})

(g/defcodec ping-c
  {:type :ping
   :other-endpoints (g/repeated c/address-c
                                :prefix :ubyte)})

(defprotocol Transport
  (endpoint [this])
  (events [this])
  (connect-to [this remote-endpoint])
  (send-to [this remote-endpoint msg])
  (try-send-to [this remote-endpoint msg])
  (broadcast [this msg])
  (shutdown [this]))

(defn create
  [executor codec {:keys [host port tcp ping-period
                          events-buffer-size
                          connection-buffer-size]
                   :or {ping-period 1000
                        events-buffer-size 1000
                        connection-buffer-size 100}}]
  (let [opts {:epoll? true
              :raw-stream? false}
        endpoint (c/socket-address host port)
        connections (atom {})
        swap-connection! (partial swap! connections update)
        del-connection!
        (fn [remote-endpoint connection]
          (swap! connections
                 #(if (= (% remote-endpoint) connection)
                    (dissoc % remote-endpoint)
                    %)))
        events (ms/stream events-buffer-size)
        events-source (ms/source-only events)
        transport (atom nil)
        wrap-connection
        (fn [connection]
          (let [out (ms/stream connection-buffer-size)]
            (ms/connect (ms/map (partial encode codec) out) connection)
            (ms/splice out (decode-stream connection codec))))
        init-connection
        (fn [remote-endpoint connection]
          (flow
            (=> (ms/put! events (EndpointConnected. remote-endpoint)))
            (ms/on-closed
             connection
             (fn []
               (ms/put! events (EndpointDisconnected. remote-endpoint))
               (del-connection! remote-endpoint connection)))
            (ms/connect-via
             connection
             (fn [msg]
               (if (= (:type msg) :ping)
                 (do
                   (when-let [tr @transport]
                     (doseq [ep (:other-endpoints msg)]
                       (connect-to tr ep)))
                   (md/success-deferred true))
                 (ms/put! events (Received. remote-endpoint msg))))
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
           (ex/with-executor executor
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
               (if> accepted? :else (ms/close! connection))
               (=> (ms/put! connection {:type :ping :other-endpoints []}))
               (init-connection remote-endpoint connection))))
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
             (ex/with-executor executor
               (md/finally'
                (flow
                  (if> watched?)
                  (=> (md/catch' (client opts)
                                 (constantly nil))
                      connection)
                  (if> connection)
                  (let> [connection (wrap-connection connection)])
                  (=> (ms/put! connection {:type :init-connection
                                           :endpoint endpoint}))
                  (=> (ms/take! connection) response)
                  (if> response)
                  (let> [cs (swap-connection!
                             remote-endpoint
                             #(if (= % sentinel) connection %))
                         accepted? (= (cs remote-endpoint) connection)])
                  (if accepted?
                    (init-connection remote-endpoint connection)
                    (ms/close! connection)))
                #(del-connection! remote-endpoint sentinel))))))

       (send-to [this remote-endpoint msg]
         (some-> @connections (get remote-endpoint) (ms/put! msg)))

       (try-send-to [this remote-endpoint msg]
         (if (some-> @connections
                     (get remote-endpoint)
                     (ms/try-put! msg 0) deref)
           :sent
           :not-sent))

       (broadcast [this msg]
         (apply md/zip'
                (map #(when (ms/stream? %) (ms/put! % msg))
                     (vals @connections))))

       (shutdown [this]
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
    (def t1 (create ex codec/message {:host "127.0.0.1" :port 9001}))
    (def t2 (create ex codec/message {:host "127.0.0.1" :port 9002}))
    (def t3 (create ex codec/message {:host "127.0.0.1" :port 9003}))
    (ms/consume (partial println "T1") (events t1))
    (ms/consume (partial println "T2") (events t2))
    (ms/consume (partial println "T3") (events t3)))
  (do
    (connect-to t1 (c/socket-address "127.0.0.1" 9002))
    (connect-to t2 (c/socket-address "127.0.0.1" 9001))
    (connect-to t3 (c/socket-address "127.0.0.1" 9002)))
  (do
    (send-to t2 (c/socket-address "127.0.0.1" 9001)
             {:type :letter
              :sender-number 666
              :receiver-numbers [777]
              :header "The header!"
              :body {#{11 22} :aaa
                     #{22 :c} (repeat 10 [1 `F 3])}}))
  (do
    (shutdown t1)
    (shutdown t2)
    (shutdown t3)))
