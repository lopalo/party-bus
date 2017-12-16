(ns user
  (:require clojure.stacktrace
            [manifold
             [deferred :as md :refer [let-flow]]
             [stream :as ms]]
            [aleph.udp :as udp]
            [gloss.io :refer [encode decode]]
            [party-bus.utils :refer [socket-address]]
            [party-bus.dht
             [curator :as c]
             [peer-interface :as p]
             [peer :as peer]
             [codec :as codec]])
  (:import [party_bus.dht.core Period Init Terminate]))

(defn udp-socket [port]
  @(md/timeout!
    (udp/socket {:port port})
    1000))

(defn udp-send
  ([remote-port message]
   (udp-send @(udp/socket {}) remote-port message))
  ([socket remote-port message]
   @(ms/put! socket {:host "127.0.0.1"
                     :port remote-port
                     :message (encode codec/message message)})))

(def curator (c/create-curator 2 nil prn))

(defn create-echo-peer [port]
  @(md/catch
    (md/chain
     (c/create-peer
      curator "127.0.0.1" port
      (fn [p {:keys [sender message] :as input}]
        (let [port (.getPort (p/get-address p))]
          (prn
           (if sender
             (format "Peer %s receives from %s: %s"
                     port
                     (.getPort sender)
                     (decode codec/message message))
             (format "Peer %s: %s" port input)))))
      {})
     #(.getPort %))
    #(clojure.stacktrace/print-stack-trace %)))

(defn create-timer-peer [port]
  @(md/chain
    (c/create-peer
     curator "127.0.0.1" port
     (fn [p msg]
       (condp instance? msg
         Init
         (do
           (p/create-deferred p)
           (p/create-period p :foo 2000)
           (p/create-period p :bar 2000)
           (let-flow [t1 (md/timeout! (p/create-deferred p) 3000 :timeout-1)
                      cp (do t1 (p/cancel-period p :foo))
                      t2 (md/timeout! (p/create-deferred p) 3000 :timeout-2)]
                     (prn "Complete init" t1 t2 cp
                          (p/update-state p (constantly :completed)))
                     (md/chain
                      (md/timeout! (p/create-deferred p) 2000 :timeout-3)
                      (fn [_] (p/terminate p)))))
         Period (prn "Period:" (:id msg))
         Terminate (prn "Terminate")))
     :initial)
    #(.getPort %)))

(comment
  (do
    (def echo-p (create-echo-peer 0))
    (def s (udp-socket 47555))
    (udp-send s echo-p {:type :ping})
    (udp-send s echo-p {:type :pong
                        :contacts [(socket-address "88.1.111.2" 6666)
                                   (socket-address "88.2.211.2" 7777)]})
    (c/terminate-peer curator (socket-address echo-p))
    (Thread/sleep 200)
    (udp-send echo-p ">!!!<"))
  (do
    (def timer-p (create-timer-peer 0))
    (c/terminate-peer curator (socket-address timer-p)))
  (do
    (def p1 @(peer/create-peer curator "127.0.0.1" 0 []))
    (def p2 @(peer/create-peer curator "127.0.0.1" 0 [p1]))
    (def p3 @(peer/create-peer curator "127.0.0.1" 0 [p2]))
    (def p4 @(peer/create-peer curator "127.0.0.1" 0 [p1]))
    (c/control-command curator p4 :put {:key "abc" :value "THE VALUE!!!"})
    (c/control-command curator p3 :get {:key "abc"})))
