(ns user
  (:require clojure.stacktrace
            [manifold
             [deferred :as md :refer [let-flow]]
             [stream :as ms]]
            [aleph.udp :as udp]
            [party-bus.dht
             [curator :as c]
             [peer-interface :as p]])
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
                     :message message})))

(def curator (c/create-curator 2 nil prn))

(defn create-echo-peer [port]
  @(md/catch
    (md/chain
     (c/create-peer
      curator "127.0.0.1" port
      #(prn (format "Peer %s: %s" (.getPort (p/get-address %1)) %2))
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
    (udp-send s echo-p "A")
    (udp-send s echo-p "B")
    (udp-send s echo-p "C")
    (c/terminate-peer curator (c/socket-address echo-p))
    (Thread/sleep 200)
    (udp-send echo-p ">!!!<"))
  (do
    (def timer-p (create-timer-peer 0))
    (c/terminate-peer curator (c/socket-address timer-p))))
