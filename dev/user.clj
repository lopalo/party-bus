(ns user
  (:require clojure.stacktrace
            [clojure.java.io :as io]
            [manifold
             [deferred :as md]
             [stream :as ms]]
            [aleph.udp :as udp]
            [gloss.io :refer [encode decode]]
            [rum.derived-atom :refer [derived-atom]]
            [figwheel-sidecar.repl-api :as fig]
            [party-bus.utils :refer [let< socket-address load-edn]]
            [party-bus.peer
             [curator :as c]
             [interface :as p]]
            [party-bus.dht
              [peer :as peer]
              [codec :as codec]]
            [party-bus.simulator.server :as sim])
  (:import [party_bus.peer.core Period Init Terminate]))

(declare curator config simulator)

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
           (let< [t1 (md/timeout! (p/create-deferred p) 3000 :timeout-1)
                  _ (p/cancel-period p :foo)
                  t2 (md/timeout! (p/create-deferred p) 3000 :timeout-2)
                  _ (prn "Complete init" t1 t2
                         (p/update-state p (constantly :completed)))
                  _ (md/timeout! (p/create-deferred p) 2000 :timeout-3)]
             (p/terminate p)))
         Period (prn "Period:" (:id msg))
         Terminate (prn "Terminate")))
     :initial)
    #(.getPort %)))

(defn create-dht-peer [& args]
  (apply peer/create-peer curator (derived-atom [config] :dht :dht) args))

(defn load-config []
  (-> "config.edn" io/resource load-edn))

(defn fig-start []
  (fig/start-figwheel!))

(defn fig-stop []
  (fig/stop-figwheel!))

(defn cljs-repl []
  (fig/cljs-repl))

(defn start-simulator []
  (sim/start-server {:config (io/resource "config.edn")
                     :listen-address "127.0.0.1:12080"
                     :connect-addresses #{"127.0.0.1:12080"}
                     :dht-ips #{"127.0.0.2" "127.0.0.3" "127.0.0.4"}}))

(defonce curator (c/create-curator 2 nil prn))
(defonce config (atom (load-config)))
(defonce simulator (start-simulator))

(defn config-reload []
  (reset! config (load-config)))

(defn sim-restart []
  (.close simulator)
  (Thread/sleep 1000)
  (alter-var-root #'simulator (constantly (start-simulator))))

(comment
  (do
    (def echo-p (create-echo-peer 0))
    (def s (udp-socket 47555))
    (udp-send s echo-p {:type :ping :request-id 777})
    (udp-send s echo-p {:type :pong
                        :request-id 888})
    (c/terminate-peer curator (socket-address echo-p))
    (Thread/sleep 200)
    (udp-send s echo-p {:type :ping :request-id 99999}))
  (do
    (def timer-p (create-timer-peer 0))
    (c/terminate-peer curator (socket-address timer-p)))
  (do
    (def p1 @(create-dht-peer "127.0.0.1" 0 []))
    (def p2 @(create-dht-peer "127.0.0.1" 0 [p1]))
    (def p3 @(create-dht-peer "127.0.0.1" 0 [p2]))
    (def p4 @(create-dht-peer "127.0.0.1" 0 [p1]))
    (deref (c/control-command curator p4 :put {:key "abc"
                                               :value "THE VALUE!!!"
                                               :ttl 10000
                                               :trace? true
                                               :trie? true}))
    (prn (deref (c/control-command curator p3 :get {:key "abc"
                                                    :trace? true})))
    (prn (deref (c/control-command curator p3 :get-trie {:prefix "a"
                                                         :trace? true}))))
  (c/terminate-all-peers curator)
  (sim-restart))
