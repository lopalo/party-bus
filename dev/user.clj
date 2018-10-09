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
            [party-bus.core :refer [flow => let>] :as c]
            [party-bus.peer
             [curator :as cur]
             [interface :as pi]]
            [party-bus.dht
             [peer :as peer]
             [codec :as dht-codec]]
            [party-bus.cluster
             [node :as node]
             [process :as proc]
             [util :as cu]]
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
                     :message (encode dht-codec/message message)})))

(defn create-echo-peer [port]
  @(md/catch
    (md/chain
     (cur/create-peer
      curator "127.0.0.1" port
      (fn [p {:keys [sender message] :as input}]
        (let [port (.getPort (pi/get-address p))]
          (prn
           (if sender
             (format "Peer %s receives from %s: %s"
                     port
                     (.getPort sender)
                     (decode dht-codec/message message))
             (format "Peer %s: %s" port input)))))
      {})
     #(.getPort %))
    #(clojure.stacktrace/print-stack-trace %)))

(defn create-timer-peer [port]
  @(md/chain
    (cur/create-peer
     curator "127.0.0.1" port
     (fn [p msg]
       (condp instance? msg
         Init
         (do
           (pi/create-deferred p)
           (pi/create-period p :foo 2000)
           (pi/create-period p :bar 2000)
           (flow
            (=> (md/timeout! (pi/create-deferred p) 3000 :timeout-1) t1)
            (pi/cancel-period p :foo)
            (let> [txt "Complete init"])
            (=> (md/timeout! (pi/create-deferred p) 3000 :timeout-2) t2)
            (prn txt t1 t2 (pi/update-state p (constantly :completed)))
            (=> (md/timeout! (pi/create-deferred p) 2000 :timeout-3))
            (pi/terminate p)))
         Period (prn "Period:" (:id msg))
         Terminate (prn "Terminate")))
     :initial)
    #(.getPort %)))

(defn create-dht-peer [& args]
  (apply peer/create-peer curator (derived-atom [config] :dht :dht) args))

(defn load-config []
  (-> "config.edn" io/resource c/load-edn))

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
                     :dht-ips #{"127.0.0.2" "127.0.0.3" "127.0.0.4"}
                     :local-node-addresses #{"127.0.0.1:12100"
                                             "127.0.0.1:12101"
                                             "127.0.0.1:12102"
                                             "127.0.0.2:12100"
                                             "127.0.0.2:12101"}}))

(defonce curator (cur/create-curator 2 nil prn))
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
    (cur/terminate-peer curator (c/socket-address echo-p))
    (Thread/sleep 200)
    (udp-send s echo-p {:type :ping :request-id 99999}))
  (do
    (def timer-p (create-timer-peer 0))
    (cur/terminate-peer curator (c/socket-address timer-p)))
  (do
    (def p1 @(create-dht-peer "127.0.0.1" 0 []))
    (def p2 @(create-dht-peer "127.0.0.1" 0 [p1]))
    (def p3 @(create-dht-peer "127.0.0.1" 0 [p2]))
    (def p4 @(create-dht-peer "127.0.0.1" 0 [p1]))
    @(cur/control-command curator p4 :put {:key "abc"
                                           :value "THE VALUE!!!"
                                           :ttl 10000
                                           :trace? true
                                           :trie? true})
    @(cur/control-command curator p3 :get {:key "abc"
                                           :trace? true})
    @(cur/control-command curator p3 :get-trie {:prefix "a"
                                                :trace? true}))
  (cur/terminate-all-peers curator)
  (sim-restart))

(defn create-node [port]
  (node/create {:num-threads 8
                :executor {}
                :transport {:host "127.0.0.1"
                            :port port
                            :tcp {:user-timeout 0
                                  :no-delay false}
                            :ping-period 1000
                            :events-buffer-size 1000
                            :connection-buffer-size 100}
                :soft-mailbox-size 100
                :hard-mailbox-size 1000
                :exception-handler println}))

(defn say [p & parts]
  (let [pid (proc/get-pid p)
        addr (conj (c/host-port (:endpoint pid)) (:number pid))]
    (apply prn addr parts)))

(comment
  (prn *e)
  (do
    (def n1 (create-node 9201))
    (def n2 (create-node 9202)))
  (do
    (def proc1
      (proc/spawn-process
       n1
       (fn [p]
         (flow
          (let> [pid (proc/get-pid p)])
          (proc/send-to p pid "rrr" "11111")
          (=> (proc/receive-with-header p "r") msg)
          (say p "receive 1" msg)
          (say p "add to groups" (proc/add-to-groups p ["foo"]))
          (say p "watch 'foo'" (proc/watch-group p "foo"))
          (say p "watch 'bar'" (proc/watch-group p "bar"))
          (=> (cu/sleep p 2000))
          (say p "del from groups" (proc/delete-from-groups p ["bar"]))
          (say p "add to groups" (proc/add-to-groups p ["bar"]))
          (=> (proc/receive p 100) msg)
          (say p "receive 2" msg)
          (=> (proc/receive p 100) msg)
          (say p "receive 3" msg)
          (proc/spawn
           p
           (fn [p]
             (flow
              (say p "watch 'foo'" (proc/watch-group p "bar"))
              (proc/add-to-groups p ["bar"])
              (proc/send-to p pid "aaa" "1111")
              (=> (proc/receive-with-header p "bb" 3000) msg)
              (say p "receive 1" msg)
              (=> (proc/receive-with-header p "ggg" 100) msg)
              (say p "receive 2" msg)
              (=> (cu/sleep p 2000))
              (say p "SHOULD HAVE DIED")))
           {:soft-mailbox-size 2})
          (=> (proc/receive p 1000) msg)
          (say p "receive 4" msg)
          (=> (proc/receive p 1000) msg)
          (say p "receive 5" msg)
          (=> (proc/receive p 1000) msg)
          (say p "receive 6" msg)
          (=> (proc/receive p 1000) msg)
          (say p "receive 7" msg)
          (=> (proc/receive p 1000) msg)
          (say p "receive 8" msg)
          (say p "add to groups" (proc/add-to-groups p ["foo"]))
          (=> (proc/receive p 1000) msg)
          (say p "receive 9" msg)
          (=> (proc/receive p 1000) msg)
          (say p "receive 10" msg)
          (=> (proc/receive p 10000) msg)
          (say p "SHOULD HAVE DIED")))))
    (Thread/sleep 2000)
    (def proc2
      (proc/spawn-process
       n2
       (fn [p]
         (flow
          (say p "watch 'foo'" (proc/watch-group p "foo"))
          (=> (cu/sleep p 2000))
          (proc/add-to-groups p ["foo"])
          (say p "watch 'bar'" (proc/watch-group p "bar"))
          (=> (proc/receive p 1000) msg)
          (say p "receive 1" msg)
          (=> (proc/receive p 1000) msg)
          (say p "receive 2" msg)
          (let> [members (proc/get-group-members p "bar")])
          (say p "multicast"
               (proc/multicast p members "gggb" "1111"))
          (=> (cu/sleep p 2000))
          (proc/kill p (-> members sort second))
          (=> (proc/receive p 1000) msg)
          (say p "receive 3" msg)
          (=> (proc/receive p 6000) msg)
          (say p "receive 4" msg)))))

    (node/connect-to n1 "127.0.0.1" 9202)
    (Thread/sleep 5000)
    (node/shutdown n1))
  (do
    (node/shutdown n1)
    (node/shutdown n2)))
