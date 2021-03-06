(ns party-bus.main
  (:require [clojure.java.io :as io]
            [clojure.string :refer [join]]
            [clojure.tools.cli :refer [parse-opts]]
            [party-bus.simulator.server :as sim])
  (:gen-class))

(defn- collect [m k v]
  (update m k conj v))

(def option-specs
  [["-h" "--help"]
   ["-C" "--config" "Configuration EDN file"
    :required "PATH"
    :validate [#(.exists (io/as-file %)) "File doesn't exist"]]
   ["-l" "--listen-address" "Simulator's listening address"
    :required "HOST:PORT"]
   ["-c" "--connect-address" "Addresses of all simulators"
    :id :connect-addresses
    :required "HOST:PORT"
    :default #{}
    :assoc-fn collect]
   ["-d" "--dht-ip" "IPs of DHT peers"
    :id :dht-ips
    :required "IP"
    :default #{}
    :assoc-fn collect]
   ["-N" "--local-node-address" "Addresses of local nodes"
    :id :local-node-addresses
    :required "HOST:PORT"
    :default #{}
    :assoc-fn collect]
   ["-n" "--remote-node-address" "Addresses of remote nodes"
    :id :remote-node-addresses
    :required "HOST:PORT"
    :default #{}
    :assoc-fn collect]])

(defn -main
  [& args]
  (let [{:keys [options errors summary]} (parse-opts args option-specs)]
    (cond
      errors
      (do
        (println (join \newline errors))
        (System/exit 1))
      (:help options)
      (do
        (println summary)
        (System/exit 1)))
    (if (:listen-address options)
      (sim/start-server options)
      (println
       "'listen-address' is not specified, so simulator is not started"))))
