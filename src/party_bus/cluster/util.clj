(ns party-bus.cluster.util
  (:require [clojure.string :refer [split]]
            [manifold.deferred :as md]
            [party-bus.core :as c :refer [flow => let> if>]]
            [party-bus.cluster.process :as p]))

(defn sleep [p interval]
  (p/receive-with-header p "*sleep*" interval))

(defn sleep-recur [p interval & args]
  (md/chain' (sleep p interval)
             (fn [_] (apply md/recur args))))

(def default-call-timeout 20000)

(defn msg-type [msg]
  (-> msg first (split #":") first keyword))

(defn- call-headers [p header]
  (let [number (p/increment p)]
    [(format "%s:%020d" header number)
     (format "response:%020d" number)]))

(defn drop-responses [p]
  (md/loop []
    (flow
      (=> (p/receive-with-header p "response:" 0) response)
      (if> (not= response :timeout))
      (md/recur))))

(defn response [p req-msg resp-body]
  (let [[header _ pid] req-msg
        resp-header (->> (split header #":") last (str "response:"))]
    (p/send-to p pid resp-header resp-body)))

(defn call
  ([p pid header body]
   (call p pid header body default-call-timeout))
  ([p pid header body timeout]
   (flow
     (let> [[req-header resp-header] (call-headers p header)])
     (p/send-to p pid req-header body)
     (=> (p/receive-with-header p resp-header timeout) resp)
     (if (= resp :timeout)
       resp
       (second resp)))))

(defn multicall
  ([p pids header body]
   (multicall p pids header body default-call-timeout))
  ([p pids header body timeout]
   (let [[req-header resp-header] (call-headers p header)]
     (p/multicast p pids req-header body)
     (md/loop [result {}
               ts (c/now-ms)
               to timeout]
       (flow
         (=> (p/receive-with-header p resp-header to) resp)
         (if (= resp :timeout)
           result
           (let [ts' (c/now-ms)
                 to' (- to (- ts' ts))
                 [_ resp-body sender-pid] resp
                 result' (assoc result sender-pid resp-body)]
             (if (= (count result') (count pids))
               result'
               (md/recur result' ts' to')))))))))

(defn receive-loop [handler p params initial-state]
  (md/loop [state initial-state]
    (md/chain' (p/receive p)
               (fn [msg]
                 (md/recur (handler p params state msg))))))

