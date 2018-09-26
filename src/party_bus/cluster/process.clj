(ns party-bus.cluster.process
  (:require [clojure.set :refer [intersection difference]]
            [medley.core :refer [deref-reset!]]
            [manifold
             [executor :as ex]
             [deferred :as md]
             [time :as mt]]
            [party-bus.core :refer [next-string]]
            [party-bus.cluster
             [core :as cc]
             [transport :as t]])
  (:import [party_bus.cluster.core
            Node
            Groups
            ProcessId
            Mailbox
            ProcessContainer]))

(def ^:private default-options
  {:soft-mailbox-size 100
   :hard-mailbox-size 1000})

(defprotocol ProcessInterface
  (get-pid [this])
  (send-to [this pid header body])
  (multicast [this pids header body])
  (receive
    [this]
    [this timeout])
  (receive-with-header
    [this header-prefix]
    [this header-prefix timeout])
  (add-to-groups [this groups])
  (delete-from-groups
    [this]
    [this groups])
  (get-group-members [this group])
  (watch-group [this group])
  (unwatch-group [this group])
  (get-self-groups [this])
  (get-all-groups [this])
  (spawn
    [this f]
    [this f options])
  (terminate [this])
  (kill [this pid]))

(defn- receive* [executor mailbox prefix timeout]
  (let [delivery (volatile! nil)]
    (swap! mailbox
           (fn [^Mailbox mb]
             (vreset! delivery nil) ;;reset the previous attempt
             (if mb
               (let [messages (.messages mb)
                     msg-num (if (empty? prefix)
                               (ffirst messages)
                               (-> (.header-index mb)
                                   (subseq >= [prefix 0]
                                           < [(next-string prefix) 0])
                                   first
                                   second))]
                 (assert (nil? (.receiving mb)) "Concurrent receiving")
                 (if msg-num
                   (let [msg (messages msg-num)]
                     (vreset! delivery msg)
                     (-> mb
                         (update :messages dissoc msg-num)
                         (update :header-index disj [(first msg) msg-num])))
                   (let [deferred (md/deferred executor)]
                     (vreset! delivery deferred)
                     (assoc mb :receiving [prefix deferred]))))
               (throw cc/terminated-error))))
    (let [d @delivery]
      (when (and timeout (md/deferred? d))
        (mt/in timeout
               (fn []
                 (let [canceled? (volatile! false)]
                   (swap! mailbox
                          (fn [^Mailbox mb]
                            (vreset! canceled? false) ;;reset the previous attempt
                            (when mb
                              (if (= (second (.receiving mb)) d)
                                (do
                                  (vreset! canceled? true)
                                  (assoc mb :receiving nil))
                                mb))))
                   (when @canceled?
                     (md/success! d :timeout))))))
      d)))

(defn- try-uncork [^Node node ^ProcessId pid ^ProcessContainer proc]
  (when (and (seq @(.corked-nodes proc))
             (not (cc/mailbox-full?* proc)))
    (doseq [endpoint (deref-reset! (.corked-nodes proc) #{})]
      (t/send-to (.transport node)
                 endpoint
                 {:type :uncork
                  :process-number (.number pid)}))))

(declare spawn-process)

(defn process-interface
  [^Node node ^ProcessId pid ^ProcessContainer proc]
  (let [mailbox (.mailbox proc)
        terminated?! #(when-not @mailbox (throw cc/terminated-error))]
    (reify ProcessInterface
      (get-pid [this]
        (terminated?!)
        pid)

      (send-to [this pid' header body]
        (assert (string? header) header)
        (terminated?!)
        (let [number' (.number ^ProcessId pid')
              endpoint' (.endpoint ^ProcessId pid')]
          (if (= (.endpoint pid) endpoint')
            (if (cc/mailbox-full? node number')
              :corked
              (cc/mailbox-put node number' header body pid))
            (if (-> node .corks deref (contains? pid'))
              :corked
              (t/try-send-to (.transport node)
                             endpoint'
                             {:type :letter
                              :sender-number (.number pid)
                              :receiver-numbers (list number')
                              :header header
                              :body body})))))

      (multicast [this pids header body]
        (assert (string? header) header)
        (terminated?!)
        (let [corks @(.corks node)
              pids (set pids)
              result (zipmap (intersection corks pids) (repeat :corked))
              grouped-pids (group-by #(.endpoint ^ProcessId %)
                                     (difference pids corks))
              local-pids (grouped-pids (.endpoint pid))
              grouped-pids (dissoc grouped-pids (.endpoint pid))]
          (as-> result $
            (reduce
             (fn [res ^ProcessId pid']
               (assoc res pid'
                      (if (cc/mailbox-full? node (.number pid'))
                        :corked
                        (cc/mailbox-put
                         node (.number pid') header body pid))))
             $
             local-pids)
            (reduce
             (fn [res [endpoint' node-pids]]
               (let [receiver-numbers (map #(.number ^ProcessId %) node-pids)
                     status (t/try-send-to (.transport node)
                                           endpoint'
                                           {:type :letter
                                            :sender-number (.number pid)
                                            :receiver-numbers receiver-numbers
                                            :header header
                                            :body body})]
                 (merge res (zipmap node-pids (repeat status)))))
             $
             grouped-pids))))

      (receive [this]
        (receive this nil))
      (receive [this timeout]
        (let [x (receive* (.executor node) mailbox "" timeout)]
          (try-uncork node pid proc)
          x))

      (receive-with-header [this prefix]
        (receive-with-header this prefix nil))
      (receive-with-header [this prefix timeout]
        (let [x (receive* (.executor node) mailbox prefix timeout)]
          (try-uncork node pid proc)
          x))

      (add-to-groups [this groups]
        (terminated?!)
        (let [groups (cc/add-member node pid groups)]
          (when (seq groups)
            (t/broadcast (.transport node)
                         {:type :add-to-groups
                          :process-number (.number pid)
                          :groups (vec groups)}))
          groups))

      (delete-from-groups [this]
        (terminated?!)
        (let [groups (cc/delete-member node pid)]
          (when (seq groups)
            (t/broadcast (.transport node)
                         {:type :delete-from-all-groups
                          :process-number (.number pid)}))
          groups))
      (delete-from-groups [this groups]
        (terminated?!)
        (let [groups (cc/delete-member node pid groups)]
          (when (seq groups)
            (t/broadcast (.transport node)
                         {:type :delete-from-groups
                          :process-number (.number pid)
                          :groups (vec groups)}))
          groups))

      (get-group-members [this group]
        (terminated?!)
        ((.group->members ^Groups @(.groups node)) group))

      (watch-group [this group]
        (let [watched-groups (swap! (.watched-groups proc)
                                    #(when % (conj % group)))]
          (if (contains? watched-groups group)
            (let [gs (cc/add-watcher node (.number pid) (list group))]
              ((.group->members gs) group))
            (throw cc/terminated-error))))

      (unwatch-group [this group]
        (let [watched-groups (swap! (.watched-groups proc)
                                    #(when % (disj % group)))]
          (if watched-groups
            (let [gs (cc/delete-watcher node (.number pid) (list group))]
              ((.group->members gs) group))
            (throw cc/terminated-error))))

      (get-self-groups [this]
        (terminated?!)
        (get (cc/member->groups node) pid))

      (get-all-groups [this]
        (terminated?!)
        (set (keys (.group->members ^Groups @(.groups node)))))

      (spawn [this f]
        (spawn this f nil))
      (spawn [this f options]
        (terminated?!)
        (spawn-process node f options)
        true)

      (terminate [this]
        (throw cc/terminated-error))

      (kill [this pid']
        (terminated?!)
        (if (= (.endpoint pid) (.endpoint ^ProcessId pid'))
          (cc/kill node (.number ^ProcessId pid'))
          (t/send-to (.transport node)
                     (.endpoint ^ProcessId pid')
                     {:type :kill
                      :process-number (.number ^ProcessId pid')}))
        true))))

(defn spawn-process
  ([node f]
   (spawn-process node f nil))
  ([^Node node f process-options]
   (let [{:keys [options executor transport process-number processes]} node
         options (merge default-options options process-options)
         number (swap! process-number inc)
         pid (ProcessId. (t/endpoint transport) number)
         mailbox (Mailbox. 0 (sorted-map) (sorted-set) nil)
         proc (ProcessContainer. options (atom mailbox) (atom #{}) (atom #{}))]
     (when (swap! processes assoc number proc)
       (ex/with-executor executor
         (md/finally'
          (md/catch'
           (md/chain (md/future-with executor
                                     (f (process-interface node pid proc))))
           #(when-not (= (-> % ex-data :reason) cc/terminated)
              ((:exception-logger options) %)))
          (fn []
            (swap! processes dissoc number)
            (reset! (.mailbox proc) nil)
            (let [groups (cc/delete-member node pid)
                  watched-groups (deref-reset! (.watched-groups proc) nil)
                  corked-nodes (deref-reset! (.corked-nodes proc) nil)]
              (when (seq groups)
                (t/broadcast transport {:type :delete-from-all-groups
                                        :process-number number}))
              (when (seq watched-groups)
                (cc/delete-watcher node number watched-groups))
              (doseq [endpoint corked-nodes]
                (t/send-to transport
                           endpoint
                           {:type :uncork
                            :process-number number}))))))))))

(defn sleep [p interval]
  (receive-with-header p "*sleep*" interval))
