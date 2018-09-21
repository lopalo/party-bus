(ns party-bus.cluster.process
  (:require [medley.core :refer [deref-reset!]]
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

(defprotocol ProcessInterface
  (get-pid [this])
  (send-to [this pid header body])
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
  (spawn [this f])
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
        (if (= (.endpoint pid) (.endpoint ^ProcessId pid'))
          (cc/mailbox-put node (.number ^ProcessId pid') header body pid)
          (t/send-to (.transport node)
                     (.endpoint ^ProcessId pid')
                     {:type :letter
                      :sender-number (.number pid)
                      :receiver-number (.number ^ProcessId pid')
                      :header header
                      :body body}))
        true) ;; was sent
      (receive [this]
        (receive this nil))
      (receive [this timeout]
        (receive* (.executor node) mailbox "" timeout))
      (receive-with-header [this prefix]
        (receive-with-header this prefix nil))
      (receive-with-header [this prefix timeout]
        (receive* (.executor node) mailbox prefix timeout))
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
            (let [^Groups
                  gs (cc/add-watcher node (.number pid) (list group))]
              ((.group->members gs) group))
            (throw cc/terminated-error))))
      (unwatch-group [this group]
        (let [watched-groups (swap! (.watched-groups proc)
                                    #(when % (disj % group)))]
          (if watched-groups
            (let [^Groups
                  gs (cc/delete-watcher node (.number pid) (list group))]
              ((.group->members gs) group))
            (throw cc/terminated-error))))
      (get-self-groups [this]
        (terminated?!)
        (get (cc/member->groups node) pid))
      (get-all-groups [this]
        (terminated?!)
        (set (keys (.group->members ^Groups @(.groups node)))))
      (spawn [this f]
        (terminated?!)
        (spawn-process node f))
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

(defn spawn-process [^Node node f]
  (let [{:keys [options executor transport process-number processes]} node
        number (swap! process-number inc)
        pid (ProcessId. (t/endpoint transport) number)
        mailbox (Mailbox. 0 (sorted-map) (sorted-set) nil)
        proc (ProcessContainer. (atom mailbox) (atom #{}))]
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
                 watched-groups (deref-reset! (.watched-groups proc) nil)]
             (when (seq groups)
               (t/broadcast transport {:type :delete-from-all-groups
                                       :process-number number}))
             (when (seq watched-groups)
               (cc/delete-watcher node number watched-groups)))))))))

(defn sleep [p interval]
  (receive-with-header p "*sleep*" interval))
