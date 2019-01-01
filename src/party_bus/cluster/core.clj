(ns party-bus.cluster.core
  (:require [clojure.set :refer [union difference]]
            [clojure.string :refer [starts-with?]]
            [medley.core :refer [deref-swap! deref-reset!]]
            [manifold.deferred :as md]
            [party-bus.core :refer [set-conj disj-dissoc dissoc-empty]]))

(defrecord Groups [group->members member->groups group->watchers])

(defrecord Node [options
                 executor
                 transport
                 process-number
                 processes
                 groups
                 corks])
(declare pid->vec)

(defrecord ProcessId [endpoint number]
  Comparable
  (compareTo [this other]
    (compare (pid->vec this) (pid->vec other))))

(defn pid->vec [^ProcessId pid]
  [(str (.endpoint pid)) (.number pid)])

(defmethod print-method ProcessId [^ProcessId pid ^java.io.Writer w]
  (.write w (str "pid:/" (.endpoint pid) "|" (.number pid))))

(defrecord Mailbox [message-number messages header-index receiving])

(defrecord ProcessContainer [options
                             mailbox
                             watched-groups
                             corked-nodes
                             counter
                             bound-numbers])

(def terminated ::terminated)

(def terminated-error
  (ex-info "Process is terminated" {:reason terminated}))

(defn terminated-error? [e]
  (= (-> e ex-data :reason) terminated))

(defn min-pid [endpoint]
  (ProcessId. endpoint 0))

(defn max-pid [endpoint]
  (ProcessId. endpoint Long/MAX_VALUE))

(defn member->groups [^Node node]
  (.member->groups ^Groups @(.groups node)))

(declare kill*)

(defn mailbox-put [^Node node number header body sender-pid]
  {:pre [(string? header)]}
  (let [^ProcessContainer
        proc (get @(.processes node) number)
        delivery (volatile! nil)]
    (if proc
      (do
        (if (>= (-> proc .mailbox ^Mailbox deref .messages count)
                (-> proc .options :hard-mailbox-size))
          (kill* proc)
          (swap!
           (.mailbox proc)
           (fn [^Mailbox mb]
             (vreset! delivery nil) ;;reset the previous attempt
             (when mb
               (let [msg-num (.message-number mb)
                     [prefix deferred] (.receiving mb)]
                 (if (and deferred (starts-with? header prefix))
                   (do
                     (vreset! delivery [[header body sender-pid] deferred])
                     (assoc mb :receiving nil))
                   (-> mb
                       (update :message-number inc)
                       (assoc-in [:messages msg-num]
                                 [header body sender-pid])
                       (update :header-index conj [header msg-num]))))))))
        (when-let [[msg deferred] @delivery]
          (md/success! deferred msg))
        :sent)
      :not-sent)))

(defn kill* [^ProcessContainer proc]
  (some-> proc .mailbox ^Mailbox (deref-reset! nil)
          .receiving second
          (md/error! terminated-error)))

(defn kill [^Node node number]
  (kill* (get @(.processes node) number)))

(defn- notify-watchers [node pid added? groups ^Groups prev-gs]
  (let [group->watchers (.group->watchers prev-gs)
        prev-groups ((.member->groups prev-gs) pid)]
    (set
     (for [g groups
           :let [contained? (contains? prev-groups g)
                 changed? (if added? (not contained?) contained?)
                 header (str "group-change:" g)
                 body {:group g
                       :pid pid
                       (if added? :added? :deleted?) true}]
           :when changed?]
       (do
         (doseq [number (group->watchers g)]
           (mailbox-put node number header body nil))
         g)))))

(defn add-member [^Node node pid groups]
  (let [prev-gs
        (deref-swap!
         (.groups node)
         (fn [gs]
           (-> gs
               (update
                :group->members
                (fn [gms]
                  (reduce #(update %1 %2 set-conj pid) gms groups)))
               (update-in [:member->groups pid] union (set groups)))))]
    (notify-watchers node pid true groups prev-gs)))

(defn delete-member
  ([node pid]
   (delete-member node pid nil))
  ([^Node node pid groups]
   (let [prev-gs
         (deref-swap!
          (.groups node)
          (fn [^Groups gs]
            (let [groups (or groups ((.member->groups gs) pid))]
              (-> gs
                  (update
                   :group->members
                   (fn [gms]
                     (reduce #(disj-dissoc %1 %2 pid) gms groups)))
                  (update-in [:member->groups pid] difference (set groups))
                  (update :member->groups dissoc-empty pid)))))
         groups (or groups ((.member->groups ^Groups prev-gs) pid))]
     (notify-watchers node pid false groups prev-gs))))

(defn ^Groups add-watcher [^Node node number groups]
  (swap! (.groups node)
         update
         :group->watchers
         (fn [gws]
           (reduce #(update %1 %2 set-conj number) gws groups))))

(defn ^Groups delete-watcher [^Node node number groups]
  (swap! (.groups node)
         update
         :group->watchers
         (fn [gws]
           (reduce #(disj-dissoc %1 %2 number) gws groups))))

(defn mailbox-full?* [^ProcessContainer proc]
  (boolean
   (some-> proc .mailbox ^Mailbox deref .messages count
           (>= (-> proc .options :soft-mailbox-size)))))

(defn mailbox-full? [^Node node number]
  (if-let [proc (get @(.processes node) number)]
    (mailbox-full?* proc)
    false))

(defn modify-corked-nodes [^Node node number f]
  (some-> node .processes deref
          ^ProcessContainer (get number)
          .corked-nodes (swap! f)))
