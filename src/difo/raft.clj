(ns difo.raft
  (:require [clojure.core.async :as async]
            [difo.redis :as redis])
  (:import (clojure.lang IAtom)))

;; So, this says "Raft" but actually is something "Raft-like". The problem we
;; are trying to solve here is: Which instance will be the "leader?" The Leader
;; is responsible for running housekeeping and maintaining stats. Those two
;; operations are not mission-critical, but provides better understanding on
;; what is happening in a Difo cluster. In the future, a leader can be
;; responsible for more operations.
;;
;; Knowing those information about how a Leader acts, we can then define
;; timeouts and behaviours:
;; Heartbeat: Each 15 seconds.
;; Heartbeat timeout: 2 * Heartbeat + rand seconds where `rand` varies between 0 and 10.
;; After a Heartbeat timeout, a node turns into a candidate, unless it has
;; detect an election in course.
;; Election timeout: 2 seconds.
;; New leader is the one that has the most votes. Leaders knows the quantity of
;; nodes into a node through the difo:processes:* pattern.
;;
;; Heartbeat message:
;; { :type :heartbeat, :from "luci.local@4201", :term 0, :quorum 1 }
;; Election message:
;; { :type :election, :from "luci.local@4201", :term 1 }
;; Vote message:
;; { :type :vote, :from "luci.local@4201", :term 1, :vote "luci.local@4201" }
;;
;; During a heartbeat, if a leader finds another heartbeat from another leader,
;; and the heartbeat quorum is >= to its own quorum from the last term, it must
;; convert itself to a follower and wait for a new term.
;; Communication is done through the difo:raft pub-sub channel.

(defn- milliseconds [n duration]
  (case duration
    :hour (recur n :hours)
    :hours (recur (* n 60) :minutes)
    :minute (recur n :minutes)
    :minutes (recur (* n 60) :seconds)
    :second (recur n :seconds)
    :seconds (recur (* n 1000) :milliseconds)
    :millisecond (recur n :milliseconds)
    :milliseconds n))

(def ^{:private true :const true} heartbeat-interval
  (milliseconds 15 :seconds))
(def ^{:private true :const true} heartbeat-timeout
  (-> heartbeat-interval
      (* 2)
      (+ (milliseconds (rand-int 10) :seconds))))
(def ^{:private true :const true} election-timeout
  (milliseconds 2 :seconds))

(defmacro swap< [^IAtom atom & opts]
  `(let [curr-val# (deref ~atom)]
     (swap! ~atom ~@opts)
     curr-val#))

(defrecord Node [name status term running? pub-sub leader channels callbacks])

(defn new-node [name]
  (map->Node {:name           name
              :status         (atom :follower)
              :term           (atom {:current 0
                                     :votes   0
                                     :quorum  0})
              :voted?         (atom false)
              :running?       (atom true)
              :pub-sub        (atom nil)
              :last-heartbeat (atom 0)
              :leader         (atom nil)
              :channels       {:heartbeat         (async/chan)
                               :heartbeat-timeout (async/chan)
                               :election          (async/chan)
                               :election-timeout  (async/chan)
                               :vote              (async/chan)
                               :stop              (async/chan)}
              :callbacks      (atom {})}))

(defn- message [self type & values]
  (let [msg {:type type :from (:name self)}]
    (merge msg (apply hash-map values))))

(defn- announce! [message]
  (redis/publish-raft message))

(defn- valid-term? [self msg]
  (let [current-term (:current @(:term self))
        hb-term (:term msg)]
    (>= hb-term current-term)))

(defn- invoke-callback [self fn-name]
  (when-let [cb (get-in @(:callbacks self) [fn-name] nil)]
    (cb self)))

(defn- become-follower [self hb]
  (when (valid-term? self hb)
    (dosync
      (reset! (:last-heartbeat self) (System/currentTimeMillis))
      (swap! (:term self) assoc :current (:term hb))
      (reset! (:leader self) (:from hb))
      (reset! (:voted? self) false)
      (let [prev-status (swap< (:status self) (constantly :follower))]
        (when (= prev-status :leader)
          (invoke-callback self :resigned-leader))
        (when-not (= prev-status :follower)
          (println "[Raft] Became follower of" (:from hb)))))))

(defn- become-leader [self]
  (dosync
    (reset! (:last-heartbeat self) (System/currentTimeMillis))
    (reset! (:leader self) nil)
    (when (= :candidate (swap< (:status self) (constantly :leader)))
      (invoke-callback self :became-leader)
      (println "[Raft] Became leader"))
    (async/go-loop []
      (when (= :leader @(:status self))
        (let [{:keys [current votes]} @(:term self)]
          (announce! (message self :heartbeat :term current :quorum votes)))
        (async/<! (async/timeout heartbeat-interval))
        (recur)))))

(defn- assert-heartbeat [self hb]
  (let [term @(:term self)
        status @(:status self)]
    (when (and (>= (:term hb) (:current term))
               (not= :follower status))
      (println "[Raft] Stepping down due to hb:" hb
               (str "(Current [term:" (:current term) "][votes:" (:votes term) "])"))
      (become-follower self hb))))

(defn- on-heartbeat [self hb]
  (case @(:status self)
    :follower (become-follower self hb)
    :candidate (assert-heartbeat self hb)
    :leader (assert-heartbeat self hb)))

(defn- determine-quorum [self]
  (let [processes (redis/enumerate-difo-processes)]
    (-> (remove #{(str "difo:processes:" (:name self))} processes)
        (count)
        (/ 2))))

(defn- schedule-timeout [self kind time]
  (async/go (async/<! (async/timeout time))
            (async/>! (get-in self [:channels kind]) {})))

(defn- start-election [self]
  (dosync
    (reset! (:status self) :candidate)
    (swap! (:term self) update-in [:current] inc)
    (swap! (:term self) merge {:votes  1
                               :quorum (determine-quorum self)})
    (if (zero? (:quorum @(:term self)))
      (become-leader self)
      (do
        (announce! (message self :election :term (:current @(:term self))))
        (schedule-timeout self :election-timeout election-timeout)))))

(defn- on-heartbeat-timeout [self]
  (when (and (>= (- (System/currentTimeMillis) @(:last-heartbeat self))
                 heartbeat-timeout)
             (= :follower @(:status self)))
    (start-election self))
  (schedule-timeout self :heartbeat-timeout heartbeat-timeout))

(defn- on-election [self election]
  (when (and (valid-term? self election)
             (not @(:voted? self)))
    (dosync
      (reset! (:voted? self) true)
      (announce! (message self :vote :term (:term election) :vote (:from election))))))

(defn- on-election-timeout [self]
  (let [{:keys [votes quorum]} @(:term self)]
    (if (> votes quorum)
      (become-leader self)
      (reset! (:status self) :follower))))

(defn- on-receive-vote [self vote]
  (let [status @(:status self)
        name (:name self)]
    (when (and (= status :candidate)
               (= (:vote vote) name))
      (swap! (:term self) update-in [:votes] inc))))

(defn- from-self? [self]
  (fn [msg] (= (:from msg) (:name self))))

(defn- message-handler [node]
  (let [self? (from-self? node)]
    (fn [msg]
      (let [chan (:channels node)]
        (when-not (self? msg)
          (case (:type msg)
            :heartbeat (async/put! (:heartbeat chan) msg)
            :election (async/put! (:election chan) msg)
            :vote (async/put! (:vote chan) msg)))))))

(defn- schedule-register [node]
  (async/go-loop []
    (redis/register-process node)
    (async/<! (async/timeout (milliseconds 30 :seconds)))
    (when @(:running? node)
      (recur))))

(defn stop [node]
  (when-not (nil? node)
    (dosync
      (reset! (:running? node) false)
      (redis/unregister-process node)
      (redis/stop-raft-listener (:pub-sub node))
      (reset! (:pub-sub node) nil)
      (run! async/close! (vals (:channels node))))))

(defn run [node]
  (reset! (:pub-sub node) (redis/new-raft-listener (message-handler node)))
  (schedule-timeout node :heartbeat-timeout heartbeat-timeout)
  (schedule-register node)
  (let [{:keys [heartbeat heartbeat-timeout election election-timeout vote stop]} (:channels node)]
    (loop []
      (async/alt!!
        heartbeat ([hb] (on-heartbeat node hb))
        heartbeat-timeout ([_] (on-heartbeat-timeout node))
        election ([el] (on-election node el))
        election-timeout ([_] (on-election-timeout node))
        vote ([vote] (on-receive-vote node vote))
        stop :bye)
      (when @(:running? node)
        (recur)))))

(defn set-callback [node type fn]
  (swap! (:callbacks node) assoc type fn)
  node)

(defn unset-callback [node type]
  (swap! (:callbacks node) dissoc type)
  node)
