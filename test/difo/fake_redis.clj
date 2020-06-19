(ns difo.fake-redis
  (:require [clojure.test :refer :all]
            [difo.redis :as redis]
            [difo.helpers :refer [curr-func]]
            [clojure.string :as str]))

(def ^:dynamic *cleanup* true)

(def heartbeats (atom #{}))
(def current-units (atom {}))
(def failed-units (atom 0))
(def processed-units (atom 0))
(def queues (atom {}))
(def calls (atom (vector)))
(def published-raft (atom []))
(def processes (atom []))

(def noop? (atom false))

(defmacro prepare-redis-fn [& body]
  `(let [fn-data# (curr-func)]
     (swap! calls conj (:fn-name fn-data#))
     (when-not @noop? ~@body)))

(defn dequeue [from]
  (prepare-redis-fn
    (loop [current (first from)
           others (rest from)]
      (if-let [item (when-let [queue (@queues current)]
                      (when-let [item (first queue)]
                        (swap! queues assoc current (rest queue))
                        item))]
        item
        (if-not (empty? others)
          (recur (first others)
                 (rest others)))))))

(defn enqueue [queue data]
  (prepare-redis-fn
    (if-let [current-queue (@queues queue)]
      (swap! queues assoc queue (conj current-queue data))
      (swap! queues assoc queue (vector data)))))

(defn register-heartbeat [worker]
  (prepare-redis-fn (swap! heartbeats conj (:id worker))))

(defn record-processed-unit [_worker]
  (prepare-redis-fn (swap! processed-units inc)))

(defn record-failed-unit [_worker]
  (prepare-redis-fn (swap! failed-units inc)))

(defn notify-current-unit [worker unit]
  (prepare-redis-fn (swap! current-units assoc (:id worker) (or unit :nil))))

(defn list-workers []
  (prepare-redis-fn))

(defn remove-worker []
  (prepare-redis-fn))

(defn get-worker [id]
  (prepare-redis-fn))

(defn enumerate-workers-keys []
  (prepare-redis-fn))

(defn enumerate-difo-processes []
  (prepare-redis-fn
    @processes))

(defn remove-key [& keys]
  (prepare-redis-fn))

(defn last-housekeeping-timestamp []
  (prepare-redis-fn))

(defn new-raft-listener []
  (prepare-redis-fn))

(defn stop-raft-listener [listener]
  (prepare-redis-fn))

(defn publish-raft [message]
  (prepare-redis-fn
    (swap! published-raft conj message)))

(defn register-process [node]
  (prepare-redis-fn))

(defn unregister-process [node]
  (prepare-redis-fn))

(def overrides {#'redis/dequeue                     dequeue
                #'redis/register-heartbeat          register-heartbeat
                #'redis/record-processed-unit       record-processed-unit
                #'redis/record-failed-unit          record-failed-unit
                #'redis/notify-current-unit         notify-current-unit
                #'redis/list-workers                list-workers
                #'redis/remove-worker               remove-worker
                #'redis/get-worker                  get-worker
                #'redis/enumerate-workers-keys      enumerate-workers-keys
                #'redis/enumerate-difo-processes    enumerate-difo-processes
                #'redis/remove-key                  remove-key
                #'redis/last-housekeeping-timestamp last-housekeeping-timestamp
                #'redis/new-raft-listener           new-raft-listener
                #'redis/stop-raft-listener          stop-raft-listener
                #'redis/publish-raft                publish-raft
                #'redis/register-process            register-process
                #'redis/unregister-process          unregister-process
                #'redis/enqueue                     enqueue})

(defn cleanup! []
  (reset! heartbeats #{})
  (reset! current-units {})
  (reset! processed-units 0)
  (reset! failed-units 0)
  (reset! queues {})
  (reset! calls (vector))
  (reset! published-raft [])
  (reset! processes []))

(defmacro without-cleanup [& body]
  `(binding [*cleanup* false]
     ~@body))

(defmacro with-fake-redis [& body]
  `(with-redefs-fn overrides
     #(try
        ~@body
        (finally (when *cleanup* (cleanup!))))))

(defmacro with-noop-redis [& body]
  `(with-fake-redis
     (let [last-noop# @noop?
           result# (atom nil)]
       (reset! noop? true)
       (try
         (reset! result# (do ~@body))
         (finally (reset! noop? last-noop#)))
       @result#)))

(defmacro called? [func]
  `(let [m# (meta (var ~func))
         n# (str (:name m#))]
     (->> @calls
          (some #(= n# %))
          (some?))))

(defn set-fake-processes! [val]
  (reset! processes val))

(defn items-enqueued-in [queue]
  (or (@queues queue)
      []))
