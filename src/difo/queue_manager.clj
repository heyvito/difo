(ns difo.queue-manager
  (:require [clojure.core.async :as async]
            [difo.worker :as worker]
            [difo.redis :as redis]
            [difo.unit :as unit]
            [difo.raft :as raft]
            [difo.id :as id]))

(defrecord QueueManager [name workers unit-chan queues stopped? stop-ch raft-node])

(defn create-manager
  "Creates a new manager, preparing it to start handling units.
  Takes the following parameters:
  workers   - A seq of Workers to manage
  unit-chan - A channel to receive any received unit
  queues    - A seq of queues to monitor"
  [workers unit-chan queues]
  (let [pid (id/process-id)]
    (map->QueueManager {:name      pid
                        :workers   workers
                        :unit-chan unit-chan
                        :queues    queues
                        :stopped?  (atom false)
                        :stop-ch   (async/chan)
                        :raft-node (raft/new-node pid)})))

;(defn- find-removable-keys [all-keys workers-alive]
;  (let [matcher (re-pattern #".*?:([^:@]+@[0-9]+@[^:]+).*")
;        selector (fn [k] (when-let [[_ worker] (re-matches matcher k)]
;                           (workers-alive worker)))]
;    (remove selector all-keys)))
;
;(defn- execute-housekeeping []
;  (dlock/with-lock :housekeeping
;    (let [known-workers (redis/list-workers)
;          all-keys (redis/enumerate-workers-keys)
;          now (System/currentTimeMillis)
;          workers-alive (->> known-workers
;                             (mapv vals)
;                             (filterv #(< (- now (last %)) 15000))
;                             (mapv first)
;                             (set))
;          removable-workers (set/difference (set (map :worker known-workers))
;                                            workers-alive)
;          removable-keys (find-removable-keys all-keys workers-alive)]
;      (doseq [worker removable-workers]
;        (redis/remove-worker worker))
;      (when (seq removable-keys)
;        (apply redis/remove-key removable-keys))
;      true)))
;
;(defn- cond-housekeeping []
;  (try
;    (dlock/with-lock :housekeeping
;      (let [last-hk (redis/last-housekeeping-timestamp)
;            now (-> (System/currentTimeMillis)
;                    (/ 1000)
;                    (/ 60))]
;        (when (<= housekeeping-timeout (- now last-hk))
;          (execute-housekeeping))))
;    (catch ExceptionInfo e
;      (println "Housekeeping skipped:" (ex-message e) (ex-data e)))))
;
;(defn- housekeeping-loop [manager]
;  (let [delay (:housekeeping-timeout manager)
;        delay-mseconds (* 60 delay 1000)
;        stop-ch (:stop-ch manager)]
;    (async/go-loop [timeout (async/timeout delay-mseconds)]
;      (async/alt!
;        timeout (do (cond-housekeeping)
;                    (recur (async/timeout delay-mseconds)))
;        stop-ch :stopped))))

(defn- has-idle-workers?
  "Returns whether a given manager has at least a single idle worker"
  [manager]
  (->> (:workers manager)
       (some #(not @(:busy? %)))
       (some?)))

(defn- dequeue-or-sleep
  "Attempts to obtain a unit from a queue, otherwise, sleeps for two seconds."
  [manager]
  (if-let [raw-unit (and (has-idle-workers? manager)
                         (redis/dequeue (:queues manager)))]
    (let [unit (unit/edn->Unit raw-unit)]
      (-> (:unit-chan manager)
          (async/put! unit)))
    (Thread/sleep 2000)))

(defn stop
  "Stops a given manager. Stopping it will cause the manager to not be part of
  the Raft cluster, and to not obtain new units. This function will notify all
  workers to also stop processing after completing any current unit."
  [manager]
  (when-not @(:stopped? manager)
    (let [{:keys [workers stopped? raft-node]} manager]
      (raft/stop raft-node)
      (run! worker/stop workers)
      (reset! stopped? true)
      (async/close! (:stop-ch manager)))))

(defn start
  "Starts a given manager. Will cause the manager to ingress into the Raft
  cluster, initialise all run loops of workers provided to `create-manager`,
  and enter a loop attempting to obtain new units from queues configured on
  this process. This function will block until `stop` is called."
  [manager]
  ;(housekeeping-loop manager)
  (async/thread (raft/run (:raft-node manager)))
  (worker/run-loop (:workers manager))
  (loop []
    (dequeue-or-sleep manager)
    (when-not @(:stopped? manager)
      (recur)))
  (async/close! (:unit-chan manager))
  :exited)
