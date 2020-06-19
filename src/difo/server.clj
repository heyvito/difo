(ns difo.server
  (:require [clojure.core.async :as async]
            [difo.core :as core]
            [difo.queue-manager :as queue-manager]
            [difo.threading :as threading]
            [difo.worker :refer [create-worker]]))

(def queue-manager (atom nil))
(def log-output (atom nil))

(defn- log-loop []
  (loop []
    (when-let [line (threading/take!! @log-output)]
      (println line)
      (recur))))

(defn start
  "Starts the server. This will establish a connection to Redis and start
  processing units. This function will block until the process is terminated
  (or `stop` is called)"
  []
  (let [unit-chan (async/chan (core/workers))
        log-chan (async/chan (async/dropping-buffer 1024))
        workers (repeatedly (core/workers) #(create-worker unit-chan log-chan))
        manager (queue-manager/create-manager workers unit-chan (core/queues))]
    (reset! queue-manager manager)
    (reset! log-output log-chan)
    (async/thread (log-loop))
    (queue-manager/start manager)))

(defn stop []
  "Requests the current server (if any) to stop. This will cause the manager to
  stop accepting new units, and wait until current ones are processed before
  exiting the loop started by start."
  (when-let [man @queue-manager]
    (queue-manager/stop man)))
