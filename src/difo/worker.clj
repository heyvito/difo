(ns difo.worker
  (:require [clojure.core.async :as async]
            [clojure.reflect :as reflect]
            [difo.id :refer [gen-worker-id]]
            [difo.redis :as redis]
            [difo.threading :as threading]
            [difo.unit :as unit]))

(defrecord Worker [id input-chan log-chan busy? stopped? now-processing])

(def ^:dynamic heartbeat-base-sleep 5000)
(def ^:dynamic heartbeat-skew-sleep 8000)

(defn create-worker
  "Creates a new worker. Workers are responsible for receiving and running units."
  [input-chan log-chan]
  (map->Worker {:id             (gen-worker-id)
                :input-chan     input-chan
                :log-chan       log-chan
                :busy?          (atom false)
                :stopped?       (atom false)
                :now-processing (atom nil)}))

(def ^:dynamic log (fn [& _]))

(defmacro with-logger [worker & body]
  `(binding [log (fn [& args#]
                   (async/put! (:log-chan ~worker)
                               (str "[" (:id ~worker) "] " (apply str args#))))]
     ~@body))

;; TODO(vito) Improve error handling (retries?)
(defn- work! [unit]
  (let [fn (resolve (symbol (:fn unit)))
        fn! (when fn (deref fn))]
    (when (nil? fn)
      (throw (ex-info (str "Symbol " (:fn unit) " could not be resolved")
                      {:symbol (:fn unit)
                       :error  :cannot-resolve})))

    (when-not (ifn? fn!)
      (throw (ex-info (str "Symbol " (:fn unit) " is not an Ifn!")
                      {:symbol (:fn unit)
                       :type   (reflect/typename (type fn!))
                       :error  :not-an-ifn})))
    (apply fn! (or (:args unit)
                   []))))

(defn- heartbeat [worker]
  (async/thread
    (threading/set-name "Worker " (:id worker) ": Heartbeat")
    (loop []
      (when-not (deref (:stopped? worker))
        (redis/register-heartbeat worker)
        (Thread/sleep (+ heartbeat-base-sleep
                         (rand-int heartbeat-skew-sleep)))
        (recur)))
    :done))

(defn- notify-current-unit [worker unit]
  (let [{:keys [busy? now-processing]} worker]
    (reset! busy? true)
    (reset! now-processing unit)
    (redis/notify-current-unit worker (unit/Unit->edn unit))))

(defn- notify-unit-done [worker status]
  (let [{:keys [busy? now-processing]} worker]
    (reset! busy? false)
    (reset! now-processing nil)
    (if (= status :failed) (redis/record-failed-unit worker)
                           (redis/record-processed-unit worker))
    (redis/notify-current-unit worker nil)))

(defn- execution-wrapper [unit]
  (let [timer (atom nil)
        executor (fn []
                   (try (work! unit)
                        :ok
                        (catch Exception ex
                          (log "Unit " (:id unit) " failed: " (ex-message ex))
                          :failed)))
        result (threading/measure timer executor)]
    (log "Unit " (:id unit) " finished in " (:elapsed @timer) "ms. Result: " result)
    result))

(defn- watch-jobs [worker]
  (async/thread
    (with-logger worker
      (threading/set-name "Worker " (:id worker))
      (log "Waiting for units.")
      (loop []
        (let [unit (threading/take!! (:input-chan worker))]
          (when unit
            (log "Obtained unit " (unit/Unit->str unit))
            (notify-current-unit worker unit)
            (->> unit
                 (execution-wrapper)
                 (notify-unit-done worker)))
          (when-not (nil? unit)
            (recur))))
      :exited)))

(defn run-loop
  "Starts the run-loop for a given worker. The run loop sends heartbeats of the
  worker itself, and watches for jobs delivered by its manager."
  [^Worker worker]
  (heartbeat worker)
  (watch-jobs worker))

(defn stop
  "Stop prevents the worker from accepting new jobs, and ends its run-loop as
  soon as it finishes processing a Unit, if any."
  [worker]
  (reset! (:stopped? worker) true)
  (async/close! (:input-chan worker)))
