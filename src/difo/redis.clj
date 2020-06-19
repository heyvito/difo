(ns difo.redis
  (:require [clojure.string :as str]
            [clojure.set :as set]
            [taoensso.carmine :as car]
            [difo.unit :as unit]
            [difo.core :as core]
            [clojure.edn :as edn]))

(def connection {:pool {}
                 :spec {:uri (-> (or (core/get-env "REDIS_URL")
                                     "redis://localhost:6379/0")
                                 (str/trim))}})

;; Here we will block for a while, but not indefinitely.
;; This way workers can check whether they should stop processing.
(def ^{:private true :const true} pop-timeout 2)
;; This indicates how many seconds until a process register expires.
(def ^{:private true :const true} process-register-timeout 40)

(def key-name
  (memoize (fn [& args]
             (let [path (map name args)]
               (str "difo:" (apply str (interpose \: path)))))))

(defmacro with-connection [& body] `(car/wcar connection ~@body))

(defn dequeue [from]
  (let [raw-from (if (coll? from) (vec from) [from])
        pop-targets (mapv #(key-name :queues %) raw-from)
        pop-arguments (conj pop-targets pop-timeout)
        [_ val] (with-connection (apply car/blpop pop-arguments))]
    val))

(defn enqueue [into unit]
  (let [queue-name (key-name :queues into)]
    (with-connection (car/rpush queue-name (unit/Unit->edn unit)))))

(defn register-heartbeat [worker]
  (let [key (key-name :workers)]
    (with-connection (car/hset key (:id worker) (System/currentTimeMillis)))))

(defn- generic-incr [worker kind]
  (with-connection
    (car/incr (key-name :stats kind))
    (car/incr (key-name :workers (:id worker) :stats kind))))

(defn record-processed-unit [worker] (generic-incr worker :processed))

(defn record-failed-unit [worker] (generic-incr worker :failed))

(defn notify-current-unit [worker unit]
  (with-connection
    (let [wid (:id worker)
          worker-status-key (key-name :workers wid :current-unit)]
      (if unit
        (car/set worker-status-key unit)
        (car/del worker-status-key)))))

(defn list-workers []
  (let [hash-name (key-name :workers)]
    (loop [cursor "0"
           result-set []]
      (let [[new-cur results] (with-connection (car/hscan hash-name cursor))
            new-result (->> results
                            (partition 2)
                            (map (fn [[id beat]] [id (Long/parseLong beat)]))
                            (map #(zipmap [:worker :heartbeat] %))
                            (apply conj result-set))]
        (if (not= "0" new-cur) (recur new-cur new-result)
                               new-result)))))

(defn remove-worker [id]
  (let [worker-status-key (key-name :workers id :current-unit)
        processed-stats-key (key-name :workers id :stats :processed)
        failed-stats-key (key-name :workers id :stats :failed)
        workers-hash (key-name :workers)]
    (with-connection
      (car/hdel workers-hash id)
      (car/del worker-status-key processed-stats-key failed-stats-key))))

(defn get-worker [id]
  (when-let [beat (with-connection (car/hget (key-name :workers) id))]
    (let [[host pid wid] (str/split id #"@")]
      {:host         host
       :pid          pid
       :id           wid
       :heartbeat    beat
       :current-unit (when-let [unit (with-connection (car/get (key-name :workers id :current-unit)))]
                       (-> unit
                           (unit/edn->Unit)
                           (unit/Unit->clean-map)))})))

(defn- scan-keys [& pattern]
  (let [pattern (apply str pattern)]
    (loop [cursor "0"
           result-set #{}]
      (let [[new-cur results] (with-connection (car/scan cursor :match pattern))
            new-result-set (set/union result-set results)]
        (if (not= "0" new-cur) (recur new-cur new-result-set)
                               new-result-set)))))

(defn enumerate-workers-keys []
  (scan-keys (key-name :workers) ":*"))

(defn enumerate-difo-processes []
  (scan-keys (key-name :processes) ":*"))

(defn remove-key [& keys]
  (with-connection (apply car/del keys)))

(defn last-housekeeping-timestamp []
  (when-let [ts (with-connection (car/get (key-name :last-housekeeping)))]
    (Long/parseLong ts)))

(defn new-raft-listener [f]
  (let [kn (key-name :raft)]
    (car/with-new-pubsub-listener connection
      {kn (fn [[type _channel val]]
            (when (= type "message")
              (-> val
                  (edn/read-string)
                  (f))))}
      (car/subscribe kn))))

(defn stop-raft-listener [listener]
  (car/close-listener listener))

(defn publish-raft [message]
  (with-connection
    (car/publish (key-name :raft)
                 (prn-str message))))

(defn register-process [node]
  (with-connection (car/setex (key-name :processes (:name node))
                              process-register-timeout
                              1)))

(defn unregister-process [node]
  (with-connection (car/del (key-name :processes (:name node)))))
