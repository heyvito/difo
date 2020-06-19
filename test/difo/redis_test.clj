(ns difo.redis-test
  (:require [clojure.test :refer :all]
            [difo.redis :refer :all]
            [taoensso.carmine :as car]
            [difo.unit :as unit]
            [difo.helpers :refer [wait-flag]]))

;; Warning: Tests within this namespace requires a Redis instance to connect to.

(deftest test-key-name
  (is (= "difo:foo:bar" (key-name :foo :bar))))

(defmacro red> [& body]
  `(difo.redis/with-connection ~@body))

(defmacro flushing [& body]
  `(do (difo.redis/with-connection (car/flushall))
       ~@body))

(defn random-str []
  (apply str (map char (repeatedly 24 #(+ (rand-int 58) 64)))))

(deftest test-dequeue
  (flushing
    (testing "with single value"
      (let [key (random-str)]
        (red> (car/rpush "difo:queues:test" key))
        (is (= (dequeue "test") key))))

    (testing "with multi value"
      (let [key (random-str)]
        (red> (car/rpush "difo:queues:other-test" key))
        (is (= (dequeue ["random" "other-test"]) key))))))

(deftest test-register-heartbeat
  (flushing
    (let [worker {:id :worker}]
      (register-heartbeat worker)
      (is (>= (System/currentTimeMillis)
              (->> (red> (car/hget "difo:workers" "worker"))
                   (Long/parseLong)))))))

(deftest test-record-processed-unit
  (flushing
    (let [worker {:id :worker}]
      (record-processed-unit worker)
      (= 1 (red> (car/get "difo:stats:processed")))
      (= 1 (red> (car/get "difo:workers:worker:stats:processed"))))))

(deftest test-record-failed-unit
  (flushing
    (let [worker {:id :worker}]
      (record-failed-unit worker)
      (= 1 (red> (car/get "difo:stats:failed")))
      (= 1 (red> (car/get "difo:workers:worker:stats:failed"))))))

(deftest test-notify-current-unit
  (testing "set"
    (flushing
      (let [worker {:id :worker}
            unit {:fn :foo}]
        (notify-current-unit worker (unit/Unit->edn unit))
        (is (= "{:fn :foo}"
               (red> (car/get "difo:workers:worker:current-unit")))))))
  (testing "unset"
    (flushing
      (red> (car/set "difo:workers:worker:current-unit" "foo"))
      (notify-current-unit {:id :worker} nil)
      (is (nil?
            (red> (car/get "difo:workers:worker:current-unit")))))))

(deftest test-list-workers
  (flushing
    (let [counter (atom 0)
          ;; 2000 may be a little too much, but I want to cause redis to return
          ;; a cursor so we can test the recursive part of list-workers
          workers (repeatedly 2000 #(str "worker-" (let [c @counter]
                                                     (swap! counter inc)
                                                     c)))]
      (red> (run! #(car/hset "difo:workers" % (System/currentTimeMillis)) workers))
      (let [result (list-workers)]
        (run! #(is (some (fn [w] (= (w :worker) %)) result)) workers)))))

(deftest test-remove-worker
  (flushing
    (red> (car/set "difo:workers:foo:current-unit" "foo")
          (car/set "difo:workers:foo:stats:processed" 0)
          (car/set "difo:workers:foo:stats:failed" 0)
          (car/hset "difo:workers" "foo" 24012020))
    (remove-worker :foo)
    (is (every? zero? (last (red>
                              (car/multi)
                              (car/exists "difo:workers:foo:current-unit")
                              (car/exists "difo:workers:foo:stats:processed")
                              (car/exists "difo:workers:foo:stats:failed")
                              (car/hexists "difo:workers" "foo")
                              (car/exec)))))))

(deftest test-get-worker
  (testing "unknown unit"
    (flushing
      (is (nil? (get-worker :foo)))))

  (testing "without unit"
    (flushing
      (let [wid "luci.local@2401@foo"]
        (red> (car/hset "difo:workers" wid 24012020))
        (let [{:keys [host pid id heartbeat current-unit]} (get-worker wid)]
          (is (= host "luci.local"))
          (is (= pid "2401"))
          (is (= id "foo"))
          (is (= heartbeat "24012020"))
          (is (nil? current-unit))))))

  (testing "with unit"
    (flushing
      (let [wid "luci.local@2401@foo"]
        (red> (car/hset "difo:workers" wid 24012020)
              (car/set (str "difo:workers:" wid ":current-unit") "{:fn :foo}"))
        (let [{:keys [host pid id heartbeat current-unit]} (get-worker wid)]
          (is (= host "luci.local"))
          (is (= pid "2401"))
          (is (= id "foo"))
          (is (= heartbeat "24012020"))
          (is (= current-unit {:fn :foo})))))))

(deftest test-enumerate-workers-keys
  (flushing
    (let [counter (atom 0)
          ;; 2000 may be a little too much, but I want to cause redis to return
          ;; a cursor so we can test the recursive part of list-workers
          workers (repeatedly 2000 #(str "difo:workers:worker-" (let [c @counter]
                                                                  (swap! counter inc)
                                                                  c)))]
      (red> (run! #(car/set % "foo") workers))
      (let [results (set (enumerate-workers-keys))]
        (run! #(is (results %)) results)))))

(deftest test-enumerate-difo-processes
  (flushing
    (let [counter (atom 0)
          ;; 2000 may be a little too much, but I want to cause redis to return
          ;; a cursor so we can test the recursive part of list-workers
          procs (repeatedly 2000 #(str "difo:processes:proc-" (let [c @counter]
                                                                (swap! counter inc)
                                                                c)))]
      (red> (run! #(car/set % "foo") procs))
      (let [results (set (enumerate-difo-processes))]
        (run! #(is (results %)) results)))))

(deftest test-remove-key
  (flushing
    (red> (car/set "foo" "bar")
          (car/set "bar" "baz"))
    (remove-key :foo :bar)
    (is (every? zero? (last (red> (car/multi)
                                  (car/exists "foo")
                                  (car/exists "bar")
                                  (car/exec)))))))

(deftest test-last-housekeeping-timestamp
  (flushing
    (red> (car/set "difo:last-housekeeping" 24012020))
    (is (= 24012020 (last-housekeeping-timestamp)))))

(deftest test-raft-listener
  (flushing
    (let [token (random-str)
          received? (atom false)
          listener (new-raft-listener #(when (= % token)
                                         (reset! received? true)))]
      (Thread/sleep 500)
      (publish-raft token)
      (wait-flag received? 2000)
      (stop-raft-listener listener)
      (is (true? @received?)))))

(deftest test-register-process
  (flushing
    (register-process {:name "luci.local"})
    (is (= "1" (red> (car/get "difo:processes:luci.local"))))))

(deftest test-unregister-process
  (flushing
    (red> (car/set "difo:processes:luci.local" 1))
    (unregister-process {:name "luci.local"})
    (is (zero? (red> (car/exists "difo:processes:luci.local"))))))
