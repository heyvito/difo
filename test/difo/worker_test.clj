(ns difo.worker-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async]
            [difo.helpers :refer [import-private-functions
                                  with-timeout
                                  mocking-logger
                                  logged-entries
                                  killing-stdout]]
            [difo.worker :refer :all]
            [difo.threading :refer [take!!]]
            [difo.fake-redis :as redis]
            [difo.worker :as worker]))

;; Worker uses a few functions from the redis namespace. We don't want to test
;; those here. So here's a list of what to rebind:
;; - redis/register-heartbeat
;; - redis/notify-current-unit
;; - redis/record-failed-unit
;; - redis/record-processed-unit
;; For those, use the fake-redis ns under `test`. It provides the same interfaces
;; as the redis ns, but aimed for testing, with a sprinkle of introspection,
;; without actually communicating with Redis itself.

;; Tests here may be a little terse, since workers are something peculiar...
;; Maybe refactor them to do less work? Maybe?

(defn make-worker []
  (let [input (async/chan 10)
        log (async/chan 10)
        worker (create-worker input log)]
    {:input input :log log :worker worker}))

(import-private-functions difo.worker work!
                          heartbeat
                          notify-current-unit
                          notify-unit-done
                          execution-wrapper
                          watch-jobs)

(deftest test-work!
  (testing "with unknown symbol"
    (let [unit {:fn "unknown/symbol"}]
      (is (thrown-with-data? {:symbol "unknown/symbol"
                              :error  :cannot-resolve}
                             (work! unit)))))

  (testing "with non-fn"
    (let [unit {:fn "difo.redis/pop-timeout"}]
      (is (thrown-with-data? {:symbol "difo.redis/pop-timeout"
                              :error  :not-an-ifn
                              :type   "java.lang.Long"}
                             (work! unit)))))
  (testing "with a fn with args"
    (let [unit {:fn   "clojure.core/print"
                :args ["Hello" "World"]}
          out (with-out-str (work! unit))]
      (is (= "Hello World" out))))
  (testing "with a fn without args"
    (let [unit {:fn "clojure.core/println"}
          out (with-out-str (work! unit))]
      (is (= "\n" out)))))

(deftest test-heartbeat
  (binding [difo.worker/heartbeat-base-sleep 100
            difo.worker/heartbeat-skew-sleep 0]
    (let [{:keys [worker]} (make-worker)]
      (redis/with-fake-redis
        (let [done (heartbeat worker)]
          (Thread/sleep 200)
          (stop worker)
          (is (= :done (take!! done)))
          (is (@redis/heartbeats (:id worker))))))))

(deftest test-notify-current-unit
  (redis/with-fake-redis
    (let [{:keys [worker]} (make-worker)
          unit {:foo :bar}]
      (notify-current-unit worker unit)
      (is @(:busy? worker))
      (is (= {:foo :bar} @(:now-processing worker)))
      (contains? @redis/current-units (:id worker)))))

(deftest test-notify-unit-done
  (testing "success"
    (redis/with-fake-redis
      (let [{:keys [worker]} (make-worker)]
        (reset! (:busy? worker) true)
        (reset! (:now-processing worker) :foo)
        (notify-unit-done worker :ok)
        (is (not @(:busy? worker)))
        (is (not @(:now-processing worker)))
        (is (= 1 @redis/processed-units))
        (is (= :nil (@redis/current-units (:id worker)))))))

  (testing "failure"
    (redis/with-fake-redis
      (let [{:keys [worker]} (make-worker)]
        (reset! (:busy? worker) true)
        (reset! (:now-processing worker) :foo)
        (notify-unit-done worker :failed)
        (is (not @(:busy? worker)))
        (is (not @(:now-processing worker)))
        (is (= 1 @redis/failed-units))
        (is (= :nil (@redis/current-units (:id worker))))))))

(deftest test-execution-wrapper
  (testing "on error"
    (with-redefs-fn {#'difo.worker/work! (fn [_]
                                           (throw (ex-info "Boom!"
                                                           {:erred? true})))}
      #(do
         (let [unit {:id :test}]
           (mocking-logger
             (let [result (execution-wrapper unit)
                   [failed-log result-log] (logged-entries)]
               (is (= :failed result))
               (is (= "Unit :test failed: Boom!" failed-log))
               (is (unit-logged-with-result? :test :failed result-log))))))))

  (testing "on success"
    (with-redefs-fn {#'difo.worker/work! (fn [_])}
      #(do
         (let [unit {:id :test}]
           (mocking-logger
             (let [result (execution-wrapper unit)
                   [result-log] (logged-entries)]
               (is (= :ok result))
               (is (unit-logged-with-result? :test :ok result-log)))))))))

(deftest test-job-watcher
  (redis/with-noop-redis
    (killing-stdout
      (let [{:keys [worker input log]} (make-worker)
            output-chan (watch-jobs worker)
            first-unit {:fn "clojure.core/println" :args ["Hello!"] :id :foo}
            second-unit {:fn "clojure.core/println" :args ["Hello!"] :id :bar}]
        (async/put! input first-unit)
        (async/put! input second-unit)
        (async/close! input)
        (stop worker)
        (is (= :exited (take!! output-chan)))
        (let [[waiting-for obtained-first result-first obtained-second result-second] (repeatedly 5 #(take!! log))]
          (is (= waiting-for (str "[" (:id worker) "] Waiting for units.")))
          (is (worker-obtained-unit? worker first-unit obtained-first))
          (is (unit-logged-with-result? :foo :ok result-first))
          (is (worker-obtained-unit? worker second-unit obtained-second))
          (is (unit-logged-with-result? :bar :ok result-second)))))))

(deftest test-run-loop
  (let [{:keys [worker]} (make-worker)
        started-heartbeat (atom false)
        started-job-watcher (atom false)
        create-setter (fn [target] (fn [w]
                                     (is (= worker w))
                                     (reset! target true)))]
  (with-redefs-fn {#'worker/heartbeat (create-setter started-heartbeat)
                   #'worker/watch-jobs (create-setter started-job-watcher)}
    #(do
       (run-loop worker)
       (is @started-heartbeat)
       (is @started-job-watcher)))))
