(ns difo.queue-manager-test
  (:require [clojure.test :refer :all]
            [difo.helpers :refer [import-private-functions
                                  with-timeout!
                                  set-flag-and-noop]]
            [difo.threading :refer [take!!]]
            [difo.queue-manager :refer :all]
            [clojure.core.async :as async]
            [difo.fake-redis :as redis]
            [clojure.tools.reader.edn :as edn]))

(import-private-functions difo.queue-manager has-idle-workers?
                          dequeue-or-sleep)

(deftest test-has-empty-workers?
  (testing "without idle workers"
    (let [manager {:workers [{:busy? (atom true)}]}]
      (is (false? (has-idle-workers? manager)))))

  (testing "with idle workers"
    (let [manager {:workers [{:busy? (atom false)}]}]
      (is (true? (has-idle-workers? manager))))))

(deftest test-stop
  (testing "not stopped"
    (let [workers [{:stopped? (atom false)} {:stopped? (atom false)}]
          stopped? (atom false)
          raft-node {:stopped? (atom false)}
          stop-ch (async/chan)
          manager {:workers workers :stopped? stopped? :raft-node raft-node :stop-ch stop-ch}]
      (with-redefs-fn {#'difo.worker/stop (fn [w] (reset! (:stopped? w) true))
                       #'difo.raft/stop   (fn [r] (reset! (:stopped? r) true))}
        #(stop manager))

      (is @stopped?)
      (is @(:stopped? raft-node))
      (is @(:stopped? (first workers)))
      (is @(:stopped? (second workers)))
      (is (nil? (take!! stop-ch)))))

  (testing "already stopped"
    (is (nil? (stop {:stopped? (atom true)})))))

(deftest test-dequeue-or-sleep
  (testing "without idle workers"
    (with-redefs-fn {#'difo.queue-manager/has-idle-workers? (fn [_] false)}
      #(with-timeout! 2010
         (dequeue-or-sleep {}))))

  (testing "when dequeue is empty"
    (let [received-unit? (atom false)]
      (redis/with-fake-redis
        (with-redefs-fn {#'difo.queue-manager/has-idle-workers? (fn [_] true)
                         #'difo.unit/edn->Unit                  (fn [_] (reset! received-unit? true))}
          #(dequeue-or-sleep {:queues ["hello"]}))
        (is (redis/called? redis/dequeue))
        (is (not @received-unit?)))))

  (testing "with idle workers, and item available"
    (let [received-unit? (atom false)
          manager {:unit-chan (async/chan 10) :queues ["hello"]}]
      (redis/with-fake-redis
        (redis/enqueue "hello" "{:fn :foo}")
        (with-redefs-fn {#'difo.queue-manager/has-idle-workers? (fn [_] true)
                         #'difo.unit/edn->Unit                  (fn [x]
                                                                  (reset! received-unit? true)
                                                                  (edn/read-string x))}
          #(dequeue-or-sleep manager)))
      (is (= {:fn :foo} (take!! (:unit-chan manager))))
      (is @received-unit?)
      (async/close! (:unit-chan manager)))))

(deftest test-start
  (testing "basic"
    (let [raft-running? (atom false)
          worker-running? (atom false)
          dequeued? (atom false)
          manager {:workers [:dummy] :stopped? (atom true) :unit-chan (async/chan)}]
      (with-redefs-fn {#'difo.raft/run                       (set-flag-and-noop raft-running?)
                       #'difo.worker/run-loop                (set-flag-and-noop worker-running?)
                       #'difo.queue-manager/dequeue-or-sleep (set-flag-and-noop dequeued?)
                       }
        #(is (= :exited (start manager))))
      (is @raft-running?)
      (is @worker-running?)
      (is @dequeued?)))

  (testing "recursion"
    (let [raft-running? (atom false)
          worker-running? (atom false)
          counter (atom 0)
          manager {:workers [:dummy] :stopped? (atom false) :unit-chan (async/chan)}
          dequeuer (fn [worker] (if (= 1 @counter)
                                  (reset! (:stopped? worker) true)
                                  (swap! counter inc)))]
      (with-redefs-fn {#'difo.raft/run                       (set-flag-and-noop raft-running?)
                       #'difo.worker/run-loop                (set-flag-and-noop worker-running?)
                       #'difo.queue-manager/dequeue-or-sleep dequeuer}
        #(with-timeout! 5000 (is (= :exited (start manager)))))
      (is @raft-running?)
      (is @worker-running?))))
