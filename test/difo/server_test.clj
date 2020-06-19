(ns difo.server-test
  (:require [clojure.test :refer :all]
            [difo.server :refer :all]
            [difo.helpers :refer [import-private-functions
                                  wait-flag]]
            [clojure.core.async :as async]
            [difo.queue-manager :as queue-manager]))

(import-private-functions difo.server log-loop)

(deftest test-log-loop
  (try
    (let [log-chan (async/chan 10)]
      (reset! log-output log-chan)
      (async/put! log-chan "A")
      (async/put! log-chan "B")
      (async/close! log-chan)
      (is (= "A\nB\n" (with-out-str (log-loop)))))
    (finally (reset! log-output nil))))

(deftest test-stop
  (testing "when manager is nil"
    (reset! queue-manager nil)
    (let [called? (atom false)]
      (with-redefs-fn {#'queue-manager/stop (fn [_] (reset! called? true))}
        #(stop))
      (is (false? @called?))))

  (testing "when manager is present"
    (reset! queue-manager :foo)
    (let [called? (atom false)]
      (with-redefs-fn {#'queue-manager/stop (fn [man]
                                              (is (= man :foo))
                                              (reset! called? true))}
        #(stop))
      (is (true? @called?)))))

(deftest test-start
  (let [log-loop-running? (atom false)
        manager-started? (atom false)]
    (with-redefs-fn {#'queue-manager/start  (fn [man]
                                              (is (= man @queue-manager))
                                              (reset! manager-started? true))
                     #'difo.server/log-loop (fn [] (reset! log-loop-running? true))}
      #(start))
    (wait-flag log-loop-running? 2000)
    (is (true? @log-loop-running?))
    (is (true? @manager-started?))))
