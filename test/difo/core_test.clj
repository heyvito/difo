(ns difo.core-test
  (:require [clojure.test :refer :all]
            [difo.core :refer :all]
            [difo.helpers :refer [without-memoize]]))

(deftest test-get-env
  (is (= (System/getenv "PATH") (get-env "PATH"))))

(deftest test-queues
  (testing "when defined"
    (without-memoize difo.core/queues
      (with-redefs-fn {#'difo.core/get-env (fn [_] "a b c")}
        #(is (= ["a" "b" "c"] (queues))))))
  (testing "when not defined"
    (without-memoize difo.core/queues
      (is (= ["default"] (queues))))))

(deftest test-workers
  (testing "when not defined"
    (without-memoize difo.core/workers
      (is (= 10 (workers)))))
  (testing "when defined"
    (without-memoize difo.core/workers
      (with-redefs-fn {#'difo.core/get-env (fn [_] "24")}
        #(is (= 24 (workers))))))
  (testing "with an invalid value"
    (without-memoize difo.core/workers
      (with-redefs-fn {#'difo.core/get-env (fn [_] "foo")}
        #(is (= "ERROR: Invalid 'WORKERS' value:  For input string: \"foo\"\n       Assuming default.\n"
                (with-out-str (is (= 10 (workers))))))))))
