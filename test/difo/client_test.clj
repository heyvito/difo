(ns difo.client-test
  (:require [clojure.test :refer :all]
            [difo.client :refer :all]
            [difo.fake-redis :as redis]
            [clj-time.coerce :as t-c]
            [clj-time.core :as t]))

(deftest test-perform
  (redis/with-fake-redis
    (with-redefs-fn {#'difo.client/timestamp* (fn [] 24)}
      #(perform println "Hello" "World"))
    (let [queue (redis/items-enqueued-in :default)
          unit (first queue)]
      (is (= 1 (count queue)))
      (let [{:keys [args fn created-at enqueued-at]} unit]
        (is (= ["Hello" "World"] args))
        (is (= "clojure.core/println" fn))
        (is (= 24 created-at))
        (is (= 24 enqueued-at))))))

(deftest test-timestamp
  (let [raw-now (t/to-time-zone (t/now) t/utc)
        later (t/plus raw-now (t/seconds 1))
        ts (timestamp*)
        then (t-c/from-epoch ts)]
    (is (t/after? later then))))
