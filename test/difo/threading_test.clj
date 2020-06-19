(ns difo.threading-test
  (:require [clojure.test :refer :all]
            [difo.threading :refer :all]
            [clojure.core.async :as async]
            [difo.helpers :refer [with-timeout]]))

(deftest test-threading-facilities
  (let [ret (with-timeout 1000
              (let [ch (async/chan)]
                (async/thread
                  (set-name "Test Thread")
                  (async/put! ch (.getName (Thread/currentThread))))
                (take!! ch)))]
    (is (= "Test Thread" ret))))

(deftest test-measuring
  (let [callback (fn [] (Thread/sleep 300))
        measurer (atom nil)]
    (measure measurer callback)
    (is (not (nil? @measurer)))
    (is (<= 300 (:elapsed @measurer) 500))))
