(ns difo.id-test
  (:require [clojure.test :refer :all]
            [difo.helpers :refer [import-private-functions]]
            [difo.id :refer :all]
            [clojure.string :as str])
  (:import (java.net InetAddress)
           (java.lang.management ManagementFactory)))

(def current-hostname (-> (InetAddress/getLocalHost)
                          .getHostName
                          str/lower-case))

(def current-pid (-> (ManagementFactory/getRuntimeMXBean)
                     (.getName)
                     (str/split #"@")
                     (first)))

(import-private-functions difo.id generate-random-id)

(deftest test-random-id
  (testing "default params"
    (let [id (generate-random-id)]
      (is (= 12 (count id)))
      (is (not (nil? (re-matches #"[a-zA-Z0-9]{12}" id))))))
  (testing "custom params"
    (let [id (generate-random-id 24)]
      (is (= 24 (count id)))
      (is (not (nil? (re-matches #"[a-zA-Z0-9]{24}" id)))))))

(deftest test-process-id
  (let [pid (process-id)]
    (is (= (str current-hostname "@" current-pid) pid))))

(deftest test-worker-id
  (let [wid (gen-worker-id)]
    (is (str/starts-with? wid (str current-hostname "@" current-pid "@")))
    (is (not (nil? (re-find #"@[a-zA-Z0-9]{12}" wid))))))
