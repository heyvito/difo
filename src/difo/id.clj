(ns difo.id
  (:require [clojure.string :as str])
  (:import (java.security SecureRandom)
           (java.net InetAddress)
           (java.lang.management ManagementFactory)
           (java.util Random UUID)))

(def ^:private secure-random (SecureRandom.))
(def ^{:private true :const true} alphabet
  (mapv str "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"))

(defn- seed [size]
  (let [seed (byte-array size)]
    (.nextBytes ^Random secure-random seed)
    seed))

(def ^:private current-pid
  (memoize (fn []
             (-> (ManagementFactory/getRuntimeMXBean)
                 (.getName)
                 (str/split #"@")
                 (first)))))

(def ^:private get-hostname
  (memoize (fn []
             (-> (InetAddress/getLocalHost)
                 .getHostName
                 str/lower-case))))

(defn- generate-random-id
  ([] (generate-random-id 12))
  ([size] (let [mask 0x3d]
            (loop [bytes (seed size)
                   id ""]
              (if bytes
                (recur (next bytes)
                       (->> (first bytes)
                            (bit-and mask)
                            alphabet
                            (str id)))
                id)))))

(def process-id
  ^{:doc "Generates an ID for the current process. The ID is represented
  by joining the current hostname and PID with an at-sign (@)"}
  (memoize (fn []
             (->> [(get-hostname) (current-pid)]
                  (interpose \@)
                  (apply str)))))

(defn gen-worker-id
  "Generates a random worker ID based on the current process ID"
  []
  (let [random-id (generate-random-id)
        pid (process-id)]
    (apply str (interpose \@ [pid random-id]))))

(defn gen-job-id
  "Generates a new job identifier by creating an UUID and stripping its dashes"
  []
  (-> (UUID/randomUUID)
      (str)
      (str/replace #"-" "")))
