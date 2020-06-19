(ns difo.core
  (:require [clojure.string :as str]
            [clojure.core.memoize :refer [memo]]))

(defn get-env [name] (System/getenv name))

(def queues
  ^{:doc "Returns a list of all queues workers will monitor. The value is
  obtained from an environment variable named QUEUES. To provide extra queues,
  split them with a space."}
  (memo (fn []
          (if-let [q-str (get-env "QUEUES")]
            (str/split q-str #"\s")
            ["default"]))))

(def workers
  ^{:doc "Defines how many threads will be initialized on a server process. The
  value is obtained from an environment variable named WORKERS. When unset,
  it assumes a number of 10 threads."}
  (memo (fn []
          (or (when-let [workers-str (get-env "WORKERS")]
                (try
                  (Integer/parseInt workers-str)
                  (catch Exception e
                    (println "ERROR: Invalid 'WORKERS' value: " (ex-message e))
                    (println "       Assuming default."))))
              10))))
