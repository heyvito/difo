(ns difo.unit
  (:require [clojure.edn :as edn]
            [clojure.string :as str]))

(defrecord Unit [fn args id created-at enqueued-at
                 retries error-message error-info failed-at retried-at
                 at])

(defn edn->Unit
  "Converts an edn representation of a Unit into a Unit instance."
  [data]
  (when-let [map-data (edn/read-string data)]
    (map->Unit map-data)))

(defn Unit->clean-map
  "Returns a map representation of a given Unit, removing any `nil` keys."
  [unit]
  (let [mapped (into {} unit)
        non-nil-keys (remove #(nil? (% unit))
                             (keys unit))]
    (select-keys mapped non-nil-keys)))

(defn Unit->edn
  "Converts an Unit to an edn string, after cleaning it through
  `Unit->clean-map`."
  [unit]
  (str/trim-newline (prn-str (Unit->clean-map unit))))

(defn Unit->str
  "Converts a given Unit into a string representation"
  [unit]
  (Unit->edn unit))
