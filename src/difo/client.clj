(ns difo.client
  (:require [clj-time.core :as t]
            [clj-time.coerce :as t-c]
            [difo.id :as id]
            [difo.redis :as redis]))

(defn timestamp*
  "Returns the current timestamp in UTC relative to the UNIX epoch"
  []
  (-> (t/now)
      (t/to-time-zone t/utc)
      (t-c/to-epoch)))

(defn dispatch*
  "Enqueues a given data object into a given queue. This function is
  internal, please use `perform-on`, or `perform` instead."
  [queue data]
  (redis/enqueue queue (assoc data :enqueued-at (timestamp*))))

(defmacro perform-on
  "Enqueues `fn` with `args` into a specified `queue`.
  Usage of non-primitives with Difo is HIGHLY discouraged. When working with
  Difo, prefer to use primitive types like keywords, strings, integers, longs,
  booleans, maps, vectors, lists, and such. Complex types such as Atoms, Refs
  and alikes will cause problems.

  (perform-on :math-queue + 1 2)"
  [queue fn & args]
  `(let [meta# (meta (resolve (symbol ~(name fn))))
         ns# (str (meta# :ns))
         fn-name# (meta# :name)
         fn# (apply str (interpose \/ [ns# fn-name#]))]
     (dispatch* ~queue {:fn fn#
                       :args (vector ~@args)
                       :id (id/gen-job-id)
                       :created-at (timestamp*)})))

(defmacro perform
  "Enqueues `fn` and `args` to the `default` queue. This is the same as
  `(perform-on :default fn arg1 arg2...)`
  See `perform-on` for more information.

  (perform println \"Hello\" \"World\")"
  [fn & args]
  `(perform-on :default ~fn ~@args))
