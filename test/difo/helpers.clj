(ns difo.helpers
  (:require [clojure.test :refer :all]
            [clojure.test :as test]
            [clojure.string :as str]
            [difo.unit :as unit]
            [clojure.core.memoize :refer [memo-clear!]])
  (:import (clojure.lang ExceptionInfo)))

(defmacro with-timeout [timeout-ms & body]
  `(let [fut# (future ~@body)
         ret# (deref fut# ~timeout-ms :timed-out)]
     (when (= ret# :timed-out)
       (future-cancel fut#))
     ret#))

(defmacro with-timeout! [timeout-ms & body]
  `(when (= :timed-out (with-timeout ~timeout-ms ~@body))
     (throw (ex-info "Timeout exceeded for call"
                     {:timeout ~timeout-ms
                      :call (quote ~body)}))))

(defmacro import-private-functions [from & forms]
  (let [def-symbol #(symbol (str (name from) "/" (name %)))
        mapper (fn [n] `(def ~n (var ~(def-symbol n))))
        items (map mapper (flatten forms))]
    `(do ~@items)))

(defmacro wait-flag [flag timeout]
  `(with-timeout! ~timeout
     (loop []
       (when-not (deref ~flag)
         (Thread/sleep 10)
         (recur)))))

(defmethod test/assert-expr 'thrown-with-data?
  [msg [_ expectation & body]]
  `(test/do-report
     (try
       ~@body
       {:type     :fail
        :message  (str (when ~msg (str ~msg ": "))
                       "Exception expected.")
        :expected ~expectation
        :actual   nil}
       (catch ExceptionInfo e#
         (let [d# (ex-data e#)]
           (if (= ~expectation d#)
             {:type     :pass
              :message  ~msg
              :expected ~expectation
              :actual   e#}
             {:type     :fail
              :message  (str (when ~msg (str ~msg ": "))
                             "Exception data is not as expected")
              :expected ~expectation
              :actual   d#}))))))

(defmethod test/assert-expr 'unit-logged-with-result?
  [msg [_ unit-name result log]]
  (let [exp (str "Unit " unit-name " finished in \\d+ms. Result: " result)
        re (re-pattern exp)]
    `(test/do-report
       (if (re-find ~re ~log)
         {:type     :pass
          :message  ~msg
          :expected (str "Must match regexp: " ~re)
          :actual   ~log}
         {:type     :fail
          :message  (str (when ~msg (str ~msg ": "))
                         "Log output is not as expected")
          :expected (str "Must match regexp: " ~re)
          :actual   ~log}))))

(defmethod test/assert-expr 'worker-obtained-unit?
  [msg [_ worker unit log]]
  `(let [dumped-unit# (str/trim-newline (unit/Unit->edn ~unit))
         exp# (str "[" (:id ~worker) "] Obtained unit " dumped-unit#)]
     (test/do-report
       (if (= ~log exp#)
         {:type     :pass
          :message  ~msg
          :expected exp#
          :actual   ~log}
         {:type     :fail
          :message  (str (when ~msg (str ~msg ": "))
                         "Log output is not as expected")
          :expected (str "Should state " dumped-unit# " was obtained")
          :actual   ~log}))))

(def mocked-log (atom (vector)))
(defn logged-entries [] @mocked-log)
(defmacro mocking-logger [& body]
  `(binding [difo.worker/log (fn [& args#]
                               (swap! mocked-log conj (apply str args#)))]
     (try ~@body
          (finally (reset! mocked-log (vector))))))

(defmacro killing-stdout [& body]
  `(let [output# (atom nil)]
     (with-out-str (reset! output# (do ~@body)))
     @output#))

(defn- symbolize-stack-trace [^StackTraceElement st]
  (let [class-name (.getClassName st)
        file-name (.getFileName st)
        method-name (.getMethodName st)
        line-num (.getLineNumber st)
        idx (str/index-of class-name \$)
        ns-name (when-not (nil? idx) (subs class-name 0 idx))
        fn-name (if-not (nil? idx)
                  (subs class-name (inc idx))
                  "")]
    {:class-name  class-name
     :file-name   file-name
     :method-name method-name
     :line-num    line-num
     :ns-name     ns-name
     :fn-name     fn-name}))


(defn- dedupe-with [key coll]
  (loop [result (vector)
         known-elements #{}
         elements coll]
    (let [current (first elements)
          next (rest elements)
          k (key current)
          known? (known-elements k)
          new-result (if known? result
                                (conj result current))
          new-known (if known? known-elements
                               (conj known-elements k))]
      (if (seq next)
        (recur new-result new-known next)
        new-result))))

(defn curr-func []
  (let [stack (.getStackTrace (RuntimeException. "dummy"))
        symbolized (map symbolize-stack-trace stack)
        deduped (dedupe-with :class-name symbolized)
        current-ns (:ns-name (first deduped))
        curr-func-re (re-pattern #"curr.func")
        filter (fn [i] (and (= current-ns (:ns-name i))
                            (re-find curr-func-re (:fn-name i))))]
    (->> deduped
         (remove filter)
         (first))))

(defn tap! [fn val]
  (fn val)
  val)

(defn noop [& _args])

(defn set-flag-and-noop [flag]
  (fn [& _]
    (reset! flag true)
    nil))

(defmacro without-memoize [fn & body]
  `(do (memo-clear! ~fn)
       (try
         ~@body
         (finally (memo-clear! ~fn)))))
