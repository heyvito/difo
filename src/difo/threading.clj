(ns difo.threading
  (:require [clojure.core.async :as async])
  (:import (clojure.lang IAtom)))

(defn set-name
  "Sets the name of the current thread to the name provided."
  [& name]
  (-> (Thread/currentThread)
      (.setName (apply str name))))

(defn take!!
  "Blocks the current thread until an item is obtained from `ch`"
  [ch]
  (let [promise (promise)
        deliverer (fn [val] (deliver promise val))
        _ (future (async/take! ch deliverer))]
    @promise))

(defn measure
  "Measures how long `f` takes to execute, placing results inside an Atom
  provided through `into`, returning the value of `f`."
  [^IAtom into f]
  (let [start (System/currentTimeMillis)
        result (f)
        end (System/currentTimeMillis)]
    (reset! into {:start start :end end :elapsed (- end start)})
    result))
