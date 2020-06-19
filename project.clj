(defproject difo "0.1.0"
  :description "Distributed FIFOs"
  :url "https://github.com/heyvito/difo"
  :license {:name "MIT"
            :url "https://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/core.async "1.2.603"]
                 [com.taoensso/carmine "2.19.1"]
                 [clj-time "0.15.2"]]
  :plugins [[jonase/eastwood "0.3.10"]
            [lein-cloverage "1.1.2"]]
  :repl-options {:init-ns difo.core})
