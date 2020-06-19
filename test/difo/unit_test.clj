(ns difo.unit-test
  (:require [clojure.test :refer :all]
            [difo.unit :refer :all]))

(deftest test-edn
  (let [edn-data "{:fn :foo}"
        unit (edn->Unit edn-data)]
    (is (= :foo (:fn unit)))
    (is (instance? difo.unit.Unit unit))))

(def dummy-unit (map->Unit {:fn :foo}))

(deftest test-clean-map
  (let [dirty-map (into {} dummy-unit)
        clean-map (Unit->clean-map dummy-unit)]
    (is (and (contains? dirty-map :args)
          (nil? (:args dirty-map))))
    (is (and (not (contains? clean-map :args))
             (= :foo (:fn clean-map))))))

(deftest to-edn
  (is (= "{:fn :foo}" (Unit->edn dummy-unit))))

(deftest to-str
  (is (= "{:fn :foo}" (Unit->str dummy-unit))))
