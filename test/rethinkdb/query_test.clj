(ns rethinkdb.query-test
  (:require [clojure.test :refer :all]
            [rethinkdb.query :refer :all]))

(deftest query-test
  (is (= [:MAKE_ARRAY [1 2 3]]
         (rarray [1 2 3])))
  (is (= [:MAKE_ARRAY [1]]
         (rarray 1))))
