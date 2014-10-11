(ns rethinkdb.query-test
  (:require [clojure.test :refer :all]
            [rethinkdb.query :refer :all]))

(deftest query-test
  (is (= [14 ["test"]]
         (parse-args [:DB ["test"]])))
  (is (= [15 [[14 ["test"]]]]
         (parse-args [:TABLE [[:DB ["test"]]]])))
  (is (= [56 [[15 ["test"]] [2 [{} {}]]]]
         (parse-args [:INSERT [[:TABLE ["test"]] [:MAKE_ARRAY [{} {}]]]]))))
