(ns rethinkdb.async-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async]
            [rethinkdb.query :as r]
            [rethinkdb.test-utils :as utils])
  (:import (clojure.core.async.impl.protocols ReadPort)))

(use-fixtures :each utils/setup-each)
(use-fixtures :once utils/setup-once)

(deftest always-return-async
  (with-open [conn (r/connect :async? true)]
    (are [query] (instance? ReadPort (r/run query conn))
      (r/db-list)
      (-> (r/db :non-existent) (r/table :nope))
      (-> (r/db utils/test-db) (r/table utils/test-table) (r/insert {:a 1})))))

(deftest async-results
  (let [conn (r/connect :async? true :db utils/test-db)
        pokemon [{:national_no 25 :name "Pikachu"}
                 {:national_no 26 :name "Raichu"}]]
    (are [expected query] (= (->> (r/run query conn)
                                  (async/into [])
                                  (async/<!!))
                             expected)
      [{:deleted   0
        :errors    0
        :inserted  2
        :replaced  0
        :skipped   0
        :unchanged 0}]
      (-> (r/table utils/test-table)
          (r/insert pokemon))
      pokemon (-> (r/table utils/test-table))
      [pokemon] (-> (r/table utils/test-table) (r/order-by :name))

      )))
