(ns rethinkdb.async-test
  (:require [clojure.test :refer :all]
            [rethinkdb.query :as r]
            [clojure.core.async :as async])
  (:import (clojure.core.async.impl.protocols ReadPort)))

(def test-db "cljrethinkdb_test")
(def test-table :pokedex)

(defn ensure-table
  "Ensures that an empty table \"table-name\" exists"
  [table-name optargs conn]
  (if (some #{table-name} (r/run (r/table-list) conn))
    (r/run (r/table-drop table-name) conn))
  (r/run (r/table-create (r/db test-db) table-name optargs) conn))

(defn ensure-db
  "Ensures that an empty database \"db-name\" exists"
  [db-name conn]
  (if (some #{db-name} (r/run (r/db-list) conn))
    (r/run (r/db-drop db-name) conn))
  (r/run (r/db-create db-name) conn))

(defn setup-each [test-fn]
  (with-open [conn (r/connect :db test-db)]
    (-> (r/table test-table)
        (r/delete {:durability :soft :return-changes false})
        (r/run conn))
    (test-fn)))

(defn setup-once [test-fn]
  (with-open [conn (r/connect)]
    (ensure-db test-db conn)
    (ensure-table test-table {:primary-key :national_no} conn)
    (test-fn)
    (r/run (r/db-drop test-db) conn)))

(use-fixtures :each setup-each)
(use-fixtures :once setup-once)

(deftest always-return-async
  (with-open [conn (r/connect :async? true)]
    (are [query] (instance? ReadPort (r/run query conn))
      (r/db-list)
      (-> (r/db :non-existent) (r/table :nope))
      (-> (r/db test-db) (r/table test-table) (r/insert {:a 1})))))

(deftest async-results
  (let [conn (r/connect :async? true :db test-db)
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
      (-> (r/table test-table)
          (r/insert pokemon))
      pokemon (-> (r/table test-table))
      [pokemon] (-> (r/table test-table) (r/order-by :name))

      )))
