(ns rethinkdb.test-utils
  (:require [rethinkdb.query :as r]))

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
  ;; Add token to help distinguish between setup and test code when tracing responses
  (with-open [conn (r/connect :db test-db :token 5000)]
    (-> (r/table test-table)
        (r/delete {:durability :soft :return-changes false})
        (r/run conn))
    (test-fn)))

(defn setup-once [test-fn]
  ;; Add token to help distinguish between setup and test code when tracing responses
  (with-open [conn (r/connect :token 4000)]
    (ensure-db test-db conn)
    (ensure-table test-table {:primary-key :national_no} conn)
    (test-fn)
    (r/run (r/db-drop test-db) conn)))
