(ns rethinkdb.test-util
  (:require [rethinkdb.query :as r]
            [clojure.core.async :as async]))

(def test-db "cljrethinkdb_test")
(def test-db-chan "cljrethinkdb_test_chan")
(def test-table :pokedex)

(defn ensure-table
  "Ensures that an empty table \"table-name\" exists"
  [db-name table-name optargs conn]
  (if (some #{table-name} (r/run (r/table-list) conn))
    (r/run (-> (r/table table-name) (r/delete)) conn)
    (r/run (r/table-create (r/db db-name) table-name optargs) conn))) ;; (r/table-drop table-name)

(defn ensure-db
  "Ensures that an empty database \"db-name\" exists"
  [db-name conn]
  (if (not (some #{db-name} (r/run (r/db-list) conn)))
    (r/run (r/db-create db-name) conn)))

(defn altout
  "Returns v if it gets a value from the channel, else :timeout" ;; TODO: better docstring
  [ch]
  (async/alt!! :priority true
               (async/go (async/<! ch)) ([v] v)
               (async/timeout 10) ([v] :timeout)))
