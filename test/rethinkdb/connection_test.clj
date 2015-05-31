(ns rethinkdb.connection-test
  (:require [clj-time.core :as t]
            [clojure.test :refer :all]
            [rethinkdb.core :refer :all]
            [rethinkdb.query :as r]))

(def test-db "cljrethinkdb_test")

(defn setup [test-fn]
  (with-open [conn (connect)]
    (if (some #{test-db} (r/run (r/db-list) conn))
      (r/run (r/db-drop test-db) conn))
    (r/run (r/db-create test-db) conn)
    (test-fn)))

(use-fixtures :once setup)

(def query
  (-> (r/db test-db)
      (r/table-list)))

(defn test-query [open close]
  (time
    (doseq [n (range 100)]
      (let [conn (open)]
        (r/run query conn)
        (close conn)))))

;; Uncomment to run test
(deftest connection-speed-test
  (println "performance (connect)") 
    (let [conn connect]
      (test-query conn #(close %))
      (is true))

  (println "performance reusing connection") 
    (let [conn (connect)]
      (test-query (constantly conn) identity)
      (close conn)
      (is true))

  (println "performance pooled connection") 
    (let [conn connect]
      ;(test-query conn identity)
      (is true)))

