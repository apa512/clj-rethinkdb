(ns rethinkdb.connection-test
  (:require [clj-time.core :as t]
            [clojure.test :refer :all]
            [rethinkdb.core :refer :all]
            [rethinkdb.query :as r]))

;;; Refernece performance 
;; performance (connect)          "Elapsed time: 4021.701763 msecs"
;; performance reusing connection "Elapsed time: 3999.424348 msecs"

(defmacro timed
  "Like clojure.core/time but returns the duration"
  {:added "1.0"}
  [expr]
  `(let [start# (. System (nanoTime))]
     ~expr
     (/ (double (- (. System (nanoTime)) start#)) 1000000.0)))

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
  (println "performance (connection per query)") 
    (let [conn connect]
      (test-query conn #(close %))
      (is true))

  (println "performance (reusing connection")
    (let [conn (connect)]
      (test-query (constantly conn) identity)
      (close conn)
      (is true))

  (println "performance (parallel, one connection)")
    (let [conn (connect)]
      (time
        (pmap (fn [v] (r/run query conn))
              (range 100)))
      (is true))

  (println "performance (pooled connection")  
    (let [conn connect]
      ;(test-query conn identity)
      (is true))
    
  (println "multiple connection test") 
    (let [conn1 (connect)
          conn2 (connect) 
          conn3 (connect)]
      (r/run query conn1)
      (future
        (do
          (r/run query conn2)
          (close conn2)))
      (future
        (with-open [conn (connect)]
          (r/run query conn)))
      (future
        (close conn1))
      (r/run query conn3)
      (close conn3)
      (is true)  
      (is true)))

