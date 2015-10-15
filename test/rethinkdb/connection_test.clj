(ns rethinkdb.connection-test
  (:require [clojure.test :refer :all]
            [rethinkdb.query :as r]
            [clojure.core.async :as async]
            [rethinkdb.test-util :as tutil])
  (:import (java.util.concurrent TimeoutException)))

;;; Reference performance
;; performance (connect)          "Elapsed time: 4021.701763 msecs"
;; performance reusing connection "Elapsed time: 3999.424348 msecs"

(defmacro timed
  "Like clojure.core/time but returns the duration"
  {:added "1.0"}
  [expr]
  `(let [start# (. System (nanoTime))]
     ~expr
     (/ (double (- (. System (nanoTime)) start#)) 1000000.0)))

(def test-db tutil/test-db)
(def test-table tutil/test-table)
(def changes-query (r/changes (r/table test-table)))
(def table-list (-> (r/db test-db) (r/table-list)))

(defn setup-each [test-fn]
  (with-open [conn (r/connect :db test-db)]
    (tutil/ensure-table test-db (name test-table) {:primary-key :national_no} conn))
  (test-fn))

(defn setup-once [test-fn]
  (with-open [conn (r/connect)]
    (tutil/ensure-db test-db conn))
  (test-fn))

;; Uncomment to run test
#_(deftest connection-speed-test
  (println "performance (connection per query)")
  (let [conn r/connect]
    (time
      (doseq [n (range 100)]
        (with-open [conn (r/connect)]
          (r/run table-list conn)))))

  (println "performance (reusing connection")
  (time
    (with-open [conn (r/connect)]
      (doseq [n (range 100)]
        (r/run table-list conn))))

  (println "performance (parallel, one connection)")
  (with-open [conn (r/connect)]
    (time
      (doall
        (pmap (fn [v] (r/run table-list conn))
              (range 100)))))

  (println "performance (pooled connection")
  #_(with-open [conn connect]
    nil)

  (println "multiple connection test")
  (let [conn1 (r/connect)
        conn2 (r/connect)
        conn3 (r/connect)]
    (r/run table-list conn1)
    (future
      (do
        (r/run table-list conn2)
        (.close conn1)))
    (future
      (with-open [conn (r/connect)]
        (r/run table-list conn)))
    (future
      (.close conn2))
    (r/run table-list conn3)
    (.close conn3)
    (is true)))

(defn run-changefeed
  "Returns the value from calling run-chan n times. Use dotimes if you don't
  need return results. Lazy."
  [n conn]
  (repeatedly n #(r/run-chan changes-query conn (async/chan))))

(deftest close-conn-sync-query-test
  (let [conn (r/connect :db test-db)]
    (dotimes [n 100] (r/run changes-query conn))
    (is (nil? (.close conn)))
    (is (empty? (:waiting @conn)))))

(deftest close-conn-chan-query-test
  (let [conn (r/connect :db test-db)
        chans (doall (map :out-ch (run-changefeed 50 conn)))]
    (is (nil? (.close conn)))
    (is (empty? (:waiting @conn)))
    (is (nil? (tutil/altout (async/merge chans))))))

(deftest close-conn-mixed-test
  (let [conn (r/connect :db test-db)
        chans (doall (map :out-ch (run-changefeed 50 conn)))]
    (dotimes [n 50] (r/run-chan changes-query conn (async/chan 1)))
    (dotimes [n 50] (r/run changes-query conn))
    (dotimes [n 50] (r/run-chan changes-query conn (async/chan 1)))
    (is (nil? (.close conn)))
    (is (empty? (:waiting @conn)))
    (is (nil? (tutil/altout (async/merge chans))))))

(deftest timeout-throw-test
  (let [conn (r/connect :db test-db :close-timeout-ms 0)]
    (dotimes [n 100] (r/run-chan changes-query conn (async/chan 1)))
    (is (thrown-with-msg? TimeoutException
                          #"Timed out after 0 ms waiting for a close response for all queries from RethinkDB. 100 queries were force closed."
                          (.close conn)))
    (is (empty? (:waiting @conn)))))

(deftest close-changefeed-test
  (with-open [conn (r/connect :db test-db)]
    (let [stop-fns (doall (map :stop-fn (run-changefeed 50 conn)))]
      (is (= 50 (count (:waiting @conn))))
      (is (every? nil? (map (fn [f] (f)) stop-fns)))
      (is (zero? (count (:waiting @conn)))))))

(deftest close-changefeed-timeout-test
  (let [conn (r/connect :db test-db)
        stop-fn (:stop-fn (first (run-changefeed 1 conn)))]
    (is (thrown-with-msg? TimeoutException
                         #"Timed out after 0 ms waiting for RethinkDB to respond to close query request. Query was force closed."
                         (stop-fn 0)))
    (is (empty? (:waiting @conn)))
    (.close conn)))

(use-fixtures :each setup-each)
(use-fixtures :once setup-once)
