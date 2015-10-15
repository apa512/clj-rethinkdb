(ns rethinkdb.core-test
  (:require [clj-time.core :as t]
            [clojure.test :refer :all]
            [rethinkdb.query :as r]
            [rethinkdb.core :as core]
            [rethinkdb.test-util :as tutil :refer [test-db test-db-chan test-table]]
            [clojure.core.async :as async]
            [rethinkdb.net :as net])
  (:import [java.util UUID]
           [clojure.lang IExceptionInfo]))

(defn run-chan-sync
  "Behaves like run (blocking), but uses run-chan.
  Used for testing equivalence between run and run-chan"
  [query conn]
  (->> (r/run-chan query conn (async/chan 30))
       :out-ch
       (async/into [])
       (async/<!!)))

(defn run-chan-sync-first
  "(first (run-chan-sync query conn))"
  [query conn]
  (first (run-chan-sync query conn)))

(defn test-query-chan
  "Executes query on conn. Returns a vector of the first raw result from the connection
   from either the result or error channel, and whether the query was successful."
  [query conn]
  (let [{:keys [err-ch out-ch]} (r/run-chan query conn (async/chan 10))
        [v p] (async/alts!! [err-ch out-ch] :priority true)]
    [v (= p out-ch)]))

(defn split-map [m]
  (map (fn [[k v]] {k v}) m))

(def pokemons [{:national_no 25
                :name        "Pikachu"
                :type        ["Electric"]}
               {:national_no 81
                :name        "Magnemite"
                :type        ["Electric" "Steel"]}])

(def bulbasaur {:national_no 1
                :name        "Bulbasaur"
                :type        ["Grass" "Poison"]})

(deftest manipulating-databases
  (with-open [conn (r/connect)]
    (is (= 1 (:dbs_created (run-chan-sync-first (r/db-create "cljrethinkdb_tmp") conn))))
    (is (= 1 (:dbs_dropped (r/run (r/db-drop "cljrethinkdb_tmp") conn))))
    (is (contains? (set (r/run (r/db-list) conn)) test-db))))

(deftest manipulating-tables
  (with-open [conn (r/connect :db test-db)
              conn-chan (r/connect :db test-db-chan)]
    (doseq [conn [conn conn-chan]
            index (-> (r/table test-table) (r/index-list) (r/run conn))]
      (r/run (-> (r/table test-table) (r/index-drop index)) conn))
    (are [term selector result] (= (selector (r/run term conn))
                                   (selector (run-chan-sync-first term conn-chan))
                                   result)
      (r/table-create :tmp) :tables_created 1
      (r/table-create :tmp2) :tables_created 1
      (-> (r/table :tmp)
          (r/insert {:id (UUID/randomUUID)})) :inserted 1
      (r/table-drop :tmp) :tables_dropped 1
      (r/table-drop :tmp2) :tables_dropped 1
      (-> (r/table test-table) (r/index-create :name)) :created 1
      (-> (r/table test-table) (r/index-create :tmp (r/fn [row] 1))) :created 1
      (-> (r/table test-table)
          (r/index-create :type (r/fn [row]
                                  (r/get-field row :type)))) :created 1
      (-> (r/table test-table) (r/index-rename :tmp :xxx)) :renamed 1
      (-> (r/table test-table) (r/index-drop :xxx)) :dropped 1)

    (testing "insert equivalence"
      (let [q (-> (r/table test-table) (r/insert {:national_no 76 :name "Golem"}))]
        (is (= (r/run q conn) (run-chan-sync-first q conn-chan)))))

    (is (= ["name" "type"] (run-chan-sync (-> (r/table test-table) r/index-list) conn)))
    (r/run (-> (r/table test-table) (r/index-drop :name)) conn)
    (r/run (-> (r/table test-table) (r/index-drop :type)) conn)))

(deftest manipulating-data
  (with-open [conn (r/connect :db test-db)
              conn-chan (r/connect :db test-db-chan)]
    (testing "writing data"
      (are [term selector result] (= (selector (r/run term conn))
                                     (selector (run-chan-sync-first term conn-chan))
                                     result)
        (-> (r/table test-table) (r/insert bulbasaur)) :inserted 1
        (-> (r/table test-table) (r/insert pokemons)) :inserted 2
        (-> (r/table test-table)
            (r/get 1)
            (r/update {:japanese "Fushigidane"})) :replaced 1
        (-> (r/table test-table)
            (r/get 1)
            (r/replace (merge bulbasaur {:weight "6.9 kg"}))) :replaced 1
        (-> (r/table test-table) (r/get 1) r/delete) :deleted 1
        (-> (r/table test-table) r/sync) :synced 1))

    (testing "transformations"
      (let [query (-> (r/table test-table)
                      (r/order-by {:index (r/asc :national_no)})
                      (r/map (r/fn [row]
                               (r/get-field row :national_no))))]
        (is (= [25 81] (r/run query conn) (run-chan-sync query conn-chan)))))

    (testing "order-by results with and without indexes"
      (is (= (run-chan-sync (-> (r/table test-table) (r/order-by {:index (r/asc :national_no)})) conn)
             (run-chan-sync (-> (r/table test-table) (r/order-by :national_no)) conn)
             (r/run (-> (r/table test-table) (r/order-by {:index (r/asc :national_no)})) conn)
             (r/run (-> (r/table test-table) (r/order-by :national_no)) conn))))

    (testing "merging values"
      (are [term result] (= (r/run term conn) (run-chan-sync-first term conn-chan) result)
        (r/merge {:a {:b :c}}) {:a {:b "c"}}
        (r/merge {:a {:b :c}} {:a {:f :g}}) {:a {:b "c" :f "g"}}
        (r/merge {:a {:b :c}} {:a {:b :x}}) {:a {:b "x"}}
        (r/merge {:a 1} {:b 2} {:c 3}) {:a 1 :b 2 :c 3}))
    ;; TODO: test with the other types that merge can take

    (testing "selecting data"
      (let [q (r/table test-table)
            q2 (-> (r/table test-table) (r/get 25))
            q3 (-> (r/table test-table) (r/get-all [25 81]))
            q4 (-> (r/table test-table)
                   (r/between r/minval r/maxval {:right-bound :closed}))
            q5 (-> (r/table test-table)
                   (r/between 80 81 {:right-bound :closed}))
            q6 (-> (r/db test-db)
                   (r/table test-table)
                   (r/filter (r/fn [row]
                               (r/eq (r/get-field row :name) "Pikachu"))))
            q7 (r/get-field {:a 1} :a)]
        (is (= (set (r/run q conn)) (set (run-chan-sync q conn-chan)) (set pokemons)))
        (is (= (r/run q2 conn) (run-chan-sync-first q2 conn) (first pokemons)))
        (is (= (set (r/run q3 conn)) (set (run-chan-sync q3 conn)) (set pokemons)))
        (is (= (sort-by :national_no (r/run q4 conn))
               (sort-by :national_no (run-chan-sync q4 conn))
               pokemons))
        (is (= (r/run q5 conn) (run-chan-sync q5 conn) [(last pokemons)]))
        (is (= (r/run q6 conn) (run-chan-sync q6 conn) [(first pokemons)]))
        (is (= 1 (r/run q7 conn) (first (run-chan-sync q7 conn))))))

    (testing "default values"
      (is (= "not found" (r/run (-> (r/get-field {:a 1} :b) (r/default "not found")) conn)))
      (is (= "not found" (r/run (-> (r/max [nil]) (r/default "not found")) conn)))
      (is (= "Cannot take the average of an empty stream.  (If you passed `avg` a field name, it may be that no elements of the stream had that field.)"
             (r/run (-> (r/avg [nil]) (r/default (r/fn [row] row))) conn))))))

(deftest db-in-connection
  (testing "run a query with an implicit database"
    (with-open [conn (r/connect :db test-db)]
      (is (= [(name test-table)]
             (-> (r/table-list) (r/run conn))))))
  (testing "precedence of db connections"
    (with-open [conn (r/connect :db "nonexistent_db")]
      (is (= [(name test-table)]
             (-> (r/db test-db) (r/table-list) (r/run conn)))))))

(deftest aggregation
  (with-open [conn (r/connect)]
    (are [term result] (= (r/run term conn) (run-chan-sync-first term conn) result)
      (r/avg [2 4]) 3
      (r/min [4 2]) 2
      (r/max [4 6]) 6
      (r/sum [3 4]) 7)))

(deftest changefeeds
    (with-open [conn (r/connect)]
      (let [changes (future
                      (-> (r/db test-db)
                          (r/table test-table)
                          r/changes
                          (r/run conn)))]
        (Thread/sleep 500)
        (r/run (-> (r/db test-db)
                   (r/table test-table)
                   (r/insert (take 2 (repeat {:name "Test"})))) conn)
        (is (= "Test" ((comp :name :new_val) (first @changes))))))
    (with-open [conn (r/connect :db test-db)]
      (let [changes (future
                      (-> (r/db test-db)
                          (r/table test-table)
                          r/changes
                          (r/run conn)))]
        (Thread/sleep 500)
        (r/run (-> (r/table test-table)
                   (r/insert (take 2 (repeat {:name "Test"}))))
               conn)
        (is (= "Test" ((comp :name :new_val) (first @changes)))))))

(deftest document-manipulation
  (with-open [conn (r/connect :db test-db)]
    (r/run (-> (r/table test-table) (r/insert pokemons)) conn)
    (let [q (-> (r/table test-table)
                (r/get 25)
                (r/without [:type :name]))]
      (is (= {:national_no 25}
             (run-chan-sync-first q conn)
             (r/run q conn))))))

(deftest string-manipulating
  (with-open [conn (r/connect)]
    (are [term result] (= (r/run term conn) (run-chan-sync term conn) result)
      (r/split "split this string") ["split" "this" "string"]
      (r/split "split,this string" ",") ["split" "this string"]
      (r/split "split this string" " " 1) ["split" "this string"])

    (are [term result] (= (r/run term conn) (run-chan-sync-first term conn) result)
      (r/match "pikachu" "^pika") {:str "pika" :start 0 :groups [] :end 4}
      (r/upcase "Shouting") "SHOUTING"
      (r/downcase "Whispering") "whispering")))

(deftest dates-and-times
  (with-open [conn (r/connect)]
    (are [term result] (= (r/run term conn) (run-chan-sync-first term conn) result)
      (r/time 2014 12 31) (t/date-time 2014 12 31)
      (r/time 2014 12 31 "+01:00") (t/from-time-zone
                                     (t/date-time 2014 12 31)
                                     (t/time-zone-for-offset 1))
      (r/time 2014 12 31 10 15 30) (t/date-time 2014 12 31 10 15 30)
      (r/epoch-time 531360000) (t/date-time 1986 11 3)
      (r/iso8601 "2013-01-01T01:01:01+00:00") (t/date-time 2013 01 01 01 01 01)
      (r/in-timezone
        (r/time 2014 12 12) "+02:00") (t/to-time-zone
                                        (t/date-time 2014 12 12)
                                        (t/time-zone-for-offset 2))
      (r/timezone
        (r/in-timezone
          (r/time 2014 12 12) "+02:00")) "+02:00"
      (r/during (r/time 2014 12 11)
                (r/time 2014 12 10)
                (r/time 2014 12 12)) true
      (r/during (r/time 2014 12 11)
                (r/time 2014 12 10)
                (r/time 2014 12 11)
                {:right-bound :closed}) true
      (r/date (r/time 2014 12 31 10 15 0)) (t/date-time 2014 12 31)
      (r/time-of-day
        (r/time 2014 12 31 10 15 0)) (+ (* 15 60) (* 10 60 60))
      (r/year (r/time 2014 12 31)) 2014
      (r/month (r/time 2014 12 31)) 12
      (r/day (r/time 2014 12 31)) 31
      (r/day-of-week (r/time 2014 12 31)) 3
      (r/day-of-year (r/time 2014 12 31)) 365
      (r/hours (r/time 2014 12 31 10 4 5)) 10
      (r/minutes (r/time 2014 12 31 10 4 5)) 4
      (r/seconds (r/time 2014 12 31 10 4 5)) 5
      (r/to-iso8601 (r/time 2014 12 31)) "2014-12-31T00:00:00+00:00"
      (r/to-iso8601 (r/time 2014 12 31 "+02:00")) "2014-12-31T00:00:00+02:00"
      (r/to-iso8601 (r/time 2014 12 31 14 12 33)) "2014-12-31T14:12:33+00:00"
      (r/to-iso8601 (r/time 2014 12 31 14 12 33 "-04:00")) "2014-12-31T14:12:33-04:00"
      (r/to-epoch-time (r/time 1970 1 1)) 0)))

(deftest control-structure
  (with-open [conn (r/connect)]
    (are [term result] (= result (r/run term conn) (run-chan-sync-first term conn))
      (r/branch true 1 0) 1
      (r/branch false 1 0) 0
      (r/or false false) false
      (r/any false true) true
      (r/all true true) true
      (r/and true false) false
      (r/coerce-to [["name" "Pikachu"]] "OBJECT") {:name "Pikachu"}
      (r/type-of [1 2 3]) "ARRAY"
      (r/type-of {:number 42}) "OBJECT"
      (r/json "{\"number\":42}") {:number 42})))

(deftest math-and-logic
  (with-open [conn (r/connect)]
    (is (<= 0 (r/run (r/random 0 2) conn) 2))
    (are [term result] (= (r/run term conn) result)
      (r/add 2 2 2) 6
      (r/add "Hello " "from " "Tokyo") "Hello from Tokyo"
      (r/add [1 2] [3 4]) [1 2 3 4])

    (are [args lt-le-eq-ne-ge-gt] (= (r/run (r/make-array
                                              (apply r/lt args)
                                              (apply r/le args)
                                              (apply r/eq args)
                                              (apply r/ne args)
                                              (apply r/ge args)
                                              (apply r/gt args)) conn)
                                     (run-chan-sync (r/make-array
                                                      (apply r/lt args)
                                                      (apply r/le args)
                                                      (apply r/eq args)
                                                      (apply r/ne args)
                                                      (apply r/ge args)
                                                      (apply r/gt args)) conn)
                                     lt-le-eq-ne-ge-gt)
      [1 1] [false true true false true false]
      [0 1] [true true false true false false]
      [0 1 2 3] [true true false true false false]
      [0 1 1 2] [false true false true false false]
      [5 4 3] [false false false true true true]
      [5 4 4 3] [false false false true true false])

    (are [n floor-round-ceil] (= (r/run (r/make-array (r/floor n) (r/round n) (r/ceil n)) conn)
                                 (run-chan-sync (r/make-array (r/floor n) (r/round n) (r/ceil n)) conn)
                                 floor-round-ceil)
      0 [0 0 0]
      0.1 [0 0 1]
      1.499999999 [1 1 2]
      1.5 [1 2 2]
      1.5M [1 2 2]
      3.99999999 [3 4 4]
      -5.1 [-6 -5 -5]
      1/2 [0 1 1])))

(deftest geospatial-commands
  (with-open [conn (r/connect)]
    (let [geojson-literal (r/geojson {:type "Point" :coordinates [50 50]})
          geo-fill (r/fill (r/line [[50 51] [51 51] [51 52] [50 51]]))
          geo-distance (r/distance (r/point 20 20)
                                   (r/circle (r/point 21 20) 2))]
      (is (= {:type "Point" :coordinates [50 50]} (r/run geojson-literal conn) (run-chan-sync-first geojson-literal conn)))
      (is (= "Polygon" (:type (r/run geo-fill conn)) (:type (run-chan-sync-first geo-fill conn))))
      (is (= 104644.93094219 (r/run geo-distance conn) (run-chan-sync-first geo-distance conn))))))

(deftest configuration
  (with-open [conn (r/connect)]
    (is (= "cljrethinkdb_test" (:name (r/run (r/config (r/db test-db)) conn))))
    (is (= "pokedex" (:name (r/run (-> (r/db test-db) (r/table test-table) r/config) conn))))
    (is (= "pokedex" (:name (r/run (-> (r/db test-db) (r/table test-table) r/status) conn))))
    (is (= "cljrethinkdb_test" (:name (r/run (r/info (r/db test-db)) conn))))))

(deftest nested-fns
  (with-open [conn (r/connect)]
    (is (= [{:a {:foo "bar"}
             :b [1 2]}]
           (r/run (-> [{:foo "bar"}]
                      (r/map (r/fn [x]
                               {:a x
                                :b (-> [1 2]
                                       (r/map (r/fn [x]
                                                x)))})))
                  conn)))
    (is (= [{:a {:foo "bar"}
             :b [{:foo "bar"} {:foo "bar"}]}]
           (r/run (-> [{:foo "bar"}]
                      (r/map (r/fn [x]
                               {:a x
                                :b (-> [1 2]
                                       (r/map (r/fn [y]
                                                x)))})))
                  conn)))))

(deftest unsweetened-fns
  (with-open [conn (r/connect)]
    (is (= [{:a {:foo "bar"}
             :b [1 2]}]
           (r/run (-> [{:foo "bar"}]
                      (r/map (r/func [::x]
                                     {:a ::x
                                      :b (-> [1 2]
                                             (r/map (r/func [::x] ::x)))})))
                  conn)))
    (is (= [{:a {:foo "bar"}
             :b [{:foo "bar"} {:foo "bar"}]}]
           (r/run (-> [{:foo "bar"}]
                      (r/map (r/func [::x]
                                     {:a ::x
                                      :b (-> [1 2]
                                             (r/map (r/func [::y] ::x)))})))
                  conn)))))

(deftest filter-with-default
  (with-open [conn (r/connect)]
    (let [twin-peaks [{:name "Cole", :job "Regional Bureau Chief"}
                      {:name "Cooper", :job "FBI Agent"}
                      {:name "Riley", :job "Colonel"}
                      {:name "Briggs", :job "Major"}
                      {:name "Harry", :job "Sheriff"}
                      {:name "Hawk", :job "Deputy"}
                      {:name "Andy", :job "Deputy"}
                      {:name "Lucy", :job "Secretary"}
                      {:name "Bobby"}]
          twin-peaks-query (-> twin-peaks
                               (r/filter (r/fn [row]
                                           (r/eq (r/get-field row :job) "Deputy"))
                                         {:default true})
                               (r/get-field :name))
          twin-peaks-error (-> twin-peaks
                               (r/filter (r/fn [row]
                                           (r/eq (r/get-field row :job) "Deputy"))
                                         {:default (r/error)})
                               (r/get-field :name))]
      (is (= ["Hawk" "Andy" "Bobby"]
             (r/run twin-peaks-query conn)
             (run-chan-sync twin-peaks-query conn)))
      (is (thrown?
            Exception
            (r/run twin-peaks-error
                   conn)))
      (is (instance? IExceptionInfo (first (test-query-chan twin-peaks-error conn))))
      (is (= ["Hawk" "Andy"]
             (r/run (-> twin-peaks
                        (r/filter (r/fn [row]
                                    (r/eq (r/get-field row :job) "Deputy"))
                                  {:default false})
                        (r/get-field :name))
                    conn))))))


(deftest run-async-chan
  (with-open [conn (r/connect :db test-db-chan)]
    (let [[resp success?] (test-query-chan (r/db-drop "cljrethinkdb_nonexistentdb") conn)]
      (is (= "Database `cljrethinkdb_nonexistentdb` does not exist." (.getMessage ^Throwable resp)))
      (is (not success?)))

    (is (some #{test-db} (run-chan-sync (r/db-list) conn)))))

(deftest stress-test-chan
  (with-open [conn (r/connect :db test-db)]
    (r/run (-> (r/table test-table) (r/insert {:name "Squirtle"})) conn)
    (let [queries (repeatedly 100 #(r/run-chan (r/table test-table) conn (async/chan 1)))]
      (is (= 100 (count (into []
                              (comp (map :out-ch) (keep async/<!!))
                              queries)))))))


(deftest close-chan
  (with-open [conn (r/connect :db test-db)]
    (let [token 1 ;; we know this is the token number that will be used as it's a fresh connection.
          _ (is (not (contains? (:waiting @conn) token))) ;; Token shouldn't exist in waiting queries
          {:keys [out-ch stop-fn]} (r/run-chan (-> (r/table test-table) (r/changes)) conn (async/chan 10))]
      (is (contains? (:waiting @conn) token)) ;; Token should exist in waiting queries
      (stop-fn)
      (is (= nil (tutil/altout out-ch)))
      (is (not (contains? (:waiting @conn) token))))))

(deftest query-conn
  (is (do (r/connect)
          true))
  (is (thrown? clojure.lang.ExceptionInfo (r/connect :port 1)))
  (with-redefs-fn {#'core/send-version (fn [out] (net/send-int out 168696 4))}
    #(is (thrown? clojure.lang.ExceptionInfo (r/connect)))))

(deftest cursor-test
  (with-open [conn (r/connect :db test-db)]
    (r/run (r/index-create (r/table test-table) "by-race-id" (r/fn [row] (r/get-field row :race-id))) conn)
    (r/run (r/insert (r/table test-table) {:test "document" :race-id "id"}) conn)
    (r/run (r/index-wait (r/table test-table)) conn)
    #_(is (every? sequential? (repeatedly 50 #(-> (r/get-all (r/table test-table) ["id" "cake"]
                                                             {:index "by-race-id"}) ;; TODO: uncomment
                                                  (r/run conn)))))
    (r/run (r/index-drop (r/table test-table) "by-race-id") conn)))

(defn setup-each [test-fn]
  (with-open [conn (r/connect :db test-db)
              conn-chan (r/connect :db test-db-chan)]
    (tutil/ensure-table test-db (name test-table) {:primary-key :national_no} conn)
    (tutil/ensure-table test-db-chan (name test-table) {:primary-key :national_no} conn-chan))
  (test-fn))

(defn setup-once [test-fn]
  (with-open [conn (r/connect)]
    (tutil/ensure-db test-db conn)
    (tutil/ensure-db test-db-chan conn))
  (test-fn))

(use-fixtures :each setup-each)
(use-fixtures :once setup-once)

