(ns rethinkdb.core-test
  (:require [clojure.java.io :as io]
            [byte-streams :as bs]
            [clj-time.core :as t]
            [clojure.test :refer :all]
            [clojure.core.async :refer [go go-loop <! take! <!!]]
            [manifold.stream :as s]
            [rethinkdb.query :as r]
            [rethinkdb.net :as net])
  (:import (clojure.lang ExceptionInfo)
           (java.util UUID Arrays)))

(def test-db "cljrethinkdb_test")
(def test-table :pokedex)

(def pokemons [{:national_no 25
                :name        "Pikachu"
                :type        ["Electric"]}
               {:national_no 81
                :name        "Magnemite"
                :type        ["Electric" "Steel"]}])

(def bulbasaur {:national_no 1
                :name        "Bulbasaur"
                :type        ["Grass" "Poison"]})

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

(deftest manipulating-databases
  (with-open [conn (r/connect)]
    (is (= 1 (:dbs_created (r/run (r/db-create "cljrethinkdb_tmp") conn))))
    (is (= 1 (:dbs_dropped (r/run (r/db-drop "cljrethinkdb_tmp") conn))))
    (is (contains? (set (r/run (r/db-list) conn)) test-db))))

(deftest manipulating-tables
  (with-open [conn (r/connect :db test-db)]
    (testing "table-create-drop"
      (are [term key result] (= (key (r/run term conn)) result)
        (r/table-create (r/db test-db) :tmp) :tables_created 1
        (r/table-create :tmp2) :tables_created 1
        (r/insert (r/table :tmp) {:id (UUID/randomUUID)}) :inserted 1
        (r/table-drop (r/db test-db) :tmp) :tables_dropped 1
        (r/table-drop :tmp2) :tables_dropped 1))

    (testing "indexes"
      (are [term result] (= (r/run (-> (r/table test-table) term) conn) result)
        (r/index-create :name) {:created 1}
        (r/index-create :tmp (r/fn [row] 1)) {:created 1}
        (r/index-create :type (r/fn [row] (r/get-field row :type))) {:created 1}
        (r/index-rename :tmp :xxx) {:renamed 1}
        (r/index-drop :xxx) {:dropped 1}
        (r/index-list) ["name" "type"])
      (are [term key result] (= (key (first (r/run (-> (r/table test-table) term) conn))) result)
        (r/index-wait) :ready true
        (r/index-status) :index "name"
        (r/index-status) :ready true))))

(deftest manipulating-data
  (with-open [conn (r/connect :db test-db)]
    (testing "writing data"
      (are [term operation result] (= (operation (r/run (-> (r/table test-table) term) conn)) result)
        (r/insert bulbasaur) :inserted 1
        (r/insert pokemons) :inserted 2
        (-> (r/get 1) (r/update {:japanese "Fushigidane"})) :replaced 1
        (-> (r/get 1) (r/replace (merge bulbasaur {:weight "6.9 kg"}))) :replaced 1
        (-> (r/get 1) r/delete) :deleted 1
        (r/sync) :synced 1))

    (testing "merging values"
      (let [trainers [{:id 1 :name "Ash" :pokemon_ids [25 81]}]]
        (are [term result] (= (r/run term conn) result)
          (r/merge {:a {:b :c}}) {:a {:b "c"}}
          (r/merge {:a {:b :c}} {:a {:f :g}}) {:a {:b "c" :f "g"}}
          (r/merge {:a {:b :c}} {:a {:b :x}}) {:a {:b "x"}}
          (r/merge {:a 1} {:b 2} {:c 3}) {:a 1 :b 2 :c 3})
        (is (= #{"Pikachu" "Magnemite"}
               (->> (r/run (-> trainers
                               (r/merge (r/fn [row]
                                          {:pokemons (-> (r/table test-table)
                                                         (r/get-all (r/get-field row "pokemon_ids"))
                                                         (r/map (r/fn [row] (r/get-field row :name)))
                                                         (r/coerce-to "array"))})))
                           conn)
                   first
                   :pokemons
                   (into #{}))))))

    (testing "selecting data"
      (is (= (set (r/run (r/table test-table) conn)) (set pokemons)))
      (is (= (r/run (-> (r/table test-table) (r/get 25)) conn) (first pokemons)))
      (is (= (set (r/run (-> (r/table test-table) (r/get-all [25 81])) conn)) (set pokemons)))
      (is (= pokemons (sort-by :national_no (r/run (-> (r/table test-table)
                                                     (r/between r/minval r/maxval {:right-bound :closed})) conn))))
      (is (= (r/run (-> (r/table test-table)
                        (r/between 80 81 {:right-bound :closed})) conn) [(last pokemons)]))
      (is (= (r/run (-> (r/db test-db)
                        (r/table test-table)
                        (r/filter (r/fn [row]
                                    (r/eq (r/get-field row :name) "Pikachu")))) conn) [(first pokemons)]))
      (is (= 1 (r/run (r/get-field {:a 1} :a) conn))))

    (testing "selecting data with optargs"
      (is (= (-> (r/table nil test-table {:read-mode :majority}) (r/get 25) (r/get-field :name) (r/run conn))
             "Pikachu"))
      (is (= (-> (r/db test-db) (r/table test-table {:read-mode :majority}) (r/get 25) (r/get-field :name) (r/run conn))
             "Pikachu")))

    (testing "default values"
      (is (= "not found" (r/run (-> (r/get-field {:a 1} :b) (r/default "not found")) conn)))
      (is (= "not found" (r/run (-> (r/max [nil]) (r/default "not found")) conn)))
      (is (= "Cannot take the average of an empty stream.  (If you passed `avg` a field name, it may be that no elements of the stream had that field.)"
             (r/run (-> (r/avg [nil]) (r/default (r/fn [row] row))) conn))))))

(deftest transformations
  (with-open [conn (r/connect :db test-db)]
    (testing "order-by + map"
      (is (= [25 81] ((r/run (-> (r/table test-table) (r/insert pokemons)) conn)
                      (r/run (-> (r/table test-table) r/sync) conn)
                      (r/run (-> (r/table test-table)
                                 (r/order-by {:index (r/asc :national_no)})
                                 (r/map (r/fn [row]
                                          (r/get-field row :national_no))))
                             conn)))))

    (testing "skip"
      (are [term result] (= (r/run term conn) result)
        (r/skip [0 1 2 3 4 5 6 7] 3) [3 4 5 6 7]
        (r/skip [0 1 2 3 4 5 6 7] 8) []))

    (testing "limit"
      (are [term result] (= (r/run term conn) result)
        (r/limit [0 1 2 3 4 5 6 7] 3) [0 1 2]
        (r/limit [0 1 2 3 4 5 6 7] 5) [0 1 2 3 4]))

    (testing "slice"
      (are [term result] (= (r/run term conn) result)
        (r/slice [0 1 2 3 4 5 6 7] 2 4) [2 3]
        (r/slice [0 1 2 3 4 5 6 7] 0 4) [0 1 2 3]))

    (testing "nth"
      (are [term result] (= (r/run term conn) result)
        (r/nth [0 1 2 3 4 5 6 7] 0) 0
        (r/nth [0 1 2 3 4 5 6 7] 2) 2
        (r/nth [0 1 2 3 4 5 6 7] -1) 7
        (r/nth [0 1 2 3 4 5 6 7] -3) 5))

    (testing "offsets-of"
      (are [term result] (= (r/run term conn) result)
        (r/offsets-of [0 1 2 3 4 5 6 7] 3) [3]
        (r/offsets-of ['a' 'b' 'c'] 'b') [1]
        (r/offsets-of ['a' 'b' 'c' 'b'] 'b') [1 3]))

    (testing "is-empty"
      (are [term result] (= (r/run term conn) result)
        (r/is-empty [0 1 2 3 4 5 6 7]) false
        (r/is-empty []) true))

    (testing "union"
      (are [term result] (= (r/run term conn) result)
        (r/union [0 1 2 3] [4 5 6 7]) [0 1 2 3 4 5 6 7]
        (r/union [0 1 2 3] []) [0 1 2 3]))

    (testing "sample"
      (is (clojure.set/subset? (set (r/run (r/sample [1 2 3 4 5] 3) conn)) #{1 2 3 4 5}))
      (is (= (count (r/run (r/sample [1 2 3 4 5] 3) conn)) 3))
      (is (= (set (r/run (r/sample [1 2 3 4 5] 10) conn)) #{1 2 3 4 5})))))

(deftest db-in-connection
  (testing "run a query with an implicit database"
    (with-open [conn (r/connect :db test-db)]
      (-> (r/table-create "test_table") (r/run conn))
      (is (= [(name test-table) "test_table"]
             (-> (r/table-list) (r/run conn))))
      (-> (r/table-drop "test_table") (r/run conn))))
  (testing "precedence of db connections"
    (with-open [conn (r/connect :db "nonexistent_db")]
      (is (= [(name test-table)]
             (-> (r/db test-db) (r/table-list) (r/run conn)))))))

(deftest aggregation
  (with-open [conn (r/connect)]
    (are [term result] (= (r/run term conn) result)
      (r/group [1 6 1 8] (r/fn [e] e)) {1 [1 1], 6 [6], 8 [8]}
      (r/group [{:x 1 :y 2} {:x 1 :y 2} {:x 1 :y 3}] [:x :y]) {[1 2] [{:x 1 :y 2}
                                                                      {:x 1 :y 2}]
                                                               [1 3] [{:x 1 :y 3}]}
      (r/ungroup (r/group [1 6 1 8] (r/fn [e] e))) [{:group 1, :reduction [1 1]}
                                                    {:group 6, :reduction [6]}
                                                    {:group 8, :reduction [8]}]
      (r/reduce [1] (r/fn [a b] (r/add a b))) 1
      (r/reduce [1 6 1 8] (r/fn [a b] (r/add a b))) 16
      (r/count [1 6 1 8]) 4
      (r/count "asdf") 4
      (r/count {:a 1 :b 2 :c 3}) 3
      (r/sum [3 4]) 7
      (r/avg [2 4]) 3
      (r/min [4 2]) 2
      (r/max [4 6]) 6
      (r/distinct [1 6 1 8]) [1 6 8]
      (r/contains [1 6 1 8] 1) true
      (r/contains [1 6 1 8] 2) false)))

(deftest changefeeds
  (with-open [conn (r/connect)]
    (let [docs (map #(hash-map :n %) (range 100))
          changes-chan (-> (r/db test-db)
                           (r/table test-table)
                           (r/changes {:include-initial true})
                           (r/run conn {:async? true}))
          changes (-> (r/db test-db)
                      (r/table test-table)
                      (r/changes {:include-initial true})
                      (r/run conn))]
      (doseq [doc docs]
        (r/run (-> (r/db test-db)
                   (r/table test-table)
                   (r/insert doc))
               conn))
      (let [received (s/stream)]
        (go-loop []
          (s/put! received (get-in (<! changes-chan) [:new_val :n]))
          (recur))
        (= (range 100)
           (map #(get-in % [:new_val :n]) (take 100 changes))
           (take 100 (s/stream->seq received)))))))

(deftest core.async
  (with-open [conn (r/connect :async? true)]
    (-> (r/db test-db)
        (r/table test-table)
        (r/insert {:national_no 172 :name "Pichu"})
        (r/run conn {:async? false}))
    (is (= [{:national_no 172 :name "Pichu"}]
           (<!! (-> (r/db test-db)
                    (r/table test-table)
                    (r/run conn)))))))

(deftest document-manipulation
  (with-open [conn (r/connect :db test-db)]
    (testing "pluck"
      (let [o {:a 1 :y 2}
            a [{:x 1 :y 2}
               {:x 3 :y 4}]]
        (are [term result] (= (r/run term conn) result)
          (r/pluck o :x) (select-keys o [:x])
          (r/pluck o [:x :y]) (select-keys o [:x :y])
          (r/pluck a :x) (map #(select-keys % [:x]) a)
          (r/pluck a [:x :y]) (map #(select-keys % [:x :y]) a))))

    (testing "without"
      (r/run (-> (r/table test-table) (r/insert pokemons)) conn)
      (is (= {:national_no 25}
             (r/run (-> (r/table test-table)
                        (r/get 25)
                        (r/without [:type :name])) conn))))

    (testing "array manipulation"
      (are [term result] (= (r/run term conn) result)
        (r/append [1 2 3] 9) [1 2 3 9]
        (r/prepend [1 2 3] 9) [9 1 2 3]
        (r/difference [1 2 3 1 4 2] [1 2]) [3 4]
        (r/insert-at [1 2 3] 0 9) [9 1 2 3]
        (r/insert-at [1 2 3] 1 9) [1 9 2 3]
        (r/splice-at [1 2 3] 1 [8 9]) [1 8 9 2 3]
        (r/splice-at [1 2 3] 2 [8 9]) [1 2 8 9 3]
        (r/delete-at [1 2 3] 0) [2 3]
        (r/delete-at [1 2 3] 2) [1 2]
        (r/change-at [1 2 3] 0 9) [9 2 3]
        (r/change-at [1 2 3] 1 9) [1 9 3]))

    (testing "set manipulation"
      (are [term result] (= (r/run term conn) result)
        (r/set-insert [1 2 3] 3) [1 2 3]
        (r/set-insert [1 2 3] 4) [1 2 3 4]
        (r/set-union [1 2 3] [4 5 6]) [1 2 3 4 5 6]
        (r/set-union [1 2 3] [1 2 4]) [1 2 3 4]
        (r/set-intersection [1 2 3] [1 2 4]) [1 2]
        (r/set-intersection [1 2 3] [4 5 6]) []
        (r/set-difference [1 2 3] [1 2 4]) [3]
        (r/set-difference [1 2 3] [4 5 6]) [1 2 3]))

    (testing "has-fields"
      (let [o {:x 1 :y 2}
            a [{:x 1 :y 2}
               {:x 3 :z 4}]]
        (are [term result] (= (r/run term conn) result)
          (r/has-fields o :x) true
          (r/has-fields o :z) false
          (r/has-fields a :x) a
          (r/has-fields a :y) [(first a)])))

    (testing "literal-values"
      (are [term result] (= (r/run term conn) result)
        (r/object :a 1) {:a 1}
        (r/keys (r/object :a 1)) ["a"]
        (r/values (r/object :a 1)) [1]))))

(deftest string-manipulation
  (with-open [conn (r/connect)]
    (are [term result] (= (r/run term conn) result)
      (r/match "pikachu" "^pika") {:str "pika" :start 0 :groups [] :end 4}
      (r/split "split this string") ["split" "this" "string"]
      (r/split "split,this string" ",") ["split" "this string"]
      (r/split "split this string" " " 1) ["split" "this string"]
      (r/upcase "Shouting") "SHOUTING"
      (r/downcase "Whispering") "whispering"

      ;; UUIDs
      (r/uuid "slava@example.com") "90691cbc-b5ea-5826-ae98-951e30fc3b2d"
      (r/uuid "a") "d0333a3b-39b1-5201-b37a-7bfbf6542b5f")
    (is (instance? UUID (UUID/fromString (r/run (r/uuid) conn))))))

(deftest dates-and-times
  (with-open [conn (r/connect)]
    (is (< (-> (t/interval (r/run (r/now) conn) (t/now))
               (t/in-seconds))
           1))

    (are [term result] (= (r/run term conn) result)
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

(deftest control-structures
  (with-open [conn (r/connect)]
    (are [term result] (= result (r/run term conn))
      (r/branch true 1 0) 1
      (r/branch false 1 0) 0
      (r/limit (r/range) 4) [0 1 2 3]
      (r/range 5) [0 1 2 3 4]
      (r/range 3 5) [3 4]
      (r/coerce-to [["name" "Pikachu"]] "OBJECT") {:name "Pikachu"}
      (r/coerce-to 1 "STRING") "1"
      (r/coerce-to "1" "NUMBER") 1
      (r/type-of [1 2 3]) "ARRAY"
      (r/type-of {:number 42}) "OBJECT"
      (r/json "{\"number\":42}") {:number 42})
    (is (= (:url (r/run (r/http "http://httpbin.org/get") conn)) "http://httpbin.org/get"))))

(deftest math-and-logic
  (with-open [conn (r/connect)]
    (are [term result] (= (r/run term conn) result)
      (r/add 2 2 2) 6
      (r/add "Hello " "from " "Tokyo") "Hello from Tokyo"
      (r/add [1 2] [3 4]) [1 2 3 4]
      (r/sub 7 2) 5
      (r/sub (r/now) (r/sub (r/now) 60)) 60
      (r/mul 2 3) 6
      (r/mul ["Hi" "there"] 2) ["Hi" "there" "Hi" "there"]
      (r/mul [1 2] 3) [1 2 1 2 1 2]
      (r/div 6 3) 2
      (r/div 7 2) 3.5
      (r/mod 2 2) 0
      (r/mod 3 2) 1
      (r/mod 6 2) 0
      (r/mod 8 3) 2
      (r/or false false) false
      (r/any false true) true
      (r/all true true) true
      (r/and true false) false
      (r/not true) false
      (r/not false) true)

    (are [args lt-le-eq-ne-ge-gt] (= (r/run (r/make-array
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

    (is (<= 0 (r/run (r/random 0 2) conn) 2))

    (are [n floor-round-ceil] (= (r/run (r/make-array (r/floor n) (r/round n) (r/ceil n)) conn) floor-round-ceil)
      0 [0 0 0]
      0.1 [0 0 1]
      1.499999999 [1 1 2]
      1.5 [1 2 2]
      1.5M [1 2 2]
      3.99999999 [3 4 4]
      -5.1 [-6 -5 -5]
      1/2 [0 1 1])))

(deftest geospatial-commands
  (with-open [conn (r/connect :db test-db)]
    (def geo-table "geo")
    (ensure-table geo-table {:primary-key :name} conn)
    (r/run (r/index-create (r/table geo-table) :area nil {:geo true}) conn)
    (r/run (r/index-wait (r/table geo-table)) conn)
    (def geo-data [{:name "A" :area (r/circle (r/point 20 20) 100)}
                   {:name "B" :area (r/circle (r/point 30 30) 100)}
                   {:name "C" :area (r/circle (r/point 40 40) 100)}])
    (r/run (r/insert (r/table geo-table) geo-data) conn)
    (is (= "A" (-> (r/run (r/get-nearest (r/table geo-table) (r/point 20 20) {:index :area}) conn)
                   (first)
                   (:doc)
                   (:name))))
    (is (= "B" (-> (r/run (r/get-intersecting (r/table geo-table) (r/point 30 30) {:index :area}) conn)
                   (first)
                   (:name))))
    (r/run (r/table-drop geo-table) conn)

    (are [term result] (= (r/run term conn) result)
      (r/geojson {:type "Point" :coordinates [50 50]}) {:type "Point" :coordinates [50 50]}
      (r/fill (r/line [[50 51] [51 51] [51 52] [50 51]])) {:type "Polygon" :coordinates [[[50 51] [51 51] [51 52] [50 51]]]}
      (r/distance (r/point 20 20) (r/circle (r/point 21 20) 2)) 104644.93094219
      (r/to-geojson (r/point 20 20)) {:type "Point" :coordinates [20 20]}
      (r/includes (r/circle (r/point 20 20) 2) (r/point 20 20)) true
      (r/includes (r/circle (r/point 20 20) 1) (r/point 40 40)) false
      (r/intersects (r/circle (r/point 20 20) 30) (r/point 21 20)) false
      (r/intersects (r/circle (r/point 20 20) 10) (r/point 20 20)) true
      (r/intersects (r/circle (r/point 20 20) 3) (r/circle (r/point 20 20) 1)) true
      (r/intersects (r/circle (r/point 20 20) 3) (r/circle (r/point 21 20) 1)) false
      (r/polygon [[50 51] [51 51] [51 52] [50 51]]) {:type "Polygon" :coordinates [[[50 51] [51 51] [51 52] [50 51]]]}
      (r/polygon-sub (r/polygon [[0 9] [0 6] [3 6] [3 9]])
                     (r/polygon [[1 7] [1 8] [2 8] [2 7]])) {:type "Polygon" :coordinates [[[0 9] [0 6] [3 6] [3 9] [0 9]] [[1 7] [1 8] [2 8] [2 7] [1 7]]]})))

(deftest administration
  (with-open [conn (r/connect :db test-db)]
    (testing "info"
      (is (= "cljrethinkdb_test" (:name (r/run (r/info (r/db test-db)) conn)))))
    (testing "configuration"
      (is (= "cljrethinkdb_test" (:name (r/run (r/config (r/db test-db)) conn))))
      (is (= "pokedex" (:name (r/run (-> (r/db test-db) (r/table test-table) r/config) conn)))))
    (testing "rebalance"
      (is (= (-> (r/db test-db) (r/rebalance) (r/run conn) (keys))
             [:rebalanced :status_changes])))
    (testing "reconfigure"
      (is (= {:reconfigured 0}
             (-> (r/db test-db) (r/reconfigure {:shards 1 :replicas 1 :dry-run true}) (r/run conn)
                 (select-keys [:reconfigured])))))
    (testing "status"
      (is (= "pokedex" (:name (r/run (-> (r/db test-db) (r/table test-table) r/status) conn)))))
    (testing "wait"
      (is (= {:ready 1} (-> (r/db test-db) (r/wait) (r/run conn))))
      (is (= {:ready 1} (-> (r/db test-db) (r/wait {:timeout 1}) (r/run conn)))))))

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
                      {:name "Bobby"}]]
      (is (= ["Hawk" "Andy" "Bobby"]
             (r/run (-> twin-peaks
                        (r/filter (r/fn [row]
                                    (r/eq (r/get-field row :job) "Deputy"))
                                  {:default true})
                        (r/get-field :name))
                    conn)))
      (is (thrown?
            Exception
            (r/run (-> twin-peaks
                       (r/filter (r/fn [row]
                                   (r/eq (r/get-field row :job) "Deputy"))
                                 {:default (r/error)})
                       (r/get-field :name))
                   conn)))
      (is (= ["Hawk" "Andy"]
             (r/run (-> twin-peaks
                        (r/filter (r/fn [row]
                                    (r/eq (r/get-field row :job) "Deputy"))
                                  {:default false})
                        (r/get-field :name))
                    conn))))))

(deftest throwing-server-exceptions
  (with-open [conn (r/connect :db test-db)]
    (is (thrown? ExceptionInfo (r/run (r/table :nope) conn)))
    (try (r/run (r/table :nope) conn)
         (catch ExceptionInfo ex
           (let [r (get-in (ex-data ex) [:response :r])
                 etype (:type (ex-data ex))
                 msg (.getMessage ex)]
             (is (= etype :op-failed))
             (is (= r ["Table `cljrethinkdb_test.nope` does not exist."]))
             (is (= "RethinkDB server: Table `cljrethinkdb_test.nope` does not exist." msg)))))))

(deftest query-conn
  (is (do (r/connect)
          true))
  (let [server-info (r/server (r/connect))]
    (is (contains? server-info :id))
    (is (contains? server-info :name)))
  (is (thrown? ExceptionInfo (r/connect :port 1)))
  (with-redefs {#'core/version 168696}
    #(is (thrown? ExceptionInfo (r/connect)))))

(deftest dont-leak-auth-key
  (try (r/connect :port 28016 :auth-key "super secret")
       (catch ExceptionInfo e
         (is (= "<auth key provided but hidden>"
                (:auth-key (ex-data e))))))
  (try (r/connect :port 28016)
       (catch ExceptionInfo e
         (is (= ""
                (:auth-key (ex-data e)))))))

(deftest utf8-compliance
  (with-open [conn (r/connect :db test-db)]
    (let [doc {:national_no (UUID/randomUUID)
               :text "üöä"}]
      (testing "writing UTF-8"
        (is (= (-> (r/table test-table)
                   (r/insert [doc])
                   (r/run conn)
                   :inserted)
               1)))
      (testing "reading UTF-8"
        (is (= (-> (r/table test-table)
                   (r/get (:national_no doc))
                   (r/run conn)
                   :text)
               (:text doc)))))))

(deftest namespaced-keywords
  (with-open [conn (r/connect :db test-db)]
    (let [id (UUID/randomUUID)]
      (is (-> (r/table test-table)
              (r/insert [{:national_no id
                          :namespaced/keyword "value"}])
              (r/run conn))
          (= (-> (r/table test-table)
                 (r/get id)
                 (r/run conn))
             {:national_no id
              :namespaced/keyword "value"})))))

(use-fixtures :each setup-each)
(use-fixtures :once setup-once)

(deftest binary
  (with-open [conn (r/connect :db test-db)]
    (let [file (io/file (io/resource "pikachu.png"))
          file-bytes (bs/to-byte-array file)]
      (-> (r/table "pokedex")
          (r/insert {:national_no 25
                     :name        "Pikachu"
                     :image       file-bytes})
          (r/run conn))
      (let [resp (-> (r/table "pokedex") (r/run conn) first :image)]
        (is (Arrays/equals ^bytes resp ^bytes file-bytes))))))
