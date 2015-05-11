(ns rethinkdb.core-test
  (:require [clj-time.core :as t]
            [clojure.test :refer :all]
            [rethinkdb.core :refer :all]
            [rethinkdb.query :as r]))

(def conn (connect))
(def test-db "cljrethinkdb_test")

(defmacro db-run [& body]
  (cons 'do (for [term body]
              `(-> (r/db test-db)
                   ~term
                   (r/run ~'conn)))))

(defn split-map [m]
  (map (fn [[k v]] {k v}) m))

(defn run [term]
  (r/run term conn))

(def pokemons [{:national_no 25
                :name "Pikachu"
                :type ["Electric"]}
               {:national_no 81
                :name "Magnemite"
                :type ["Electric" "Steel"]}])

(def bulbasaur {:national_no 1
                :name "Bulbasaur"
                :type ["Grass" "Poison"]})

(defn setup [test-fn]
  (if (some #{test-db} (r/run (r/db-list) conn))
    (r/run (r/db-drop test-db) conn))
  (r/run (r/db-create test-db) conn)
  (test-fn))

(defn between-notional-no [from to]
  (-> (r/db test-db)
      (r/table :pokedex)
      (r/between from to {:right-bound :closed})))

(def min-to-max
  (-> (r/db test-db)
      (r/table :pokedex)
      (r/between r/minval r/maxval {:right-bound :closed})))

(defn with-name [name]
  (-> (r/db test-db)
      (r/table :pokedex)
      (r/filter (r/fn [row]
                  (r/eq (r/get-field row :name) name)))))

(deftest core-test
  (with-open [conn (connect)]
    (testing "manipulating databases"
      (is (= 1 (:dbs_created (r/run (r/db-create "cljrethinkdb_tmp") conn))))
      (is (= 1 (:dbs_dropped (r/run (r/db-drop "cljrethinkdb_tmp") conn))))
      (is (contains? (set (r/run (r/db-list) conn)) test-db)))

    (testing "manipulating tables"
      (db-run (r/table-create :tmp))
      (are [term result] (contains? (set (split-map (db-run term))) result)
        (-> (r/table :tmp)
            (r/insert {:id (java.util.UUID/randomUUID)}))            {:inserted 1}
        (r/table-create :pokedex {:primary-key :national_no})        {:tables_created 1}
        (r/table-drop :tmp) {:tables_dropped 1}
        (-> (r/table :pokedex) (r/index-create :tmp (r/fn [row] 1))) {:created 1}
        (-> (r/table :pokedex)
            (r/index-create :type (r/fn [row]
                                    (r/get-field row :type))))       {:created 1}
        (-> (r/table :pokedex) (r/index-rename :tmp :xxx))           {:renamed 1}
        (-> (r/table :pokedex) (r/index-drop :xxx))                  {:dropped 1})
      (is (= ["type"] (db-run (-> (r/table :pokedex) r/index-list)))))

    (testing "writing data"
      (are [term result] (contains? (set (split-map (db-run term))) result)
        (-> (r/table :pokedex) (r/insert bulbasaur))          {:inserted 1}
        (-> (r/table :pokedex) (r/insert pokemons))           {:inserted 2}
        (-> (r/table :pokedex)
            (r/get 1)
            (r/update {:japanese "Fushigidane"}))             {:replaced 1}
        (-> (r/table :pokedex)
            (r/get 1)
            (r/replace (merge bulbasaur {:weight "6.9 kg"}))) {:replaced 1}
        (-> (r/table :pokedex) (r/get 1) r/delete)            {:deleted 1}
        (-> (r/table :pokedex) r/sync)                        {:synced 1}))

    (testing "transformations"
      (is (= [25 81] (db-run (-> (r/table :pokedex)
                                 (r/order-by {:index (r/asc :national_no)})
                                 (r/map (r/fn [row]
                                          (r/get-field row :national_no))))))))

    (testing "selecting data"
      (is (= (set (db-run (r/table :pokedex))) (set pokemons)))
      (is (= (db-run (-> (r/table :pokedex) (r/get 25))) (first pokemons)))
      (is (= (into [] (db-run (-> (r/table :pokedex) (r/get-all [25 81])))) pokemons))
      (is (= (run min-to-max) pokemons))
      (is (= (run (between-notional-no 80 81)) [(last pokemons)]))
      (is (= (run (with-name "Pikachu")) [(first pokemons)])))

    (testing "aggregation"
      (are [term result] (= (run term) result)
        (r/avg [2 4]) 3
        (r/min [4 2]) 2
        (r/max [4 6]) 6
        (r/sum [3 4]) 7))

    (testing "feeds"
      (let [tmp-conn (connect)
            changes (future
                      (-> (r/db test-db)
                          (r/table :pokedex)
                          r/changes
                          (r/run tmp-conn)))]
        (Thread/sleep 500)
        (db-run (-> (r/table :pokedex)
                    (r/insert (take 10000 (repeat {:name "Test"})))))
        (is (= "Test" ((comp :name :new_val) (first @changes))))
        (close tmp-conn)))

    (testing "document manipulation"
      (is (= (db-run (-> (r/table :pokedex)
                         (r/get 25)
                         (r/without [:type :name]))) {:national_no 25})))

    (testing "string manipulating"
      (are [term result] (= (run term) result)
        (r/match "pikachu" "^pika")         {:str "pika" :start 0 :groups [] :end 4}
        (r/split "split this string")       ["split" "this" "string"]
        (r/split "split,this string" ",")   ["split" "this string"]
        (r/split "split this string" " " 1) ["split" "this string"]
        (r/upcase "Shouting")               "SHOUTING"
        (r/downcase "Whispering")           "whispering"))

    (testing "dates and times"
      (are [term result] (= (run term) result)
        (r/time 2014 12 31)                     (t/date-time 2014 12 31)
        (r/time 2014 12 31 "+01:00")            (t/from-time-zone
                                                  (t/date-time 2014 12 31)
                                                  (t/time-zone-for-offset 1))
        (r/time 2014 12 31 10 15 30)            (t/date-time 2014 12 31 10 15 30)
        (r/epoch-time 531360000)                (t/date-time 1986 11 3)
        (r/iso8601 "2013-01-01T01:01:01+00:00") (t/date-time 2013 01 01 01 01 01)
        (r/in-timezone
          (r/time 2014 12 12) "+02:00")         (t/to-time-zone
                                                  (t/date-time 2014 12 12)
                                                  (t/time-zone-for-offset 2))
        (r/timezone
          (r/in-timezone
            (r/time 2014 12 12) "+02:00"))      "+02:00"
        (r/during (r/time 2014 12 11)
                  (r/time 2014 12 10)
                  (r/time 2014 12 12))          true
        (r/during (r/time 2014 12 11)
                  (r/time 2014 12 10)
                  (r/time 2014 12 11)
                  {:right-bound :closed})       true
        (r/date (r/time 2014 12 31 10 15 0))    (t/date-time 2014 12 31)
        (r/time-of-day
          (r/time 2014 12 31 10 15 0))          (+ (* 15 60) (* 10 60 60))
        (r/year (r/time 2014 12 31))            2014
        (r/month (r/time 2014 12 31))           12
        (r/day (r/time 2014 12 31))             31
        (r/day-of-week (r/time 2014 12 31))     3
        (r/day-of-year (r/time 2014 12 31))     365
        (r/hours (r/time 2014 12 31 10 4 5))    10
        (r/minutes (r/time 2014 12 31 10 4 5))  4
        (r/seconds (r/time 2014 12 31 10 4 5))  5
        (r/to-iso8601 (r/time 2014 12 31))      "2014-12-31T00:00:00+00:00"
        (r/to-epoch-time (r/time 1970 1 1))     0))

    (testing "control structure"
      (are [term result] (= result (run term))
        (r/branch true 1 0)                         1
        (r/branch false 1 0)                        0
        (r/coerce-to [["name" "Pikachu"]] "OBJECT") {:name "Pikachu"}
        (r/type-of [1 2 3])                         "ARRAY"
        (r/type-of {:number 42})                    "OBJECT"
        (r/json "{\"number\":42}")                  {:number 42})
      (is (= (:name (run (r/info (r/db test-db)))) "cljrethinkdb_test")))

    (testing "math and logic"
      (is (< (run (r/random 0 2)) 2))
      (are [term result] (= (run term) result)
        (r/add 2 2 2) 6
        (r/add "Hello " "from " "Tokyo") "Hello from Tokyo"
        (r/add [1 2] [3 4]) [1 2 3 4]))

    (testing "geospatial commands"
      (is (= {:type "Point" :coordinates [50 50]}
             (run (r/geojson {:type "Point" :coordinates [50 50]}))))
      (is (= "Polygon" (:type (run (r/fill (r/line [[50 51] [51 51] [51 52] [50 51]]))))))
      (is (= 104644.93094219 (run (r/distance (r/point 20 20)
                                              (r/circle (r/point 21 20) 2))))))

    (testing "configuration"
      (is (= "cljrethinkdb_test" (:name (run (r/config (r/db "cljrethinkdb_test"))))))
      (is (= "pokedex" (:name (run (-> (r/db "cljrethinkdb_test") (r/table :pokedex) r/config)))))
      (is (= "pokedex" (:name (db-run (-> (r/table :pokedex)
                                          r/status))))))

    (testing "nested fns"
      (is (= [{:a {:foo "bar"}
               :b [1 2]}]
             (run (-> [{:foo "bar"}]
                      (r/map (r/fn [x]
                               {:a x
                                :b (-> [1 2]
                                       (r/map (r/fn [x]
                                                x)))}))))))
      (is (= [{:a {:foo "bar"}
               :b [{:foo "bar"} {:foo "bar"}]}]
            (run (-> [{:foo "bar"}]
                      (r/map (r/fn [x]
                               {:a x
                                :b (-> [1 2]
                                       (r/map (r/fn [y]
                                                x)))})))))))
    (close conn)))

(use-fixtures :once setup)
