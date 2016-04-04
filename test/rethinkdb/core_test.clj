(ns rethinkdb.core-test
  (:require [clojure.java.io :as io]
            [clojure.data.codec.base64 :as base64]
            [byte-streams :as bs]
            [clj-time.core :as t]
            [clojure.test :refer :all]
            [clojure.core.async :refer [go go-loop <! take! <!!]]
            [manifold.stream :as s]
            [rethinkdb.query :as r]
            [rethinkdb.net :as net])
  (:import (clojure.lang ExceptionInfo)
           (java.util UUID)))

(def test-db "cljrethinkdb_test")
(def test-table :pokedex)

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
    (ensure-table (name test-table) {:primary-key :national_no} conn)
    (test-fn)
    (r/run (r/table-drop test-table) conn)))

(defn setup-once [test-fn]
  (with-open [conn (r/connect)]
    (ensure-db test-db conn)
    (test-fn)
    (r/run (r/db-drop test-db) conn)))

(deftest manipulating-databases
  (with-open [conn (r/connect)]
    (is (= 1 (:dbs_created (r/run (r/db-create "cljrethinkdb_tmp") conn))))
    (is (= 1 (:dbs_dropped (r/run (r/db-drop "cljrethinkdb_tmp") conn))))
    (is (contains? (set (r/run (r/db-list) conn)) test-db))))

(deftest manipulating-tables
  (with-open [conn (r/connect :db test-db)]
    (are [term result] (contains? (set (split-map (r/run term conn))) result)
      (r/table-create (r/db test-db) :tmp) {:tables_created 1}
      (r/table-create (r/db test-db) :tmp2) {:tables_created 1}
      (-> (r/table :tmp)
          (r/insert {:id (UUID/randomUUID)})) {:inserted 1}
      (r/table-drop (r/db test-db) :tmp) {:tables_dropped 1}
      (r/table-drop :tmp2) {:tables_dropped 1}
      (-> (r/table test-table) (r/index-create :name)) {:created 1}
      (-> (r/table test-table) (r/index-create :tmp (r/fn [row] 1))) {:created 1}
      (-> (r/table test-table)
          (r/index-create :type (r/fn [row]
                                  (r/get-field row :type)))) {:created 1}
      (-> (r/table test-table) (r/index-rename :tmp :xxx)) {:renamed 1}
      (-> (r/table test-table) (r/index-drop :xxx)) {:dropped 1})
    (is (= ["name" "type"] (r/run (-> (r/table test-table) r/index-list) conn)))))

(deftest manipulating-data
  (with-open [conn (r/connect :db test-db)]
    (testing "writing data"
      (are [term result] (contains? (set (split-map (r/run term conn))) result)
        (-> (r/table test-table) (r/insert bulbasaur)) {:inserted 1}
        (-> (r/table test-table) (r/insert pokemons)) {:inserted 2}
        (-> (r/table test-table)
            (r/get 1)
            (r/update {:japanese "Fushigidane"})) {:replaced 1}
        (-> (r/table test-table)
            (r/get 1)
            (r/replace (merge bulbasaur {:weight "6.9 kg"}))) {:replaced 1}
        (-> (r/table test-table) (r/get 1) r/delete) {:deleted 1}
        (-> (r/table test-table) r/sync) {:synced 1}))

    (testing "transformations"
      (is (= [25 81] (r/run (-> (r/table test-table)
                                (r/order-by {:index (r/asc :national_no)})
                                (r/map (r/fn [row]
                                         (r/get-field row :national_no))))
                            conn))))

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
      (is (= (first (r/run (-> (r/table test-table) (r/get-all 25)) conn)) (first (set pokemons))))
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
      (r/avg [2 4]) 3
      (r/min [4 2]) 2
      (r/max [4 6]) 6
      (r/sum [3 4]) 7)))

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
    (r/run (-> (r/table test-table) (r/insert pokemons)) conn)
    (is (= {:national_no 25}
           (r/run (-> (r/table test-table)
                      (r/get 25)
                      (r/without [:type :name])) conn)))))

(deftest string-manipulating
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

(deftest literal-values
  (with-open [conn (r/connect)]
    (is (= (r/run (r/object :a 1) conn) {:a 1}))
    (is (= (r/run (r/keys (r/object :a 1)) conn) ["a"]))
    (is (= (r/run (r/values (r/object :a 1)) conn) [1]))))

(deftest dates-and-times
  (with-open [conn (r/connect)]
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

(deftest control-structure
  (with-open [conn (r/connect)]
    (are [term result] (= result (r/run term conn))
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
                                     lt-le-eq-ne-ge-gt)
      [1 1] [false true true false true false]
      [0 1] [true true false true false false]
      [0 1 2 3] [true true false true false false]
      [0 1 1 2] [false true false true false false]
      [5 4 3] [false false false true true true]
      [5 4 4 3] [false false false true true false])

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
  (with-open [conn (r/connect)]
    (is (= {:type "Point" :coordinates [50 50]}
           (r/run (r/geojson {:type "Point" :coordinates [50 50]}) conn)))
    (is (= "Polygon" (:type (r/run (r/fill (r/line [[50 51] [51 51] [51 52] [50 51]])) conn))))
    (is (= 104644.93094219 (r/run (r/distance (r/point 20 20)
                                              (r/circle (r/point 21 20) 2)) conn)))))

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
      (is (= {:ready 1} (-> (r/wait) (r/run conn))))
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
    (let [file (io/file (io/resource "pikachu.png"))]
      (-> (r/table "pokedex")
          (r/insert {:national_no 25
                     :name "Pikachu"
                     :image (bs/to-byte-array file)})
          (r/run conn))
      (let [resp (-> (r/table "pokedex") (r/run conn) first :image)]
        (is (= (String. resp "UTF-8") (String. (bs/to-byte-array file) "UTF-8")))))))
