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

(def pokemons [{:national_no 25
                :name "Pikachu"
                :type ["Electric"]}
               {:national_no 81
                :name "Magnemite"
                :type ["Electric" "Steel"]}])

(defn setup [test-fn]
  (if (some #{test-db} (r/run (r/db-list) conn))
    (r/run (r/db-drop test-db) conn))
  (r/run (r/db-create test-db) conn)
  (test-fn))

(deftest core-test
  (let [conn (connect)]
    (testing "manipulating databases"
      (is (= (r/run (r/db-create "cljrethinkdb_tmp") conn) {:created 1}))
      (is (= (r/run (r/db-drop "cljrethinkdb_tmp") conn) {:dropped 1}))
      (is (contains? (set (r/run (r/db-list) conn)) test-db)))

    (testing "manipulating tables"
      (db-run (r/table-create :tmp))
      (are [term result] (= (db-run term) result)
        (r/table-create :pokedex {:primary-key :national_no})        {:created 1}
        (r/table-drop :tmp) {:dropped 1}
        (-> (r/table :pokedex) (r/index-create :tmp (r/fn [row] 1))) {:created 1}
        (-> (r/table :pokedex)
            (r/index-create :type (r/fn [row]
                                    (r/get-field row :type))))       {:created 1}
        (-> (r/table :pokedex) (r/index-rename :tmp :xxx))           {:renamed 1}
        (-> (r/table :pokedex) (r/index-drop :xxx))                  {:dropped 1}
        (-> (r/table :pokedex) r/index-list)                         ["type"]))
    (close conn)))

(use-fixtures :once setup)
