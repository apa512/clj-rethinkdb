(ns rethinkdb.core-test
  (:require [clj-time.core :as t]
            [clojure.test :refer :all]
            [rethinkdb.core :refer :all]
            [rethinkdb.query :as r]))

(def test-db "test")

(defn clear-db [test-fn]
  (let [conn (connect)
        db-list (-> (r/db-list) (r/run conn))]
    (if (some #{test-db} db-list)
      (-> (r/db-drop test-db) (r/run conn)))
    (r/run (r/db-create test-db) conn)
    (close conn))
  (test-fn))

(defmacro with-test-db [& body]
  (conj (for [term body]
          `(-> (r/db test-db)
               ~term
               (r/run ~'conn)))
        'do))

(deftest core-test
  (let [conn (connect)]
    (testing "table management"
      (with-test-db
        (r/table-create "pokedex" :primary-key :national_no)
        (-> (r/table "pokedex")
            (r/index-create "type" (r/lambda [row]
                                     (r/get-field row :type))))))
    (testing "writing data"
      (with-test-db
        (-> (r/table "pokedex")
            (r/insert {:national_no 25
                       :name "Pikachu"
                       :type "Electric"
                       :moves [{:name "Tail Whip"
                                :type "Normal"}]}))))
    (testing "selecting data"
      (let [pikachu-with-pk (with-test-db (-> (r/table "pokedex") (r/get 25)))
            pikachu-with-idx (first (with-test-db (-> (r/table "pokedex") (r/get-all "Electric" :index :type))))]
        (is (= pikachu-with-pk pikachu-with-idx))))
    (close conn)))

(use-fixtures :once clear-db)
