(ns rethinkdb.core-test
  (:require [clojure.test :refer :all]
            [rethinkdb.core :refer :all]
            [rethinkdb.query :as r]))

(defn clear-db [test-fn]
  (let [conn (connect)
        db-list (-> (r/db-list) (r/run conn))]
    (if (some #{"test"} db-list)
      (-> (r/db-drop "test") (r/run conn))))
  (test-fn))

(deftest core-test
  (let [conn (connect)]
    (testing "creating and dropping databases"
      (-> (r/db-create "test") (r/run conn))
      (-> (r/db-create "test_temp") (r/run conn))
      (-> (r/db-drop "test_temp") (r/run conn))
      (-> (r/db "test") (r/table-create "pokemons") (r/run conn))
      (let [db-list (-> (r/db-list) (r/run conn))]
        (is (some #{"test"} db-list))
        (is (not (some #{"test_temp"} db-list)))))
    (println (-> (r/db "test")
        (r/table "pokemons")
        (r/insert (take 100 (iterate identity {})))
        (r/run conn)))
    (println (-> (r/db "test") (r/table "pokemons") (r/run conn)))
    (-> (r/db-drop "test") (r/run conn))))

(use-fixtures :once clear-db)
