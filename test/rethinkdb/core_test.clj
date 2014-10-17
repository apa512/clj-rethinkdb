(ns rethinkdb.core-test
  (:require [clojure.test :refer :all]
            [rethinkdb.core :refer :all]
            [rethinkdb.query :as r]))

(def test-db "test_superheroes")

(defn clear-db [test-fn]
  (let [conn (connect)
        db-list (-> (r/db-list) (r/run conn))]
    (if (some #{test-db} db-list)
      (-> (r/db-drop test-db) (r/run conn))))
  (test-fn))

(deftest core-test
  (let [conn (connect)]
    (testing "creating and dropping databases"
      (-> (r/db-create "test_tmp") (r/run conn))
      (-> (r/db-create test-db) (r/run conn))
      (-> (r/db-drop "test_tmp") (r/run conn))
      (-> (r/db test-db) (r/table-create "dc_universe") (r/run conn))
      (let [db-list (-> (r/db-list) (r/run conn))]
        (is (some #{test-db} db-list))
        (is (not (some #{"test_tmp"} db-list)))))
    (testing "inserting and retrieving documents"
      (-> (r/db test-db)
          (r/table "dc_universe")
          (r/insert {:hero "Batman"
                     :gadgets ["Batarangs" "Batclaw" "Batrope"]
                     :real_name "Bruce Wayne"})
          (r/run conn))
      (-> (r/db test-db)
          (r/table "dc_universe")
          (r/insert [{:hero "Superman"
                      :real_name "Clark Kent"}
                     {:hero "Nightwing"
                      :real_name "Dick Grayson"}])
          (r/run conn))
      (is (= 3 (-> (r/db test-db) (r/table "dc_universe") (r/count) (r/run conn))))
      (is (= 1 (count (-> (r/db test-db)
                          (r/table "dc_universe")
                          (r/filter
                            (r/lambda [row]
                              (r/eq (r/get-field row "hero") "Superman")))
                          (r/run conn)))))
      (is (= 3 (count (-> (r/db test-db)
                          (r/table "dc_universe")
                          (r/filter
                            (r/lambda [row]
                              (r/has-fields row "hero")))
                          (r/run conn))))))
    (testing "mapping"
      (is (= (set ["Superman" "Nightwing" "Batman"])
             (set (-> (r/db test-db)
                      (r/table "dc_universe")
                      (r/map
                        (r/lambda [row]
                          (r/get-field row "hero")))
                      (r/run conn))))))
    (println (close conn))))

(use-fixtures :once clear-db)
