(ns rethinkdb.joins-test
  (:require [clojure.test :refer :all]
            [rethinkdb.query :as r]
            [rethinkdb.test-utils :as u]))

(def names [{:id 1 :name "Pikachu", :type 2},
            {:id 2 :name "Magnemite", :type [1 2]}])
(def types [{:id 1 :type "Steel"},
            {:id 2 :type "Electric"}])

(defn reset [table data conn]
  (-> (r/table table)
      (r/delete {:durability :soft :return-changes false})
      (r/run conn))
  (-> (r/table table)
      (r/insert data)
      (r/run conn)))

(defn setup-each [test-fn]
  (with-open [conn (r/connect :db u/test-db)]
    (reset :names names conn)
    (reset :types types conn)
    (test-fn)))

(defn setup-once [test-fn]
  (with-open [conn (r/connect :db u/test-db)]
    (u/ensure-db u/test-db conn)
    (u/ensure-table :names {} conn)
    (u/ensure-table :types {} conn))
  (test-fn)
  (r/run (r/db-drop u/test-db) (r/connect :db u/test-db)))

(deftest eq-join
  "Magnemite is not picked up because its type is an array of ints instead of an int."
  (with-open [conn (r/connect :db u/test-db)]
    (testing
        "Simple eq-join."
      (is (=
           (-> (r/table :names)
               (r/eq-join :type (r/table :types))
               (r/run conn)))
          [{:left {:id 1, :name "Pikachu", :type 2}, :right {:id 2, :type "Electric"}}]))

    (testing "Simple eq-join with zip"
      (is (-> (r/table :names)
              (r/eq-join :type (r/table :types))
              (r/zip)
              (r/run conn))
          (= [{:id 1, :name "Pikachu", :type "Electric"}])))))

(deftest inner-join
  "Magnemite will not be picked up because it has an array of ints instead of a single int"
  (with-open [conn (r/connect :db u/test-db)]
    (testing
        "Simple inner-join"
      (is (=
           (-> (r/table :names)
               (r/inner-join (r/table :types) (r/fn [row1 row2]
                                                (r/eq (r/get-field row1 :type)
                                                      (r/get-field row2 :id))))
               (r/run conn))
           [{:left {:id 1, :name "Pikachu", :type 2}, :right {:id 2, :type "Electric"}}])))
    (testing
        "Simple inner-join with zip"
      (is (=
           (-> (r/table :names)
               (r/inner-join (r/table :types) (r/fn [row1 row2]
                                                (r/eq (r/get-field row1 :type)
                                                      (r/get-field row2 :id))))
               (r/zip)
               (r/run conn))
           [{:id 2, :name "Pikachu", :type "Electric"}])))))

(deftest outer-join
  "Steel won't be picked up because it doesn't match the exact type of any individual pokemon."
  (with-open [conn (r/connect :db u/test-db)]
    (testing
        "Simple outer-join"
      (is (=
           (-> (r/table :names)
               (r/outer-join (r/table :types) (r/fn [row1 row2]
                                                (r/eq (r/get-field row1 :type)
                                                      (r/get-field row2 :id))))
               (r/run conn))
           [{:left {:id 2, :name "Magnemite", :type [1 2]}}
            {:left {:id 1, :name "Pikachu", :type 2}
             :right {:id 2, :type "Electric"}}])))
    (testing
        "Outer-join with zip"
      (is (=
           (-> (r/table :names)
               (r/outer-join (r/table :types) (r/fn [row1 row2]
                                               (r/eq (r/get-field row1 :type)
                                                     (r/get-field row2 :id))))
               (r/zip)
               (r/run conn))
           [{:id 2, :name "Magnemite" :type [1 2]}
            {:id 2, :name "Pikachu", :type "Electric"}])))))


(use-fixtures :once setup-once)
(use-fixtures :each setup-each)
