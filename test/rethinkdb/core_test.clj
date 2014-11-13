(ns rethinkdb.core-test
  (:require [clj-time.core :as t]
            [clojure.test :refer :all]
            [rethinkdb.core :refer :all]
            [rethinkdb.query :as r]))

(def test-db :test)

(defn clear-db [test-fn]
  (let [conn (connect)
        db-list (-> (r/db-list) (r/run conn))]
    (if (some #{(name test-db)} db-list)
      (-> (r/db-drop test-db) (r/run conn)))
    (r/run (r/db-create test-db) conn)
    (close conn))
  (test-fn))

(defmacro with-test-db [& body]
  (cons 'do (for [term body]
              `(-> (r/db test-db)
                   ~term
                   (r/run ~'conn)))))

(deftest core-test
  (let [conn (connect)]
    (testing "table management"
      (with-test-db
        (r/table-create :pokedex {:primary-key :national_no})
        (-> (r/table :pokedex)
            (r/index-create :type (r/lambda [row]
                                     (r/get-field row :type))))
        (r/table-create :temp)
        (r/table-drop :temp))
      (is (= ["pokedex"] (with-test-db (r/table-list)))))
    (testing "writing data"
      (with-test-db
        (-> (r/table :pokedex)
            (r/insert {:national_no 25
                       :name "Pikachu"
                       :type "Electric"
                       :last_seen (t/date-time 2014 10 20)
                       :moves ["Tail Whip" "Tail Whip" "Growl"]}))))
    (testing "selecting data"
      (let [pikachu-with-pk (with-test-db (-> (r/table :pokedex) (r/get 25)))
            pikachu-with-index (first (with-test-db (-> (r/table :pokedex) (r/get-all "Electric" {:index :type}))))]
        (is (= pikachu-with-pk pikachu-with-index))))
    (testing "manipulating documents"
      (with-test-db
        (-> (r/table :pokedex)
            (r/get 25)
            (r/update
              (r/lambda [row]
                {:moves (r/set-insert (r/get-field row :moves) "Thunder Shock")}))))
      (is (= ["Tail Whip" "Growl" "Thunder Shock"]
             (with-test-db
               (-> (r/table :pokedex)
                   (r/get 25)
                   (r/get-field :moves)))))
      (is (= ["Tail Whip"]
             (with-test-db
               (-> (r/table :pokedex)
                   (r/get 25)
                   (r/get-field :moves)
                   (r/set-difference ["Growl" "Thunder Shock"]))))))
    (close conn)))

(use-fixtures :once clear-db)
