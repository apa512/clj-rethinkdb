(ns rethinkdb.core-test
  (:require [clj-time.core :as t]
            [clojure.test :refer :all]
            [rethinkdb.core :refer :all]
            [rethinkdb.query :as r]))

(def test-db "cljrethinkdb_test")
(def tmp-db "cljrethinkdb_tmp")

(def pikachu {:national_no 25
              :name "Pikachu"
              :abilities ["Tail Whip" "Tail Whip" "Growl"]
              :coolest_pokemon true
              :added (t/date-time 2014 12 2)
              :type ["Electric"]})
(def magnemite {:national_no 81
                :name "Magnemite"
                :added (t/date-time 2014 12 1)
                :abilities ["Magnet Pull" "Sturdy" "Analytic"]
                :type ["Electric" "Steel"]})

(defn setup [test-fn]
  (let [conn (connect)
        db-list (-> (r/db-list) (r/run conn))]
    (if (some #{test-db} db-list)
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
    (testing "database management"
      (r/run (r/db-create tmp-db) conn)
      (is (clojure.set/subset? #{test-db tmp-db} (set (r/run (r/db-list) conn))))
      (r/run (r/db-drop tmp-db) conn))

    (testing "table management"
      (with-test-db
        (r/table-create "pokedex" {:primary-key "national_no"})
        (r/table-create "tmp" {:durability "hard"})
        (-> (r/table "pokedex")
            (r/index-create "height" (r/fn [row]
                                       (r/get-field row "height"))))
        (-> (r/table "pokedex")
            (r/index-drop "height"))
        (r/table-drop "tmp")
        (-> (r/table "pokedex")
            (r/index-create "types" (r/fn [row]
                                      (r/get-field row "type")) {:multi true}))
        (-> (r/table "pokedex")
            (r/index-rename "types" "type"))
        (-> (r/table "pokedex")
            r/index-wait))
      (is (= ["type"] (map :index (with-test-db (-> (r/table "pokedex") r/index-status)))))
      (is (= ["type" (with-test-db (-> (r/table "pokedex") r/index-list))]))
      (is (= ["pokedex"] (with-test-db (r/table-list)))))

    (testing "writing data"
      (with-test-db
        (-> (r/table "pokedex")
            (r/insert pikachu))
        (-> (r/table "pokedex")
            (r/insert [magnemite
                       {:national_no 52
                        :name "Meowth"}
                       {:national_no 6
                        :name "Charisard"
                        :type ["Fire"]
                        :moves ["Blaze" "Solar Power"]}]))
        (-> (r/table "pokedex")
            (r/get 6)
            (r/update {:name "Charizard"}
                      {:return_changes true}))
        (-> (r/table "pokedex")
            (r/get 6)
            (r/replace {:name "Charizard"
                        :type ["Fire" "Flying"]
                        :abilities ["Blaze" "Solar Power"]}))
        (-> (r/table "pokedex")
            (r/get 52)
            (r/delete {:durability "hard"}))
        (-> (r/table "pokedex")
            r/sync)))

    (testing "selecting data"
      (with-test-db
        (r/table-create "xy")
        (-> (r/table "xy")
            (r/insert [{:id 25
                        :name "Pikachu"
                        :type ["Electric"]}
                       {:id 312
                        :name "Minun"
                        :type ["Electric"]}])))
      (is (some #(= pikachu %) (with-test-db (r/table "pokedex"))))
      (is (= pikachu (with-test-db (-> (r/table "pokedex") (r/get 25)))))
      (is (= 2 (count (with-test-db (-> (r/table "pokedex") (r/get-all ["Electric"] {:index "type"}))))))
      (is (= [magnemite] (with-test-db (-> (r/table "pokedex") (r/between 81 82)))))
      (is (= [pikachu] (with-test-db (-> (r/table "pokedex") (r/filter (r/fn [row]
                                                                         (r/eq "Pikachu" (r/get-field row "name"))))))))
      (is (= pikachu (:left (first (with-test-db
                                     (-> (r/table "pokedex")
                                         (r/inner-join (r/table (r/db test-db) "xy")
                                                       (r/fn [row1 row2]
                                                         (r/eq (r/get-field row1 "name") (r/get-field row2 "name"))))))))))
      (with-test-db (-> (r/table "pokedex")
                        (r/outer-join (r/table (r/db test-db) "xy")
                                      (r/fn [row1 row2]
                                        (r/not (r/is-empty (r/set-intersection (r/get-field row1 "type") (r/get-field row2 "type"))))))))
      (is (apply = 25
            ((juxt :id :national_no) (first (with-test-db
                                              (-> (r/table "pokedex")
                                                  (r/eq-join "national_no" (r/table (r/db test-db) "xy"))
                                                  r/zip)))))))

    (testing "transformations"
      (is (= #{6 25 81} (set (with-test-db
                               (-> (r/table "pokedex")
                                   (r/map (r/fn [row]
                                            (r/get-field row "national_no"))))))))
      (is (= [{:coolest_pokemon true}] (with-test-db
                                         (-> (r/table "pokedex")
                                             (r/get-all ["Electric"] {:index "type"})
                                             (r/with-fields ["coolest_pokemon"])))))
      (is (= ["Magnet Pull" "Sturdy" "Analytic" "Tail Whip" "Tail Whip" "Growl"]
             (with-test-db
               (-> (r/table "pokedex")
                   (r/order-by (r/desc "national_no"))
                   (r/has-fields "abilities")
                   (r/concat-map (r/fn [row]
                                   (r/get-field row "abilities")))))))
      (is (= 2 (with-test-db (-> (r/table "pokedex")
                                 (r/skip 1)
                                 r/count))))
      (is (= 1 (with-test-db (-> (r/table "pokedex")
                                 (r/limit 1)
                                 r/count))))
      (is (= 2 (with-test-db (-> (r/table "pokedex")
                                 (r/slice 1 3)
                                 r/count))))
      (is (= pikachu (with-test-db (-> (r/table "pokedex")
                                       (r/order-by "national_no")
                                       (r/nth 1)))))
      (is (= [1] (with-test-db (-> (r/table "pokedex")
                                   (r/order-by "national_no")
                                   (r/map (r/fn [row]
                                            (r/get-field row "national_no")))
                                   (r/indexes-of 25)))))
      (with-test-db (-> (r/table "pokedex")
                        (r/sample 3))))))

(use-fixtures :once setup)
