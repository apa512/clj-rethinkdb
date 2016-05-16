(ns rethinkdb.async-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async]
            [rethinkdb.query :as r]
            [rethinkdb.test-utils :as utils])
  (:import (clojure.core.async.impl.protocols ReadPort)
           (org.slf4j LoggerFactory)
           (ch.qos.logback.classic Logger Level)))

(use-fixtures :each utils/setup-each)
(use-fixtures :once utils/setup-once)

(def pokemon [{:national_no 25 :name "Pikachu"}
              {:national_no 26 :name "Raichu"}])
(def changefeed-pokemon (map #(hash-map :new_val %) pokemon))


(deftest always-return-async
  (let [root-logger ^Logger (LoggerFactory/getLogger "rethinkdb.net")
        level (.getLevel root-logger)]
    (.setLevel root-logger Level/OFF)
    (with-open [conn (r/connect :async? true)]
      (are [query] (instance? ReadPort (r/run query conn))
        (r/db-list)
        (-> (r/db :non-existent) (r/table :nope))
        (-> (r/db utils/test-db) (r/table utils/test-table) (r/insert {:a 1}))))
    (.setLevel root-logger level)))

(deftest async-results
  (with-open [conn (r/connect :async? true :db utils/test-db)]
    (testing "Shape of async results"
      (are [expected query] (= (->> (r/run query conn)
                                    (async/into [])
                                    (async/<!!))
                               expected)
        ;; Insert (success atom)
        [{:deleted   0
          :errors    0
          :inserted  2
          :replaced  0
          :skipped   0
          :unchanged 0}]
        (-> (r/table utils/test-table)
            (r/insert pokemon))

        ;; Success sequence
        pokemon (-> (r/table utils/test-table))

        ;; Success atom
        [pokemon] (-> (r/table utils/test-table) (r/order-by :name))

        ;; Changefeed
        changefeed-pokemon
        (-> (r/table utils/test-table)
            (r/changes {:include-initial true})
            (r/limit 2))))

    #_(testing "Closing a changefeed with results"
        (let [changefeed (-> (r/table utils/test-table)
                             (r/changes {:include-initial true})
                             (r/run conn))]
          (async/go (async/<! (async/timeout 1000))
                    (async/close! changefeed))
          (is (= changefeed-pokemon
                 (async/alt!!
                   (async/into [] changefeed) ([v _] v)
                   (async/timeout 1000) ([val _] :timed-out))))))))
