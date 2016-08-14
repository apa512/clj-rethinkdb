(ns rethinkdb.cljs-test
  (:require [cljs.test :refer-macros [deftest is]]
            [rethinkdb.query :as r]
            [clojure.walk :as walk]))

(defn normalise-keywords
  "Recursively transforms all map keys from strings to keywords.
  Removes namespace from existing keywords in map.
  From clojure.walk"
  {:added "1.1"}
  [m]
  (let [f (fn [[k v]] (if (or (string? k) (keyword? k))
                        [(keyword (name k)) v] [k v]))]
    ;; only apply to maps
    (walk/postwalk (fn [x] (if (map? x) (into {} (map f x)) x)) m)))

(deftest test-doesnt-throw
  (is (-> (r/table :abc) (r/group :type)))
  (is (-> (r/table :abc) (r/order-by :type)))
  (is (-> (r/table :abc) (r/get-field [:type]))))

(deftest test-func
  (is (= (normalise-keywords
           (r/fn [arg1 arg2]
             (r/get-field arg1 arg2)))
         {:term :FUNC,
          :args [[{:temp-var :arg1} {:temp-var :arg2}]
                 {:term :GET_FIELD
                  :args [{:temp-var :arg1} {:temp-var :arg2}]
                  :optargs nil}]
          :optargs nil}))
  (is (= (normalise-keywords
           (r/replace-vars
             (r/fn [arg1 arg2]
               (r/get-field arg1 arg2))))
         {:term :FUNC,
          :args ['(0 1)
                 {:term :GET_FIELD
                  :args [{:term :VAR, :args [0], :optargs nil}
                         {:term :VAR, :args [1], :optargs nil}]
                  :optargs nil}]
          :optargs nil})))
