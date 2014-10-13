(ns rethinkdb.query-builder
  (:require [clojure.data.json :as json]
            [rethinkdb.protodefs :refer [tt->int qt->int]]))

(defn parse-args [args]
  (letfn [(parse [arg]
            (cond
              (keyword? arg) (tt->int (name arg))
              (vector? arg) (parse-args arg)
              :else arg))]
    (map parse args)))

(defn query->json
  ([type]
   (json/write-str [(qt->int (name type))]))
  ([type args]
   (json/write-str [(qt->int (name type)) (parse-args args)])))
