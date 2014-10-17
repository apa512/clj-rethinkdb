(ns rethinkdb.query-builder
  (:require [clojure.data.json :as json]
            [rethinkdb.protodefs :refer [tt->int qt->int]]))

(defmulti parse-args*
  (fn [args]
    (if (sequential? args)
      :sequential
      (type args))))

(defmethod parse-args* :sequential [args]
  (if (keyword? (first args))
    (map parse-args* args)
    [(tt->int "MAKE_ARRAY") args]))

(defmethod parse-args* clojure.lang.Keyword [arg]
  (tt->int (name arg)))

(defmethod parse-args* :default [arg]
  arg)

(defn parse-args [args]
  (letfn [(parse [arg]
            (cond
              (keyword? arg) (tt->int (name arg))
              (vector? arg) (parse-args arg)
              (seq? arg) (parse-args arg)
              :else arg))]
    (map parse args)))

(defn query->json
  ([type]
   (json/write-str [(qt->int (name type))]))
  ([type args]
   (json/write-str [(qt->int (name type)) (parse-args args)])))
