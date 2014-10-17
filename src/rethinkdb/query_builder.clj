(ns rethinkdb.query-builder
  (:require [clojure.data.json :as json]
            [rethinkdb.protodefs :refer [tt->int qt->int]]))

(defmulti parse-args
  (fn [args]
    (if (sequential? args)
      :sequential
      (type args))))

(defmethod parse-args :sequential [args]
  (let [[fst nxt] args]
    (if (keyword? fst)
      [(tt->int (name fst)) (map parse-args nxt)]
      [(tt->int "MAKE_ARRAY") (map parse-args args)])))

(defmethod parse-args clojure.lang.PersistentArrayMap [arg]
  (zipmap (keys arg) (map parse-args (vals arg))))

(defmethod parse-args clojure.lang.Keyword [arg]
  (tt->int (name arg)))

(defmethod parse-args :default [arg]
  arg)

(defn query->json
  ([type]
   (json/write-str [(qt->int (name type))]))
  ([type args]
   (json/write-str [(qt->int (name type)) (parse-args args)])))
