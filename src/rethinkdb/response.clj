(ns rethinkdb.response
  (:require [clj-time.coerce :as c]))

(defmulti parse-reql-type (fn [resp] (get resp "$reql_type$")))

(defmethod parse-reql-type "TIME" [resp]
  (c/from-long (get resp "epoch_time")))

(defmulti parse-response
  (fn [args]
    (if (sequential? args)
      :sequential
      (type args))))

(defmethod parse-response :sequential [resp]
  (map parse-response resp))

(defmethod parse-response clojure.lang.PersistentArrayMap [resp]
  (if-let [reql-type (get resp "$reql_type$")]
    (parse-reql-type resp)
    (zipmap (keys resp) (map parse-response (vals resp)))))

(defmethod parse-response :default [resp]
  resp)
