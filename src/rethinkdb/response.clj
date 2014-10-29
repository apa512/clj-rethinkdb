(ns rethinkdb.response
  (:require [clj-time.coerce :as c]))

(defmulti parse-reql-type (fn [resp] (get resp :$reql_type$)))

(defmethod parse-reql-type "TIME" [resp]
  (let [epoch (:epoch_time resp)
        epoch-milli (* epoch 1000)]
    (c/from-long epoch-milli)))

(defmethod parse-reql-type "BINARY" [resp]
  resp)

(defmulti parse-response
  (fn [args]
    (cond
      (sequential? args) :sequential
      (map? args) :hash
      :else (type args))))

(defmethod parse-response :sequential [resp]
  (map parse-response resp))

(defmethod parse-response :hash [resp]
  (if-let [reql-type (:$reql_type$ resp)]
    (parse-reql-type resp)
    (zipmap (keys resp) (map parse-response (vals resp)))))

(defmethod parse-response :default [resp]
  resp)
