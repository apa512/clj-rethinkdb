(ns rethinkdb.response
  (:require [clj-time.core :as t]
            [clj-time.coerce :as c])
  (:import [java.util Base64 Base64$Decoder]))

(declare parse-response)
(def decoder (atom nil))

(defmulti parse-reql-type (fn [resp] (get resp :$reql_type$)))

(defmethod parse-reql-type "TIME" [resp]
  (let [epoch (:epoch_time resp)
        timezone (Integer/parseInt (re-find #"..." (:timezone resp)))
        epoch-milli (* epoch 1000)]
    (t/to-time-zone
      (c/from-long (long epoch-milli))
      (t/time-zone-for-offset timezone))))

(defmethod parse-reql-type "GROUPED_DATA" [resp]
  (apply hash-map (apply concat (parse-response (:data resp)))))

(defmethod parse-reql-type "BINARY" [resp]
  ;; Decoders are thread safe, so we only need one.
  (let [decoder (or @decoder (let [mime-decoder (Base64/getMimeDecoder)]
                               (reset! decoder mime-decoder)))]
    (.decode ^Base64$Decoder decoder ^String (:data resp))))

(defmethod parse-reql-type "GEOMETRY" [resp]
  (dissoc resp :$reql_type$))

(defmulti parse-response
  (fn [args]
    (cond
      (sequential? args) :sequential
      (map? args) :map
      :else (type args))))

(defmethod parse-response :sequential [resp]
  (into [] (map parse-response resp)))

(defmethod parse-response :map [resp]
  (if (:$reql_type$ resp)
    (parse-reql-type resp)
    (zipmap (keys resp) (map parse-response (vals resp)))))

(defmethod parse-response :default [resp]
  resp)
