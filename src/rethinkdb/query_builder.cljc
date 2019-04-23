(ns rethinkdb.query-builder
  (:require [clojure.string :as string]
            [rethinkdb.types :refer [tt->int qt->int]]
    #?@(:clj [[clj-time.coerce :as c]]))
  #?(:clj
     (:import (org.joda.time DateTime)
              (java.util Base64 Base64$Encoder Date UUID))))

(declare parse-term)

(def encoder (atom nil))

(defn term [term args & [optargs]]
  {::term term
   ::args args
   ::optargs optargs})

#?(:clj (defn date-time->long [t]
          (condp instance? t
            DateTime (double (/ (c/to-long t) 1000))
            Date (double (/ (.getTime ^Date t) 1000)))))

(defn snake-case [s]
  (string/replace (name s) \- \_))

(defn snake-case-keys [m]
  (into {}
    (for [[k v] m]
      [(snake-case k) v])))

(defmulti parse-arg
  (fn [arg]
    (cond
      (::term arg) :query
      (or (sequential? arg) (seq? arg) (set? arg)) :sequential
      (map? arg) :map
      #?(:clj (#{DateTime Date} (type arg))
         :cljs (= (type arg) js/Date)) :time
      (= (type arg) UUID) :uuid
      #?@(:clj [(instance? (type (byte-array [])) arg) :binary]))))

(defmethod parse-arg :query [arg]
  (parse-term arg))

(defmethod parse-arg :sequential [arg]
  (parse-term (term :MAKE_ARRAY arg)))

(defmethod parse-arg :map [arg]
  (zipmap (keys arg) (map parse-arg (vals arg))))

(defmethod parse-arg :time [arg]
  (parse-term (term :EPOCH_TIME [#?(:clj (date-time->long arg)
                                    :cljs (.getTime arg))])))

#?(:clj (defmethod parse-arg :binary [arg]
          ;; Base64 decoders are thread safe, so we only need one of these.
          (let [encoder (or @encoder (let [base64-encoder (Base64/getEncoder)]
                                       (reset! encoder base64-encoder)))]
            {"$reql_type$" "BINARY"
             "data" (.encodeToString ^Base64$Encoder encoder arg)})))


(defmethod parse-arg :uuid [arg]
  (str arg))

(defmethod parse-arg :default [arg]
  arg)

(defn parse-term [{term ::term args ::args optargs ::optargs}]
  (filter
    identity
    [(tt->int term)
     (map parse-arg (seq args))
     (if optargs (snake-case-keys (->> optargs
                                       (map (fn [[k v]]
                                              [k (if (::term v)
                                                   (parse-term v)
                                                   v)]))
                                       (into {}))))]))

(defn parse-query
  ([type]
   [(qt->int type)])
  ([type term]
   [(qt->int type) (parse-term term)]))
