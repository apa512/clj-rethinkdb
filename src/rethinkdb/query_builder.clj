(ns rethinkdb.query-builder
  (:require [clojure.data.json :as json]
            [clj-time.coerce :as c]
            [rethinkdb.types :refer [tt->int qt->int]]
            [rethinkdb.utils :as u :refer [snake-case]]))

(declare parse-term)

(defn snake-case-keys [m]
  (u/mapk snake-case m))

(defn term [term args & [optargs]]
  {::term term
   ::args args
   ::optargs optargs})

(defmulti parse-arg
  (fn [arg]
    (cond
      (::term arg) :query
      (or (sequential? arg) (seq? arg)) :sequential
      (map? arg) :map
      (instance? org.joda.time.DateTime arg) :time
      (instance? java.util.UUID arg) :uuid)))

(defmethod parse-arg :query [arg]
  (parse-term arg))

(defmethod parse-arg :sequential [arg]
  (parse-term (term :MAKE_ARRAY arg)))

(defmethod parse-arg :map [arg]
  (u/mapv parse-arg arg))

(defmethod parse-arg :time [arg]
  (parse-term (term :EPOCH_TIME [(c/to-epoch arg)])))

(defmethod parse-arg :uuid [arg]
  (str arg))

(defmethod parse-arg :default [arg]
  arg)

(defn parse-term [{term ::term args ::args optargs ::optargs}]
  (filter
    identity
    [(tt->int term)
     (map parse-arg (seq args))
     (if optargs (snake-case-keys optargs))]))

(defn parse-query
  ([type]
   [(qt->int type)])
  ([type term]
   [(qt->int type) (parse-term term)]))
