(ns rethinkdb.query-builder
  (:require [clojure.string :as string]
            [rethinkdb.types :refer [tt->int qt->int]]
    #?@(:clj [[clj-time.coerce :as c]
              [clojure.data.codec.base64 :as base64]
              [byte-streams :as bs]])))

(defn term [term args & [optargs]]
  {::term term
   ::args args
   ::optargs optargs})

(declare parse-term)

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
      (or (sequential? arg) (seq? arg)) :sequential
      (map? arg) :map
      (= (type arg) #?(:clj org.joda.time.DateTime :cljs js/Date)) :time
      (= (type arg) #?(:clj java.util.UUID :cljs UUID)) :uuid
      #?@(:clj [(instance? (type (byte-array [])) arg) :binary]))))

(defmethod parse-arg :query [arg]
  (parse-term arg))

(defmethod parse-arg :sequential [arg]
  (parse-term (term :MAKE_ARRAY arg)))

(defmethod parse-arg :map [arg]
  (zipmap (keys arg) (map parse-arg (vals arg))))

(defmethod parse-arg :time [arg]
  (parse-term (term :EPOCH_TIME [#?(:clj (c/to-epoch arg)
                                    :cljs (.getTime arg))])))

#?(:clj (defmethod parse-arg :binary [arg]
          {"$reql_type$" "BINARY"
           "data" (String. ^bytes (base64/encode arg) "UTF-8")}))

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
