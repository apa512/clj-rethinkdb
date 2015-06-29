(ns rethinkdb.query-builder
  (:require [clojure.data.json :as json]
            [clj-time.coerce :as c]
            [clojure.walk :as walk]
            [rethinkdb.types :refer [tt->int qt->int]]
            [rethinkdb.utils :refer [snake-case]]))

(declare parse-term)

(defn snake-case-keys [m]
  (into {}
    (for [[k v] m]
      [(snake-case k) v])))

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
  (zipmap (keys arg) (map parse-arg (vals arg))))

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

(defn replace-vars [query]
  (let [var-counter (atom 0)]
    (walk/postwalk
      #(if (clojure.core/and (map? %) (= :FUNC (:rethinkdb.query-builder/term %)))
        (let [vars (first (:rethinkdb.query-builder/args %))
              new-vars (range @var-counter (+ @var-counter (clojure.core/count vars)))
              new-args (clojure.core/map
                         (clojure.core/fn [arg]
                           (term :VAR [arg]))
                         new-vars)
              var-replacements (zipmap vars new-args)]
          (swap! var-counter + (clojure.core/count vars))
          (walk/postwalk-replace
            var-replacements
            (assoc-in % [:rethinkdb.query-builder/args 0] new-vars)))
        %)
      query)))

(defn prepare-query
  ([type]
   (->> (parse-query type)
        (json/write-str)))
  ([type term]
   (->> (replace-vars term)
        (parse-query type)
        (json/write-str))))
