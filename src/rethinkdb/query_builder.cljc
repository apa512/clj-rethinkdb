(ns rethinkdb.query-builder
  (:require [rethinkdb.utils :refer [snake-case]]
            [clojure.walk :as walk]
            [rethinkdb.types :refer [tt->int qt->int]]
    #?@(:clj [
            [clojure.data.json :as json]
            [clj-time.coerce :as c]])))

(defn term [term args & [optargs]]
  {::term term
   ::args args
   ::optargs optargs})

(declare parse-term)

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
      (= (type arg) #?(:clj java.util.UUID :cljs UUID)) :uuid)))

(defmethod parse-arg :query [arg]
  (parse-term arg))

(defmethod parse-arg :sequential [arg]
  (parse-term (term :MAKE_ARRAY arg)))

(defmethod parse-arg :map [arg]
  (zipmap (keys arg) (map parse-arg (vals arg))))

(defmethod parse-arg :time [arg]
  (parse-term (term :EPOCH_TIME [#?(:clj (c/to-epoch arg)
                                    :cljs (.getTime arg))])))

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

#?(:clj (defn prepare-query
          "If only type is provided, then the query is assumed to be a
          :STOP or :CONTINUE query. global-optargs may be nil."
          ([type]
           (->> (parse-query type)
                (json/write-str)))
          ([type term]
           (prepare-query type term nil))
          ([type term global-optargs]
           (as-> term $
                 (replace-vars $)
                 (parse-query type $)
                 (concat $ global-optargs)
                 (json/write-str $)))))
