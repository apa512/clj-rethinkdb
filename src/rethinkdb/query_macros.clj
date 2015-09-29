(ns rethinkdb.query-macros
  (:require [clojure.walk :as walk]
            [rethinkdb.query-builder :refer [term]]))

(defmacro fn [args & [body]]
  (let [new-args (into [] (clojure.core/map #(hash-map :temp-var (keyword %)) args))
        new-replacements (zipmap args new-args)
        new-terms (walk/postwalk-replace new-replacements body)]
    (term :FUNC [new-args new-terms])))
