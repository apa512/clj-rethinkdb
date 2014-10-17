(ns rethinkdb.query
  (:refer-clojure :exclude [count filter map])
  (:require [clojure.data.json :as json]
            [rethinkdb.net :refer [send-start-query]]))

(defn rarray [args]
  (let [args (if (sequential? args)
               args
               [args])]
    [:MAKE_ARRAY args]))

(defn rvar [arg]
  [:VAR [arg]])

(defmacro lambda [arglist & [body]]
  (let [arg-replacements (zipmap arglist
                                 (clojure.core/map (fn [n]
                                                     (rvar (inc n)))
                                                   (range)))
        func-args (into [] (take (clojure.core/count arglist) (iterate inc 1)))
        func-terms (clojure.walk/postwalk-replace arg-replacements body)]
    [:FUNC [(rarray func-args) func-terms]]))

;;;; DB manipulation

(defn db-create [db]
  [:DB_CREATE [db]])

(defn db-drop [db]
  [:DB_DROP [db]])

(defn db-list []
  [:DB_LIST])

;;;; Table manipulation

(defn table-create [db table]
  [:TABLE_CREATE [db table]])

(defn table-drop [db table]
  [:TABLE_DROP [db table]])

;;;; Writing data

(defn insert [table objs]
  [:INSERT [table (rarray objs)]])

;;;; Selecting data

(defn db [db]
  [:DB [db]])

(defn table [db table]
  [:TABLE [db table]])

(defn filter [sq obj-or-func]
  [:FILTER [sq obj-or-func]])

(defn get [table id]
  [:GET [table id]])

(defn get-field [obj-or-sq s]
  [:GET_FIELD [obj-or-sq (name s)]])

(defn has-fields [obj-or-sq s]
  [:HAS_FIELDS [obj-or-sq (name s)]])

;;;; Aggregation

(defn count [sq]
  [:COUNT [sq]])

;;;; Transformations

(defn map [sq obj-or-func]
  [:MAP [sq obj-or-func]])

;;;; Math and logic

(defn eq [& args]
  [:EQ args])

(defn add [& args]
  [:ADD args])

;;;; Run query

(defn run [args conn]
  (swap! conn update-in [:token] inc)
  (send-start-query @conn args))
