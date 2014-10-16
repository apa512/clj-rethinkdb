(ns rethinkdb.query
  (:refer-clojure :exclude [count filter map])
  (:require [clojure.data.json :as json]
            [rethinkdb.net :refer [send-start-query]]))

(defn db [db]
  [:DB [db]])

(defn table [db table]
  [:TABLE [db table]])

(defn count [sq]
  [:COUNT [sq]])

(defn insert [table objs]
  [:INSERT [table [:MAKE_ARRAY (flatten (vector objs))]]])

(defn get-field [obj-or-sq s]
  [:GET_FIELD [obj-or-sq (name s)]])

(defn eq [& args]
  [:EQ args])

(defn add [& args]
  [:ADD args])

(defmacro lambda [arglist & [body]]
  (let [arg-replacements (zipmap arglist
                                 (clojure.core/map (fn [n]
                                                     [:VAR [(inc n)]])
                                                   (range)))
        func-args (into [] (take (clojure.core/count arglist) (iterate inc 1)))
        func-terms (clojure.walk/postwalk-replace arg-replacements body)]
    [:FUNC [[:MAKE_ARRAY func-args] func-terms]]))

(defn filter [sq obj-or-func]
  [:FILTER [sq obj-or-func]])

(defn map [sq obj-or-func]
  [:MAP [sq obj-or-func]])

;;;; Management

(defn db-create [db]
  [:DB_CREATE [db]])

(defn db-drop [db]
  [:DB_DROP [db]])

(defn db-list []
  [:DB_LIST])

(defn table-create [db table]
  [:TABLE_CREATE [db table]])

(defn table-drop [db table]
  [:TABLE_DROP [db table]])

;;;; Run query

(defn run [args conn]
  (swap! conn update-in [:token] inc)
  (send-start-query @conn args))
