(ns rethinkdb.query
  (:refer-clojure :exclude [count filter map get not])
  (:require [clojure.data.json :as json]
            [rethinkdb.net :refer [send-start-query]]))

(defmacro lambda [arglist & [body]]
  (let [arg-replacements (zipmap arglist
                                 (clojure.core/map (fn [n]
                                                     [:VAR [(inc n)]])
                                                   (range)))
        func-args (into [] (take (clojure.core/count arglist) (iterate inc 1)))
        func-terms (clojure.walk/postwalk-replace arg-replacements body)]
    [:FUNC [func-args func-terms]]))

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

(defn insert [table objs & {:as optargs}]
  [:INSERT [table objs] optargs])

(defn update [obj-or-sq obj-or-func & {:as optargs}]
  [:UPDATE [obj-or-sq obj-or-func] optargs])

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

(defn contains [sq x-or-func]
  [:CONTAINS [sq x-or-func]])

;;;; Document manipulation

(defn has-fields [obj-or-sq s]
  [:HAS_FIELDS [obj-or-sq (name s)]])

(defn pluck [obj-or-sq s]
  [:PLUCK [obj-or-sq (name s)]])

;;;; Aggregation

(defn count [sq]
  [:COUNT [sq]])

;;;; Transformations

(defn map [sq obj-or-func]
  [:MAP [sq obj-or-func]])

(defn limit [sq n]
  [:LIMIT [sq n]])

;;;; Math and logic

(defn eq [& args]
  [:EQ args])

(defn add [& args]
  [:ADD args])

(defn not [bool]
  [:NOT [bool]])

;;;; Run query

(defn run [args conn]
  (swap! conn update-in [:token] inc)
  (send-start-query @conn args))
