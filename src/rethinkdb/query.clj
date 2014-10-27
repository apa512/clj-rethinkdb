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

(defn table-create [db table & {:as optargs}]
  [:TABLE_CREATE [db table] optargs])

(defn table-drop [db table]
  [:TABLE_DROP [db table]])

(defn table-list [db]
  [:TABLE_LIST [db]])

(defn index-create [table idx lambda1 & {:as optargs}]
  [:INDEX_CREATE [table idx lambda1] optargs])

(defn index-drop [table idx]
  [:INDEX_DROP [table idx]])

(defn index-list [table]
  [:INDEX_LIST [table]])

(defn index-rename [table old-idx new-idx & {:as optargs}]
  [:INDEX_RENAME [table old-idx new-idx] optargs])

(defn index-wait [table & idxs]
  [:INDEX_WAIT [table idxs]])

;;;; Writing data

(defn insert [table objs & {:as optargs}]
  [:INSERT [table objs] optargs])

(defn update [obj-or-sq obj-or-func & {:as optargs}]
  [:UPDATE [obj-or-sq obj-or-func] optargs])

(defn delete [table-or-obj-or-sq]
  [:DELETE [table-or-obj-or-sq]])

;;;; Selecting data

(defn db [db]
  [:DB [db]])

(defn table [db table]
  [:TABLE [db table]])

(defn filter [sq obj-or-func]
  [:FILTER [sq obj-or-func]])

(defn get [table id]
  [:GET [table id]])

(defn get-all [table s & {:as optargs}]
  [:GET_ALL [table s] optargs])

(defn get-field [obj-or-sq s]
  [:GET_FIELD [obj-or-sq s]])

(defn contains [sq x-or-func]
  [:CONTAINS [sq x-or-func]])

;;;; Document manipulation

(defn has-fields [obj-or-sq s]
  [:HAS_FIELDS [obj-or-sq s]])

(defn pluck [obj-or-sq s]
  [:PLUCK [obj-or-sq s]])

(defn set-insert [sq x]
  [:SET_INSERT [sq x]])

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
