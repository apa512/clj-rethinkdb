(ns rethinkdb.query
  (:refer-clojure :exclude [count])
  (:require [clojure.data.json :as json]
            [rethinkdb.net :refer [send-start-query]]))

(defn db [db]
  [:DB [db]])

(defn table [db table]
  [:TABLE [db table]])

(defn count [sq]
  [:COUNT [sq]])

(defn insert [table objs]
  [:INSERT [table [:MAKE_ARRAY (vec objs)]]])

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

(defn run [args conn]
  (swap! conn update-in [:token] inc)
  (send-start-query @conn args))
