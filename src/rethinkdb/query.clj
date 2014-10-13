(ns rethinkdb.query
  (:refer-clojure :exclude [count])
  (:require [clojure.data.json :as json]
            [rethinkdb.net :refer [send-query process-response read-response]]
            [rethinkdb.query-builder :refer [query->json]]))

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
  (send conn update-in [:token] inc)
  (println "Running query with token" (:token @conn))
  (let [json (query->json :START args)
        conn @conn]
    (send-query conn json)
    (process-response (read-response (:in conn)) conn)))
