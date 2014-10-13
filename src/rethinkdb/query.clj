(ns rethinkdb.query
  (:refer-clojure :exclude [count])
  (:require [clojure.data.json :as json]
            [rethinkdb.net :refer [send-query read-response]]
            [rethinkdb.protodefs :refer [tt->int qt->int]]))

(defn parse-args [args]
  (letfn [(parse [arg]
            (cond
              (keyword? arg) (tt->int (name arg))
              (vector? arg) (parse-args arg)
              :else arg))]
    (map parse args)))

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

;;;; Run query

(defn query->json
  ([args]
   (json/write-str (parse-args args)))
  ([type args]
   (json/write-str [(qt->int (name type)) (parse-args args)])))

(defn process-response [resp]
  (let [{t "t" r "r"} resp]
    (condp = t
      1 (first r)
      2 r)))

(defn run [args conn]
  (let [json (query->json :START (parse-args args))
        in (:in @conn)]
    (send conn update-in [:token] inc)
    (send-query @conn json)
    (process-response (read-response in))))
