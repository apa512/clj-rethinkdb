(ns rethinkdb.query
  (:refer-clojure :exclude [count filter map get not])
  (:require [clojure.data.json :as json]
            [rethinkdb.net :refer [send-start-query]]
            [rethinkdb.query-builder :refer [term]]))

(defmacro lambda [arglist & [body]]
  (let [var-replacements (zipmap arglist
                                 (clojure.core/map (fn [n]
                                                     (term :VAR [(inc n)]))
                                                   (range)))
        func-args (into [] (take (clojure.core/count arglist) (iterate inc 1)))
        func-terms (clojure.walk/postwalk-replace var-replacements body)]
    (term :FUNC [func-args func-terms])))

;;;; DB manipulation

(defn db-create [db-name]
  (term :DB_CREATE [db-name]))

(defn db-drop [db-name]
  (term :DB_DROP [db-name]))

(defn db-list []
  (term :DB_LIST []))

;;;; Table manipulation

(defn table-create [db table-name & [optargs]]
  (term :TABLE_CREATE [db table-name] optargs))

(defn table-drop [db table-name]
  (term :TABLE_DROP [db table-name]))

(defn table-list [db]
  (term :TABLE_LIST [db]))

(defn index-create [table index-name lambda1 & [optargs]]
  (term :INDEX_CREATE [table index-name lambda1] optargs))

(defn index-drop [table index-name]
  (term :INDEX_DROP [table index-name]))

(defn index-list [table]
  (term :INDEX_LIST [table]))

(defn index-rename [table old-index new-index & [optargs]]
  (term :INDEX_RENAME [table old-index new-index] optargs))

(defn index-status [table & index-names]
  (term :INDEX_STATUS (concat [table] index-names)))

(defn index-wait [table & index-names]
  (term :INDEX_WAIT (concat [table] index-names)))

(defn changes [table]
  (term :CHANGES [table]))

;;;; Writing data

(defn insert [table objs & {:as optargs}]
  (term :INSERT [table objs] optargs))

(defn update [obj-or-sq obj-or-func & [optargs]]
  (term :UPDATE [obj-or-sq obj-or-func] optargs))

(defn delete [table-or-obj-or-sq]
  (term :DELETE [table-or-obj-or-sq]))

;;;; Selecting data

(defn db [db-name]
  (term :DB [db-name]))

(defn table [db table-name]
  (term :TABLE [db table-name]))

(defn filter [sq obj-or-func]
  (term :FILTER [sq obj-or-func]))

(defn get [table id]
  (term :GET [table id]))

(defn get-all [table x & [optargs]]
  (term :GET_ALL [table x] optargs))

(defn get-field [obj-or-sq x]
  (term :GET_FIELD [obj-or-sq x]))

(defn contains [sq x-or-func]
  (term :CONTAINS [sq x-or-func]))

;;;; Document manipulation

(defn has-fields [obj-or-sq x]
  (term :HAS_FIELDS [obj-or-sq x]))

(defn pluck [obj-or-sq x]
  (term :PLUCK [obj-or-sq x]))

(defn set-insert [sq x]
  (term :SET_INSERT [sq x]))

(defn set-difference [sq1 sq2]
  (term :SET_DIFFERENCE [sq1 sq2]))

;;;; Aggregation

(defn count [sq]
  (term :COUNT [sq]))

;;;; Transformations

(defn map [sq obj-or-func]
  (term :MAP [sq obj-or-func]))

(defn limit [sq n]
  (term :LIMIT [sq n]))

;;;; Math and logic

(defn eq [& args]
  (term :EQ args))

(defn add [& args]
  (term :ADD args))

(defn not [bool]
  (term :NOT [bool]))

;;;; Run query

(defn run [args conn]
  (swap! conn update-in [:token] inc)
  (send-start-query @conn args))
