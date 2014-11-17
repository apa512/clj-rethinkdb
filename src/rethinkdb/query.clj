(ns rethinkdb.query
  (:refer-clojure :exclude [count filter map get not replace merge make-array sync])
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

(defn index-create [table index-name func & [optargs]]
  (term :INDEX_CREATE [table index-name func] optargs))

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

(defn insert [table objs & [optargs]]
  (term :INSERT [table objs] optargs))

(defn update [obj-or-sq obj-or-func & [optargs]]
  (term :UPDATE [obj-or-sq obj-or-func] optargs))

(defn replace [obj-or-sq func & [optargs]]
  (term :REPLACE [obj-or-sq func] optargs))

(defn delete [obj-or-sq & [optargs]]
  (term :DELETE [obj-or-sq] optargs))

(defn sync [table]
  (term :SYNC [table]))

(defn delete [obj-or-sq]
  (term :DELETE [obj-or-sq]))

;;;; Selecting data

(defn db [db-name]
  (term :DB [db-name]))

(defn table [db table-name]
  (term :TABLE [db table-name]))

(defn get [table id]
  (term :GET [table id]))

(defn get-all [table x & [optargs]]
  (term :GET_ALL (concat [table] x) optargs))

(defn get-field [obj-or-sq x]
  (term :GET_FIELD [obj-or-sq x]))

(defn between [lower-key upper-key & [optargs]]
  (term :BETWEEN [lower-key upper-key] optargs))

(defn filter [sq obj-or-func]
  (term :FILTER [sq obj-or-func]))

;;;; Joins

(defn inner-join [sq1 sq2 func]
  (term :INNER_JOIN [sq1 sq2 func]))

(defn outer-join [sq1 sq2 func]
  (term :OUTER_JOIN [sq1 sq2 func]))

(defn eq-join [sq s table & [optargs]]
  (term :EQ_JOIN [sq s table] optargs))

(defn zip [sq]
  (term :ZIP [sq]))

;;;; Document manipulation

(defn pluck [obj-or-sq x]
  (term :PLUCK [obj-or-sq x]))

(defn merge [obj-or-sq1 obj-or-sq2]
  (term :MERGE [obj-or-sq1 obj-or-sq2]))

(defn set-insert [sq x]
  (term :SET_INSERT [sq x]))

(defn set-difference [sq1 sq2]
  (term :SET_DIFFERENCE [sq1 sq2]))

(defn has-fields [obj-or-sq x]
  (term :HAS_FIELDS [obj-or-sq x]))

(defn object [& key-vals]
  (term :OBJECT key-vals))

;;;; Transformations

(defn map [sq obj-or-func]
  (term :MAP [sq obj-or-func]))

(defn limit [sq n]
  (term :LIMIT [sq n]))

(defn union [& sqs]
  (term :UNION sqs))

;;;; Math and logic

(defn add [& args]
  (term :ADD args))

(defn eq [& args]
  (term :EQ args))

(defn gt [& args]
  (term :GT args))

(defn not [bool]
  (term :NOT [bool]))

;;;; Aggregation

(defn count [sq]
  (term :COUNT [sq]))

(defn contains [sq x-or-func]
  (term :CONTAINS [sq x-or-func]))

;;;; Control structure

(defn all [& bools]
  (term :ALL bools))

(defn coerce-to [top s]
  (term :COERCE_TO [top s]))

;;;; Run query

(defn make-array [& xs]
  (term :MAKE_ARRAY xs))

(defn run [args conn]
  (swap! conn update-in [:token] inc)
  (send-start-query @conn args))
