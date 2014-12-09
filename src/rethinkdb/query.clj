(ns rethinkdb.query
  (:refer-clojure :exclude [count filter map get not mod replace merge
                            make-array distinct nth do fn sync])
  (:require [clojure.data.json :as json]
            [rethinkdb.net :refer [send-start-query]]
            [rethinkdb.query-builder :refer [term]]))

(defmacro fn [args & [body]]
  (let [arg-replacements (zipmap args
                                 (clojure.core/map (clojure.core/fn [n]
                                                     (term :VAR [(inc n)]))
                                                   (range)))
        func-args (into [] (take (clojure.core/count args) (iterate inc 1)))
        func-terms (clojure.walk/postwalk-replace arg-replacements body)]
    (term :FUNC [func-args func-terms])))

;;; Manipulating databases

(defn db-create [db-name]
  (term :DB_CREATE [db-name]))

(defn db-drop [db-name]
  (term :DB_DROP [db-name]))

(defn db-list []
  (term :DB_LIST []))

;;; Manipulating tables

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

(defn index-rename [table old-name new-name & [optargs]]
  (term :INDEX_RENAME [table old-name new-name] optargs))

(defn index-status [table & index-names]
  (term :INDEX_STATUS (concat [table] index-names)))

(defn index-wait [table & index-names]
  (term :INDEX_WAIT (concat [table] index-names)))

(defn changes [table]
  (term :CHANGES [table]))

;;; Writing data

(defn insert [table objs & [optargs]]
  (term :INSERT [table objs] optargs))

(defn update [sel obj-or-func & [optargs]]
  (term :UPDATE [sel obj-or-func] optargs))

(defn replace [sel obj-or-func & [optargs]]
  (term :REPLACE [sel obj-or-func] optargs))

(defn delete [obj-or-sq & [optargs]]
  (term :DELETE [obj-or-sq] optargs))

(defn sync [table]
  (term :SYNC [table]))

;;; Selecting data

(defn db [db-name]
  (term :DB [db-name]))

(defn table [db table-name]
  (term :TABLE [db table-name]))

(defn get [table id]
  (term :GET [table id]))

(defn get-all [table ids & [optargs]]
  (term :GET_ALL (concat [table] ids) optargs))

(defn get-field [sel field-name]
  (term :GET_FIELD [sel field-name]))

(defn between [sel lower-key upper-key & [optargs]]
  (term :BETWEEN [sel lower-key upper-key] optargs))

(defn filter [sq obj-or-func]
  (term :FILTER [sq obj-or-func]))

;;; Joins

(defn inner-join [sq1 sq2 func]
  (term :INNER_JOIN [sq1 sq2 func]))

(defn outer-join [sq1 sq2 func]
  (term :OUTER_JOIN [sq1 sq2 func]))

(defn eq-join [sq index-name table & [optargs]]
  (term :EQ_JOIN [sq index-name table] optargs))

(defn zip [sq]
  (term :ZIP [sq]))

;;; Transformations

(defn map [sq obj-or-func]
  (term :MAP [sq obj-or-func]))

(defn with-fields [sq fields]
  (term :WITH_FIELDS (concat [sq] fields)))

(defn concat-map [sel func]
  (term :CONCATMAP [sel func]))

(defn order-by [sel field-or-ordering]
  (term :ORDERBY [sel field-or-ordering]))

(defn skip [sel n]
  (term :SKIP [sel n]))

(defn limit [sq n]
  (term :LIMIT [sq n]))

(defn slice [sq n1 n2]
  (term :SLICE [sq n1 n2]))

(defn nth [sq n]
  (term :NTH [sq n]))

(defn indexes-of [sq obj-or-func]
  (term :INDEXES_OF [sq obj-or-func]))

(defn is-empty [sq]
  (term :IS_EMPTY [sq]))

(defn union [& sqs]
  (term :UNION sqs))

(defn sample [sq n]
  (term :SAMPLE [sq n]))

;;; Aggregation

(defn group [sq s]
  (term :GROUP [sq s]))

(defn count [sq]
  (term :COUNT [sq]))

(defn contains [sq x-or-func]
  (term :CONTAINS [sq x-or-func]))

(defn distinct [sq]
  (term :DISTINCT [sq]))

;;; Document manipulation

(defn pluck [obj-or-sq x]
  (term :PLUCK [obj-or-sq x]))

(defn merge [obj-or-sq1 obj-or-sq2]
  (term :MERGE [obj-or-sq1 obj-or-sq2]))

(defn set-insert [sq x]
  (term :SET_INSERT [sq x]))

(defn set-intersection [sq1 sq2]
  (term :SET_INTERSECTION [sq1 sq2]))

(defn set-difference [sq1 sq2]
  (term :SET_DIFFERENCE [sq1 sq2]))

(defn has-fields [obj-or-sq x]
  (term :HAS_FIELDS [obj-or-sq x]))

(defn object [& key-vals]
  (term :OBJECT key-vals))

;;; String manipulating

(defn match [s regex-str]
  (term :MATCH [s regex-str]))

(defn split
  ([s] (term :SPLIT [s]))
  ([s separator] (term :SPLIT [s separator]))
  ([s separator max-splits] (term :SPLIT [s separator max-splits])))

(defn upcase [s]
  (term :UPCASE [s]))

(defn downcase [s]
  (term :DOWNCASE [s]))

;;; Math and logic

(defn add [& args]
  (term :ADD args))

(defn sub [& args]
  (term :SUB args))

(defn mul [& args]
  (term :MUL args))

(defn div [& args]
  (term :DIV args))

(defn mod [& args]
  (term :MOD args))

(defn eq [& args]
  (term :EQ args))

(defn gt [& args]
  (term :GT args))

(defn not [bool]
  (term :NOT [bool]))

;;; Control structure

(defn all [& bools]
  (term :ALL bools))

(defn any [& bools]
  (term :ANY bools))

(defn branch [bool true-branch false-branch]
  (term :BRANCH [bool true-branch false-branch]))

(defn for-each [sq func]
  (term :FOREACH [sq func]))

(defn coerce-to [top s]
  (term :COERCE_TO [top s]))

(defn do [args fun]
  (term :FUNCALL [fun args]))

;;; Sorting

(defn asc [field-name]
  (term :ASC [field-name]))

(defn desc [field-name]
  (term :DESC [field-name]))

;;; Run query

(defn make-array [& xs]
  (term :MAKE_ARRAY xs))

(defn run [args conn]
  (let [token (:token (swap! conn update-in [:token] inc))]
    (send-start-query conn token args)))
