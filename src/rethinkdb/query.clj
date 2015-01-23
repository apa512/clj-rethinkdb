(ns rethinkdb.query
  (:refer-clojure :exclude [count filter map get not mod replace merge
                            reduce make-array distinct keys nth min max
                            do fn sync time])
  (:require [clojure.data.json :as json]
            [clojure.walk :refer [postwalk postwalk-replace]]
            [rethinkdb.net :refer [send-start-query]]
            [rethinkdb.query-builder :refer [term]]))

(defmacro fn [args & [body]]
  (let [new-args (into [] (clojure.core/map #(hash-map :temp-var (keyword %)) args))
        new-replacements (zipmap args new-args)
        new-terms (postwalk-replace new-replacements body)]
    (term :FUNC [new-args new-terms])))

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

(defn ungroup [grouped]
  (term :UNGROUP [grouped]))

(defn reduce [sq func]
  (term :REDUCE [sq func]))

(defn count [sq]
  (term :COUNT [sq]))

(defn sum
  ([sq] (term :SUM [sq]))
  ([sq str-or-func] (term :SUM [sq str-or-func])))

(defn avg
  ([sq] (term :AVG [sq]))
  ([sq str-or-func] (term :AVG [sq str-or-func])))

(defn min
  ([sq] (term :MIN [sq]))
  ([sq str-or-func] (term :MIN [sq str-or-func])))

(defn max
  ([sq] (term :MAX [sq]))
  ([sq str-or-func] (term :MAX [sq str-or-func])))

(defn contains [sq x-or-func]
  (term :CONTAINS [sq x-or-func]))

(defn distinct [sq]
  (term :DISTINCT [sq]))

;;; Document manipulation

(defn pluck [obj-or-sq x]
  (term :PLUCK [obj-or-sq x]))

(defn without [obj-or-sq fields]
  (term :WITHOUT (concat [obj-or-sq] fields)))

(defn merge [obj-or-sq1 obj-or-sq2]
  (term :MERGE [obj-or-sq1 obj-or-sq2]))

(defn append [sq x]
  (term :APPEND [sq x]))

(defn prepend [sq x]
  (term :PREPEND [sq x]))

(defn difference [sq1 sq2]
  (term :DIFFERENCE [sq1 sq2]))

(defn set-insert [sq x]
  (term :SET_INSERT [sq x]))

(defn set-union [sq1 sq2]
  (term :SET_UNION [sq1 sq2]))

(defn set-intersection [sq1 sq2]
  (term :SET_INTERSECTION [sq1 sq2]))

(defn set-difference [sq1 sq2]
  (term :SET_DIFFERENCE [sq1 sq2]))

(defn has-fields [obj-or-sq x]
  (term :HAS_FIELDS [obj-or-sq x]))

(defn insert-at [sq n x]
  (term :INSERT_AT [sq n x]))

(defn splice-at [sq1 n sq2]
  (term :SPLICE_AT [sq1 n sq2]))

(defn delete-at
  ([sq idx] (term :DELETE_AT [sq idx]))
  ([sq idx end-idx] (term :DELETE_AT [sq idx end-idx])))

(defn change-at [sq n x]
  (term :CHANGE_AT [sq n x]))

(defn keys [obj]
  (term :KEYS [obj]))

(defn literal [x]
  (term :LITERAL [x]))

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

(defn ne [& args]
  (term :NE args))

(defn gt [& args]
  (term :GT args))

(defn ge [& args]
  (term :GE args))

(defn lt [& args]
  (term :LT args))

(defn le [& args]
  (term :LE args))

(defn not [bool]
  (term :NOT [bool]))

(defn random [n1 n2 & [optargs]]
  (term :RANDOM [n1 n2] optargs))

;;; Dates and times

(defn now []
  (term :NOW []))

(defn time [& date-time-parts]
  (let [args (concat date-time-parts
                     (if (instance? String (last date-time-parts))
                       []
                       ["+00:00"]))]
    (term :TIME args)))

(defn epoch-time [i]
  (term :EPOCH_TIME [i]))

(defn iso8601 [s & [optargs]]
  (term :ISO8601 [s] optargs))

(defn in-timezone [time-obj s]
  (term :IN_TIMEZONE [time-obj s]))

(defn timezone [time-obj]
  (term :TIMEZONE [time-obj]))

(defn during [time-obj start-time end-time & [optargs]]
  (term :DURING [time-obj start-time end-time] optargs))

(defn date [time-obj]
  (term :DATE [time-obj]))

(defn time-of-day [time-obj]
  (term :TIME_OF_DAY [time-obj]))

(defn year [time-obj]
  (term :YEAR [time-obj]))

(defn month [time-obj]
  (term :MONTH [time-obj]))

(defn day [time-obj]
  (term :DAY [time-obj]))

(defn day-of-week [time-obj]
  (term :DAY_OF_WEEK [time-obj]))

(defn day-of-year [time-obj]
  (term :DAY_OF_YEAR [time-obj]))

(defn hours [time-obj]
  (term :HOURS [time-obj]))

(defn minutes [time-obj]
  (term :MINUTES [time-obj]))

(defn seconds [time-obj]
  (term :SECONDS [time-obj]))

(defn to-iso8601 [time-obj]
  (term :TO_ISO8601 [time-obj]))

(defn to-epoch-time [time-obj]
  (term :TO_EPOCH_TIME [time-obj]))

;;; Control structure

(defn all [& bools]
  (term :ALL bools))

(defn any [& bools]
  (term :ANY bools))

(defn do [args fun]
  (term :FUNCALL [fun args]))

(defn branch [bool true-branch false-branch]
  (term :BRANCH [bool true-branch false-branch]))

(defn for-each [sq func]
  (term :FOREACH [sq func]))

(defn coerce-to [top s]
  (term :COERCE_TO [top s]))

(defn type-of [top]
  (term :TYPEOF [top]))

(defn info [top]
  (term :INFO [top]))

(defn json [s]
  (term :JSON [s]))

(defn http [url & [optargs]]
  (term :HTTP [url] optargs))

(defn uuid []
  (term :UUID []))

;;; Sorting

(defn asc [field-name]
  (term :ASC [field-name]))

(defn desc [field-name]
  (term :DESC [field-name]))

;;; Geospatial commands

(defn circle [point radius & [optargs]]
  (term :CIRCLE [point radius] optargs))

(defn distance [point1 point2 & [optargs]]
  (term :DISTANCE [point1 point2] optargs))

(defn fill [point]
  (term :FILL [point]))

(defn geojson [obj]
  (term :GEOJSON [obj]))

(defn to-geojson [geo]
  (term :TO_GEOJSON [geo]))

(defn get-intersection [table geo & [optargs]]
  (term :GET_INTERSECTION [table geo] optargs))

(defn get-nearest [table geo & [optargs]]
  (term :GET_NEAREST [table geo] optargs))

(defn includes [geo1 geo2]
  (term :INCLUDES [geo1 geo2]))

(defn intersects [geo1 geo2]
  (term :INTERSECTS [geo1 geo2]))

(defn line [points]
  (term :LINE points))

(defn point [x y & [optargs]]
  (term :POINT [x y] optargs))

(defn polygon [points]
  (term :POLYGON points))

(defn polygon-sub [outer-polygon inner-polygon]
  (term :POLYGON_SUB [outer-polygon inner-polygon]))

;;; Run query

(defn replace-vars [query]
  (let [var-counter (atom 0)]
    (postwalk
      #(if (and (map? %) (= :FUNC (:rethinkdb.query-builder/term %)))
         (let [vars (first (:rethinkdb.query-builder/args %))
               new-vars (range @var-counter (+ @var-counter (clojure.core/count vars)))
               new-args (clojure.core/map
                          (clojure.core/fn [arg]
                            (term :VAR [arg]))
                          new-vars)
               var-replacements (zipmap vars new-args)]
           (swap! var-counter + (clojure.core/count vars))
           (postwalk-replace
             var-replacements
             (assoc-in % [:rethinkdb.query-builder/args 0] new-vars)))
         %)
      query)))

(defn make-array [& xs]
  (term :MAKE_ARRAY xs))

(defn run [query conn]
  (replace-vars query)
  (let [token (:token (swap! conn update-in [:token] inc))]
    (send-start-query conn token (replace-vars query))))
