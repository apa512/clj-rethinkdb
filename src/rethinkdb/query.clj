(ns rethinkdb.query
  (:refer-clojure :exclude [count filter map get not mod replace merge
                            reduce make-array distinct keys nth min max
                            do fn sync time])
  (:require [clojure.data.json :as json]
            [clojure.walk :refer [postwalk postwalk-replace]]
            [rethinkdb.net :refer [send-start-query]]
            [rethinkdb.query-builder :refer [term parse-term]]))

(defmacro fn [args & [body]]
  (let [new-args (into [] (clojure.core/map #(hash-map :temp-var (keyword %)) args))
        new-replacements (zipmap args new-args)
        new-terms (postwalk-replace new-replacements body)]
    (term :FUNC [new-args new-terms])))

;;; Manipulating databases

(defn db-create
  "Create a database. A RethinkDB database is a collection of tables, similar
  to relational databases.

  Note: that you can only use alphanumeric characters and underscores for the
  database name."
  [db-name]
  (term :DB_CREATE [db-name]))

(defn db-drop
  "Drop a database. The database, all its tables, and corresponding data will
  be deleted."
  [db-name]
  (term :DB_DROP [db-name]))

(defn db-list
  "List all database names in the system. The result is a list of strings."
  []
  (term :DB_LIST []))

;;; Manipulating tables

(defn table-create
  "Create a table. A RethinkDB table is a collection of JSON documents."
  [db table-name & [optargs]]
  (term :TABLE_CREATE [db table-name] optargs))

(defn table-drop
  "Drop a table. The table and all its data will be deleted."
  [db table-name]
  (term :TABLE_DROP [db table-name]))

(defn table-list
  "List all table names in a database. The result is a list of strings."
  [db]
  (term :TABLE_LIST [db]))

(defn index-create
  "Create a new secondary index on a table."
  [table index-name func & [optargs]]
  (term :INDEX_CREATE [table index-name func] optargs))

(defn index-drop
  "Delete a previously created secondary index of this table."
  [table index-name]
  (term :INDEX_DROP [table index-name]))

(defn index-list
  "List all the secondary indexes of this table."
  [table]
  (term :INDEX_LIST [table]))

(defn index-rename
  "Rename an existing secondary index on a table. If the optional argument
  overwrite is specified as ```true```, a previously existing index with the new name
  will be deleted and the index will be renamed. If overwrite is ```false``` (the
  default) an error will be raised if the new index name already exists."
  [table old-name new-name & [optargs]]
  (term :INDEX_RENAME [table old-name new-name] optargs))

(defn index-status
  "Get the status of the specified indexes on this table, or the status of all
  indexes on this table if no indexes are specified."
  [table & index-names]
  (term :INDEX_STATUS (concat [table] index-names)))

(defn index-wait
  "Wait for the specified indexes on this table to be ready, or for all indexes
  on this table to be ready if no indexes are specified."
  [table & index-names]
  (term :INDEX_WAIT (concat [table] index-names)))

(defn changes
  "Return an infinite stream of objects representing changes to a table or a document."
  [table]
  (term :CHANGES [table]))

;;; Writing data

(defn insert
  "Insert documents into a table. Accepts a list of documents."
  [table objs & [optargs]]
  (term :INSERT [table objs] optargs))

(defn update
  "Update JSON documents in a table. Accepts a JSON document, a ReQL
  expression, or a combination of the two."
  [sel obj-or-func & [optargs]]
  (term :UPDATE [sel obj-or-func] optargs))

(defn replace
  "Replace documents in a table. Accepts a JSON document or a ReQL expression,
  and replaces the original document with the new one. The new document must
  have the same primary key as the original document."
  [sel obj-or-func & [optargs]]
  (term :REPLACE [sel obj-or-func] optargs))

(defn delete
  "Delete one or more documents from a table."
  [obj-or-sq & [optargs]]
  (term :DELETE [obj-or-sq] optargs))

(defn sync
  "```sync``` ensures that writes on a given table are written to permanent storage.
  Queries that specify soft durability ```({:durability \"soft\"})``` do not give such
  guarantees, so ```sync``` can be used to ensure the state of these queries. A call
  to ```sync``` does not return until all previous writes to the table are
  persisted."
  [table]
  (term :SYNC [table]))

;;; Selecting data

(defn db
  "Reference a database."
  [db-name]
  (term :DB [db-name]))

(defn table
  "Select all documents in a table. This command can be chained with other
  commands to do further processing on the data."
  [db table-name]
  (term :TABLE [db table-name]))

(defn get
  "Get a document by primary key.

  If no document exists with that primary key, get will return ```nil```"
  [table id]
  (term :GET [table id]))

(defn get-all
  "Get all documents where the given value matches the value of the requested
  index.

  Accepts a list of values."
  [table ids & [optargs]]
  (term :GET_ALL (concat [table] ids) optargs))

(defn get-field
  "Get a single field from an object."
  [sel field-name]
  (term :GET_FIELD [sel field-name]))

(defn between
  "Get all documents between two keys. Accepts three optional arguments: ```index```,
  ```left-bound```, and ```right-bound```. If ```index``` is set to the name of a secondary
  index, ```between``` will return all documents where that index's value is in the
  specified range (it uses the primary key by default). ```left-bound``` or
  ```right-bound``` may be set to open or closed to indicate whether or not to
  include that endpoint of the range (by default, ```left-bound``` is closed and
  ```right-bound``` is open)."
  [sel lower-key upper-key & [optargs]]
  (term :BETWEEN [sel lower-key upper-key] optargs))

(defn filter
  "Get all the documents for which the given predicate is true.

  filter can be called on a sequence, selection, or a field containing an array
  of elements. The return type is the same as the type on which the function
  was called on.

  The body of every filter is wrapped in an implicit ```{:default false}```, which means
  that if a non-existence errors is thrown (when you try to access a field that
  does not exist in a document), RethinkDB will just ignore the document. Passing
  the optional argument ```{:default (r/error)}``` will cause any non-existence errors
  to raise an exception."
  [sq obj-or-func]
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

(defn concat-map
  "Concatenate one or more elements into a single sequence using a mapping
  function."
  [sel func]
  (term :CONCAT_MAP [sel func]))

(defn order-by [sel field-or-ordering]
  (if-let [index (or (clojure.core/get field-or-ordering "index")
                     (clojure.core/get field-or-ordering :index))]
    (term :ORDER_BY [sel] {:index (parse-term index)})
    (term :ORDER_BY [sel field-or-ordering])))

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

(defn count
  "Count the number of elements in the sequence. With a single argument, count
  the number of elements equal to it. If the argument is a function, it is
  equivalent to calling filter before count."
  ([sq] (term :COUNT [sq]))
  ([sq x-or-func] (term :COUNT [sq x-or-func])))

(defn sum
  ([sq] (term :SUM [sq]))
  ([sq field-or-func] (term :SUM [sq field-or-func])))

(defn avg
  "Averages all the elements of a sequence. If called with a field name,
  averages all the values of that field in the sequence, skipping elements of
  the sequence that lack that field. If called with a function, calls that
  function on every element of the sequence and averages the results, skipping
  elements of the sequence where that function returns ```nil``` or a non-existence
  error."
  ([sq] (term :AVG [sq]))
  ([sq field-or-func] (term :AVG [sq field-or-func])))

(defn min
  ([sq] (term :MIN [sq]))
  ([sq field-or-func] (term :MIN [sq field-or-func])))

(defn max
  ([sq] (term :MAX [sq]))
  ([sq field-or-func] (term :MAX [sq field-or-func])))

(defn contains
  "Returns whether or not a sequence contains all the specified values, or if
  functions are provided instead, returns whether or not a sequence contains
  values matching all the specified functions."
  [sq x-or-func]
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

(defn append
  "Append a value to an array."
  [sq x]
  (term :APPEND [sq x]))

(defn prepend
  "Prepend a value to an array."
  [sq x]
  (term :PREPEND [sq x]))

(defn difference
  "Remove the elements of one array from another array."
  [sq1 sq2]
  (term :DIFFERENCE [sq1 sq2]))

(defn set-insert
  "Add a value to an array and return it as a set (an array with distinct values)."
  [sq x]
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

(defn change-at
  "Change a value in an array at a given index. Returns the modified array."
  [sq n x]
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

(defn add
  "Sum numbers, concatenate strings or concatenate arrays."
  [& args]
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

(defn date
  "Return a new time object only based on the day, month and year (i.e. the same
  day at 00:00)."
  [time-obj]
  (term :DATE [time-obj]))

(defn time-of-day [time-obj]
  (term :TIME_OF_DAY [time-obj]))

(defn year [time-obj]
  (term :YEAR [time-obj]))

(defn month [time-obj]
  (term :MONTH [time-obj]))

(defn day
  "Return the day of a time object as a number between 1 and 31."
  [time-obj]
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

(defn all
  "Compute the logical \"and\" of two or more values."
  [& bools]
  (term :ALL bools))

(defn any
  "Compute the logical \"or\" of two or more values."
  [& bools]
  (term :ANY bools))

(defn do [args fun]
  (term :FUNCALL [fun args]))

(defn branch
  "If the ```bool``` expression returns ```false``` or ```nil```, the ```false-branch``` will be
  evaluated. Otherwise, the ```true-branch``` will be evaluated."
  [bool true-branch false-branch]
  (term :BRANCH [bool true-branch false-branch]))

(defn for-each [sq func]
  (term :FOR_EACH [sq func]))

(defn error
  "Throw a runtime error. If called with no arguments inside the second
  argument to ```:default```, re-throw the current error."
  ([] (term :ERROR []))
  ([s] (term :ERROR [s])))

(defn coerce-to
  "Convert a value of one type into another."
  [x s]
  (term :COERCE_TO [x s]))

(defn type-of [x]
  (term :TYPE_OF [x]))

(defn info [x]
  (term :INFO [x]))

(defn json [s]
  (term :JSON [s]))

(defn http [url & [optargs]]
  (term :HTTP [url] optargs))

(defn uuid []
  (term :UUID []))

;;; Sorting

(defn asc
  "Specify ascending order."
  [field-name]
  (term :ASC [field-name]))

(defn desc
  "Specify descending order."
  [field-name]
  (term :DESC [field-name]))

;;; Geospatial commands

(defn circle
  "Construct a circular line or polygon. A circle in RethinkDB is a polygon or
  line *approximating* a circle of a given radius around a given center,
  consisting of a specified number of vertices (default 32)."
  [point radius & [optargs]]
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

;;; Configuration

(defn config
  "Query (read and/or update) the configurations for individual tables or
  databases."
  [table-or-db]
  (term :CONFIG [table-or-db]))

(defn rebalance [table-or-db]
  (term :REBALANCE [table-or-db]))

(defn status [table]
  (term :STATUS [table]))

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
  (let [token (:token (swap! conn update-in [:token] inc))]
    (send-start-query conn token (replace-vars query))))
