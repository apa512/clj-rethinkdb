(ns rethinkdb.query
  "Public interface to clj-rethinkdb. You should only need to require this namespace.

  Priority of databases used in queries:
  1. If a database is explicitly specified as part of a query it will always be used.
  2. If there is no database used in the query AND a database is set on the database
     at connection time (with the :db parameter), then this will be used.
  3. If there is no database in the query or connection, then RethinkDB will fall back
     to using the \"test\" database (You probably don't want this).

  N.B. Database names are validated at query time, not connection time."
  (:refer-clojure :exclude [count filter map get not mod replace merge
                            reduce make-array distinct keys nth min max
                            or and do fn sync time update uuid range])
  #?(:cljs (:use-macros [rethinkdb.query :only [fn]]))
  (:require [clojure.walk :as walk]
            [rethinkdb.query-builder :as qb :refer [term]]
    #?@(:clj [[manifold.stream :as s]
              [rethinkdb.net :as net]
              [rethinkdb.core :as core]]))
  #?(:clj
     (:import [rethinkdb.core Connection])))

(defn func
  "Unsweetened alternative to fn. The fn macro has the benefit of
  providing a syntax that mirrors the native clojure fn, but
  oftentimes it's necessary to have the added versatility of a
  definition that accepts pure data.

  Args is a vector of keywords. Terms is a term map.
  e.g. (func [::my-arg] terms)"
  [args terms]
  (term :FUNC [args terms]))

(defn- wrap-args [x]
  (if (= (:rethinkdb.query-builder/term x) :ARGS)
    x
    (term :ARGS [x])))

(defn term? [x]
  (:rethinkdb.query-builder/term x))

(defmacro fn [args & [body]]
  (let [new-args (into [] (clojure.core/map
                            #(hash-map :temp-var (keyword %)) args))
        new-replacements (zipmap args new-args)
        new-terms (walk/postwalk-replace new-replacements body)]
    (func new-args new-terms)))

#?(:clj (def close s/close!))

;;; Import connect

#?(:clj (def ^Connection connect
          "Creates a database connection to a RethinkDB host
          [& {:keys [host port token auth-key db]
               :or {host \"127.0.0.1\"
                    port 28015
                    token 0
                    auth-key \"\"
                    db nil}}"
          core/connect))

;;; Manipulating databases

(defn db-create
  "Creates a database."
  [db-name]
  (term :DB_CREATE [db-name]))

(defn db-drop
  "Drops a database."
  [db-name]
  (term :DB_DROP [db-name]))

(defn db-list
  "List all database names in the system."
  []
  (term :DB_LIST []))

;;; Manipulating tables

(defn table-create
  "Create a table."
  ([table-name]
   (term :TABLE_CREATE [table-name]))
  ([db table-name]
   (term :TABLE_CREATE [db table-name]))
  ([db table-name optargs]
   (term :TABLE_CREATE (if db [db table-name] [table-name]) optargs)))

(defn table-drop
  "Drop a table. If no db is provided then precedence follows the
  order given in the rethinkdb.query ns documentation."
  ([table-name]
   (term :TABLE_DROP [table-name]))
  ([db table-name]
   (term :TABLE_DROP [db table-name])))

(defn table-list
  "List all table names in a database. If no db is provided then precedence
  follows the order given in the rethinkdb.query ns documentation."
  ([]
   (term :TABLE_LIST []))
  ([db]
   (term :TABLE_LIST [db])))

(defn grant
  "Grant or deny access permissions for a user account, globally or on a per-database or per-table basis.
   conf is a map containing the optional {:keys [read write connect config]} with Boolean values"
  ([username conf]
   (term :GRANT [username conf]))            ; whole cluster grant
  ([table username conf]
   (term :GRANT [table username conf])))

(defn index-create
  "Create a new secondary index on a table. To create a simple index based on the
  value of a single field, pass the field as the index-name. If you need to
  pass optargs without a function, then set func to nil."
  [table index-name & [func optargs]]
  (if (some? func)
    (term :INDEX_CREATE [table index-name func] optargs)
    (term :INDEX_CREATE [table index-name] optargs)))

(defn index-drop
  "Delete a previously created secondary index."
  [table index-name]
  (term :INDEX_DROP [table index-name]))

(defn index-list
  "List all the secondary indexes of a table."
  [table]
  (term :INDEX_LIST [table]))

(defn index-rename
  "Rename an existing secondary index on a table. If the optional argument
  ```overwrite``` is specified as ```true```, a previously existing index with the new name
  will be deleted and the index will be renamed. If ```overwrite``` is ```false``` (the
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
  "Return an infinite stream of objects representing changes to a table, sequence,
   or a document."
  ([xs]
   (term :CHANGES [xs]))
  ([xs optargs]
   (term :CHANGES [xs] optargs)))

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
  commands to do further processing on the data. If no db is provided then precedence
  follows the order given in the rethinkdb.query ns documentation.

  To provide optargs without explicitly passing a db (instead using the db on the
  connection), pass nil as the db, e.g.
  (table nil :authors {:read-mode :outdated})"
  ([table-name]
   (term :TABLE [table-name]))
  ([db table-name]
   (term :TABLE [db table-name]))
  ([db table-name optargs]
   (term :TABLE (if db [db table-name] [table-name]) optargs)))

(defn get
  "Get a document by primary key.

  If no document exists with that primary key, get will return ```nil```"
  [table id]
  (term :GET [table id]))

(defn get-all
  "Get all documents where the given value matches the value of the requested
  index."
  [table ids & [optargs]]
  (term :GET_ALL
        (cond
          (sequential? ids) (concat [table] ids)
          (map? ids)        [table (wrap-args ids)]
          :else #?(:clj (throw (IllegalArgumentException. "get-all expects a collection or a map."))
                   :cljs (throw (js/Error. "get-all expects a collection or a map."))))
        optargs))

(defn get-field
  "Get a single field from an object."
  [sel field-names]
  (if (sequential? field-names)
    (clojure.core/reduce #(get-field %1 %2)
                         sel
                         field-names)
    (term :GET_FIELD [sel field-names])))

(defn args [sq]
  (term :ARGS [sq]))

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

  Filter can be called on a sequence, selection, or a field containing an array
  of elements. The return type is the same as the type on which the function
  was called on.

  Passing a ```:default``` optional argument can change the handling of
  documents with missing fields.

  - ```{:default true}``` will return documents with missing fields,
    rather than ignore them.
  - ```{:default (r/error))``` will cause any non-existence error to
    raise an exception.
  - ```{:default false}``` (the default) will ignore documents where
    a specified field is missing."
  [sq obj-or-func & [optargs]]
  (term :FILTER [sq obj-or-func] optargs))

;;; Joins

(defn inner-join
  "Returns an inner join of two sequences."
  [sq1 sq2 func]
  (term :INNER_JOIN [sq1 sq2 func]))

(defn outer-join
  "Returns a left outer join of two sequences."
  [sq1 sq2 func]
  (term :OUTER_JOIN [sq1 sq2 func]))

(defn eq-join
  "Join tables using a field on the left-hand sequence matching primary keys or
  secondary indexes on the right-hand table. ```eq-join``` is more efficient than
  other ReQL join types, and operates much faster. Documents in the result set
  consist of pairs of left-hand and right-hand documents, matched when the
  field on the left-hand side exists and is non-null and an entry with that
  field's value exists in the specified index on the right-hand side."
  [sq index-name table & [optargs]]
  (term :EQ_JOIN [sq index-name table] optargs))

(defn zip
  "Used to 'zip' up the result of a join by merging the 'right' fields into 'left' fields of each member of the sequence."
  [sq]
  (term :ZIP [sq]))

;;; Transformations

(defn map
  "Transform each element of one or more sequences by applying a mapping
  function to them. If ```map``` is run with two or more sequences, it will iterate
  for as many items as there are in the shortest sequence."
  [sq obj-or-func]
  (term :MAP [sq obj-or-func]))

(defn with-fields
  "Plucks one or more attributes from a sequence of objects, filtering out any
  objects in the sequence that do not have the specified fields. Functionally,
  this is identical to ```has-fields``` followed by ```pluck``` on a sequence."
  [sq fields]
  (term :WITH_FIELDS (concat [sq] fields)))

(defn concat-map
  "Concatenate one or more elements into a single sequence using a mapping
  function."
  [sel func]
  (term :CONCAT_MAP [sel func]))

(defn order-by
  "Sort the sequence by document values of the given key(s). To specify the
  ordering, wrap the attribute with either ```r.asc``` or ```r.desc``` (defaults to
  ascending).

  Sorting without an index requires the server to hold the sequence in memory,
  and is limited to 100,000 documents (or the setting of the ```array-limit``` option
  for run). Sorting with an index can be done on arbitrarily large tables, or
  after a between command using the same index."
  [sel field-or-ordering]
  (if-let [index (clojure.core/or (clojure.core/get field-or-ordering "index")
                                  (clojure.core/get field-or-ordering :index))]
    (term :ORDER_BY [sel] {:index (qb/parse-term index)})
    (term :ORDER_BY (flatten [sel field-or-ordering]))))

(defn skip
  "Skip a number of elements from the head of the sequence."
  [sel n]
  (term :SKIP [sel n]))

(defn limit
  "End the sequence after the given number of elements."
  [sq n]
  (term :LIMIT [sq n]))

(defn slice
  "Return the elements of a sequence within the specified range."
  [sq n1 n2]
  (term :SLICE [sq n1 n2]))

(defn nth
  "Get the *nth* element of a sequence."
  [sq n]
  (term :NTH [sq n]))

(defn offsets-of
  "Get the indexes of an element in a sequence. If the argument is a predicate,
  get the indexes of all elements matching it."
  [sq obj-or-func]
  (term :OFFSETS_OF [sq obj-or-func]))

(defn is-empty
  "Test if a sequence is empty."
  [sq]
  (term :IS_EMPTY [sq]))

(defn union
  "Concatenate two sequences."
  [& sqs]
  (term :UNION sqs))

(defn sample
  "Select a given number of elements from a sequence with uniform random
  distribution. Selection is done without replacement."
  [sq n]
  (term :SAMPLE [sq n]))

;;; Aggregation

(defn group
  "Takes a stream and partitions it into multiple groups based on the fields or
  functions provided. Commands chained after ```group``` will be called on each of
  these grouped sub-streams, producing grouped data."
  [sq field-or-index-or-optargs]
  (if (clojure.core/and (map? field-or-index-or-optargs)
                        (clojure.core/not (term? field-or-index-or-optargs)))
    (term :GROUP [sq] field-or-index-or-optargs)
    (term :GROUP (if (sequential? field-or-index-or-optargs)
                   (into [sq] field-or-index-or-optargs)
                   [sq field-or-index-or-optargs]))))

(defn ungroup
  "Takes a grouped stream or grouped data and turns it into an array of objects
  representing the groups. Any commands chained after ```ungroup``` will operate on
  this array, rather than operating on each group individually. This is useful
  if you want to e.g. order the groups by the value of their reduction."
  [grouped]
  (term :UNGROUP [grouped]))

(defn reduce
  "Produce a single value from a sequence through repeated application of a
  reduction function."
  [sq func]
  (term :REDUCE [sq func]))

(defn count
  "Count the number of elements in the sequence. With a single argument, count
  the number of elements equal to it. If the argument is a function, it is
  equivalent to calling filter before count."
  ([sq] (term :COUNT [sq]))
  ([sq x-or-func] (term :COUNT [sq x-or-func])))

(defn sum
  "Sums all the elements of a sequence. If called with a field name, sums all the
  values of that field in the sequence, skipping elements of the sequence that
  lack that field. If called with a function, calls that function on every
  element of the sequence and sums the results, skipping elements of the sequence
  where that function returns ```nil``` or a non-existence error."
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
  "Finds the minimum element of a sequence."
  ([sq] (term :MIN [sq]))
  ([sq field-or-func] (term :MIN [sq field-or-func])))

(defn max
  "Finds the maximum element of a sequence."
  ([sq] (term :MAX [sq]))
  ([sq field-or-func] (term :MAX [sq field-or-func])))

(defn distinct
  "Remove duplicate elements from the sequence."
  ([sq & [optargs]]
   (term :DISTINCT [sq] optargs)))

(defn contains
  "Returns whether or not a sequence contains all the specified values, or if
  functions are provided instead, returns whether or not a sequence contains
  values matching all the specified functions."
  [sq x-or-func]
  (term :CONTAINS [sq x-or-func]))

;;; Document manipulation

(defn pluck
  "Plucks out one or more attributes from either an object or a sequence of
  objects (projection)."
  [obj-or-sq x]
  (term :PLUCK [obj-or-sq x]))

(defn without
  "The opposite of pluck; takes an object or a sequence of objects, and
  a sequence of selectors, and returns them with the specified paths removed.
  Selectors can either be a single field, or a nested path to a field.
  See http://rethinkdb.com/api/javascript/without/ for selector examples."
  ;; TODO: provide docs for selectors.
  [obj-or-sq selectors]
  (term :WITHOUT (concat [obj-or-sq] selectors)))

(defn merge
  "Merge one or more objects together to construct a new object with properties from all.
  If a key occurs in more than one object, the mapping from the latter (left-to-right)
  will be the mapping in the result."
  [& objs]
  (term :MERGE objs))

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

(defn set-union
  "Add a several values to an array and return it as a set (an array with
  distinct values)."
  [sq1 sq2]
  (term :SET_UNION [sq1 sq2]))

(defn set-intersection
  "Intersect two arrays returning values that occur in both of them as a set
  (an array with distinct values)."
  [sq1 sq2]
  (term :SET_INTERSECTION [sq1 sq2]))

(defn set-difference
  "Remove the elements of one array from another and return them as a set (an
  array with distinct values)."
  [sq1 sq2]
  (term :SET_DIFFERENCE [sq1 sq2]))

(defn has-fields
  "Test if an object has one or more fields. An object has a field if it has
  that key and the key has a non-null value."
  [obj-or-sq x]
  (term :HAS_FIELDS [obj-or-sq x]))

(defn insert-at
  "Insert a value in to an array at a given index. Returns the modified array."
  [sq n x]
  (term :INSERT_AT [sq n x]))

(defn splice-at
  "Insert several values in to an array at a given index. Returns the modified
  array."
  [sq1 n sq2]
  (term :SPLICE_AT [sq1 n sq2]))

(defn delete-at
  "Remove one or more elements from an array at a given index. Returns the
  modified array."
  ([sq idx] (term :DELETE_AT [sq idx]))
  ([sq idx end-idx] (term :DELETE_AT [sq idx end-idx])))

(defn change-at
  "Change a value in an array at a given index. Returns the modified array."
  [sq n x]
  (term :CHANGE_AT [sq n x]))

(defn keys
  "Return an array containing all of the object's keys."
  [obj]
  (term :KEYS [obj]))

(defn values
  "Return an array containing all of an object’s values.
  values guarantees the values will come out in the same order as keys.
  Only supported by RethinkDB 2.2 and up."
  [obj]
  ;; TODO: stop using VALUESX, see https://github.com/rethinkdb/rethinkdb/issues/5121
  ;; for more details.
  (term :VALUESX [obj]))

(defn literal
  "Replace an object in a field instead of merging it with an existing object
  in a merge or update operation."
  [x]
  (term :LITERAL [x]))

(defn object
  "Creates an object from a list of key-value pairs."
  [& key-vals]
  (term :OBJECT key-vals))

;;; String manipulating

(defn match
  "Matches against a regular expression. If there is a match, returns an object
  with the fields:

  - ```str```: The matched string
  - ```start```: The matched string's start
  - ```end```: The matched string's end
  - ```groups```: The capture groups defined with parentheses

  If no match is found, returns ```nil```."
  [s regex-str]
  (term :MATCH [s regex-str]))

(defn split
  "Splits a string into substrings. Splits on whitespace when called with no
  arguments. When called with a separator, splits on that separator. When
  called with a separator and a maximum number of splits, splits on that
  separator at most ```max-splits``` times. (Can be called with ```nil``` as the separator
  if you want to split on whitespace while still specifying ```max-splits```.)"
  ([s] (term :SPLIT [s]))
  ([s separator] (term :SPLIT [s separator]))
  ([s separator max-splits] (term :SPLIT [s separator max-splits])))

(defn upcase
  "Uppercases a string."
  [s]
  (term :UPCASE [s]))

(defn downcase
  "Lowercases a string."
  [s]
  (term :DOWNCASE [s]))

;;; Math and logic

(defn add
  "Sum numbers, concatenate strings or concatenate arrays."
  [& args]
  (term :ADD args))

(defn sub
  "Subtract numbers."
  [& args]
  (term :SUB args))

(defn mul
  "Multiply numbers, or make a periodic array."
  [& args]
  (term :MUL args))

(defn div
  "Divide numbers."
  [& args]
  (term :DIV args))

(defn mod
  "Find the remainder when dividing numbers."
  [& args]
  (term :MOD args))

(defn eq
  "Test if values are equal."
  [& args]
  (term :EQ args))

(defn ne
  "Test if values are not equal."
  [& args]
  (term :NE args))

(defn gt
  "Test if every value is greater than the following."
  [& args]
  (term :GT args))

(defn ge
  "Test if every value is greater than or equal to the following"
  [& args]
  (term :GE args))

(defn lt
  "Test if every value is less than the following"
  [& args]
  (term :LT args))

(defn le
  "Test if every value is less than or equal to the following"
  [& args]
  (term :LE args))

(defn not
  "Compute the logical inverse (not) of an expression."
  [bool]
  (term :NOT [bool]))

(defn random
  "Generate a random number between given bounds."
  [n1 n2 & [optargs]]
  (term :RANDOM [n1 n2] optargs))

(defn round
  "Rounds the given value to the nearest whole integer.
  Less than n.5 rounds down, n.5 and greater rounds up."
  [n]
  (term :ROUND [n]))

(defn ceil
  "Rounds the given value up, returning the smallest integer value
  greater than or equal to the given value (the value’s ceiling)."
  [n]
  (term :CEIL [n]))

(defn floor
  "Rounds the given value down, returning the largest integer value less than or equal to the given value (the value’s floor)."
  [n]
  (term :FLOOR [n]))

;;; Dates and times

(defn now
  "Return a time object representing the current time in UTC. The command ```now```
  is computed once when the server receives the query, so multiple instances of
  ```now``` will always return the same time inside a query."
  []
  (term :NOW []))

(defn time
  "Create a time object for a specific time.

  - year is an integer between 1400 and 9999.
  - month is an integer between 1 and 12.
  - day is an integer between 1 and 31.
  - hour is an integer.
  - minutes is an integer.
  - seconds is a double. Its value will be rounded to three decimal places (millisecond-precision).
  timezone can be 'Z' (for UTC) or a string with the format ±[hh]:[mm].

  If tz is not supplied then UTC is used."
  ([year month day]
    (time year month day "Z"))
  ([year month day tz]
    (term :TIME [year month day tz]))
  ([year month day hour minute second]
    (time year month day hour minute second "Z"))
  ([year month day hour minute second tz]
    (term :TIME [year month day hour minute second tz])))

(defn epoch-time
  "Create a time object based on seconds since epoch. The first argument is a
  double and will be rounded to three decimal places (millisecond-precision)."
  [i]
  (term :EPOCH_TIME [i]))

(defn iso8601
  "Create a time object based on an ISO 8601 date-time string (e.g.
  '2013-01-01T01:01:01+00:00'). We support all valid ISO 8601 formats except
  for week dates. If you pass an ISO 8601 date-time without a time zone, you must
  specify the time zone with the ```default-timezone``` argument."
  [s & [optargs]]
  (term :ISO8601 [s] optargs))

(defn in-timezone
  "Return a new time object with a different timezone. While the time stays the
  same, the results returned by functions such as ```hours``` will change since they
  take the timezone into account. The timezone argument has to be of the ISO
  8601 format."
  [time-obj s]
  (term :IN_TIMEZONE [time-obj s]))

(defn timezone
  "Return the timezone of the time object."
  [time-obj]
  (term :TIMEZONE [time-obj]))

(defn during
  "Return if a time is between two other times (by default, inclusive for the
  start, exclusive for the end)."
  [time-obj start-time end-time & [optargs]]
  (term :DURING [time-obj start-time end-time] optargs))

(defn date
  "Return a new time object only based on the day, month and year (i.e. the same
  day at 00:00)."
  [time-obj]
  (term :DATE [time-obj]))

(defn time-of-day
  "Return the number of seconds elapsed since the beginning of the day stored
  in the time object."
  [time-obj]
  (term :TIME_OF_DAY [time-obj]))

(defn year
  "Return the year of a time object."
  [time-obj]
  (term :YEAR [time-obj]))

(defn month
  "Return the month of a time object as a number between 1 and 12."
  [time-obj]
  (term :MONTH [time-obj]))

(defn day
  "Return the day of a time object as a number between 1 and 31."
  [time-obj]
  (term :DAY [time-obj]))

(defn day-of-week
  "Return the day of week of a time object as a number between 1 and 7
  (following ISO 8601 standard). "
  [time-obj]
  (term :DAY_OF_WEEK [time-obj]))

(defn day-of-year
  "Return the day of the year of a time object as a number between 1 and 366
  (following ISO 8601 standard)."
  [time-obj]
  (term :DAY_OF_YEAR [time-obj]))

(defn hours
  "Return the hour in a time object as a number between 0 and 23."
  [time-obj]
  (term :HOURS [time-obj]))

(defn minutes
  "Return the minute in a time object as a number between 0 and 59."
  [time-obj]
  (term :MINUTES [time-obj]))

(defn seconds
  "Return the seconds in a time object as a number between 0 and 59.999 (double
  precision)."
  [time-obj]
  (term :SECONDS [time-obj]))

(defn to-iso8601
  "Convert a time object to its ISO 8601 format."
  [time-obj]
  (term :TO_ISO8601 [time-obj]))

(defn to-epoch-time
  "Convert a time object to its epoch time."
  [time-obj]
  (term :TO_EPOCH_TIME [time-obj]))

;;; Control structure

(defn and
  "Compute the logical \"and\" of two or more values."
  [& bools]
  (term :AND bools))

(defn or
  "Compute the logical \"or\" of two or more values."
  [& bools]
  (term :OR bools))

(def all and)
(def any or)

(defn do
  "Evaluate an expression and pass its values as arguments to a function or to
  an expression."
  [args fun]
  (term :FUNCALL [fun args]))

(defn branch
  "If the ```bool``` expression returns ```false``` or ```nil```, the ```false-branch``` will be
  evaluated. Otherwise, the ```true-branch``` will be evaluated."
  [bool true-branch false-branch]
  (term :BRANCH [bool true-branch false-branch]))

(defn for-each
  "Loop over a sequence, evaluating the given write query for each element."
  [sq func]
  (term :FOR_EACH [sq func]))

(defn range
  "Generate a stream of sequential integers in a specified range."
  ([]
   (term :RANGE []))
  ([end-value]
   (term :RANGE [end-value]))
  ([start-value end-value]
   (term :RANGE [start-value end-value])))

(defn error
  "Throw a runtime error."
  ([] (term :ERROR []))
  ([s] (term :ERROR [s])))

(defn default
  "Provide a default value in case of non-existence errors. default evaluates its
  first argument (the value it’s chained to). If that argument returns null or a
  non-existence error is thrown in evaluation, then default returns its second argument.
  The second argument is usually a default value, but it can be a RethinkDB function that
  returns a value."
  [any default]
  (term :DEFAULT [any default]))

(defn coerce-to
  "Convert a value of one type into another."
  [x s]
  (term :COERCE_TO [x s]))

(defn type-of
  "Gets the type of a value."
  [x]
  (term :TYPE_OF [x]))

(defn info
  "Get information about a ReQL value."
  [x]
  (term :INFO [x]))

(defn json
  "Parse a JSON string on the server."
  [s]
  (term :JSON [s]))

(defn http
  "Retrieve data from the specified URL over HTTP. The return type depends on
  the ```result-format``` option, which checks the Content-Type of the response by
  default."
  [url & [optargs]]
  (term :HTTP [url] optargs))

(defn uuid
  "Return a UUID (universally unique identifier), a string that can be used as
  a unique ID. If a string is passed, a UUID is generated based on a SHA-1 hash
  of the string. Passing a string is only supported by RethinkDB 2.2 and up."
  ([]
   (term :UUID []))
  ([string]
   (term :UUID [string])))

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

(defn distance
  "Compute the distance between a point and another geometry object. At least
  one of the geometry objects specified must be a point."
  [point1 point2 & [optargs]]
  (term :DISTANCE [point1 point2] optargs))

(defn fill
  "Convert a Line object into a Polygon object. If the last point does not
  specify the same coordinates as the first point, ```fill``` will close the
  polygon by connecting them."
  [point]
  (term :FILL [point]))

(defn geojson
  "Convert a GeoJSON object to a ReQL geometry object."
  [obj]
  (term :GEOJSON [obj]))

(defn to-geojson
  "Convert a ReQL geometry object to a GeoJSON object."
  [geo]
  (term :TO_GEOJSON [geo]))

(defn get-intersecting
  "Get all documents where the given geometry object intersects the geometry
  object of the requested geospatial index."
  [table geo & [optargs]]
  (term :GET_INTERSECTING [table geo] optargs))

(defn get-nearest
  "Get all documents where the specified geospatial index is within a certain
  distance of the specified point (default 100 kilometers)."
  [table geo & [optargs]]
  (term :GET_NEAREST [table geo] optargs))

(defn includes
  "Tests whether a geometry object is completely contained within another. When
  applied to a sequence of geometry objects, ```includes``` acts as a filter,
  returning a sequence of objects from the sequence that include the argument."
  [geo1 geo2]
  (term :INCLUDES [geo1 geo2]))

(defn intersects
  "Tests whether two geometry objects ```intersect``` with one another. When applied
  to a sequence of geometry objects, intersects acts as a filter, returning a
  sequence of objects from the sequence that intersect with the argument."
  [geo1 geo2]
  (term :INTERSECTS [geo1 geo2]))

(defn line
  "Construct a geometry object of type Line. The line can be specified in one
  of two ways:

  - Two or more two-item arrays, specifying longitude and latitude numbers of
    the line's vertices;
  - Two or more Point objects specifying the line's vertices."
  [points]
  (term :LINE points))

(defn point
  "Construct a geometry object of type Point. The point is specified by two
  floating point numbers, the longitude (−180 to 180) and the latitude (−90 to
  90) of the point on a perfect sphere."
  [x y & [optargs]]
  (term :POINT [x y] optargs))

(defn polygon
  "Construct a geometry object of type Polygon. The Polygon can be specified in
  one of two ways:

  - Three or more two-item arrays, specifying longitude and latitude numbers of the
    polygon's vertices;
  - Three or more Point objects specifying the polygon's vertices."
  [points]
  (term :POLYGON points))

(defn polygon-sub
  "Use ```inner-polygon``` to \"punch out\" a hole in ```outer-polygon```. ```inner-polygon```
  must be completely contained within ```outer-polygon``` and must have no holes
  itself (it must not be the output of ```polygon-sub``` itself)."
  [outer-polygon inner-polygon]
  (term :POLYGON_SUB [outer-polygon inner-polygon]))

;;; Configuration

(defn config
  "Query (read and/or update) the configurations for individual tables or
  databases."
  [table-or-db]
  (term :CONFIG [table-or-db]))

(defn rebalance
  "Rebalances the shards of a table. When called on a database, all the tables
  in that database will be rebalanced."
  [table-or-db]
  (term :REBALANCE [table-or-db]))

(defn reconfigure
  "Reconfigure a database or table’s sharding and replication."
  [table-or-db optargs]
  (term :RECONFIGURE [table-or-db] optargs))

(defn status
  "Return the status of a table."
  [table]
  (term :STATUS [table]))

(defn wait
  "Wait for a table or all the tables in a database to be ready. A table may be
   temporarily unavailable after creation, rebalancing or reconfiguring. The wait
   command blocks until the given table (or database) is fully up to date.

   If wait is called with no table or db specified, it will wait on the default db
   for the connection."
  ([] (term :WAIT []))
  ([table-or-db] (term :WAIT [table-or-db]))
  ([table-or-db optargs] (term :WAIT [table-or-db] optargs)))

;;; Misc

(def minval
  (term :MINVAL []))

(def maxval
  (term :MAXVAL []))

;;; Run query

(defn replace-vars [query]
  (let [var-counter (atom 0)]
    (walk/postwalk
      #(if (clojure.core/and (map? %) (= :FUNC (:rethinkdb.query-builder/term %)))
        (let [vars (first (:rethinkdb.query-builder/args %))
              new-vars (clojure.core/range @var-counter (+ @var-counter (clojure.core/count vars)))
              new-args (clojure.core/map
                         (clojure.core/fn [arg]
                           (term :VAR [arg]))
                         new-vars)
              var-replacements (zipmap vars new-args)]
          (swap! var-counter + (clojure.core/count vars))
          (walk/postwalk-replace
            var-replacements
            (assoc-in % [:rethinkdb.query-builder/args 0] new-vars)))
        %)
      query)))

(defn make-array [& xs]
  (term :MAKE_ARRAY xs))

#?(:clj (defn run [query conn & [opts]]
          (net/send-start-query conn (replace-vars query) opts)))

#?(:clj (defn server
          "Returns the server name and server UUID being used by a connection.
          Only supported by RethinkDB 2.2 and up"
          [conn]
          (net/send-server-query conn)))
