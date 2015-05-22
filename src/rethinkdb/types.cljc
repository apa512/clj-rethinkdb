(ns rethinkdb.types)

#?(:clj (import Ql2$Query$QueryType
                Ql2$Term$TermType))

#?(:cljs (def query-types
           {"START"        1
            "CONTINUE"     2
            "STOP"         3
            "NOREPLY_WAIT" 4}))

#?(:cljs (def term-types
           {"DATUM"              1
            "MAKE_ARRAY"         2
            "MAKE_OBJ"           3
            "VAR"                10
            "JAVASCRIPT"         11
            "UUID"               169
            "HTTP"               153
            "ERROR"              12
            "IMPLICIT_VAR"       13
            "DB"                 14
            "TABLE"              15
            "GET"                16
            "GET_ALL"            78
            "EQ"                 17
            "NE"                 18
            "LT"                 19
            "LE"                 20
            "GT"                 21
            "GE"                 22
            "NOT"                23
            "ADD"                24
            "SUB"                25
            "MUL"                26
            "DIV"                27
            "MOD"                28
            "FLOOR"              183
            "CEIL"               184
            "ROUND"              185
            "APPEND"             29
            "PREPEND"            80
            "DIFFERENCE"         95
            "SET_INSERT"         88
            "SET_INTERSECTION"   89
            "SET_UNION"          90
            "SET_DIFFERENCE"     91
            "SLICE"              30
            "SKIP"               70
            "LIMIT"              71
            "OFFSETS_OF"         87
            "CONTAINS"           93
            "GET_FIELD"          31
            "KEYS"               94
            "OBJECT"             143
            "HAS_FIELDS"         32
            "WITH_FIELDS"        96
            "PLUCK"              33
            "WITHOUT"            34
            "MERGE"              35
            "BETWEEN_DEPRECATED" 36
            "BETWEEN"            182
            "REDUCE"             37
            "MAP"                38
            "FILTER"             39
            "CONCAT_MAP"         40
            "ORDER_BY"           41
            "DISTINCT"           42
            "COUNT"              43
            "IS_EMPTY"           86
            "UNION"              44
            "NTH"                45
            "BRACKET"            170
            "INNER_JOIN"         48
            "OUTER_JOIN"         49
            "EQ_JOIN"            50
            "ZIP"                72
            "RANGE"              173
            "INSERT_AT"          82
            "DELETE_AT"          83
            "CHANGE_AT"          84
            "SPLICE_AT"          85
            "COERCE_TO"          51
            "TYPE_OF"            52
            "UPDATE"             53
            "DELETE"             54
            "REPLACE"            55
            "INSERT"             56
            "DB_CREATE"          57
            "DB_DROP"            58
            "DB_LIST"            59
            "TABLE_CREATE"       60
            "TABLE_DROP"         61
            "TABLE_LIST"         62
            "CONFIG"             174
            "STATUS"             175
            "WAIT"               177
            "RECONFIGURE"        176
            "REBALANCE"          179
            "SYNC"               138
            "INDEX_CREATE"       75
            "INDEX_DROP"         76
            "INDEX_LIST"         77
            "INDEX_STATUS"       139
            "INDEX_WAIT"         140
            "INDEX_RENAME"       156
            "FUNCALL"            64
            "BRANCH"             65
            "OR"                 66
            "AND"                67
            "FOR_EACH"           68}))

(defn qt->int [enum]
  #?(:clj  (.getNumber (Enum/valueOf Ql2$Query$QueryType (name enum)))
     :cljs (get query-types (name enum))))

(defn tt->int [enum]
  #?(:clj  (.getNumber (Enum/valueOf Ql2$Term$TermType (name enum)))
     :cljs (get term-types (name enum))))
