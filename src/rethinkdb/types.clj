(ns rethinkdb.types)

(import Ql2$Query$QueryType
        Ql2$Term$TermType)

(defn qt->int
  "Takes a RethinkDB Query type and returns its protocol buffer representation."
  [enum]
  (.getNumber (Enum/valueOf Ql2$Query$QueryType (name enum))))

(defn tt->int
  "Takes a RethinkDB Term type and returns its protocol buffer representation."
  [enum]
  (.getNumber (Enum/valueOf Ql2$Term$TermType (name enum))))
