(ns rethinkdb.types
  (:import [rethinkdb Ql2$Query$QueryType Ql2$Term$TermType]
           [com.google.protobuf ProtocolMessageEnum]))

(defn qt->int
  "Takes a RethinkDB Query type and returns its protocol buffer representation."
  [enum]
  (.getNumber ^ProtocolMessageEnum (Enum/valueOf Ql2$Query$QueryType (name enum))))

(defn tt->int
  "Takes a RethinkDB Term type and returns its protocol buffer representation."
  [enum]
  (.getNumber ^ProtocolMessageEnum (Enum/valueOf Ql2$Term$TermType (name enum))))
