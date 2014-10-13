(ns rethinkdb.protodefs)

(import Rethinkdb$Query$QueryType
        Rethinkdb$Term$TermType)

(defn qt->int [enum]
  (.getNumber (Enum/valueOf Rethinkdb$Query$QueryType enum)))

(defn tt->int [enum]
  (.getNumber (Enum/valueOf Rethinkdb$Term$TermType enum)))
