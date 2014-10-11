(ns rethinkdb.protodefs)

(import Ql2$Query$QueryType
        Ql2$Term$TermType)

(defn qt->int [enum]
  (.getNumber (Enum/valueOf Ql2$Query$QueryType enum)))

(defn tt->int [enum]
  (.getNumber (Enum/valueOf Ql2$Term$TermType enum)))
