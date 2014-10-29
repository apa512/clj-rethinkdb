(ns rethinkdb.types)

(import Ql2$Query$QueryType
        Ql2$Term$TermType)

(defn qt->int [enum]
  (.getNumber (Enum/valueOf Ql2$Query$QueryType (name enum))))

(defn tt->int [enum]
  (.getNumber (Enum/valueOf Ql2$Term$TermType (name enum))))
