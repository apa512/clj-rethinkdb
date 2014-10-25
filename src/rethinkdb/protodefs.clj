(ns rethinkdb.protodefs)

(import Rethinkdb$Query$QueryType
        Rethinkdb$Term$TermType)

(def tt-values
  (into #{}
    (map str (vec (Rethinkdb$Term$TermType/values)))))

(defn qt->int [enum]
  (.getNumber (Enum/valueOf Rethinkdb$Query$QueryType enum)))

(defn tt->int [enum]
  (if (get tt-values enum)
    (.getNumber (Enum/valueOf Rethinkdb$Term$TermType enum))
    enum))
