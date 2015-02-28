(defproject rethinkdb "0.4.39"
  :description "RethinkDB client"
  :url "http://github.com/apa512/clj-rethinkdb"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :codox {:defaults {:doc/format :markdown}
          :src-dir-uri "https://github.com/apa512/clj-rethinkdb/blob/master/"
          :src-linenum-anchor-prefix "L"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/data.json "0.2.5"]
                 [org.flatland/protobuf "0.8.1"]
                 [rethinkdb-protobuf "0.2.0"]
                 [clj-time "0.9.0"]])
