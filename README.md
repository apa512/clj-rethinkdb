# clj-rethinkdb

[![Circle CI](https://circleci.com/gh/apa512/clj-rethinkdb.svg?style=svg)](https://circleci.com/gh/apa512/clj-rethinkdb)

## Install

[![Clojars Project](http://clojars.org/rethinkdb/latest-version.svg)](http://clojars.org/rethinkdb)

## Example

```clojure
(require '[rethinkdb.core :refer [connect close]])
(require '[rethinkdb.query :as r])

(let [conn (connect :host "127.0.0.1" :port 28015)]
  (r/run (r/db-create "test") conn)
  (-> (r/db "test")
      (r/table-create "authors")
      (r/run conn))
  (-> (r/db "test")
      (r/table "authors")
      (r/index-create "genre" (r/fn [row]
                                (r/get-field row :genre)))
      (r/run conn))
  (-> (r/db "test")
      (r/table "authors")
      (r/insert [{:name "E.L. Jamas"
                  :genre "crap"
                  :books ["Fifty Shades of Grey"
                          "Fifty Shades Darker"
                          "Fifty Shades Freed"]}
                 {:name "Stephenie Meyer"
                  :genre "crap"
                  :books ["Twilight" "New Moon" "Eclipse" "Breaking Dawn"]}])
      (r/run conn))
  (-> (r/db "test")
      (r/table "authors")
      (r/get-all ["crap"] {:index "genre"})
      (r/filter (r/fn [row]
                  (r/eq "Stephenie Meyer" (r/get-field row "name"))))
      (r/run conn))
  (close conn))
```
