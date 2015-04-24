# clj-rethinkdb

A RethinkDB client for Clojure. Tested with 1.16.x but should with work all versions that support the JSON protocol (i.e. >= 1.13).

[![Circle CI](https://circleci.com/gh/apa512/clj-rethinkdb.svg?style=svg)](https://circleci.com/gh/apa512/clj-rethinkdb)
[![Dependencies Status](http://jarkeeper.com/apa512/clj-rethinkdb/status.svg)](http://jarkeeper.com/apa512/clj-rethinkdb)

## Install

[![Clojars Project](http://clojars.org/rethinkdb/latest-version.svg)](http://clojars.org/rethinkdb)

## Usage

```clojure
(require '[rethinkdb.core :refer [connect close]])
(require '[rethinkdb.query :as r])

(with-open [conn (connect :host "127.0.0.1" :port 28015)]
  (r/run (r/db-create "test") conn)

  (-> (r/db "test")
      (r/table-create "authors")
      (r/run conn))

  ;; Create an index on the field "genre".
  (-> (r/db "test")
      (r/table "authors")
      (r/index-create "genre" (r/fn [row]
                                (r/get-field row :genre)))
      (r/run conn))

  (-> (r/db "test")
      (r/table "authors")
      (r/insert [{:name "E.L. Jamas"
                  :genre "crap"
                  :country "UK"
                  :books ["Fifty Shades of Grey"
                          "Fifty Shades Darker"
                          "Fifty Shades Freed"]
                  :tags ["serious" "adult" "spicy"]}
                 {:name "Stephenie Meyer"
                  :genre "crap"
                  :country "USA"
                  :books ["Twilight" "New Moon" "Eclipse" "Breaking Dawn"]
                  :tags ["weird" "serious"]}])
      (r/run conn))

  ;; Use the "genre" index we created to get all books with the genre of "crap".
  (-> (r/db "test")
      (r/table "authors")
      (r/get-all ["crap"] {:index "genre"})
      (r/filter (r/fn [row]
                  (r/eq "Stephenie Meyer" (r/get-field row "name"))))
      (r/run conn))

  (-> (r/db "test")
      (r/table "authors")
      ;; Filter the table (one would normally use an index for that).
      (r/filter (r/fn [author]
                  (r/eq "E.L. Jamas" (r/get-field author :name))))
      ;; Update the books for all authors mathing the above filter by appending a new title to the array field :books.
      (r/update (r/fn [author]
                  {:books (-> author (r/get-field :books) (r/append "Fifty More Gray Books"))}))
      (r/run conn))

  ;; Update all authors with a field called :number-of-books that contains the count of things in the :books field.
  (-> (r/db "test")
      (r/table "authors")
      (r/update (r/fn [author]
                  {:number-of-books (-> author (r/get-field :books) (r/count))}))
      (r/run conn))

  ;; Create a compound index on coutry and genre.
  (-> (r/db "test")
      (r/table "authors")
      (r/index-create "country-genre" (r/fn [row]
                                        [(r/get-field row :country) (r/get-field row :genre)]))
      (r/run conn))

  ;; Use the compound index to access all books of a given genre published by authors from a given country.
  (-> (r/db "test")
      (r/table "authors")
      (r/get-all [["UK" "crap"]] {:index "country-genre"})
      (r/run conn))

  ;; Create a compound multi index to access all authors in a given country with a given tag.
  (-> (r/db "test")
      (r/table "authors")
      (r/index-create "country-tags" (r/fn [row]
                                       (r/map (r/get-field row :tags)
                                              (r/fn [tag]
                                                [(r/get-field row :country) tag])))
                      {:multi true})
      (r/run conn))

  ;; While creating indices, it is often useful to see what actually gets generated for every row:
  (-> (r/db "test")
      (r/table "authors")
      (r/map (r/fn [row]
               (r/map (r/get-field row :tags)
                      (r/fn [tag]
                        [(r/get-field row :country) tag]))))
      (r/run conn))

  ;; Use the country/tags index to access all authors within a country that have the tag.
  (-> (r/db "test")
      (r/table "authors")
      (r/get-all [["USA" "weird"]] {:index "country-tags"})
      (r/run conn)))
```

See full documentation at http://apa512.github.io/clj-rethinkdb/ (work in progress).
