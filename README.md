# clj-rethinkdb

A RethinkDB client for Clojure. Tested and supported on RethinkDB 2.0.x but should with work all versions that support the JSON protocol (i.e. >= 1.13).

[![Circle CI](https://circleci.com/gh/apa512/clj-rethinkdb.svg?style=svg)](https://circleci.com/gh/apa512/clj-rethinkdb)
[![Dependencies Status](https://versions.deps.co/apa512/clj-rethinkdb/status.svg)](https://versions.deps.co/apa512/clj-rethinkdb)
[![Join the chat at https://gitter.im/apa512/clj-rethinkdb](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/apa512/clj-rethinkdb?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

## Install

[![Clojars Project](http://clojars.org/com.apa512/rethinkdb/latest-version.svg)](http://clojars.org/com.apa512/rethinkdb)

## Changes

All changes are published in the [CHANGELOG](CHANGELOG.md). Of particular note, `0.10.x` is the last release track supporting Clojure 1.6 and below. `0.11.x` is the last release track that supports versions of RethinkDB < 2.0. `0.15.x` is the last minor version that supports Java 7.

## Usage

The clj-rethinkdb query API aims to match the [JavaScript](http://rethinkdb.com/api/javascript/) API as much as possible. The docstrings in `rethinkdb.query` show basic usage, but more advanced usage and optarg specs can be found in the official [RethinkDB docs](http://rethinkdb.com/docs/). **It is highly recommended to read the [official JavaScript docs](http://rethinkdb.com/api/javascript/), to understand the query terms available in clj-rethinkdb.**

```clojure
(require '[rethinkdb.query :as r])

(with-open [conn (r/connect :host "127.0.0.1" :port 28015 :db "test")]
  (r/run (r/db-create "test") conn)

  (-> (r/db "test")
      (r/table-create "authors")
      (r/run conn))

  (comment "This is equivalent to the previous query; db on connection is implicitly
  used if no db is provided."
  (-> (r/table-create "authors")
      (r/run conn)))

  ;; Create an index on the field "genre".
  (-> (r/db "test")
      (r/table "authors")
      (r/index-create "genre" (r/fn [row]
                                (r/get-field row :genre)))
      (r/run conn))

  (-> (r/db "test")
      (r/table "authors")
      (r/insert [{:name "E.L. James"
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
                  (r/eq "E.L. James" (r/get-field author :name))))
      ;; Update the books for all authors matching the above filter by appending a new title to the array field :books.
      (r/update (r/fn [author]
                  {:books (-> author (r/get-field :books) (r/append "Fifty More Gray Books"))}))
      (r/run conn))

  ;; Update all authors with a field called :number-of-books that contains the count of things in the :books field.
  (-> (r/db "test")
      (r/table "authors")
      (r/update (r/fn [author]
                  {:number-of-books (-> author (r/get-field :books) (r/count))}))
      (r/run conn))

  ;; Create a compound index on country and genre.
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

### Optargs

Many query terms take an `optargs` parameter. This is a map of `:snake-cased` keywords (or string keys if you prefer) to values. Keyword values are converted to strings. In the JavaScript driver you could write

```js
r.db("artists").table("singers").insert({id: 1, name: "Carly Rae"}, {durability: "hard", returnChanges: false, conflict: "error"}]).run(conn)
```

In Clojure this would be

```clj
(-> (r/db "test")
    (r/table "authors")
    (r/insert {:id 1 :name "Carly Rae"}
              {:conflict :update :return-changes true :durability :hard})
    (r/run conn)))
=>
{:changes [{:new_val {:id 1, :name "Carly Rae"}, :old_val {:id 1}}],
 :deleted 0,
 :errors 0,
 :inserted 0,
 :replaced 1,
 :skipped 0,
 :unchanged 0}
```

note particularly that `returnChanges` is written as `:return-changes` in Clojure.

### core.async

Starting with version 0.14.8, clj-rethinkdb can be used asynchronously by passing ```:async? true``` to ```connect```. This can also be
handled on a per query basis by passing ```{:async? true}``` or ```{:async? false}``` to ```run```.

```clj
(with-open [conn (r/connect :host "127.0.0.1" :port 28015 :db "test" :async? true)]
  (go
    (-> (r/table "singers")
        (r/insert {:id 2 :name "Brian Eno"})
        (r/run conn {:async? false}))
    (<! (-> (r/table "authors")
            (r/get 2)
            (r/run conn)))))
```

### Advanced Operations

#### Upsert while maintaining created / updated times

An example using loops, functions, branching and various object operations (select, add & remove fields).

`upsert-items!` takes a list of items `[{:id "item-1" ...}, {:id "item-2" ...} ...]` and insert them if the id is not present in the database, or does an update if the item already exists.

```clj
(defn upsert-items! [items]
  (let [table (r/table "my-table")]
    (-> items
        (r/for-each
          (r/fn [item]
            ;; Find a document using the upsert'd item id.
            (let [doc (r/get table (r/get-field item :id))]
              (r/branch (r/eq nil doc)
                ;; Item is new, set its updated/created time and insert it.
                (r/insert table
                          (r/merge {:updated (r/now) :created (r/now)} item)
                          {:conflict "update"})
                ;; Item already exists, set the updated time and update the doc.
                ;; Take care of removing the id in the update-object to avoid upsetting RethinkDB.
                (r/update doc
                          (r/merge {:updated (r/now)} (r/without item [:id])))))))
      (r/run conn))))
```

Note that for inserts we resolve conflicts by an update. At worst this may override an object that has been added while the query was running, which should never happen since this function is recognized by RethinkDB as atomic.

Also note that the ordering in `r/merge` is important, preference is given to fields in the rightmost object in the argument list. The upsert'd items can define a default `:updated` and `:created` field that'll override `(r/now)`.

### Complete Reference

See full documentation at http://apa512.github.io/clj-rethinkdb/ (work in progress).
