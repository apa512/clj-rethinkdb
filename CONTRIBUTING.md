# Contributing to clj-rethinkdb

:+1::tada: First off, thanks for taking the time to contribute! :tada::+1:

The following is a set of guidelines for contributing to clj-rethinkdb which is hosted on [Github](https://github.com/apa512/clj-rethinkdb).
These are just guidelines, not rules, use your best judgment and feel free to propose changes to this document in a pull request.

## Creating issues for bugs

Check if the issue has already been reported. If possible provide:

* RethinkDB version
* Version of clj-rethinkdb being used
* Minimal reproduction steps

## Creating issues for features

Use your best judgement on what is needed here.

## Pull requests for bugs

If possible provide:

* Code that fixes the bug
* Failing tests which pass with the new changes
* Improvements to documentation to make it less likely that others will run into issues (if relevant).
* Add the change to the Unreleased section of [CHANGELOG.md](CHANGELOG.md)

## Pull requests for features

If possible provide:

* Code that implements the new feature
* Tests to cover the new feature including all of the code paths
* Docstrings for functions
* Documentation examples
* Add the change to the Unreleased section of [CHANGELOG.md](CHANGELOG.md)

## Running the tests

The test suite talks to a real RethinkDB server on `localhost:28015`. The quickest way to get one is Docker:

```sh
docker compose up -d
```

Then run the JVM suite:

```sh
lein test
```

The ClojureScript tests only exercise query building (no network) and run on Node:

```sh
lein doo node test once
lein cljsbuild once advanced
```

Note that `lein test` creates and drops a `cljrethinkdb_test` database, so point it at a throwaway server rather than one holding data you care about.

## Developer notes

clj-rethinkdb relies on protobuf definitions created in [rethinkdb-protobuf](https://github.com/apa512/rethinkdb-protobuf/). If you're wanting to add query terms for new versions of RethinkDB, the protobuf file definition will need to be updated. At the time of writing, the original copy is stored at [ql2.proto](https://github.com/rethinkdb/rethinkdb/blob/next/src/rdb_protocol/ql2.proto).
