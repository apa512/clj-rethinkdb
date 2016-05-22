# Changelog

All notable changes to this project will be documented in this file. This change log follows the conventions of [keepachangelog.com](http://keepachangelog.com).

## Unreleased
### Changed
- `get-field` can get nested fields when passed a vector
- **Breaking** Use Java 8's Base64 class instead of the `clojure.data.codec` library. This requires Java 8.

## [0.15.19] - 2016-05-08
### Changed
- **Breaking** renamed `indexes-of` to `offsets-of`
- Renamed `get-intersection` to `get-intersecting`
- Added support for `group` with multiple fields

### Added
- Added `range`

## [0.15.15] - 2016-04-23
- Add TLS support

## [0.15.13] - 2016-04-17
### Changed
- Use netty on the backend and make connections thread safe
- Use connection DB with `table-create`
- Dropped the `read-timeout` option to connect
- Allow namespaced keywords
- Add support for binary

### Added
- Added options to make queries return a core.async channel

## [0.13.14] - 2016-02-21
### Changed
- Disable Nagle's algorithm (set TCP NO_DELAY to true). This provides a speedup of around 30-40x for small queries on Linux. [#114](https://github.com/apa512/clj-rethinkdb/pull/114)
- Renamed changes arg from `table` to `xs`. [#76](https://github.com/apa512/clj-rethinkdb/issues/76)
- Prefix RethinkDB server exceptions with `RethinkDB server:`. [#100](https://github.com/apa512/clj-rethinkdb/pull/100)
- Mask auth-key when logging connection exceptions [#90](https://github.com/apa512/clj-rethinkdb/issues/90)
- Upgraded to support RethinkDB 2.2 protobuf query language
- Add optargs arity to `table`. [#78](https://github.com/apa512/clj-rethinkdb/issues/78)
- Added support for 2.1's error types. These give more information about why an error happened. See http://rethinkdb.com/docs/error-types/ for more details. **Breaking** the structure of exception data has changed from `<response map>` to {:type :op-failed :response <response map>}`. [#77](https://github.com/apa512/clj-rethinkdb/issues/77)

### Added
- Added a new arity to `changes` that allows you to pass optargs. [#112](https://github.com/apa512/clj-rethinkdb/issues/112)
- Added new arity to `uuid` to allow you to pass a string. The string is then SHA-1 hashed to a UUID. Only valid in RethinkDB >= 2.2. [#109](https://github.com/apa512/clj-rethinkdb/issues/109)
- Added new RethinkDB term `values`. This is analogous to Clojure's `vals` function. Only valid in RethinkDB >= 2.2. [#110](https://github.com/apa512/clj-rethinkdb/issues/110)
- Added new RethinkDB query term `server`. This is used to retrieve information about the server you are connected to. [#111](https://github.com/apa512/clj-rethinkdb/issues/111)
- Add new RethinkDB query term `reconfigure`. [#78](https://github.com/apa512/clj-rethinkdb/issues/78)
- Add new RethinkDB query term `wait`. [#78](https://github.com/apa512/clj-rethinkdb/issues/78)

### Removed
- Support for RethinkDB < 2.0. [Changes](https://github.com/rethinkdb/rethinkdb/issues/5124) in RethinkDB 2.2 mean we can't (easily) support changefeeds in versions older than 2.0. The rest of the driver commands should work, but explicit support is being removed.

## [0.11.0] - 2015-10-19
### Added
- clojure.tools.logging support. [#72](https://github.com/apa512/clj-rethinkdb/pull/72)
- `fn` macro to ClojureScript `rethinkdb.query` namespace. [#64](https://github.com/apa512/clj-rethinkdb/issues/64)
- `rethinkdb.query/order-by` function to ClojureScript. [#65](https://github.com/apa512/clj-rethinkdb/issues/65)
- `rethinkdb.query/default` function, for supplying a default value/function for missing values. [90b58cd](https://github.com/apa512/clj-rethinkdb/commit/90b58cd14179fb4eec6e8a28387fe4eda1397adb)

### Changed
- Change Maven Group ID from `rethinkdb` to `com.apa512`. [#102](https://github.com/apa512/clj-rethinkdb/pull/102), [#92](https://github.com/apa512/clj-rethinkdb/issues/92)
- Update dependency to Clojure 1.7. [#59](https://github.com/apa512/clj-rethinkdb/pull/59)
- The query parts of this library have been converted to use Clojure 1.7 Reader Conditionals. This means that you can generate queries in ClojureScript and run them on the server (be very careful with this!). [#59](https://github.com/apa512/clj-rethinkdb/pull/59)
- Update protobuf support to RethinkDB 2.1.0.
- Update docstring for `rethinkdb.query/without`.
- Update arity and docstring for `rethinkdb.query/merge` to support merging any number of objects.
- Add new arity to `rethinkdb.query/index-create` to allow creating simple indexes from field names. [#86](https://github.com/apa512/clj-rethinkdb/pull/86)
- Update `rethinkdb.query/time` arity to only allow 3, 4, 6, and 7 arguments. Also update docstring to make usage clearer. This was already enforced on the server, but will now be enforced by the client library too. [#87](https://github.com/apa512/clj-rethinkdb/issues/87)

### Removed
- Support for Clojure 1.6 and below

## [0.10.1] - 2015-07-08
### Added
- Add docstring for `rethinkdb.core/close`. [#44](https://github.com/apa512/clj-rethinkdb/pull/44)
- Add alias for `rethinkdb.core/connect` into `rethinkdb.query/connect` so you don't need to import the `rethinkdb.core` namespace. [#44](https://github.com/apa512/clj-rethinkdb/pull/44)
- Add CHANGELOG.md [#47](https://github.com/apa512/clj-rethinkdb/pull/47)
- Added explicit support for RethinkDB 2.0 (It worked before but wasn't documented as such).

### Changed
- Add new arity for the queries `table-drop`, and `table-list` which doesn't require a db. [#54](https://github.com/apa512/clj-rethinkdb/pull/54/files)
- Add docstring to `rethinkdb.query` ns explaining DB priority [#54](https://github.com/apa512/clj-rethinkdb/pull/54)
- Exceptions thrown when connecting are more descriptive, and are now of type `clojure.lang.ExceptionInfo`. [#41](https://github.com/apa512/clj-rethinkdb/issues/41) [#56](https://github.com/apa512/clj-rethinkdb/pull/56)
- Add docstrings to many functions [#56](https://github.com/apa512/clj-rethinkdb/pull/56)

### Fixed
- Fix close method on Connection record [#50](https://github.com/apa512/clj-rethinkdb/pull/50)
- Fix handling of sending CONTINUE queries to RethinkDB when using an implicit db on the connection. Affects any query that returns a Cursor. [#52](https://github.com/apa512/clj-rethinkdb/pull/52)
- Fix reflection warnings [#58](https://github.com/apa512/clj-rethinkdb/pull/58)

### Deprecated
- `0.10.1` is the last release that will support Clojure 1.6. Future release will require Clojure 1.7 or above. This is to allow the use of cljc Reader Conditionals.

NB `0.10.0` was partly released but due to a Clojars snafu `0.10.1` is the recommended release to use.

## [0.9.40]
### Changed
- Add implicit database to Connection. This database will be used if a user query doesn't specify a database. [#46](https://github.com/apa512/clj-rethinkdb/pull/46)
- Update `rethinkdb.query/filter` to allow optargs [#43](https://github.com/apa512/clj-rethinkdb/pull/43)
- Add new arity for `rethinkdb.query/table` to not require a db. [#39](https://github.com/apa512/clj-rethinkdb/pull/39)
