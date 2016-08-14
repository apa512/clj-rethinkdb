(ns rethinkdb.test-runner
  (:require [doo.runner :refer-macros [doo-tests]]
            [rethinkdb.cljs-test]))

(doo-tests 'rethinkdb.cljs-test)
