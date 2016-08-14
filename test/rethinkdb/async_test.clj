(ns rethinkdb.async-test
  (:require [clojure.test :refer :all]
            [rethinkdb.net :as net]))

(deftest async-query?-test
  (are [async? opts conn]
    (= async? (boolean (net/async-query? opts conn)))

    ;; Synchronous queries
    false nil {}
    false nil {:async? false}
    false {} {}
    false {} {:async? false}
    false {:async? false} {}
    false {:async? false} {:async? false}
    false {:async? false} {:async? true}

    ;; Asynchronous queries
    true nil {:async? true}
    true {} {:async? true}
    true {:async? true} {}
    true {:async? true} {:async? true}))
