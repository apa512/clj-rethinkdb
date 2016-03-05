(ns rethinkdb.core
  (:require [aleph.tcp :as tcp]
            [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [manifold.stream :as s]
            [rethinkdb.net :refer [init-connection setup-producer
                                   setup-consumer read-init-response
                                   send-stop-query]])
  (:import [clojure.lang IDeref]
           [io.netty.bootstrap Bootstrap]
           [io.netty.channel ChannelOption]
           [java.io Closeable]
           [rethinkdb Ql2$VersionDummy$Version Ql2$VersionDummy$Protocol]))

(def version  Ql2$VersionDummy$Version/V0_4_VALUE)
(def protocol Ql2$VersionDummy$Protocol/JSON_VALUE)

(defn close
  "Closes RethinkDB database connection, stops all running queries
  and waits for response before returning."
  [conn]
  (let [{:keys [start-query-chan query-chan results cursors client]} @conn]
    (doseq [token (keys (:results @conn))]
      (send-stop-query conn token))
    (async/close! query-chan)
    (s/close! client)
    :closed))

(defrecord Connection [conn]
  IDeref
  (deref [_] @conn)
  Closeable
  (close [this] (close this)))

(defmethod print-method Connection
  [r writer]
  (print-method (:conn r) writer))

(defn connection [m]
  (->Connection (atom m)))

(defn ^Connection connect
  "Creates a database connection to a RethinkDB host.
  If db is supplied, it is used in any queries where a db
  is not explicitly set. Default values are used for any parameters
  not provided.

  (connect :host \"dbserver1.local\")"
  [& {:keys [^String host ^int port token auth-key db async?
             ^int connect-timeout
             ^int read-timeout]
      :or {host "127.0.0.1"
           port 28015
           token 0
           auth-key ""
           db nil
           async? false
           connect-timeout 5000
           read-timeout 5000}}]
  (let [auth-key-printable (if (= "" auth-key) "" "<auth key provided but hidden>")]
    (try
     (let [client @(tcp/client {:host host :port port
                                :bootstrap-transform
                                (fn [^Bootstrap bs]
                                  (doto bs
                                    (.option ChannelOption/CONNECT_TIMEOUT_MILLIS connect-timeout)
                                    (.option ChannelOption/SO_TIMEOUT read-timeout)
                                    (.option ChannelOption/TCP_NODELAY true)))})]
       (init-connection client version protocol auth-key)
       (let [init-response (read-init-response client)]
         (log/trace "Initial response while establishing RethinkDB connection:" init-response)
         (when-not (= init-response "SUCCESS")
           (throw (ex-info init-response {:host host :port port :auth-key auth-key-printable :db db}))))
       (let [conn (connection {:client client
                               :db db
                               :start-query-chan (async/chan)
                               :query-chan (async/chan)
                               :results {}
                               :cursors {}
                               :async? async?
                               :token token})]
         (setup-producer conn)
         (setup-consumer conn)
         conn))
     (catch Exception e
       (log/error e "Error connecting to RethinkDB database")
       (throw (ex-info "Error connecting to RethinkDB database"
                       {:host host :port port :auth-key auth-key-printable :db db} e))))))
