(ns rethinkdb.core
  (:require [rethinkdb.net :refer [send-int send-str read-init-response send-stop-query make-connection-loops close-connection-loops]]
            [clojure.tools.logging :as log])
  (:import [clojure.lang IDeref]
           [java.io Closeable DataInputStream DataOutputStream]
           [java.net Socket InetSocketAddress]
           [rethinkdb Ql2$VersionDummy$Version Ql2$VersionDummy$Protocol]))

(defn send-version
  "Sends protocol version to RethinkDB when establishing connection.
  Hard coded to use v3."
  [out]
  (send-int out Ql2$VersionDummy$Version/V0_3_VALUE 4))

(defn send-protocol
  "Sends protocol type to RethinkDB when establishing connection.
  Hard coded to use JSON protocol."
  [out]
  (send-int out Ql2$VersionDummy$Protocol/JSON_VALUE 4))

(defn send-auth-key
  "Sends auth-key to RethinkDB when establishing connection."
  [out auth-key]
  (let [n (count auth-key)]
    (send-int out n 4)
    (send-str out auth-key)))

(defn close
  "Closes RethinkDB database connection, stops all running queries
  and waits for response before returning."
  [conn]
  (let [{:keys [^Socket socket ^DataOutputStream out ^DataInputStream in waiting]} @conn]
    (doseq [token waiting]
      (send-stop-query conn token))
    (close-connection-loops conn)
    (.close out)
    (.close in)
    (.close socket)
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

  (connect :host \"dbserver1.local\")
  "
  [& {:keys [^String host ^int port token auth-key db
             ^int connect-timeout
             ^int read-timeout]
      :or {host "127.0.0.1"
           port 28015
           token 0
           auth-key ""
           db nil
           connect-timeout 5000
           read-timeout    5000}}]
  (let [auth-key-printable (if (= "" auth-key) "" "<auth key provided but hidden>")]
    (try
      (let [socket (doto (Socket.)
                     (.connect (InetSocketAddress. host port) connect-timeout))
            out (DataOutputStream. (.getOutputStream socket))
            in (DataInputStream. (.getInputStream socket))]
        ;; Read timeouts
        (.setSoTimeout socket read-timeout)
        ;; Disable Nagle's algorithm on the socket
        (.setTcpNoDelay socket true)

        ;; Initialise the connection
        (send-version out)
        (send-auth-key out auth-key)
        (send-protocol out)
        (let [init-response (read-init-response in)]
          (log/trace "Initial response while establishing RethinkDB connection:" init-response)
          (when-not (= init-response "SUCCESS")
            (throw (ex-info init-response {:host host :port port :auth-key auth-key-printable :db db}))))
        ;; Once initialised, create the connection record
        (connection
          (merge
            {:socket  socket
             :out     out
             :in      in
             :db      db
             :waiting #{}
             :token   token}
            (make-connection-loops in out))))
      (catch Exception e
        (log/error e "Error connecting to RethinkDB database")
        (throw (ex-info "Error connecting to RethinkDB database"
                        {:host host :port port :auth-key auth-key-printable :db db} e))))))
