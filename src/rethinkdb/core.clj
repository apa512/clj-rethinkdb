(ns rethinkdb.core
  (:require [rethinkdb.net :refer [send-int send-str read-init-response send-stop-query make-connection-loops close-connection-loops]])
  (:import [clojure.lang IDeref]
           [java.io Closeable DataInputStream DataOutputStream]
           [java.net Socket]))

(defn send-version [out]
  (let [v1 1063369270
        v2 1915781601
        v3 1601562686
        v4 1074539808]
    (send-int out v3 4)))

(defn send-protocol [out]
  (let [protobuf 656407617
        json 2120839367]
    (send-int out json 4)))

(defn send-auth-key [out auth-key]
  (let [n (count auth-key)]
    (send-int out n 4)
    (send-str out auth-key)))

(defn close
  "Closes RethinkDB database connection, stops all running queries
  and waits for response before returning"
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
  is not explicitly set."
  [& {:keys [^String host ^int port token auth-key db]
      :or {host "127.0.0.1"
           port 28015
           token 0
           auth-key ""
           db nil}}]
  (try
    (let [socket (Socket. host port)
          out (DataOutputStream. (.getOutputStream socket))
          in (DataInputStream. (.getInputStream socket))]
      ;; Initialise the connection
      (send-version out)
      (send-auth-key out auth-key)
      (send-protocol out)
      (let [init-response (read-init-response in)]
        (if-not (= init-response "SUCCESS")
          (throw (ex-info init-response {:host host :port port :auth-key auth-key :db db}))))
      ;; Once initialised, create the connection record
      (connection
        (merge
          {:socket socket
           :out out
           :in in
           :db db
           :waiting #{}
           :token token}
          (make-connection-loops in out))))
    (catch Exception e
      (throw (ex-info "Error connecting to RethinkDB database" {:host host :port port :auth-key auth-key :db db} e)))))
