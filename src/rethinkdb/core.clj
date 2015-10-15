(ns rethinkdb.core
  (:require [rethinkdb.net :refer [send-int send-str read-init-response send-stop-query make-connection-loops close-connection-loops]]
            [clojure.tools.logging :as log]
            [clojure.core.async :as async])
  (:import [clojure.lang IDeref]
           [java.io Closeable DataInputStream DataOutputStream]
           [java.net Socket]
           [java.util.concurrent TimeoutException]))

(defn send-version
  "Sends protocol version to RethinkDB when establishing connection.
  Hard coded to use v4."
  [out]
  (let [v1 1063369270
        v2 1915781601
        v3 1601562686
        v4 1074539808]
    (send-int out v4 4)))

(defn send-protocol
  "Sends protocol type to RethinkDB when establishing connection.
  Hard coded to use JSON protocol."
  [out]
  (let [protobuf 656407617
        json 2120839367]
    (send-int out json 4)))

(defn send-auth-key
  "Sends auth-key to RethinkDB when establishing connection."
  [out auth-key]
  (let [n (count auth-key)]
    (send-int out n 4)
    (send-str out auth-key)))

(defn close-connection
  "Closes RethinkDB database connection, stops all running queries
  and waits for response before returning.

  Don't try and run queries on a connection after calling close on it."
  [conn]
  (let [{:keys [^Socket socket ^DataOutputStream out ^DataInputStream in waiting close-timeout-ms]} @conn
        [close-type count-remaining]
        (async/alt!!

          (async/go ;; need a go block because I want to return a channel here
            (doseq [[token chanset] waiting]
              (if chanset
                (async/>! (:ctrl-in-ch chanset) :stop)
                (send-stop-query conn token)))
            (when-let [ctrl-chans (keep (fn [[token chanset]] (:ctrl-out-ch chanset)) waiting)]
              (log/debug "Query ctrl chans" ctrl-chans)
              (async/<! (async/merge ctrl-chans))
              (log/debug "All chans have returned")))
          ([close-val] [:closed])

          (async/timeout close-timeout-ms)
          ([timeout-val]
            (log/warnf "Closing connection timed out before all queries received close responses, manually closing all queries")
            (let [remaining (:waiting @conn)]
              (log/warnf "Cleaning up %d remaining queries %s" (count remaining) (keys remaining))
              (doseq [[token chanset] remaining]
                (when-let [clean-up-fn (:clean-up-fn chanset)]
                  (clean-up-fn)))
              [:closed-timeout (count remaining)])))]

    (close-connection-loops conn)                           ;; TODO: do these need to be part of the timeout too?
    (.close out)
    (.close in)
    (.close socket)
    (when (= :closed-timeout close-type)
      (throw (TimeoutException. (format "Timed out after %d ms waiting for a close response for all queries from RethinkDB. %d queries were force closed." close-timeout-ms count-remaining))))))

(defrecord Connection [conn]
  IDeref
  (deref [_] @conn)
  Closeable                                                 ;; TODO: add timeout to close
  (close [this]
    (close-connection this)))

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
  [& {:keys [^String host ^int port token auth-key db close-timeout-ms]
      :or {host "127.0.0.1"
           port 28015
           token 0
           auth-key ""
           db nil
           close-timeout-ms 5000}}]
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
           :waiting {}
           :close-timeout-ms close-timeout-ms
           :token token}
          (make-connection-loops in out))))
    (catch Exception e
      (log/error e "Error connecting to RethinkDB database")
      (throw (ex-info "Error connecting to RethinkDB database" {:host host :port port :auth-key auth-key :db db} e)))))
