(ns rethinkdb.core
  (:require [rethinkdb.net :refer [read-init-response send-stop-query make-connection-loops
                                   wrap-duplex-stream handshake]]
            [clojure.tools.logging :as log]
            [manifold.deferred :as d]
            [manifold.stream :as s]
            [aleph.tcp :as tcp]
            [clojure.core.async :as async])
  (:import [clojure.lang IDeref]
           [java.io Closeable]))

(def versions
  {:v1 1063369270
   :v2 1915781601
   :v3 1601562686
   :v4 1074539808})

(def protocols
  {:protobuf 656407617
   :json 2120839367})

(defn close
  "Closes RethinkDB database connection, stops all running queries
  and waits for response before returning."
  [conn]
  (let [{:keys [client waiting parsed-in]} @conn]
    (doseq [token waiting]
      (send-stop-query conn token))
     (s/close! @client)
     (async/close! parsed-in)
    :closed))

(defrecord Connection [conn]
  IDeref
  (deref [_] @conn)
  Closeable
  (close [this] (close this)))

(defmethod print-method Connection
  [r writer]
  (print-method (:conn r) writer))

(defn wrap-client
  [client]
  (d/chain  (d/chain client
    #(wrap-duplex-stream %))))

(defn connection [m]
  (->Connection (atom m)))

(defn ^Connection connect
  "Creates a database connection to a RethinkDB host.
  If db is supplied, it is used in any queries where a db
  is not explicitly set. Default values are used for any parameters
  not provided.

  (connect :host \"dbserver1.local\")"
  [& {:keys [^String host ^int port token auth-key db version protocol]
      :or {host "127.0.0.1"
           port 28015
           token 0
           version :v3
           protocol :json
           db nil}}]
  (try
    (let [client (tcp/client {:host host :port port})
          init-response (handshake (version versions) auth-key (protocol protocols) @client)]
        (if-not (= init-response "SUCCESS")
          (throw (ex-info init-response {:host host :port port :auth-key auth-key :db db})))
      ;; Once initialised, create the connection record
      (let [wrapped-client (wrap-client client)]
      (connection
        (merge
          {:client wrapped-client
           :db db
           :waiting #{}
           :token token}
      (make-connection-loops wrapped-client)))))
    (catch Exception e
      (log/error e "Error connecting to RethinkDB database")
      (throw (ex-info "Error connecting to RethinkDB database" {:host host :port port :auth-key auth-key :db db} e)))))
