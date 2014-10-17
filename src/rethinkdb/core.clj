(ns rethinkdb.core
  (:require [rethinkdb.net :refer [send-int send-str read-init-response]])
  (:import [java.io DataInputStream DataOutputStream]
           [java.net Socket]))

(defn send-version [out]
  (let [v1 1063369270
        v2 1915781601
        v3 1601562686]
    (send-int out v3 4)))

(defn send-protocol [out]
  (let [protobuf 656407617
        json 2120839367]
    (send-int out json 4)))

(defn send-auth-key [out auth-key]
  (let [n (count auth-key)]
    (send-int out n 4)
    (send-str out auth-key)))

(defn close [conn]
  (let [conn @conn
        socket (:socket conn)
        out (:out conn)
        in (:in conn)]
    (.close out)
    (.close in)
    (.close socket)
    true))

(defn connect [& {:keys [host port token auth-key]
                  :or {host "127.0.0.1"
                       port 28015
                       token 0
                       auth-key ""}}]
  (let [socket (Socket. host port)
        out (DataOutputStream. (.getOutputStream socket))
        in  (DataInputStream. (.getInputStream socket))
        conn (atom {:socket socket
                    :out out
                    :in in
                    :token token})]
    (send-version out)
    (send-auth-key out auth-key)
    (send-protocol out)
    (let [init-response (read-init-response in)]
      (if-not (= init-response "SUCCESS")
        (throw (Exception. init-response))))
    conn))
