(ns rethinkdb.net
  (:require [cheshire.core :as cheshire]
            [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [manifold.stream :as s]
            [rethinkdb.query-builder :refer [parse-query]]
            [rethinkdb.types :as types]
            [rethinkdb.response :refer [parse-response]]
            [rethinkdb.utils :refer [str->bytes
                                      int->bytes bytes->int
                                      pp-bytes]]
            [gloss.core :as gloss]
            [gloss.io :as io])
  (:import [java.io Closeable]))

(gloss/defcodec query-frame (gloss/compile-frame
                               (gloss/finite-frame
                              (gloss/prefix :int32-le)
                              (gloss/string :utf-8))
            cheshire/generate-string
            #(cheshire/parse-string % true)))

(gloss/defcodec id :int64-le)
(gloss/defcodec msg-protocol [id query-frame])

(defn wrap-duplex-stream
  [s]
  (let [out (s/stream)]
    (s/connect
      (s/map #(io/encode msg-protocol %) out)
      s)

    (s/splice
      out
      (io/decode-stream s msg-protocol))))

(declare send-continue-query send-stop-query)

(defn close
  "Clojure proxy for java.io.Closeable's close."
  [^Closeable x]
  (.close x))

(deftype Cursor [conn token coll]
  Closeable
  (close [this] (and (send-stop-query conn token) :closed))
  clojure.lang.Seqable
  (seq [this] (do
                (Thread/sleep 250)
                (lazy-seq (concat coll
                  (send-continue-query conn token))))))

(defn read-init-response [resp]
  (-> resp
   String.
   (clojure.string/replace  #"\W*$" "")))

(defn handshake [version auth proto client]
  (let [auth-bytes (if (some? auth)
            (str->bytes auth)
            (int->bytes 0 4))
         msg-bytes (byte-array (concat
            (int->bytes version 4)
            auth-bytes
            (int->bytes proto 4)))]
  @(s/put! client msg-bytes)
  (read-init-response @(s/take! client))))

(defn make-connection-loops [client]
 (let [parsed-in (async/chan (async/sliding-buffer 100))
       pub (async/pub parsed-in first)
        publish-loop
        (async/go-loop []
        (when-let [result @(s/take! @client)]
          (async/>! parsed-in result)
          (recur)))]
  {:loops [publish-loop]
   :parsed-in parsed-in
   :pub pub}))

(defn send-query* [{:keys [client pub] :as conn}
                    token query]
  (let [chan (async/chan)]
    (async/sub pub token chan)
    (s/put! @client [token query])
    (let [[recvd-token json]
          (async/<!! chan)]
    (assert (= recvd-token token)
      "Must not receive response with different token")
    (async/unsub pub token chan)
    json)))

(defn send-query [conn token query]
  (let [{:keys [db]} @conn
        json (if (and db (= 2 (count query))) ;; If there's only 1 element in query then this is a continue or stop query.
                ;; TODO: Could provide other global optargs too
                (concat query [{:db [(types/tt->int :DB) [db]]}])
                query)
        {type :t resp :r :as json-resp} (send-query* @conn token json)
        resp (parse-response resp)]
    (condp get type
      #{1} (first resp) ;; Success Atom, Query returned a single RQL datatype
      #{2} (do ;; Success Sequence, Query returned a sequence of RQL datatypes.
             (swap! (:conn conn) update-in [:waiting] #(disj % token))
             resp)
      #{3 5} (if (get (:waiting @conn) token) ;; Success Partial, Query returned a partial sequence of RQL datatypes
               (lazy-seq (concat resp (send-continue-query conn token)))
               (do
                 (swap! (:conn conn) update-in [:waiting] #(conj % token))
                 (Cursor. conn token resp)))
      (let [ex (ex-info (str (first resp)) json-resp)]
        (log/error ex)
        (throw ex)))))

(defn send-start-query [conn token query]
  (log/debugf "Sending start query with token %d, query: %s" token query)
  (send-query conn token (parse-query :START query)))

(defn send-continue-query [conn token]
  (log/debugf "Sending continue query with token %d" token)
  (send-query conn token (parse-query :CONTINUE)))

(defn send-stop-query [conn token]
  (log/debugf "Sending stop query with token %d" token)
  (send-query conn token (parse-query :STOP)))
