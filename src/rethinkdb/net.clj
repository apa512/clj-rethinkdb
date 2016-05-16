(ns rethinkdb.net
  (:require [byte-streams :as bs]
            [cheshire.core :as json]
            [clojure.core.async :as async]
            [clojure.core.async.impl.protocols :refer [ReadPort]]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [gloss.core :as gloss]
            [gloss.io :as io]
            [manifold.deferred :as d]
            [manifold.stream :as s]
            [manifold.stream.core]
            [rethinkdb.query-builder :as qb]
            [rethinkdb.response :refer [parse-response]]
            [rethinkdb.types :as types])
  (:import [java.io Closeable]
           [clojure.lang Keyword]))

(declare send-continue-query send-stop-query)

(gloss/defcodec query-protocol
  [:uint64-le (gloss/finite-frame :int32-le (gloss/string :utf-8))])

(gloss/defcodec init-protocol
  [:int32-le
   (gloss/finite-frame :int32-le (gloss/string :utf-8))
   :int32-le])

(defn make-rethink-exception [msg m]
  (let [ex (ex-info (str "RethinkDB server: " msg) m)]
    (log/error ex)
    ex))

(defn close
  "Clojure proxy for java.io.Closeable's close."
  [^Closeable x]
  (.close x))

(defn close-cursor [conn token]
  (when-let [cursor (get-in @conn [:pending token :cursor])]
    (close cursor)))

(deftype Cursor [conn stream token]
  Closeable
  (close [this]
    (s/close! stream)
    (when (get-in @conn [:pending token])
      (swap! (:conn conn) update :pending #(dissoc % token))
      (send-stop-query conn token)))
  clojure.lang.Counted
  (count [this]
    (count (into [] (seq this))))
  clojure.lang.Seqable
  (seq [this]
    (s/stream->seq stream))
  java.lang.Iterable
  (iterator [this]
    (.iterator ^Iterable (seq this)))
  java.util.Collection
  (toArray [this]
    (into-array Object this))
  manifold.stream.core.IEventSink
  (put [this x _]
    (case x
      ::more (send-continue-query conn token)
      ::done (close this)
      (s/put! stream x))))

(defn init-connection [client version protocol auth-key]
  (s/put! client (io/encode init-protocol
                            [version auth-key protocol])))

(defn read-init-response [client]
  (string/replace (bs/to-string @(s/take! client)) #"\W*$" ""))

(defn deliver-result [conn token resp]
  (when-let [{:keys [result cursor]} (get-in @conn [:pending token])]
    (if cursor
      (do (swap! (:conn conn) update-in [:pending token] #(dissoc % :cursor))
          (s/put-all! cursor (conj resp ::done)))
      (do (swap! (:conn conn) update :pending #(dissoc % token))
          (s/put-all! result resp)
          (s/close! result)))))

(defn append-result [conn token resp]
  (when-let [{:keys [result async? cursor]} (get-in @conn [:pending token])]
    (let [cursor (or cursor
                     (let [cursor (Cursor. conn (if async? result (s/stream)) token)]
                       (swap! (:conn conn) assoc-in [:pending token :cursor] cursor)
                       (when-not async?
                         (s/put! result cursor))
                       cursor))]
      (s/put-all! cursor (conj resp ::more)))))

(defn handle-response [conn token resp]
  (let [{type :t resp :r etype :e notes :n :as json-resp} resp]
    (case (int type)
      (1 5) ;; Success atom, server info
      (deliver-result conn token resp)

      2 ;; Success sequence
      (deliver-result conn token resp)

      3 ;; Success partial
      (append-result conn token resp)

      16 ;; Client error value
      (deliver-result conn token
                      (make-rethink-exception
                       (first resp)
                       {:type :client :response json-resp}))

      17 ;; Compile error value
      (deliver-result conn token
                      (make-rethink-exception
                       (first resp)
                       {:type :compile :response json-resp}))

      18 ;; Runtime error value
      (deliver-result conn token
                      (make-rethink-exception
                       (first resp)
                       {:type (case (int etype)
                                1000000 :internal
                                2000000 :resource-limit
                                3000000 :query-logic
                                3100000 :non-existence
                                4100000 :op-failed
                                4200000 :op-indeterminate
                                5000000 :user
                                :unknown)
                        :response json-resp})))))

(defn setup-consumer [conn]
  (s/consume
   (fn [[token json]]
     (handle-response conn token
                      (-> json
                          (json/parse-string-strict true)
                          parse-response)))
   (io/decode-stream (:client @conn) query-protocol)))


(defn add-global-optargs [{:keys [db]} query]
  (if (and db (= 2 (count query))) ;; If there's only 1 element in query then this is a continue or stop query
    ;; TODO: Could provide other global optargs too
    (concat query [{:db [(types/tt->int :DB) [db]]}])
    query))

(defn add-token [conn query]
  (let [token (:token (swap! (:conn conn) update-in [:token] inc))
        query (assoc query :token token)]
    (swap! (:conn conn) assoc-in [:pending token] query)
    query))

(defn setup-producer [conn]
  (let [{:keys [initial-query-chan query-chan client]} @conn]
    (async/pipeline 1 query-chan (map (partial add-token conn)) initial-query-chan)
    (async/go-loop []
      (when-let [{:keys [term query-type token]} (async/<! query-chan)]
        (let [term (add-global-optargs @conn
                                       (if term
                                         (qb/parse-query query-type term)
                                         (qb/parse-query query-type)))
              json (json/generate-string term {:key-fn #(subs (str %) 1)})]
          (when-not (and (= ^Keyword query-type :CONTINUE)
                         (not (get-in @conn [:pending token])))
            (s/put! client (io/encode query-protocol [token json])))
          (recur))))))

;;; ========================================================================
;;; Sending initial query
;;; ========================================================================

(defn send-initial-query [conn query]
  (let [{:keys [async?]} query
        {:keys [initial-query-chan]} @conn
        stream (s/stream)]
    (async/go (async/>! initial-query-chan
                        (assoc query :result stream)))
    (if async?
      (let [result-chan (async/chan)]
        (s/connect stream result-chan)
        result-chan)
      (let [response @(s/take! stream)]
        (if (instance? Throwable response)
          (throw response)
          response)))))

(defn send-stop-query [conn token]
  (async/>!! (:query-chan @conn) {:query-type :STOP
                                  :token token}))

(defn send-continue-query [conn token]
  (async/go
   (async/>! (:query-chan @conn) {:query-type :CONTINUE
                                  :token token})))

(defn send-start-query [conn term & [opts]]
  (let [async? (->> [opts @conn]
                    (map :async?)
                    (remove nil?)
                    first)]
    (send-initial-query conn {:term term
                              :query-type :START
                              :async? async?})))

(defn send-server-query [conn]
  (send-initial-query conn {:query-type :SERVER_INFO}))
