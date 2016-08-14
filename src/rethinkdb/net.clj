(ns rethinkdb.net
  (:require [byte-streams :as bs]
            [cheshire.core :as json]
            [clojure.core.async :as async]
            [clojure.core.async.impl.protocols :refer [Channel ReadPort]]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [gloss.core :as gloss]
            [gloss.io :as io]
            [manifold.deferred :as d]
            [manifold.stream :as s]
            [manifold.stream.core]
            [rethinkdb.query-builder :as qb]
            [rethinkdb.response :refer [parse-response]]
            [rethinkdb.types :as types]
            [cheshire.parse :as parse]
            [cheshire.factory :as factory])
  (:import [manifold.stream.default Stream]
           (com.fasterxml.jackson.core JsonFactory)
           (java.nio CharBuffer)))

(declare send-continue-query send-stop-query)

(gloss/defcodec send-protocol
  [:uint64-le (gloss/finite-frame :int32-le (gloss/string :utf-8 :char-sequence true))])

(gloss/defcodec receive-protocol
  [:uint64-le (gloss/finite-frame :int32-le (gloss/string :utf-8 :char-sequence true))])

(gloss/defcodec init-protocol
  [:int32-le
   (gloss/finite-frame :int32-le (gloss/string :utf-8))
   :int32-le])

(defn make-rethink-exception [msg m]
  (ex-info (str "RethinkDB server: " msg) m))

(defn close-cursor [conn token]
  (when-let [cursor (get-in @conn [:pending token :cursor])]
    (s/close! cursor)))

(extend-type manifold.stream.default.Stream
  ReadPort
  (take! [this fn1-handler]
    (s/take! this)))

(defn init-connection [client version protocol auth-key]
  (s/put! client (io/encode init-protocol
                            [version auth-key protocol])))

(defn read-init-response [client]
  (string/replace (bs/to-string @(s/take! client) {:encoding "UTF-8"}) #"\W*$" ""))

(defn deliver-result [conn token resp]
  (when-let [{:keys [result cursor async?]} (get-in @conn [:pending token])]
    (swap! (:conn conn) update :pending #(dissoc % token))
    (if-let [stream (or cursor (and async? (sequential? resp) result))]
      (d/finally (s/put-all! stream resp)
                 (fn [] (s/close! stream)))
      (do (s/put! result resp)
          (s/close! result)))))

(defn append-result [conn token resp]
  (when-let [{:keys [result async? cursor]} (get-in @conn [:pending token])]
    (let [cursor (or cursor
                     (let [stream (if async? result (s/stream))]
                       (s/on-closed stream
                                    (fn []
                                      (when (get-in @conn [:pending token])
                                        (swap! (:conn conn) update :pending #(dissoc % token))
                                        (send-stop-query conn token))))
                       (swap! (:conn conn) assoc-in [:pending token :cursor] stream)
                       (when-not async?
                         (s/put! result stream))
                       stream))]
      (d/finally (s/put-all! cursor resp)
                 (fn [] (send-continue-query conn token))))))

(defn handle-response [conn token resp]
  (let [{type :t resp :r etype :e notes :n :as json-resp} resp]
    (case (int type)
      (1 5) ;; Success atom, server info
      (deliver-result conn token (first resp))

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

(defn parse-char-array [^chars ca]
  (parse/parse (.createParser ^JsonFactory (or factory/*json-factory*
                                               factory/json-factory)
                              ca)
               true nil nil))

;; Test also byte-streams to reader
;; (json/parse-stream (bs/to-reader myjson))

(defn setup-consumer [conn]
  (s/consume
   (fn [[token ^CharBuffer charbuffer]]
     (log/trace "Resp:" token charbuffer)
     (def mybuff charbuffer)
     (handle-response conn token
                      (-> (.array charbuffer)
                          (parse-char-array)
                          parse-response)))
   (io/decode-stream (:client @conn) receive-protocol)))


(defn add-global-optargs [{:keys [db]} query]
  (if (and db (= 2 (count query))) ;; If there's only 1 element in query then this is a continue or stop query
    ;; TODO: Could provide other global optargs too
    (concat query [{:db [(types/tt->int :DB) [db]]}])
    query))

(defn setup-producer [conn]
  (let [{:keys [query-chan client]} @conn]
    (async/go-loop []
      (when-let [{:keys [term query-type token]} (async/<! query-chan)]
        (let [term (add-global-optargs @conn
                                       (if term
                                         (qb/parse-query query-type term)
                                         (qb/parse-query query-type)))
              json (json/generate-string term {:key-fn #(subs (str %) 1)})]
          (when-not (and (= query-type :CONTINUE)
                         (not (get-in @conn [:pending token])))
            (log/trace "Send:" token json)
            (s/put! client (io/encode send-protocol [token json])))
          (recur))))))

(defn async-query? [opts conn]
  (let [not-found (:async? conn)]
    (get opts :async? not-found)))

;;; ========================================================================
;;; Sending initial query
;;; ========================================================================

(defn get-next-token [conn]
  (:token (swap! (:conn conn) update :token inc)))

(defn send-initial-query [conn query]
  (let [{:keys [async?]} query
        {:keys [query-chan]} @conn
        stream (s/stream)
        token (get-next-token conn)
        query (assoc query :token token :result stream)]
    (swap! (:conn conn) assoc-in [:pending token] query)
    (async/put! query-chan query)
    (if async?
      stream
      (let [response @(s/take! stream)]
        (condp instance? response
          Stream (s/stream->seq response)
          Throwable (throw response)
          response)))))

(defn send-stop-query [conn token]
  (async/>!! (:query-chan @conn) {:query-type :STOP
                                  :token token}))

(defn send-continue-query [conn token]
  (async/go
   (async/>! (:query-chan @conn) {:query-type :CONTINUE
                                  :token token})))

(defn send-start-query [conn term & [opts]]
  (let [async? (or (= (::qb/term term) :CHANGES)
                   (async-query? opts @conn))]
    (send-initial-query conn {:term term
                              :query-type :START
                              :async? async?})))

(defn send-server-query [conn]
  (send-initial-query conn {:query-type :SERVER_INFO}))
