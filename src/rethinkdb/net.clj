(ns rethinkdb.net
  (:require [byte-streams :as bs]
            [cheshire.core :as json]
            [clojure.core.async :as async]
            [clojure.core.async.impl.protocols :refer [ReadPort]]
            [clojure.tools.logging :as log]
            [gloss.core :as gloss]
            [gloss.io :as io]
            [manifold.deferred :as d]
            [manifold.stream :as s]
            [manifold.stream.core]
            [rethinkdb.query-builder :as qb]
            [rethinkdb.response :refer [parse-response]]
            [rethinkdb.types :as types])
  (:import [java.io Closeable]))

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

(deftype Cursor [conn stream token]
  Closeable
  (close [this]
    (when (get-in @conn [:cursors token])
      (send-stop-query conn token))
    (swap! (:conn conn) update-in [:cursors] #(dissoc % token))
    (s/close! stream))
  clojure.lang.Counted
  (count [this]
    (count (seq this)))
  clojure.lang.Seqable
  (seq [this]
    (s/stream->seq stream))
  manifold.stream.core.IEventSink
  (put [this x _]
    (s/put! stream x)))

(defn init-connection [client version protocol auth-key]
  (s/put! client (io/encode init-protocol
                            [version auth-key protocol])))

(defn read-init-response [client]
  (clojure.string/replace (bs/to-string @(s/take! client)) #"\W*$" ""))

(defn deliver-result [conn token resp]
  (let [result (get-in @conn [:results token])
        cursor (get-in @conn [:cursors token])]
    (when cursor
      (swap! (:conn conn) update-in [:cursors] #(dissoc % token))
      (d/on-realized (s/put-all! cursor resp)
                     (fn [_]
                       (close cursor))
                     (constantly nil)))
    (d/on-realized (s/put! result (or cursor resp))
                   (fn [_]
                     (s/close! result))
                   (constantly nil))))

(defn append-result [conn token resp]
  (let [query-chan (:query-chan @conn)
        cursor (get-in @conn [:cursors token])]
    (if cursor
      (s/put-all! cursor resp)
      (let [result (get-in @conn [:results token])
            cursor (Cursor. conn (s/stream) token)]
        (swap! (:conn conn) assoc-in [:cursors token] cursor)
        (s/put-all! cursor resp)
        (s/put! result cursor)))
    (send-continue-query conn token)))

(defn append-changes [conn token resp]
  (let [query-chan (:query-chan @conn)
        result (get-in @conn [:results token])]
    (if (:async? (s/description result))
      (s/put-all! result resp)
      (append-result conn token resp))
    (send-continue-query conn token)))

(defn handle-response [conn token resp]
  (let [{type :t resp :r etype :e notes :n :as json-resp} resp]
    (case (int type)
      (1 5) ;; Success atom, server info
      (deliver-result conn token (first resp))

      2 ;; Success sequence
      (deliver-result conn token resp)

      3 ;; Success partial
      (if (seq notes)
        (append-changes conn token resp)
        (append-result conn token resp))

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
   (io/decode-channel (:client @conn) query-protocol)))

(defn add-global-optargs [{:keys [db]} query]
  (if (and db (= 2 (count query))) ;; If there's only 1 element in query then this is a continue or stop query
    ;; TODO: Could provide other global optargs too
    (concat query [{:db [(types/tt->int :DB) [db]]}])
    query))

(defn with-next-token [conn [stream query]]
  (let [token (:token (swap! (:conn conn) update-in [:token] inc))]
    (s/on-closed stream
                 (fn []
                   (swap! (:conn conn) update-in [:results] #(dissoc % token))))
    (swap! (:conn conn) assoc-in [:results token] stream)
    [token query]))

(defn setup-producer [conn]
  (let [{:keys [start-query-chan query-chan client]} @conn]
    (async/pipeline 1 query-chan (map (partial with-next-token conn)) start-query-chan)
    (async/go-loop []
      (when-let [[token query] (async/<! query-chan)]
        (let [query (add-global-optargs @conn query)
              json (json/generate-string query)]
          (s/put! client (io/encode query-protocol [token json]))
          (recur))))))

(defn send-first-query [conn query async?]
  (let [stream (s/stream* {:description #(assoc % :async? async?)})
        {:keys [start-query-chan]} @conn]
    (async/go (async/>! start-query-chan [stream query]))
    (if async?
      (let [result-chan (async/chan)]
        (s/connect stream result-chan)
        result-chan)
      (let [result @(s/take! stream)]
        (if (instance? Throwable result)
          (throw result)
          result)))))

(defn send-stop-query [conn token]
  (async/>!! (:query-chan @conn) [token (qb/parse-query :STOP)]))

(defn send-continue-query [conn token]
  (async/go
   (async/>! (:query-chan @conn) [token (qb/parse-query :CONTINUE)])))

(defn send-start-query [conn query & [opts]]
  (let [async? (->> [opts @conn]
                    (remove nil?)
                    (map :async?)
                    first)]
    (send-first-query conn (qb/parse-query :START query) async?)))

(defn send-server-query [conn]
  (send-first-query conn (qb/parse-query :SERVER_INFO) false))
