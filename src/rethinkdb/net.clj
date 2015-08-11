(ns rethinkdb.net
  (:require [clojure.data.json :as json]
            [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [rethinkdb.query-builder :refer [parse-query]]
            [rethinkdb.types :as types]
            [rethinkdb.response :refer [parse-response]]
            [rethinkdb.utils :as utils :refer [str->bytes int->bytes bytes->int pp-bytes]]
            [rethinkdb.query-builder :as qb])
  (:import [java.io Closeable InputStream OutputStream DataInputStream]))

(declare send-continue-query send-stop-query)

(defn close
  "Clojure proxy for java.io.Closeable's close."
  [^Closeable x]
  (.close x))

(deftype Cursor [conn token coll]
  Closeable
  (close [this] (and (send-stop-query conn token) :closed))
  clojure.lang.Seqable
  (seq [this]
    (lazy-seq (concat coll (send-continue-query conn token)))))

(defn send-int [^OutputStream out i n]
  (.write out (int->bytes i n) 0 n))

(defn send-str [^OutputStream out s]
  (let [n (count s)]
    (.write out (str->bytes s) 0 n)))

(defn read-str [^DataInputStream in n]
  (let [resp (byte-array n)]
    (.readFully in resp 0 n)
    (String. resp)))

(defn ^String read-init-response [^InputStream in]
  (let [resp (byte-array 4096)]
    (.read in resp 0 4096)
    (clojure.string/replace (String. resp) #"\W*$" "")))


(defn read-response* [^InputStream in]
  (let [recvd-token (byte-array 8)
        length (byte-array 4)]
    (.read in recvd-token 0 8)
    (.read in length 0 4)
    (let [recvd-token (bytes->int recvd-token 8)
          length (bytes->int length 4)
          json (read-str in length)]
      [recvd-token json])))

(defn write-query [out [token json]]
  (send-int out token 8)
  (send-int out (count json) 4)
  (send-str out json))

(defn make-connection-loops [in out]
  (let [recv-chan (async/chan)
        send-chan (async/chan)
        pub (async/pub recv-chan first)
        ;; Receive loop
        recv-loop (async/go-loop []
                    (when (try
                            (let [resp (read-response* in)]
                              (log/trace "Received raw response %s" resp)
                              (async/>! recv-chan resp))
                            (catch java.net.SocketException e
                              false))
                      (recur)))
        ;; Send loop
        send-loop (async/go-loop []
                    (when-let [query (async/<! send-chan)]
                      (log/trace "Sending raw query %s")
                      (write-query out query)
                      (recur)))]
    ;; Return as map to merge into connection
    {:pub pub
     :loops [recv-loop send-loop]
     :recv-chan recv-chan
     :send-chan send-chan}))

(defn close-connection-loops
  [conn]
  (let [{:keys [pub send-chan recv-chan] [recv-loop send-loop] :loops} @conn]
    (async/unsub-all pub)
    ;; Close send channel and wait for loop to complete
    (async/close! send-chan)
    (async/<!! send-loop)
    ;; Close recv channel
    (async/close! recv-chan)))

(defn add-to-waiting [conn token chan-set]
  (swap! (:conn conn) assoc-in [:waiting token] chan-set))

(defn remove-from-waiting [conn token]
  (swap! (:conn conn) utils/dissoc-in [:waiting token]))

;; Sync

(defn send-query* [conn token query]
  (let [query-resp-chan (async/chan)
        {:keys [pub send-chan]} @conn]
    (async/sub pub token query-resp-chan)
    (async/>!! send-chan [token query])
    (let [[recvd-token json] (async/<!! query-resp-chan)]
      (assert (= recvd-token token) "Must not receive response with different token")
      (async/unsub pub token query-resp-chan)
      (json/read-str json :key-fn keyword))))

(defn send-query [conn token query]
  (let [{:keys [db]} @conn
        query (if (and db (= 2 (count query))) ;; If there's only 1 element in query then this is a continue or stop query.
                ;; TODO: Could provide other global optargs too
                (concat query [{:db [(types/tt->int :DB) [db]]}])
                query)
        json (json/write-str query)
        {type :t resp :r :as json-resp} (send-query* conn token json)
        resp (parse-response resp)]
    (condp get type
      #{1} (first resp) ;; Success Atom, Query returned a single RQL datatype.
      #{2} (do ;; Success Sequence, Query returned a sequence of RQL datatypes.
             (remove-from-waiting conn token)
             resp)
      #{3 5} (if (get (:waiting @conn) token) ;; Success Partial, Query returned a partial sequence of RQL datatypes.
               (lazy-seq (concat resp (send-continue-query conn token)))
               (do
                 (add-to-waiting conn token nil)
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

;; Async

(defn run-query-chan [query conn result-chan] ;; TODO: chan set instead of just result-chan?
  (let [{:keys [db]} @conn
        token (:token (swap! (:conn conn) update-in [:token] inc))
        global-optargs (when db [{:db [(types/tt->int :DB) [db]]}])
        payload (qb/prepare-query :START query global-optargs)
        control-in-chan (async/chan 10)
        control-out-chan (async/chan 10)
        error-chan (async/chan 10)
        chan-set {:result-chan result-chan :error-chan error-chan :control-in-chan control-in-chan :control-out-chan control-out-chan :token token}
        pub-resp-chan (async/chan) ;; Internal channel for receiving RethinkDB responses for this query's token.
        {:keys [pub send-chan]} @conn
        _ (add-to-waiting conn token chan-set)
        clean-up (fn [close-type]
                   (async/>!! control-out-chan close-type)
                   (utils/close-chans [result-chan error-chan control-in-chan])
                   (remove-from-waiting conn token))]
    (async/go-loop [payload payload]
      (async/sub pub token pub-resp-chan) ;; Subscribe to connection publication channel for our query token
      (async/>! send-chan [token payload])
      (async/alt!
        :priority true

        control-in-chan
        ([ctrl-value]
          (do ;; Can't close channel, as we'll always alt! onto the closed channel. Need another way to express this pattern.
            #_(async/close! control-in-chan) ;; Shut the door on the way out, don't want to send a STOP query twice.
            (recur (qb/prepare-query :STOP))))

        pub-resp-chan
        ([resp]
          (let [[recvd-token json] resp ;; TODO: use Transducers for this section
                _ (assert (= recvd-token token)
                          "Must not receive response for different token") ;;TODO: Is this really necessary with the async/sub?
                _ (async/unsub pub token pub-resp-chan)
                {type :t resp :r :as msg} (json/read-str json :key-fn keyword)
                parsed-resp (parse-response resp)]
            (case (int type)
              ;; 1 is a single result, 2 is a result sequence. However they are both wrapped in a vector
              ;; so onto-chan works correctly for both.
              (1 2) (do (async/<! (async/onto-chan result-chan parsed-resp true)) ;; Need to wait for all values to go onto chan before closing
                        (clean-up :closed))
              ;; 3 is a partial sequence.
              3 (do (async/<! (async/onto-chan result-chan parsed-resp false)) ;; Need to wait for all values to go onto chan before recurring to preserve backpressure
                    ;; Recur with a continue query, same token will be used.
                    (recur (qb/prepare-query :CONTINUE)))
              ;; else an error occurred
              (do (async/>! error-chan parsed-resp)
                  (clean-up :error)))))))
    chan-set))
