(ns rethinkdb.ssl
  (:require [clojure.java.io :as io])
  (:import [io.netty.channel ChannelPipeline]
           [io.netty.handler.ssl SslContext SslContextBuilder]))

(defn ssl-context [ca-cert]
  (-> (SslContextBuilder/forClient)
      (.trustManager (io/file ca-cert))
      .build))

(defn ssl-pipeline [^ChannelPipeline p ca-cert]
  (.addFirst p "ssl-handler"
             (.newHandler ^SslContext (ssl-context ca-cert)
                          (-> p .channel .alloc))))
