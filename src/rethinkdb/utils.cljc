(ns rethinkdb.utils
  (:require [clojure.string :as string])
  #?(:clj (:import [java.nio ByteOrder ByteBuffer])))

#?(:clj (defn int->bytes
          "Creates a ByteBuffer of size n bytes containing int i"
          [i n]
          (let [buf (ByteBuffer/allocate n)]
            (doto buf
              (.order ByteOrder/LITTLE_ENDIAN)
              (.putInt i))
            (.array buf))))

#?(:clj (defn str->bytes
          "Creates a ByteBuffer of size n bytes containing string s converted to bytes"
          [^String s]
          (let [n (count s)
                buf (ByteBuffer/allocate n)]
            (doto buf
              (.put (.getBytes s)))
            (.array buf))))

#?(:clj (defn bytes->int
          "Converts bytes to int"
          [^bytes bs n]
          (let [buf (ByteBuffer/allocate (or n 4))]
            (doto buf
              (.order ByteOrder/LITTLE_ENDIAN)
              (.put bs))
            (.getInt buf 0))))

#?(:clj (defn pp-bytes [bs]
          (vec (map #(format "%02x" %) bs))))

(defn snake-case [s]
  (string/replace (name s) \- \_))
