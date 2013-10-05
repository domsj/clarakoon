(ns clarakoon.codec.helper
  (:import
   [java.nio.charset
    Charset]
   [org.jboss.netty.buffer
    ChannelBuffers]
   [java.nio
    ByteOrder]
))

(defn new-buffer []
  (ChannelBuffers/dynamicBuffer ByteOrder/LITTLE_ENDIAN 256))


(defn buf-read-string [buf]
  (let [length (.readInt buf)
        value (.toString (.readSlice buf length) (Charset/forName "UTF-8"))]
    value))

(defn buf-read-option [buf read-value]
  (let [none-or-some-byte (.readByte buf)
        result            (case none-or-some-byte
                            0x00 (list :none)
                            0x01 (list :some (read-value buf)))]
    result))

(defn buf-read-bool [buf]
  (case (.readByte buf)
    0x00 false
    0x01 true))

(defn buf-read-unit [buf]
  :unit)

(defn buf-read-string-option [buf]
  (buf-read-option buf buf-read-string))


(defn buf-write-int [buf i]
  (.writeInt buf i))

(defn buf-write-bool [buf value]
  (.writeByte buf (if value 1 0)))

(defn buf-write-string [buf ^String s]
  (buf-write-int buf (.length s))
  (.writeBytes buf (.getBytes s)))
(ns clarakoon.codec
  (:import
   [java.nio.charset
    Charset]
   [org.jboss.netty.buffer
    ChannelBuffers]
   [java.nio
    ByteOrder]
))

(defn new-buffer []
  (ChannelBuffers/dynamicBuffer ByteOrder/LITTLE_ENDIAN 256))


(defn buf-read-string [buf]
  (let [length (.readInt buf)
        value (.toString (.readSlice buf length) (Charset/forName "UTF-8"))]
    value))

(defn buf-read-option [buf read-value]
  (let [none-or-some-byte (.readByte buf)
        result            (case none-or-some-byte
                            0x00 (list :none)
                            0x01 (list :some (read-value buf)))]
    result))

(defn buf-read-bool [buf]
  (case (.readByte buf)
    0x00 false
    0x01 true))

(defn buf-read-unit [buf]
  :unit)

(defn buf-read-string-option [buf]
  (buf-read-option buf buf-read-string))


(defn buf-write-int [buf i]
  (.writeInt buf i))

(defn buf-write-bool [buf value]
  (.writeByte buf (if value 1 0)))

(defn buf-write-string [buf ^String s]
  (buf-write-int buf (.length s))
  (.writeBytes buf (.getBytes s)))
