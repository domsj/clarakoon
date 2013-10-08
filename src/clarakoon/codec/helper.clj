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

(defn buf-read-int32 [buf]
  (.readInt buf))

(defn buf-read-int64 [buf]
  (.readLong buf))

(defn buf-read-list [buf read-value]
  (let [length (buf-read-int32 buf)]
    (loop [i length acc []]
      (if (zero? i)
        acc
        (recur (dec i) (conj acc (read-value buf)))))))

(defn buf-read-string-list [buf]
  (buf-read-list buf buf-read-string))

(defn buf-read-string-option-list [buf]
  (buf-read-list buf buf-read-string-option))

(defn buf-read-string-string-list [buf]
  (buf-read-list
   buf
   (fn [buf] [(buf-read-string buf) (buf-read-string buf)])))

(defn buf-write-int32 [buf i]
  (.writeInt buf i))

(defn buf-write-bool [buf value]
  (.writeByte buf (if value 1 0)))

(defn buf-write-string [buf ^String s]
  (buf-write-int32 buf (.length s))
  (.writeBytes buf (.getBytes s)))

(defn buf-write-option [buf write-value value-option]
  (if (nil? value-option)
    (.writeByte buf 0x00)
    (do
      (.writeByte buf 0x01)
      (write-value buf value-option))))

(defn buf-write-list [buf write-value values]
  (buf-write-int32 buf (count values))
  (doseq [v values]
    (write-value buf v)))

(defn buf-write-string-option [buf value]
  (buf-write-option buf-write-string value))

(defn buf-write-string-list [buf values]
  (buf-write-list buf buf-write-string values))
