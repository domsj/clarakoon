(ns clarakoon.core
  (:import [java.net InetSocketAddress]
           [java.nio ByteOrder]
           [java.nio.charset Charset]
           [java.util.concurrent Executors]
           [org.jboss.netty.bootstrap ClientBootstrap]
           [org.jboss.netty.channel
            ChannelPipelineFactory
            SimpleChannelHandler
            SimpleChannelUpstreamHandler
            Channels
            ChannelHandlerContext]
           [org.jboss.netty.handler.codec.frame
            FrameDecoder]
           [org.jboss.netty.handler.codec.replay
            ReplayingDecoder]
           [org.jboss.netty.channel.socket.nio
            NioClientSocketChannelFactory
            NioSocketChannel]
           [org.jboss.netty.buffer
            HeapChannelBufferFactory
            ChannelBuffers]
           ))

(require '[clojure.core.async :as async :refer [<! >! <!! >!! timeout chan alt! go]])

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

(defn client-handler [cluster-id decode-with connected result-channel]
  (proxy [ReplayingDecoder] []
    (channelConnected [^ChannelHandlerContext ctx e]
      (.write
       (.getChannel ctx)
       (doto (new-buffer)               ; write prologue
         (buf-write-int 0xb1ff0000)     ; magic
         (buf-write-int 1)              ; version
         (buf-write-string cluster-id)))
      (>!! connected 0))
    (decode [ctx channel buf state]
      (let [return-code (.readInt buf)]
        (case return-code
          0
          (do
            (>!! result-channel (@decode-with buf))
            0))))))
;    #_(exceptionCaught [ctx cause]
;        (.close ctx)))

(defn bootstrap [cluster-id decode-with connected result-channel]
  (doto
      (ClientBootstrap.
       (NioClientSocketChannelFactory.
        (Executors/newCachedThreadPool)
        (Executors/newCachedThreadPool)))
    (.setOption "bufferFactory" (HeapChannelBufferFactory. ByteOrder/LITTLE_ENDIAN))
    (.setPipelineFactory
     (proxy [ChannelPipelineFactory] []
       (getPipeline []
         (doto (Channels/pipeline)
           (.addLast "handler" (client-handler cluster-id decode-with connected result-channel))))))))

(defn command [code]
  (doto (new-buffer)
    (.writeInt (+ 0xb1ff0000 code))))

(defn set-decode [decode-with decoder]
  (swap! decode-with (fn [_] decoder)))

(defn who-master [channel decode-with]
  (set-decode decode-with buf-read-string-option)
  (.write channel (command 2)))

(defn set [channel decode-with key value]
  (set-decode decode-with buf-read-unit)
  (let [buf (command 9)]
    (buf-write-string buf key)
    (buf-write-string buf key)
    (.write channel buf)))

(defn exists [channel decode-with key]
  (set-decode decode-with buf-read-bool)
  (let [buf (command 7)]
    (buf-write-bool buf false)
    (buf-write-string buf key)
    (.write channel buf)))

(defn -main [& args]
  (let [decode-with (atom nil)
        result-channel (chan)
        connected (chan)
        bootstrap (bootstrap "ricky" decode-with connected result-channel)
        future (.connect bootstrap (InetSocketAddress. "localhost" 4000))
        channel (.getChannel (.sync future))]
    (<!! connected)
    (exists channel decode-with "key")
    (println (<!! result-channel))
    (set channel decode-with "key" "value")
    (println (<!! result-channel))
    (send-who-master channel decode-with)
    (println (<!! result-channel))
    #_(.awaitUninterruptibly (.getCloseFuture channel))
    (.releaseExternalResources bootstrap)))
