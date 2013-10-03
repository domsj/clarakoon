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
; (require '[clojure.core.async :as async :refer [<! >! <!! >!! timeout chan alt! go]])

(defn new-buffer []
  (ChannelBuffers/dynamicBuffer ByteOrder/LITTLE_ENDIAN 256))

(def c (chan))

(def result)

(defn client-handler [cluster-id decode-as]
  (proxy [ReplayingDecoder] []
    (channelConnected [^ChannelHandlerContext ctx e]
      (.write
       (.getChannel ctx)
                                        ; write prologue
       (doto (new-buffer)
         (.writeInt 0xb1ff0000)         ; magic
         (.writeInt 1)                  ; version
         (.writeInt (.length cluster-id))
         (.writeBytes (.getBytes cluster-id)))))
    (decode [ctx channel buf state]
      (let [return-code (.readInt buf)]
        (def result '(return-code "fsd"))
        (case return-code
          0 (case @decode-as
              :who-master
               (let [none-or-some-byte (.readByte buf)
                     none-or-some (case none-or-some-byte
                                    0x00 :none
                                    0x01 :some)]
                 (def result
                   (case none-or-some
                     :none '(:none)
                     :some (let [length (.readInt buf)
                                 master (.toString (.readSlice buf length) (Charset/forName "UTF-8"))]
                             (list :some master))))))
          )))))
;    #_(exceptionCaught [ctx cause]
;        (.close ctx)))

(defn bootstrap [cluster-id decode-as]
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
           (.addLast "handler" (client-handler cluster-id decode-as))))))))

(defn command [code]
  (doto (new-buffer)
    (.writeInt (+ 0xb1ff0000 code))))

(defn send-who-master [channel decode-as]
  (swap! decode-as (fn [old] :who-master))
  (.write channel (command 2)))

(defn -main [& args]
  (let [decode-as (atom nil)
        bootstrap (bootstrap "ricky" decode-as)
        future (.connect bootstrap (InetSocketAddress. "localhost" 4000))
        channel (.getChannel (.sync future))]
    (send-who-master channel decode-as)
    (.awaitUninterruptibly (.getCloseFuture channel))
    (.releaseExternalResources bootstrap)))
