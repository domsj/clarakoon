(ns clarakoon.core
  (:require [clarakoon.codec :as c])
  (:import [java.net
            InetSocketAddress]
           [java.nio
            ByteOrder]
           [java.util.concurrent
            Executors]
           [org.jboss.netty.bootstrap
            ClientBootstrap]
           [org.jboss.netty.channel
            ChannelPipelineFactory
            Channels
            ChannelHandlerContext]
           [org.jboss.netty.handler.codec.replay
            ReplayingDecoder]
           [org.jboss.netty.channel.socket.nio
            NioClientSocketChannelFactory]
           [org.jboss.netty.buffer
            HeapChannelBufferFactory]
           ))

(require '[clojure.core.async :as async :refer [<! >! <!! >!! timeout chan alt! go]])

(defn client-handler [cluster-id decode-with connected result-channel]
  (proxy [ReplayingDecoder] []
    (channelConnected [^ChannelHandlerContext ctx e]
      (.write
       (.getChannel ctx)
       (c/prologue cluster-id))
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

(defn -main [& args]
  (let [decode-with (atom nil)
        result-channel (chan)
        connected (chan)
        bootstrap (bootstrap "ricky" decode-with connected result-channel)
        future (.connect bootstrap (InetSocketAddress. "localhost" 4000))
        channel (.getChannel (.sync future))]
    (<!! connected)
    (c/exists channel decode-with "key")
    (println (<!! result-channel))
    (c/set channel decode-with "key" "value")
    (println (<!! result-channel))
    (c/who-master channel decode-with)
    (println (<!! result-channel))
    #_(.awaitUninterruptibly (.getCloseFuture channel))
    (.releaseExternalResources bootstrap)))
