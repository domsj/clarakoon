(ns clarakoon.core
  (:require [clojure.core.async
             :as async
             :refer [<! >! <!! >!! timeout chan alt! go close!]]
            [clarakoon.codec
             :as c])
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

(defn make-handler [connected decode-with result-channel]
  (proxy [ReplayingDecoder] []
    (channelConnected [ctx e]
      (close! connected))
    (decode [ctx channel buf state]
      (>!! result-channel (@decode-with buf)))))

(defn make-pipeline-factory [handler]
  (proxy [ChannelPipelineFactory] []
    (getPipeline []
      (doto (Channels/pipeline)
        (.addLast "handler" handler)))))

(defn make-channel
  [socket-address cluster-id decode-with result-channel]
  (let [bootstrap (ClientBootstrap.
                   (NioClientSocketChannelFactory.
                    (Executors/newCachedThreadPool)
                    (Executors/newCachedThreadPool)))
        connected (chan)
        my-handler (make-handler connected decode-with result-channel)]
    (doto bootstrap
      (.setOption "bufferFactory"
                  (HeapChannelBufferFactory. ByteOrder/LITTLE_ENDIAN))
      (.setPipelineFactory (make-pipeline-factory my-handler)))
    (let [future (.connect bootstrap socket-address)]
      (<!! connected)
      (.getChannel (.sync future)))))

(defn make-arakoon-client [socket-address cluster-id]
  (let [decode-with (atom nil)
        result-channel (chan)
        channel (make-channel socket-address cluster-id decode-with result-channel)]
    (.write channel (c/prologue cluster-id))
    {:channel channel
     :decode-with decode-with
     :result-channel result-channel}))

(def address
  (InetSocketAddress. "localhost" 4000))
(def cluster-id
  "ricky")

; TODOS
; - handle error return codes
; - handle exceptions while decoding answers? exceptions and async? -> test, play
; - handle other arakoon calls, but first rewrite codec/commands
;   to something like
;   {:who-master {:command 2
;                 :args [[key string] [value string]]
;                 :return-type [string option] }}
; - write some integration tests, based on core/-main
; - make 'cluster'-client a la what's available in python client

(defn -main [& args]
  (let [client (make-arakoon-client address cluster-id)]
    (c/send-command client :who-master)
    (println (<!! (:result-channel client)))
    (c/send-command client :exists "key")
    (println (<!! (:result-channel client)))
    (c/send-command client :set "key" "value")
    (println (<!! (:result-channel client)))
    #_(.awaitUninterruptibly (.getCloseFuture channel))
    #_(.releaseExternalResources bootstrap)))
