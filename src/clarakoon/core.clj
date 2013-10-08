(ns clarakoon.core
  (:require [clojure.core.async
             :as async
             :refer [<! >! <!! >!! timeout chan alt! alts!! go close! thread]]
            [clojure.core.match
             :as match
             :refer (match)]
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

(defn make-handler [event-channel decode-with result-channel]
  (proxy [ReplayingDecoder] []
    (decode [ctx channel buf state]
      (>!! result-channel (@decode-with buf)))
    (channelBound [ctx e]
      (thread (>!! event-channel :bound)))
    (channelConnected [ctx e]
      (thread (>!! event-channel :connected)))
    (channelClosed [ctx e]
      (>!! event-channel :closed))
    (channelDisconnected [ctx e]
      (>!! event-channel :disconnected))
    (channelInterestChanged [ctx e]
      (thread (>!! event-channel :interest-changed)))
    (channelOpen [ctx e]
      (thread (>!! event-channel :open)))
    (channelUnbound [ctx e]
      (>!! event-channel :unbound))))

; cleanup [ctx e]
;  channelDisconnected [ctx e]
;  channelClosed [ctx e]

; exceptionCaught [ctx e]

; FrameDecoder
;  actualReadableBytes
;  messageReceived [ctx e]

; SimpleChannelUpstreamHandler

; channelBound(ChannelHandlerContext ctx, ChannelStateEvent e)
; channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e)
; channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e)
; channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e)
; channelInterestChanged(ChannelHandlerContext ctx, ChannelStateEvent e)
; channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e)
; channelUnbound(ChannelHandlerContext ctx, ChannelStateEvent e)
; childChannelClosed(ChannelHandlerContext ctx, ChildChannelStateEvent e)
; childChannelOpen(ChannelHandlerContext ctx, ChildChannelStateEvent e)
; exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
; handleUpstream(ChannelHandlerContext ctx, ChannelEvent e)
; messageReceived(ChannelHandlerContext ctx, MessageEvent e)
; writeComplete(ChannelHandlerContext ctx, WriteCompletionEvent e)


(defn make-pipeline-factory [handler]
  (proxy [ChannelPipelineFactory] []
    (getPipeline []
      (doto (Channels/pipeline)
        (.addLast "handler" handler)))))

(defn make-bootstrap
  [handler]
  (let [bootstrap (ClientBootstrap.
                   (NioClientSocketChannelFactory.
                    (Executors/newCachedThreadPool)
                    (Executors/newCachedThreadPool)))]
    (doto bootstrap
      (.setOption "bufferFactory"
                  (HeapChannelBufferFactory. ByteOrder/LITTLE_ENDIAN))
      (.setPipelineFactory (make-pipeline-factory handler)))))

(defn make-node-connection [cluster-id socket-address result-channel]
  (let [decode-with (atom nil)
        event-channel (chan)
        handler (make-handler event-channel decode-with result-channel)
        bootstrap (make-bootstrap handler)
        channel (let [future (.connect bootstrap socket-address)]
                  (<!! event-channel)
                  (.getChannel (.sync future)))]
    (<!! event-channel)
    (.write channel (c/prologue cluster-id))
    {:channel channel
     :decode-with decode-with
     :result-channel result-channel
     :event-channel event-channel}))

(defn sync-command [connection command & args]
  (apply c/send-command connection command args)
  (<!! (:result-channel connection)))

(defn make-cluster-client [cluster-id nodes]
  {:cluster-id cluster-id
   :nodes nodes
   :master (atom nil)
   :connections (atom {})
   :result-channel (chan)})

(defn get-or-create-connection [client name]
  (let [connection (@(:connections client) name)]
    (if (or (nil? connection) false) ;; TODO check isConnected
      (let [[ip port] ((:nodes client) name)
            c (make-node-connection (:cluster-id client) (InetSocketAddress. ip port) (:result-channel client))]
        (swap! (:connections client) (fn [cs] (assoc cs name c)))
        c)
      connection)))

(defn determine-master [client]
  (loop [[[name _] & rest] (shuffle (seq (:nodes client)))]
    (let [connection (get-or-create-connection client name)
          master (sync-command connection :who-master)]
      (match master
             [:none] (recur rest)
             [:some m] (swap! (:master client) (fn [_] m))))))

(defn send-command [client command & args]
  (when (nil? @(:master client))
    (determine-master client))
  (let [conn (get-or-create-connection client @(:master client))]
    (apply c/send-command conn command args)))

(def nodes
  {"arakoon_0" ["127.0.0.1" 4000]
   "arakoon_1" ["127.0.0.1" 4001]
   "arakoon_2" ["127.0.0.1" 4002]})
(def cluster-id
  "ricky")

; TODOS
; - handle exceptions while decoding answers? exceptions and async? -> test, play
; - handle sequences
; - handle other arakoon calls
; - write some integration tests, based on core/-main
; - make 'cluster'-client a la what's available in python client

(defn get-result [client]
  (<!! (:result-channel client))
  #_(let [disconnected (:event-channel client)
        [v ch] (alts!! [disconnected (:result-channel client)])]
    [v ch]
    #_(if (= ch disconnected)
      )))

(defn -main [& args]
  (let [client (make-cluster-client cluster-id nodes)]
    (send-command client :who-master)
    (println "who-master:" (get-result client))
    (send-command client :exists false "key")
    (println "exists:" (get-result client))
    (send-command client :set "key" "thevalue")
    (println "set:" (get-result client))
    (send-command client :get false "key")
    (println "get:" (get-result client))
    (send-command client :delete "key")
    (println "delete:" (get-result client))
    (send-command client :exists false "key")
    (println "exists:" (get-result client))
    #_(.awaitUninterruptibly (.getCloseFuture channel))
    #_(.releaseExternalResources bootstrap)))
