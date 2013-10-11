(ns clarakoon.core
  (:require [clojure.core.async
             :as async
             :refer [<! >! timeout chan alt! go close! thread]]
            [clojure.core.match
             :as match
             :refer (match)]
            [clarakoon.codec
             :as c]
            [clj-time.core
             :as time])
  (:import [java.net
            ConnectException
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
            Channel
            ChannelFuture
            ChannelHandlerContext]
           [org.jboss.netty.handler.codec.replay
            ReplayingDecoder]
           [org.jboss.netty.channel.socket.nio
            NioClientSocketChannelFactory]
           [org.jboss.netty.buffer
            HeapChannelBufferFactory]
           ))

(set! *warn-on-reflection* true)

(defn make-handler [event-channel decode-with result-channel]
  (proxy [ReplayingDecoder] []
    (decode [ctx channel buf state]
      (go (>! result-channel (@decode-with buf))))
    (channelBound [ctx e]
      (go (>! event-channel :bound)))
    (channelConnected [ctx e]
      (go (>! event-channel :connected)))
    (channelClosed [ctx e]
      (close! event-channel))
    (channelDisconnected [ctx e]
      (close! event-channel))
    (channelInterestChanged [ctx e]
      (go (>! event-channel :interest-changed)))
    (channelOpen [ctx e]
      (go (>! event-channel :open)))
    (channelUnbound [ctx e]
      (close! event-channel))))


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

(defn make-node-connection [cluster-id socket-address]
  (go
   (let [decode-with (atom nil)
         event-channel (chan)
         result-channel (chan)
         handler (make-handler event-channel decode-with result-channel)
         bootstrap (make-bootstrap handler)
         ^ChannelFuture future (.connect bootstrap socket-address)
         ^Channel channel (loop [event (<! event-channel)] ; loop until connected
                            (case event
                              :open (recur (<! event-channel))
                              :bound (recur (<! event-channel))
                              :connected (.getChannel future)
                              nil))]
     (when channel
         (.write channel (c/prologue cluster-id))
         {:channel channel
          :decode-with decode-with
          :event-channel event-channel
          :result-channel result-channel}))))

(defprotocol ConnectionPool
  (with-connection [this node-name f]
    "method that provides a connection to the specified node returns the result from")
  (with-random-connections [this f]
    "method that provides connections until a non-nil value is returned from f"))

(defrecord ClusterConnectionPool [cluster-id nodes conns]
  ;; todo keep previous connections in private state
  ;; todo don't just shuffle but first connect to existing open connections, if any
  ConnectionPool
  (with-connection [this node-name f]
    (go
     (when-let [connection (or
                            (@conns node-name)
                            (let [[^String ip ^int port] (nodes node-name)
                                  conn (<! (make-node-connection cluster-id (InetSocketAddress. ip port)))]
                              (go (loop [e (<! (:event-channel conn))]
                                    (match e
                                           nil (swap! conns assoc node-name nil)
                                           (recur (<! (:event-channel conn))))))
                              conn)
                            )]
       (swap! conns assoc node-name connection)
       (try
         (do
           (f connection)
           (<! (:result-channel connection)))
         (catch Exception e (do
                              (swap! conns assoc node-name nil)
                              e)))
       )))
  (with-random-connections [this f]
    (go
     (loop [[[node-name _] & rest] (shuffle (seq nodes))]
       (if-let [result (<! (with-connection this node-name f))]
         result
         (recur rest))))))

(defn with-client [c-pool command & args]
  (with-random-connections c-pool #(apply c/send-command % command args)))

(defn find-master [c-pool]
  (with-client c-pool :who-master))

(defn with-node-client [c-pool node-name command & args]
  (with-connection c-pool node-name #(apply c/send-command % command args)))

(defn with-master-client [c-pool command & args]
  (go
   (match (<! (find-master c-pool))
          [:none] nil
          [:some m] (<! (apply with-node-client c-pool m command args)))))

(def nodes
  {"arakoon_0" ["127.0.0.1" 4000]
   "arakoon_1" ["127.0.0.1" 4001]
   "arakoon_2" ["127.0.0.1" 4002]})
(def cluster-id
  "ricky")

(def c-pool
  (ClusterConnectionPool. cluster-id nodes (atom {})))

; TODOS
; - handle exceptions while decoding answers? exceptions and async? -> test, play
; - handle sequences
; - handle other arakoon calls
; - write some integration tests, based on core/-main
; - make 'cluster'-client a la what's available in python client
; - add core.typed
; - clean up connections van connection pool ...


(defn -main [& args]
  (go
   (println (<! (find-master c-pool)))
   (println (<! (with-client c-pool :who-master)))
   (println (<! (with-master-client c-pool :set "key" "avalue")))
   (println (<! (with-master-client c-pool :get false "key")))
   (println (<! (with-master-client c-pool :exists false "key")))
   (println (<! (with-master-client c-pool :delete "key")))
   (println (<! (with-master-client c-pool :exists false "key")))))
