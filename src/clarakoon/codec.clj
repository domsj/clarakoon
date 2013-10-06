(ns clarakoon.codec
  (:require [clojure.core.async
             :as async
             :refer [<! >! <!! >!! timeout chan alt! go close!]]
            [clarakoon.codec.helper :as h]))

(defn command [code]
  (doto (new-buffer)
    (.writeInt (+ 0xb1ff0000 code))))

(defn prologue [cluster-id]
  (doto (h/new-buffer)
    (h/buf-write-int 0xb1ff0000)     ; magic
    (h/buf-write-int 1)              ; version
    (h/buf-write-string cluster-id)))

(defn swap-decode [decode-with decoder]
  (swap! decode-with (fn [_] decoder)))

(def commands
  {:who-master
   [h/buf-read-string-option
    (fn [] (command 2))]
   :set
   [h/buf-read-unit
    (fn [key value] (doto (command 9)
                      (h/buf-write-string key)
                      (h/buf-write-string value)))]
   :exists
   [h/buf-read-bool
    (fn [key] (doto (command 7)
                (h/buf-write-bool false)
                (h/buf-write-string key)))]})

(defn send-command [client command & args]
  (let [[read-response generate-request] (commands command)
        decoder (fn [buf]
                  (let [return-code (.readInt buf)]
                    (case return-code
                      0
                      (read-response buf))))]
    (swap-decode (:decode-with client) decoder)
    (.write (:channel client) (apply generate-request args))))
