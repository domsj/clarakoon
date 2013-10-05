(ns clarakoon.codec
  (:require [clarakoon.codec.helper :as h]))

(defn command [code]
  (doto (new-buffer)
    (.writeInt (+ 0xb1ff0000 code))))

(defn prologue [cluster-id]
  (doto (h/new-buffer)
    (h/buf-write-int 0xb1ff0000)     ; magic
    (h/buf-write-int 1)              ; version
    (h/buf-write-string cluster-id)))

(defn set-decode [decode-with decoder]
  (swap! decode-with (fn [_] decoder)))

(defn who-master [channel decode-with]
  (set-decode decode-with h/buf-read-string-option)
  (.write channel (command 2)))

(defn set [channel decode-with key value]
  (set-decode decode-with h/buf-read-unit)
  (let [buf (command 9)]
    (h/buf-write-string buf key)
    (h/buf-write-string buf key)
    (.write channel buf)))

(defn exists [channel decode-with key]
  (set-decode decode-with h/buf-read-bool)
  (let [buf (command 7)]
    (h/buf-write-bool buf false)
    (h/buf-write-string buf key)
    (.write channel buf)))
