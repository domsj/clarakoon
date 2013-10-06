(ns clarakoon.codec
  (:require [clojure.core.async
             :as async
             :refer [<! >! <!! >!! timeout chan alt! go close!]]
            [clarakoon.codec.helper :as h]))

(defn command-buffer [code]
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
  {:who-master {:code 2
                :args []
                :return-type :string-option}
   :set {:code 9
         :args [:string :string]
         :return-type :unit}
   :exists {:code 7
            :args [:bool :string]
            :return-type :bool}})

(defn write-arg [buf arg-type arg]
  ((case arg-type
    :string h/buf-write-string
    :bool h/buf-write-bool) buf arg))

(defn write-args [buf arg-types arguments]
  (doall
   (map (fn [arg-type arg] (write-arg buf arg-type arg)) arg-types arguments)))

(defn read-response [return-type]
  (fn [buf]
    (let [return-code (.readInt buf)]
      (case return-code
        0 (case return-type
            :unit (h/buf-read-unit buf)
            :bool (h/buf-read-bool buf)
            :string-option (h/buf-read-string-option buf))
        (let [error-string (h/buf-read-string buf)
              error-code (case return-code
                           1 :error-no-magic
                           2 :error-too-many-dead-nodes
                           3 :error-no-hello
                           4 :error-not-master
                           5 :not-found
                           6 :wrong-cluster
                           7 :assertion-failed
                           0xff :unknown-failure)]
          (list error-code error-string))))))

(defn send-command [client command & arguments]
  (let [{:keys [code args return-type]} (commands command)
        request-buffer (command-buffer code)]
    (write-args request-buffer args arguments)
    (swap-decode (:decode-with client) (read-response return-type))
    (.write (:channel client) request-buffer)))
