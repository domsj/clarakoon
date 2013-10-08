(ns clarakoon.codec
  (:require [clojure.core.async
             :as async
             :refer [<! >! <!! >!! timeout chan alt! go close!]]
            [clarakoon.codec.helper :as h]))

(defn command-buffer [code]
  (doto (h/new-buffer)
    (.writeInt (+ 0xb1ff0000 code))))

(defn prologue [cluster-id]
  (doto (h/new-buffer)
    (h/buf-write-int32 0xb1ff0000)     ; magic
    (h/buf-write-int32 1)              ; version
    (h/buf-write-string cluster-id)))

(defn swap-decode [decode-with decoder]
  (swap! decode-with (fn [_] decoder)))

(def commands
  {
   :ping [1
          []
          :unit]
   :who-master [2
                []
                :string-option]
   :exists [7
            [:bool :string]
            :bool]
   :get [8
         [:bool :string]
         :string]
   :set [9
         [:string :string]
         :unit]
   :delete [0x0a
            [:string]
            :unit]
   :range [0x0b
           [:string-option :bool :string-option :bool :int32]
           :string-list]
   :prefix-keys [0x0c
                 [:bool :string :int32]
                 :string-list]
   :test-and-set [0x0d
                  [:string :string :string]
                  :string-option]
   #_:last-entries #_{:code 0x0e}
   :range-entries [0x0f
                   [:bool :string-option :bool :string-option :bool :int32]
                   :string-string-list]
   #_:sequence
   :multi-get [0x11
               [:bool :string-list]
               :string-option-list]
   :expect-progress-possible [0x12
                              []
                              :bool]
   :user-function [0x15
                   [:string :string-option]
                   :string-option]
   :assert [0x16
            [:string :string-option]
            :unit]
   :get-key-count [0x1a
                   []
                   :int64]
   :confirm [0x1b
             [:string :string]
             :unit]
   :rev-range-entries [0x23
                       [:string-option
                        :bool
                        :string-option
                        :bool
                        :int32]
                        :string-string-list]
   #_:synced-sequence #_[0x24]
   :delete-prefix [0x27
                   [:string]
                   :int32]
   })

(defn write-arg [buf arg-type arg]
  ((case arg-type
    :string h/buf-write-string
    :bool h/buf-write-bool
    :string-option h/buf-write-string-option
    :int32 h/buf-write-int32
    :string-list h/buf-write-string-list) buf arg))

(defn write-args [buf arg-types arguments]
  (doall
   (map (fn [arg-type arg] (write-arg buf arg-type arg)) arg-types arguments)))

(defn read-response [return-type]
  (fn [buf]
    (let [return-code (.readInt buf)]
      (if (= return-code 0)
        (let [read-f (case return-type
                       :unit h/buf-read-unit
                       :bool h/buf-read-bool
                       :string-option h/buf-read-string-option
                       :string h/buf-read-string
                       :string-list h/buf-read-string-list
                       :string-string-list h/buf-read-string-string-list
                       :string-option-list h/buf-read-string-option-list
                       :int32 h/buf-read-int32
                       :int64 h/buf-read-int64)]
          (read-f buf))
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

(defn send-command [connection command & arguments]
  (let [[code args return-type] (commands command)
        request-buffer (command-buffer code)]
    (write-args request-buffer args arguments)
    (swap-decode (:decode-with connection) (read-response return-type))
    (.write (:channel connection) request-buffer)))
