(ns clarakoon.core-test
  (:require [clojure.test :refer :all]
            [clarakoon.core :refer :all]
            [clojure.core.async
             :as async
             :refer [<!! >! timeout chan alt! go close! thread]]))

(def nodes
  {"arakoon_0" ["127.0.0.1" 4000]
   "arakoon_1" ["127.0.0.1" 4001]
   "arakoon_2" ["127.0.0.1" 4002]})

(def cluster-id
  "ricky")

(def c-pool
  (make-connection-pool cluster-id nodes))

(deftest a-test
  (testing "Integrated set, exists, get, delete test"
      (let [key "key"
            val "a value"]
        (is (= :unit (<!! (with-master-client c-pool :set key val))))
        (is (= val (<!! (with-master-client c-pool :get false key))))
        (is (<!! (with-master-client c-pool :exists false key)))
        (is (= :unit (<!! (with-master-client c-pool :delete key))))
        (is (not (<!! (with-master-client c-pool :exists false key)))))))
