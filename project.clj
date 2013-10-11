(defproject domsj/clarakoon "0.0.1-SNAPSHOT"
  :description "A Clojure client for Arakoon"
  :url "https://github.com/domsj/clarakoon"
  :license {:name "GNU AFFERO GENERAL PUBLIC LICENSE Version 3"
            :url "https://www.gnu.org/licenses/agpl-3.0.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/core.match "0.2.0"]
                 [io.netty/netty "3.7.0.Final"]
                 [org.clojure/core.async "0.1.242.0-44b1e3-alpha"]]
  :globals-vars {*warn-on-reflection* true})
