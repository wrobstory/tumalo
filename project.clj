(defproject tumalo "0.2.1"
  :description "Clojure Elasticsearch Indexing Tools"
  :license {:name "MIT"
            :url "https://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.clojure/core.async "0.2.374"]
                 [ch.qos.logback/logback-classic "1.1.3"]
                 [clojurewerkz/elastisch "2.2.1" :exclusions [com.fasterxml.jackson.core/jackson-core]]
                 [com.amazonaws/aws-java-sdk-s3 "1.11.35"]
                 [prismatic/schema "1.1.1"]
                 [clj-http "2.2.0"]
                 [clj-time "0.11.0"]]
  :resource-paths ["resources"]
  :profiles {:dev {:resource-paths ["resources/test"]}
             :uberjar {:aot :all}}
  :global-vars {*warn-on-reflection* true})
