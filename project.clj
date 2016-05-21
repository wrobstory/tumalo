(defproject tumalo "0.1.0-SNAPSHOT"
  :description "Clojure Elasticsearch Indexing Tools"
  :license {:name "MIT"
            :url "https://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [clojurewerkz/elastisch "2.2.1"]
                 [prismatic/schema "1.1.1"]
                 [clj-http "2.2.0"]]
  :resource-paths ["resources"]
  :profiles {:dev {:resource-paths ["resources/test"]}}
  :global-vars {*warn-on-reflection* true})
