(ns tumalo.test-util
  (:require [clojure.java.io :as io]
            [clojurewerkz.elastisch.rest.index :as esi]
            [clojurewerkz.elastisch.rest.document :as esd]
            [clj-time.format :as f]
            [clj-time.core :as ctime]
            [clojurewerkz.elastisch.rest.bulk :as esb]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Connection Helpers

;; Fetch test config from resources/test/
(def ^:const test-config (-> "test.edn"
                             io/resource
                             clojure.java.io/reader
                             java.io.PushbackReader.
                             clojure.edn/read))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Elasticsearch Mapping Helpers

(def test-mapping-1
  {:test-mapping-1
   {:dynamic "strict"
    :properties {"uuid"          {:type "string" :index "not_analyzed" :doc_values true}
                 "when_recorded" {:type "date"}
                 "bird"          {:type "string"}
                 "count"         {:type "long"}}}})

(def ^:const test-mapping-1-name "test-mapping-1")

(def test-mapping-2
  {:test-mapping-1
   {:dynamic "strict"
    :properties {"uuid"          {:type "string" :index "not_analyzed" :doc_values true}
                 "when_received" {:type "date"}
                 "animal_type"   {:type "string"}
                 "animal_name"   {:type "string"}
                 "animal_count"  {:type "long"}}}})

(defn create-test-mapping-1-document
  "Create a test document for test-mapping-1"
  [index-name mapping-type-name]
  {:_id (str (java.util.UUID/randomUUID))
   :_index index-name
   :_type mapping-type-name
   :uuid (str (java.util.UUID/randomUUID))
   :when_recorded (f/unparse (f/formatters :date-time) (ctime/now))
   :bird (rand-nth ["sparrow" "crow" "swift" "jay" "heron" "finch"])
   :count (rand-int 1000)})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Test Setup Helpers

(defn put-test-data-to-index!
  "Creates 'test-index;, then puts `doc-count` documents to the `test-index` with `mapping`"
  [pool doc-count index-name mapping mapping-name]
  (let [index-settings {:number_of_shards 5
                        :number_of_replicas 1}
        create-test-docs-fn (partial create-test-mapping-1-document index-name mapping-name)
        docs (repeatedly doc-count create-test-docs-fn)
        docs-partitioned (partition-all (max 2000 (/ doc-count 5)) docs)]


    (if-not (esi/exists? pool index-name)
      (esi/create pool index-name :settings index-settings :mappings mapping))

    (doseq [doc-group docs-partitioned
            :let [bulk-doc-group (esb/bulk-create doc-group)]]
      (esb/bulk-with-index-and-type pool index-name mapping-name bulk-doc-group))

    (esi/refresh pool index-name)))
