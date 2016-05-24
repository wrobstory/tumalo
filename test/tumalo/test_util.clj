(ns tumalo.test-util
  (:require [clojure.java.io :as io]
            [clojurewerkz.elastisch.rest.index :as esi]
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

(defn create-test-mapping-1-document
  "Create a test document for test-mapping-1"
  []
  {:uuid (str (java.util.UUID/randomUUID))
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
        docs (repeatedly doc-count create-test-mapping-1-document)
        docs-partitioned (partition-all (max 2000 (/ doc-count 5)) docs)]


    (if-not (esi/exists? pool index-name)
      (esi/create pool index-name :settings index-settings :mappings mapping))

    (doseq [doc-group docs-partitioned
            :let [bulk-doc-group (esb/bulk-create doc-group)]]
      (esb/bulk-with-index-and-type pool index-name mapping-name bulk-doc-group))))
