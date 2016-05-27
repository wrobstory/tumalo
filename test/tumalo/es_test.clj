(ns tumalo.es-test
  (:require [clojure.test :refer :all]
            [schema.test]
            [clojurewerkz.elastisch.rest.admin :as esa]
            [clojurewerkz.elastisch.rest.index :as esi]
            [clojurewerkz.elastisch.rest.document :as esd]
            [tumalo.test-util :as tu]
            [tumalo.es :as tes]))

(use-fixtures :once schema.test/validate-schemas)

(deftest ^:es test-es-connection
  (testing "Test that an ES connection can be made"
    (let [es-config (:elasticsearch tu/test-config)
          es-conn-pool (tes/get-connection-pool es-config)
          cluster-status (esa/cluster-health es-conn-pool)]
      (is (false? (:timed_out cluster-status))))))

(deftest ^:es test-get-lazy-doc-seq
  (let [es-config (:elasticsearch tu/test-config)
        pool (tes/get-connection-pool es-config)
        doc-count 100]
    (try
      (tu/put-test-data-to-index! pool
                                  doc-count
                                  "test_index_1"
                                  tu/test-mapping-1
                                  tu/test-mapping-1-name)

      (testing "Test get lazy doc seq"
        (let [doc-seq (tes/get-lazy-doc-seq pool
                                            "test_index_1"
                                            "test-mapping-1"
                                            {:match_all {}})]
          (is (= doc-count (count doc-seq)))))

      (finally
        (esi/delete pool "test_index_1")))))

(deftest ^:es test-reindex-docs
  (let [es-config (:elasticsearch tu/test-config)
        pool (tes/get-connection-pool es-config)
        doc-count 1000]
    (try
      (tu/put-test-data-to-index! pool
                                  doc-count
                                  "test_index_1"
                                  tu/test-mapping-1
                                  tu/test-mapping-1-name)

      (testing "Test reindex docs without fn modifier"
        (try
          (esi/create pool
                      "test_index_2"
                      :settings {:number_of_shards 5 :number_of_replicas 1}
                      :mappings tu/test-mapping-1)
          (tes/reindex-docs pool
                            "test_index_1"
                            "test-mapping-1"
                            "test_index_2"
                            "test-mapping-2"
                            200)
          (esi/refresh pool "test_index_2")
          (is (= doc-count (:count (esd/count pool "test_index_2" "test-mapping-2"))))
          (finally
            (esi/delete pool "test_index_2"))))

      (testing "Test reindex docs with modifier fn"
        (try
          (esi/create pool
                      "test_index_2"
                      :settings {:number_of_shards 5 :number_of_replicas 1}
                      :mappings tu/test-mapping-2)
          (let [modifier-fn (fn [doc-seq]
                              (map #(identity {:_index (:_index %)
                                               :_type (:_type %)
                                               :uuid (:uuid %)
                                               :when_received (:when_recorded %)
                                               :animal_type "bird"
                                               :animal_name (:bird %)
                                               :animal_count (:count %)}) doc-seq))]
            (tes/reindex-docs pool
                              "test_index_1"
                              "test-mapping-1"
                              "test_index_2"
                              "test-mapping-2"
                              200
                              modifier-fn)
            (esi/refresh pool "test_index_2")
            (is (= doc-count (:count (esd/count pool "test_index_2" "test-mapping-2")))))
          (finally
            (esi/delete pool "test_index_2"))))

      (finally
        (esi/delete pool "test_index_1")))))
