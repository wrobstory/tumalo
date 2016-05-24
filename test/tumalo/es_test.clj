(ns tumalo.es-test
  (:require [clojure.test :refer :all]
            [schema.test]
            [clojurewerkz.elastisch.rest.admin :as esa]
            [clojurewerkz.elastisch.rest.index :as esi]
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
        pool (tes/get-connection-pool es-config)]
    (try
      (tu/put-test-data-to-index! pool
                                  100
                                  "test_index_1"
                                  tu/test-mapping-1
                                  tu/test-mapping-1-name)

      (testing "Test get lazy doc seq"
        (let [doc-seq (tes/get-lazy-doc-seq pool
                                            "test_index_1"
                                            "test-mapping-1"
                                            {:match_all {}})]

          (clojure.pprint/pprint (count doc-seq))))

      (finally
        (esi/delete pool "test_index_1")))))
