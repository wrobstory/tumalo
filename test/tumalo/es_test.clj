(ns tumalo.es_test
  (:require [clojure.test :refer :all]
            [schema.test]
            [clojurewerkz.elastisch.rest.admin :as esa]
            [tumalo.test-util :as tu]
            [tumalo.es :as tes]))

(use-fixtures :once schema.test/validate-schemas)

(deftest ^:es test-es-connection
  (testing "Test that an ES connection can be made"
    (let [es-config (:elasticsearch tu/test-config)
          es-conn-pool (tes/get-connection-pool es-config)
          cluster-status (esa/cluster-health es-conn-pool)]
      (is (= "green" (:status cluster-status))))))

