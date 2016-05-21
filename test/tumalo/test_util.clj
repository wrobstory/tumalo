(ns tumalo.test-util
  (:require [clojure.java.io :as io]))

;; Fetch test config from resources/test/
(def ^:const test-config (-> "test.edn"
                             io/resource
                             clojure.java.io/reader
                             java.io.PushbackReader.
                             clojure.edn/read))
