(ns tumalo.es
  (:require [clojure.tools.logging :refer [log logf]]
            [clojurewerkz.elastisch.rest :as esr]
            [schema.core :as s]
            [tumalo.schemas :as ts])
  (:import [java.util HashMap]
           [clojurewerkz.elastisch.rest Connection]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Connections

(s/defn get-connection-pool :- Connection
        "Get connection to Elasticsearch given a port and URL. pool-connection-timeout is in seconds
        and indicates how long an open connection should idle before closing. pool-request-timeout is in milliseconds,
        and indicates how long a request should wait before timing out and throwing an error"
        [config :- ts/ElasticsearchConfiguration]
        (let [host (:host config)
              port (:port config)
              es-conn-str (format "http://%s:%s" host port)
              pool-connection-timeout (:pool-connection-timeout config)
              pool-request-timeout (:pool-request-timeout config)
              connection-threads (:connection-threads config)
              conn-params {:timeout pool-connection-timeout
                           :threads connection-threads
                           :insecure? false}
              conn-manager (clj-http.conn-mgr/make-reusable-conn-manager conn-params)]
          (logf :info "Initializing ElasticSearch connection to %s..." es-conn-str)
          (esr/connect es-conn-str {:connection-manager conn-manager
                                    :conn-timeout pool-request-timeout
                                    :socket-timeout pool-request-timeout})))
