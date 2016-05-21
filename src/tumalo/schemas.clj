(ns tumalo.schemas
  (:require [schema.core :as s]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Configuration Schemas

(s/defschema ElasticsearchConfiguration
  "Elasticsearch configuration params"
  {:host (s/named s/Str "ES Connection URL")
   :port (s/named s/Num "ES Connection port")
   :pool-connection-timeout (s/named s/Num "ES HTTP Connection pool timeout")
   :pool-request-timeout (s/named s/Num "ES HTTP Connection request timeout")
   :connection-threads (s/named s/Num "ES HTTP Connection pool thread count")})
