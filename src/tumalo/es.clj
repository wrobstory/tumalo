(ns tumalo.es
  (:require [clojure.tools.logging :refer [log logf]]
            [clojurewerkz.elastisch.rest.bulk :as esb]
            [clojurewerkz.elastisch.rest.document :as esd]
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Bulk Reads

(s/defn get-lazy-doc-seq :- (s/if vector? [] clojure.lang.LazySeq)
  "Get a lazy-seq of docs from the ES `index` and `mapping-type` for the given `query`.

  The scroll query will be kept open for `scroll-duration`, which defaults to 1m."
  ([pool index mapping-type query]
    (get-lazy-doc-seq pool index mapping-type query "1m"))
  ([pool :- Connection
    index :- s/Str
    mapping-type :- s/Str
    query :- {s/Any s/Any}
    scroll-duration :- s/Str]
   (let [first-resp (esd/search pool index mapping-type
                                :query query
                                :size 10
                                :scroll scroll-duration)]
     (esd/scroll-seq pool first-resp))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Reindexing


(s/defn reindex-docs
  "Given an ES connection (`pool`), reindex all data from `source-index-name and `source-mapping-type`
  to `target-index-name` and `target-index-type`.

  If a function `f` is passed in, it will be applied to the sequence of documents after the _index and
  _type fields have been set. For example, the seq passed to `f` could look like:
  ({_index: index_1 _type: type_mapping_1 field_1: foo field_2: bar}
   {_index: index_1 _type: type_mapping_1 field_1: baz field_2: qux}
   {_index: index_1 _type: type_mapping_1 field_1: fizz field_2: buzz}

   The only contract for `f` is that it returns a sequence of maps with the _index and _type fields intact. The
   :_id field will be made available, but can be changed or left intact.

   `batch-size` dictates the size of the bulk batches."
  ([pool source-index-name source-mapping-type target-index-name target-mapping-type batch-size]
    (reindex-docs pool source-index-name source-mapping-type target-index-name target-mapping-type batch-size identity))
  ([pool :- Connection
    source-index-name :- s/Str
    source-mapping-type :- s/Str
    target-index-name :- s/Str
    target-mapping-type :- s/Str
    batch-size :- s/Num
    f]
   (let [source-idx-seq (get-lazy-doc-seq pool
                                          source-index-name
                                          source-mapping-type
                                          {:match_all {}})
         get-source-&-assign-target #(merge (:_source %) {:_index target-index-name
                                                          :_type target-mapping-type
                                                          :_id (:_id %)})
         assigned-target-index-and-type (map get-source-&-assign-target source-idx-seq)
         source-seq-user-fn-applied (f assigned-target-index-and-type)
         partitioned-docs (partition-all batch-size source-seq-user-fn-applied)]
     (doseq [doc-batch partitioned-docs
             :let [bulk-batch (esb/bulk-create doc-batch)]]
       (esb/bulk-with-index-and-type pool target-index-name target-mapping-type bulk-batch)))))
