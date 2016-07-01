(ns tumalo.es
  (:require [clojure.tools.logging :refer [log logf]]
            [clojure.core.async :refer [chan thread close! >!! <!!]]
            [amazonica.aws.s3 :as s3]
            [clojurewerkz.elastisch.rest.bulk :as esb]
            [clojurewerkz.elastisch.rest.document :as esd]
            [clojurewerkz.elastisch.rest :as esr]
            [schema.core :as s]
            [tumalo.schemas :as ts])
  (:import [java.util HashMap]
           [clojurewerkz.elastisch.rest Connection]
           [clojure.core.async.impl.channels ManyToManyChannel]))

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
;;; Bulk Writes

(s/defn bulk-write-seq
  "Given a sequence of documents prepared for bulk indexing, e.g. they contain :_index,
  :_type, and :_id, partition the sequence into `batch-size` batches and bulk write to ES"
  [pool :- Connection
   target-index-name :- s/Str
   target-mapping-type :- s/Str
   docs :- [{s/Keyword s/Any}]
   batch-size :- s/Num]
  (let [partitioned-docs (partition-all batch-size docs)]
    (doseq [doc-batch partitioned-docs
            :let [bulk-batch (esb/bulk-index doc-batch)]]
      (esb/bulk-with-index-and-type pool target-index-name target-mapping-type bulk-batch))))


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

   The only contract for `f` is that it returns a seq of maps with the _index and _type fields intact. The
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
         source-seq-user-fn-applied (f assigned-target-index-and-type)]
     (bulk-write-seq pool target-index-name target-mapping-type source-seq-user-fn-applied batch-size))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Indexing from S3

(defn reduce-objects-and-streams
  "Reducer: get the S3 object, process it, concat the returned values and streams to the :values and
  :streams sequences in the accumulating map"
  [processing-fn accum s3-object]
  (let [s3-object (s3/get-object (:bucket-name s3-object) (:key s3-object))
        processed-obj-and-streams (processing-fn s3-object)]
    {:values  (concat (:values accum) (:values processed-obj-and-streams))
     :streams (concat (:streams accum) (:streams processed-obj-and-streams))}))


(s/defn get-next-s3-object-batch!
  "Get next batch of S3 objects and put them on the `chan`"
  [s3-chan :- ManyToManyChannel
   last-object-batch :- {s/Keyword s/Any}
   processing-fn]
  (logf :info "Fetching next batch of %s objects from %s"
        (:max-keys last-object-batch) (:bucket-name last-object-batch))
  (let [object-summaries (:object-summaries last-object-batch)
        reducer (partial reduce-objects-and-streams processing-fn)
        processed-objects (reduce reducer [] object-summaries)]
    (log :info "S3 Objects fetched, putting on channel to ES writer!")
    (>!! s3-chan processed-objects)
    (if (:truncated? last-object-batch)
      (get-next-s3-object-batch! s3-chan
                                 (s3/list-next-batch-of-objects last-object-batch)
                                 processing-fn)
      (do
        (log :info "Finished fetching S3 data! Closing channel.")
        (close! s3-chan)))))

(s/defn index-from-s3
  [pool :- Connection
   bucket-name :- s/Str
   prefix :- s/Str
   target-index-name :- s/Str
   target-mapping-type :- s/Str
   es-batch-size :- s/Num
   s3-batch-size :- s/Num
   f]
  (let [s3-chan (chan 10)
        s3-first-batch (s3/list-objects :bucket-name bucket-name
                                        :prefix prefix
                                        :max-keys s3-batch-size)]
    (thread (get-next-s3-object-batch! s3-chan s3-first-batch f))
    (loop [batches-and-streams (<!! s3-chan)]
      (let [bulk-batch (:values batches-and-streams)
            streams (:streams batches-and-streams)]
        (if bulk-batch
          (do
            (logf :info "Writing batch to mapping %s for index %s"
                  target-mapping-type
                  target-index-name)
            (bulk-write-seq pool target-index-name target-mapping-type bulk-batch es-batch-size)
            (log :info "Closing S3 streams for batch...")
            (doseq [stream streams]
              (.close stream))
            (recur (<!! s3-chan)))
          (log :info "Finished processing data!"))))))
