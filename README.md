```
 _________  __  __   ___ __ __   ________   __       ______      
/________/\/_/\/_/\ /__//_//_/\ /_______/\ /_/\     /_____/\     
\__.::.__\/\:\ \:\ \\::\| \| \ \\::: _  \ \\:\ \    \:::_ \ \    
   \::\ \   \:\ \:\ \\:.      \ \\::(_)  \ \\:\ \    \:\ \ \ \   
    \::\ \   \:\ \:\ \\:.\-/\  \ \\:: __  \ \\:\ \____\:\ \ \ \  
     \::\ \   \:\_\:\ \\. \  \  \ \\:.\ \  \ \\:\/___/\\:\_\ \ \ 
      \__\/    \_____\/ \__\/ \__\/ \__\/\__\/ \_____\/ \_____\/ 
```
## Elasticsearch Backfill Tools

This library is intended to be a small set of helpers for backfilling from one Elasticsearch (ES) index to another. While Elasticsearch is starting to introduce reindexing APIs for simple index-to-index transfers, there are many cases where you may want to transform the data from one index to another. Enter tumalo. 

### Reindexing

You can reindex from one index to another without applying any transformation:

```clojure
(require '[tumalo.es :as tes])
(require '[clojurewerkz.elastisch.rest :as esr])

;; You'll need an HTTP connection or connection pool to connect with ES
(def url "http://localhost:9200")
(def conn-manager (clj-http.conn-mgr/make-reusable-conn-manager
                    {:timeout 1 :threads 4 :insecure? false}))
(def pool (esr/connect url {:connection-manager conn-manager 
                            :conn-timeout 3000 
                            :socket-timeout 3000}))

(def batch-size 200)
(tes/reindex-docs pool
                  "source_index"
                  "source_mapping"
                  "target_index"
                  "target_mapping"
                  batch-size)
```

You can also provide a function that will transform from one index mapping type to another. The only contract for the function is that it accepts a sequence of maps that will always contain `_index`, `_type`, and `_id` keys and return a sequence of maps that contain `_index` and `_type` keys. If `_id` is omitted, new `_id` fields will be generated in the new index. 

Let's say that `bird_index` uses the following mapping type: 
```
  {:birds
   {:dynamic "strict"
    :properties {"uuid"          {:type "string" :index "not_analyzed" :doc_values true}
                 "when_recorded" {:type "date"}
                 "bird"          {:type "string"}
                 "count"         {:type "long"}}}}
```

and we want to reindex to a new mapping in a new index: 
```
  {:animals
   {:dynamic "strict"
    :properties {"uuid"          {:type "string" :index "not_analyzed" :doc_values true}
                 "when_received" {:type "date"}
                 "animal_type"   {:type "string"}
                 "animal_name"   {:type "string"}
                 "animal_count"  {:type "long"}}}}
```

We need a transformation fn to map from one index mapping type to the other: 

```clojure
(defn birds-to-animals
  [doc-seq]
    (map #(identity {:_index (:_index %)
                     :_type (:_type %)
                     :_id (:_id %)
                     :uuid (:uuid %)
                     :when_received (:when_recorded %)
                     :animal_type "bird"
                     :animal_name (:bird %)
                     :animal_count (:count %)}) doc-seq))
```

Finally, we can pass this fn to `reindex-docs` and it will reindex from the `birds` index to the `animals` index:

```clojure
(tes/reindex-docs pool
                  "birds_index"
                  "birds"
                  "animals_index"
                  "animals"
                  batch-size
                  birds-to-animals)
```
