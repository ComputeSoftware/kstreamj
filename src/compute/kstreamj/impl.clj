(ns compute.kstreamj.impl
  (:require [clojure.zip :as zip]
            [compute.kstreamj.dsl :as dsl]
            [compute.kstreamj.util :as u])
  (:import (org.apache.kafka.streams StreamsBuilder)
           (org.apache.kafka.streams.kstream KStream Predicate Windows SessionWindows KGroupedStream TimeWindowedKStream SessionWindowedKStream KTable KGroupedTable)))

(declare ->StreamImpl)
(declare ->GroupedStreamImpl)
(declare ->TimeWindowedStreamImpl)
(declare ->SessionWindowedStreamImpl)
(declare ->TableImpl)
(declare ->GroupedTableImpl)

(defrecord StreamsBuilderImpl [builder]
  dsl/StreamsBuilder
  (stream [_ topics]
    (->StreamImpl builder
                  (.stream builder (if (string? topics) topics (vec topics)))))
  (add-state-store [this supplier]
    (.addStateStore builder supplier)
    this))

(deftype StreamImpl [^StreamsBuilder builder ^KStream stream]
  dsl/Stream
  (filter [_ pred]
    (->StreamImpl builder
                  (.filter stream (u/->predicate pred))))
  (filter-not [_ pred]
    (->StreamImpl builder
                  (.filterNot stream (u/->predicate pred))))
  (map [_ kvm]
    (->StreamImpl builder
                  (.map stream (u/->key-value-mapper kvm))))
  (map-values [_ vm]
    (->StreamImpl builder
                  (.mapValues stream (u/->value-mapper vm))))
  (flat-map [_ kvm]
    (->StreamImpl builder
                  (.flatMap stream (u/->key-value-mapper kvm))))
  (flat-map-values [_ vm]
    (->StreamImpl builder
                  (.flatMapValues stream (u/->value-mapper vm))))
  (print [_ printed]
    (.print stream printed)
    nil)
  (foreach [_ action]
    (.forEach stream (u/->for-each-action action))
    nil)
  (peek [this action]
    (->StreamImpl builder
                  (.peek stream (u/->for-each-action action))))
  (branch [_ preds]
    (mapv #(->StreamImpl builder %) (.branch stream (into-array Predicate (map u/->predicate preds)))))
  (merge [_ s]
    (->StreamImpl builder
                  (.merge stream s)))
  (through [_ topic]
    (->StreamImpl builder
                  (.through stream topic)))
  (through [_ topic produced]
    (->StreamImpl builder
                  (.through stream topic produced)))
  (to [_ topic]
    (->StreamImpl builder
                  (.to stream topic)))
  (to [_ topic produced]
    (->StreamImpl builder
                  (.to stream topic produced)))
  (transform [this ts]
    (dsl/transform this ts []))
  (transform [_ ts store-names]
    (->StreamImpl builder
                  (.transform stream (u/->transformer-supplier ts) (into-array String store-names))))
  (transform-values [this vts]
    (dsl/transform-values this vts []))
  (transform-values [_ vts store-names]
    (->StreamImpl builder
                  (.transformValues stream (u/->value-transformer-supplier vts) (into-array String store-names))))
  (process [this ps]
    (dsl/process this ps []))
  (process [_ ps store-names]
    (->StreamImpl builder
                  (.process stream (u/->processor-supplier ps) (into-array String store-names))))
  (group-by-key [_]
    (->GroupedStreamImpl builder
                         (.groupByKey stream)))
  (group-by-key [_ serialized]
    (->GroupedStreamImpl builder
                         (.groupByKey stream serialized)))
  (group-by [_ kvm]
    (->GroupedStreamImpl builder
                         (.groupBy stream (u/->key-value-mapper kvm))))
  (group-by [_ kvm serialized]
    (->GroupedStreamImpl builder
                         (.groupBy stream (u/->key-value-mapper kvm) serialized)))
  (join [this stream-or-table value-joiner join-windows]
    (throw (UnsupportedOperationException.)))
  (join [this stream-or-table value-joiner join-windows joined]
    (throw (UnsupportedOperationException.)))
  (left-join [this stream-or-table value-joiner join-windows]
    (throw (UnsupportedOperationException.)))
  (left-join [this stream-or-table value-joiner join-windows joined]
    (throw (UnsupportedOperationException.)))
  (outer-join [this stream- value-joiner join-windows]
    (throw (UnsupportedOperationException.)))
  (outer-join [this stream value-joiner join-windows joined]
    (throw (UnsupportedOperationException.))))

(deftype GroupedStreamImpl [builder ^KGroupedStream stream]
  dsl/GroupedStream
  (count [_]
    (->TableImpl builder
                 (.count stream)))
  (count [_ materialized]
    (->TableImpl builder
                 (.count stream materialized)))
  (reduce [_ r]
    (->TableImpl builder
                 (.reduce stream (u/->reducer r))))
  (reduce [_ r materialized]
    (->TableImpl builder
                 (.reduce stream (u/->reducer r) materialized)))
  (aggregate [_ init agg]
    (->TableImpl builder
                 (.aggregate stream (u/->initializer init) (u/->aggregator agg))))
  (aggregate [_ init agg materialized]
    (->TableImpl builder
                 (.aggregate stream (u/->initializer init) (u/->aggregator agg) materialized)))
  (windowed-by [_ windows]
    (cond
      (instance? Windows windows)
      (->TimeWindowedStreamImpl builder
                                (.windowedBy stream windows))

      (instance? SessionWindows windows)
      (->SessionWindowedStreamImpl builder
                                   (.windowedBy stream windows)))))

(deftype TimeWindowedStreamImpl [builder ^TimeWindowedKStream stream]
  dsl/TimeWindowedStream
  (count [_]
    (->TableImpl builder
                 (.count stream)))
  (count [_ materialized]
    (->TableImpl builder
                 (.count stream materialized)))
  (reduce [_ r]
    (->TableImpl builder
                 (.reduce stream (u/->reducer r))))
  (reduce [_ r materialized]
    (->TableImpl builder
                 (.reduce stream (u/->reducer r) materialized)))
  (aggregate [_ init agg]
    (->TableImpl builder
                 (.aggregate stream (u/->initializer init) (u/->aggregator agg))))
  (aggregate [_ init agg materialized]
    (->TableImpl builder
                 (.aggregate stream (u/->initializer init) (u/->aggregator agg) materialized))))

(deftype SessionWindowedStreamImpl [builder ^SessionWindowedKStream stream]
  dsl/SessionWindowedStream
  (count [_]
    (->TableImpl builder
                 (.count stream)))
  (count [_ materialized]
    (->TableImpl builder
                 (.count stream materialized)))
  (reduce [_ r]
    (->TableImpl builder
                 (.reduce stream (u/->reducer r))))
  (reduce [_ r materialized]
    (->TableImpl builder
                 (.reduce stream (u/->reducer r) materialized)))
  (aggregate [_ init merg agg]
    (->TableImpl builder
                 (.aggregate stream (u/->initializer init) (u/->aggregator agg) (u/->merger merg))))
  (aggregate [_ init agg merg materialized]
    (->TableImpl builder
                 (.aggregate stream (u/->initializer init) (u/->aggregator agg) (u/->merger merg) materialized))))

(deftype TableImpl [builder ^KTable table]
  dsl/Table
  (filter [_ pred]
    (->TableImpl builder
                 (.filter table (u/->predicate pred))))
  (filter [_ pred materialized]
    (->TableImpl builder
                 (.filter table (u/->predicate pred) materialized)))
  (filter-not [_ pred]
    (->TableImpl builder
                 (.filterNot table (u/->predicate pred))))
  (filter-not [_ pred materialized]
    (->TableImpl builder
                 (.filter table (u/->predicate pred) materialized)))
  (map-values [_ vm]
    (->TableImpl builder
                 (.mapValues table (u/->value-mapper vm))))
  (map-values [_ vm materialized]
    (->TableImpl builder
                 (.mapValues table (u/->value-mapper vm) materialized)))
  (to-stream [_]
    (->StreamImpl builder
                  (.toStream table)))
  (to-stream [_ kvm]
    (->StreamImpl builder
                  (.toStream table (u/->key-value-mapper kvm))))
  (transform-values [_ vts store-names]
    (->TableImpl builder
                 (.transformValues table (u/->value-transformer-with-key-supplier vts) (into-array String store-names))))
  (transform-values [_ vts store-names materialized]
    (->TableImpl builder
                 (.transformValues table (u/->value-transformer-with-key-supplier vts) (into-array String store-names) materialized)))
  (group-by [_ kvm]
    (->GroupedTableImpl builder
                         (.groupBy table (u/->key-value-mapper kvm))))
  (group-by [_ kvm serialized]
    (->GroupedTableImpl builder
                        (.groupBy table (u/->key-value-mapper kvm) serialized)))
  (join [this table value-joiner]
    (throw (UnsupportedOperationException.)))
  (join [this table value-joiner materialized]
    (throw (UnsupportedOperationException.)))
  (left-join [this table value-joiner]
    (throw (UnsupportedOperationException.)))
  (left-join [this table value-joiner materialized]
    (throw (UnsupportedOperationException.)))
  (outer-join [this table value-joiner]
    (throw (UnsupportedOperationException.)))
  (outer-join [this table value-joiner materialized]
    (throw (UnsupportedOperationException.))))

(deftype GroupedTableImpl [builder ^KGroupedTable table]
  dsl/GroupedTable
  (count [_]
    (->TableImpl builder
                 (.count table)))
  (count [_ materialized]
    (->TableImpl builder
                 (.count table materialized)))
  (reduce [_ r-add r-sub]
    (->TableImpl builder
                 (.reduce table (u/->reducer r-add) (u/->reducer r-sub))))
  (reduce [_ r-add r-sub materialized]
    (->TableImpl builder
                 (.reduce table (u/->reducer r-add) (u/->reducer r-sub) materialized)))
  (aggregate [_ init agg-add agg-sub]
    (->TableImpl builder
                 (.aggregate table (u/->initializer init) (u/->aggregator agg-add) (u/->aggregator agg-sub))))
  (aggregate [_ init agg-add agg-sub materialized]
    (->TableImpl builder
                 (.aggregate table (u/->initializer init) (u/->aggregator agg-add) (u/->aggregator agg-sub) materialized))))

(defn builder
  []
  (->StreamsBuilderClj (StreamsBuilder.)))