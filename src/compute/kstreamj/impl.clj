(ns compute.kstreamj.impl
  (:require [compute.kstreamj.dsl :as dsl]
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

(defrecord StreamImpl [^StreamsBuilder builder ^KStream stream]
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
    (.peek stream (u/->for-each-action action))
    this)
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

(defrecord GroupedStreamImpl [builder ^KGroupedStream stream]
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

(defrecord TimeWindowedStreamImpl [builder ^TimeWindowedKStream stream]
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

(defrecord SessionWindowedStreamImpl [builder ^SessionWindowedKStream stream]
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

(defrecord TableImpl [builder ^KTable table]
  dsl/Table
  (filter [this pred])
  (filter [this pred materialized])
  (filter-not [this pred])
  (filter-not [this pred materialized])
  (map-values [this vm])
  (map-values [this vm materialized])
  (to-stream [this])
  (to-stream [this kvm])
  (transform-values [this vts])
  (transform-values [this vts materialized])
  (join [this table value-joiner])
  (join [this table value-joiner materialized])
  (left-join [this table value-joiner])
  (left-join [this table value-joiner materialized])
  (outer-join [this table value-joiner])
  (outer-join [this table value-joiner materialized]))

(defrecord GroupedTableImpl [builder ^KGroupedTable table]
  dsl/GroupedTable
  (count [this])
  (count [this materialized])
  (reduce [this r-add r-sub])
  (reduce [this r-add r-sub materialized])
  (aggregate [this init agg-add agg-sub])
  (aggregate [this init agg-add agg-sub materialized]))

(defn builder
  []
  (->StreamsBuilderClj (StreamsBuilder.)))