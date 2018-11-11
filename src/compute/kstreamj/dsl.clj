(ns compute.kstreamj.dsl
  (:require [compute.kstreamj.util :as u])
  (:import (org.apache.kafka.streams StreamsBuilder)
           (org.apache.kafka.streams.kstream KStream Predicate Windows SessionWindows KGroupedStream
                                             TimeWindowedKStream SessionWindowedKStream KTable KGroupedTable)))

(defmulti add-state-store! (fn [this & args] [(type this) (inc (count args))]))
(defmethod add-state-store! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti aggregate! (fn [this & args] [(type this) (inc (count args))]))
(defmethod aggregate! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti branch! (fn [this & args] [(type this) (inc (count args))]))
(defmethod branch! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti count! (fn [this & args] [(type this) (inc (count args))]))
(defmethod count! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti filter! (fn [this & args] [(type this) (inc (count args))]))
(defmethod filter! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti filter-not! (fn [this & args] [(type this) (inc (count args))]))
(defmethod filter-not! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti flat-map! (fn [this & args] [(type this) (inc (count args))]))
(defmethod flat-map! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti flat-map-values! (fn [this & args] [(type this) (inc (count args))]))
(defmethod flat-map-values! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti foreach! (fn [this & args] [(type this) (inc (count args))]))
(defmethod foreach! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti group-by! (fn [this & args] [(type this) (inc (count args))]))
(defmethod group-by! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti group-by-key! (fn [this & args] [(type this) (inc (count args))]))
(defmethod group-by-key! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti join! (fn [this & args] [(type this) (inc (count args))]))
(defmethod join! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti left-join! (fn [this & args] [(type this) (inc (count args))]))
(defmethod left-join! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti map! (fn [this & args] [(type this) (inc (count args))]))
(defmethod map! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti map-values! (fn [this & args] [(type this) (inc (count args))]))
(defmethod map-values! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti merge! (fn [this & args] [(type this) (inc (count args))]))
(defmethod merge! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti outer-join! (fn [this & args] [(type this) (inc (count args))]))
(defmethod outer-join! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti peek! (fn [this & args] [(type this) (inc (count args))]))
(defmethod peek! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti print! (fn [this & args] [(type this) (inc (count args))]))
(defmethod print! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti process! (fn [this & args] [(type this) (inc (count args))]))
(defmethod process! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti reduce! (fn [this & args] [(type this) (inc (count args))]))
(defmethod reduce! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti stream! (fn [this & args] [(type this) (inc (count args))]))
(defmethod stream! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti through! (fn [this & args] [(type this) (inc (count args))]))
(defmethod through! :default
  [this & args]
  (throw (UnsupportedOperationException.)))

(defmulti to! (fn [this & args] [(type this) (inc (count args))]))
(defmethod to! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti to-stream! (fn [this & args] [(type this) (inc (count args))]))
(defmethod to-stream! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti transform! (fn [this & args] [(type this) (inc (count args))]))
(defmethod transform! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti transform-values! (fn [this & args] [(type this) (inc (count args))]))
(defmethod transform-values! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti windowed-by! (fn [this & args] [(type this) (inc (count args))]))
(defmethod windowed-by! :default
  [& _]
  (throw (UnsupportedOperationException.)))

;;; StreamsBuilder

(defmethod stream! [StreamsBuilder 2]
  [builder topics]
  (.stream builder (if (string? topics) topics (vec topics))))

(defmethod add-state-store! [StreamsBuilder 2]
  [builder supplier]
  (.addStateStore builder supplier))

;;; KStream

(defmethod filter! [KStream 2]
  [stream pred]
  (.filter stream (u/->predicate pred)))

(defmethod filter-not! [KStream 2]
  [stream pred]
  (.filterNot stream (u/->predicate pred)))

(defmethod map! [KStream 2]
  [stream kvm]
  (.map stream (u/->key-value-mapper kvm)))

(defmethod map-values! [KStream 2]
  [stream vm]
  (.mapValues stream (u/->value-mapper vm)))

(defmethod flat-map! [KStream 2]
  [stream kvm]
  (.flatMap stream (u/->key-value-mapper kvm)))

(defmethod flat-map-values! [KStream 2]
  [stream vm]
  (.flatMapValues stream (u/->value-mapper vm)))

(defmethod print! [KStream 2]
  [stream printed]
  (.print stream printed))

(defmethod foreach! [KStream 2]
  [stream action]
  (.foreach stream (u/->for-each-action action)))

(defmethod peek! [KStream 2]
  [stream action]
  (.peek stream (u/->for-each-action action)))

(defmethod branch! [KStream 2]
  [stream preds]
  (vec (.branch stream (into-array Predicate (map u/->predicate preds)))))

(defmethod merge! [KStream 2]
  [stream s]
  (.merge stream s))

(defmethod through! [KStream 2]
  [stream topic]
  (.through stream topic))

(defmethod to! [KStream 2]
  [stream topic]
  (.to stream topic))

(defmethod to! [KStream 3]
  [stream topic produced]
  (.to stream topic produced))

(defmethod transform! [KStream 2]
  [stream ts]
  (transform! stream ts []))

(defmethod transform! [KStream 3]
  [stream ts store-names]
  (.transform stream (u/->transformer-supplier ts) (into-array String store-names)))

(defmethod transform-values! [KStream 2]
  [stream vts]
  (transform-values! stream vts []))

(defmethod transform-values! [KStream 3]
  [stream vts store-names]
  (.transformValues stream (u/->value-transformer-supplier vts) (into-array String store-names)))

(defmethod process! [KStream 2]
  [stream ps]
  (process! stream ps []))

(defmethod process! [KStream 3]
  [stream ps store-names]
  (.process stream (u/->processor-supplier ps) (into-array String store-names)))

(defmethod group-by-key! [KStream 1]
  [stream]
  (.groupByKey stream))

(defmethod group-by-key! [KStream 2]
  [stream serialized]
  (.groupByKey stream serialized))

(defmethod group-by! [KStream 2]
  [stream kvm]
  (.groupBy stream (u/->key-value-mapper kvm)))

(defmethod group-by! [KStream 3]
  [stream kvm serialized]
  (.groupBy stream (u/->key-value-mapper kvm) serialized))

(defmethod join! [KStream 4]
  [this stream-or-table value-joiner join-windows]
  (throw (UnsupportedOperationException. "Not implemented")))

(defmethod join! [KStream 5]
  [this stream-or-table value-joiner join-windows joined]
  (throw (UnsupportedOperationException. "Not implemented")))

(defmethod left-join! [KStream 4]
  [this stream-or-table value-joiner join-windows]
  (throw (UnsupportedOperationException. "Not implemented")))

(defmethod left-join! [KStream 5]
  [this stream-or-table value-joiner join-windows joined]
  (throw (UnsupportedOperationException. "Not implemented")))

(defmethod outer-join! [KStream 4]
  [this stream value-joiner join-windows]
  (throw (UnsupportedOperationException. "Not implemented")))

(defmethod outer-join! [KStream 5]
  [this stream value-joiner join-windows joined]
  (throw (UnsupportedOperationException. "Not implemented")))

;;; KGroupedStream

(defmethod count! [KGroupedStream 1]
  [stream]
  (.count stream))

(defmethod count! [KGroupedStream 2]
  [stream materialized]
  (.count stream materialized))

(defmethod reduce! [KGroupedStream 2]
  [stream r]
  (.reduce stream (u/->reducer r)))

(defmethod reduce! [KGroupedStream 3]
  [stream r materialized]
  (.reduce stream (u/->reducer r) materialized))

(defmethod aggregate! [KGroupedStream 3]
  [stream init agg]
  (.aggregate stream (u/->initializer init) (u/->aggregator agg)))

(defmethod aggregate! [KGroupedStream 3]
  [stream init agg materialized]
  (.aggregate stream (u/->initializer init) (u/->aggregator agg) materialized))

(defmethod windowed-by! [KGroupedStream 2]
  [stream windows]
  (.windowedBy stream windows))

;;; TimeWindowedKStream

(defmethod count! [TimeWindowedKStream 1]
  [stream]
  (.count stream))

(defmethod count! [TimeWindowedKStream 2]
  [stream materialized]
  (.count stream materialized))

(defmethod reduce! [TimeWindowedKStream 2]
  [stream r]
  (.reduce stream (u/->reducer r)))

(defmethod reduce! [TimeWindowedKStream 3]
  [stream r materialized]
  (.reduce stream (u/->reducer r) materialized))

(defmethod aggregate! [TimeWindowedKStream 3]
  [stream init agg]
  (.aggregate stream (u/->initializer init) (u/->aggregator agg)))

(defmethod aggregate! [TimeWindowedKStream 4]
  [stream init agg materialized]
  (.aggregate stream (u/->initializer init) (u/->aggregator agg) materialized))

;;; SessionWindowedKStream

(defmethod count! [SessionWindowedKStream 1]
  [stream]
  (.count stream))

(defmethod count! [SessionWindowedKStream 2]
  [stream materialized]
  (.count stream materialized))

(defmethod reduce! [SessionWindowedKStream 2]
  [stream r]
  (.reduce stream (u/->reducer r)))

(defmethod reduce! [SessionWindowedKStream 3]
  [stream r materialized]
  (.reduce stream (u/->reducer r) materialized))

(defmethod aggregate! [SessionWindowedKStream 4]
  [stream init merg agg]
  (.aggregate stream (u/->initializer init) (u/->aggregator agg) (u/->merger merg)))

(defmethod aggregate! [SessionWindowedKStream 5]
  [stream init agg merg materialized]
  (.aggregate stream (u/->initializer init) (u/->aggregator agg) (u/->merger merg) materialized))

;;; KTable

(defmethod filter! [KTable 2]
  [table pred]
  (.filter table (u/->predicate pred)))

(defmethod filter! [KTable 3]
  [table pred materialized]
  (.filter table (u/->predicate pred) materialized))

(defmethod filter-not! [KTable 2]
  [table pred]
  (.filterNot table (u/->predicate pred)))

(defmethod filter-not! [KTable 3]
  [table pred materialized]
  (.filterNot table (u/->predicate pred) materialized))

(defmethod map-values! [KTable 2]
  [table vm]
  (.mapValues table (u/->value-mapper vm)))

(defmethod map-values! [KTable 3]
  [table vm materialized]
  (.mapValues table (u/->value-mapper vm) materialized))

(defmethod to-stream! [KTable 1]
  [table]
  (.toStream table))

(defmethod to-stream! [KTable 2]
  [table kvm]
  (.toStream table (u/->key-value-mapper kvm)))

(defmethod transform-values! [KTable 3]
  [table vts store-names]
  (.transformValues table (u/->value-transformer-with-key-supplier vts) (into-array String store-names)))

(defmethod transform-values! [KTable 4]
  [table vts store-names materialized]
  (.transformValues table (u/->value-transformer-with-key-supplier vts) (into-array String store-names)))

(defmethod group-by! [KTable 2]
  [table kvm]
  (.groupBy table (u/->key-value-mapper kvm)))

(defmethod group-by! [KTable 3]
  [table kvm serialized]
  (.groupBy table (u/->key-value-mapper kvm) serialized))

(defmethod join! [KTable 3]
  [this table value-joiner]
  (throw (UnsupportedOperationException.)))

(defmethod join! [KTable 4]
  [this table value-joiner materialized]
  (throw (UnsupportedOperationException.)))

(defmethod left-join! [KTable 3]
  [this table value-joiner]
  (throw (UnsupportedOperationException.)))

(defmethod left-join! [KTable 4]
  [this table value-joiner materialized]
  (throw (UnsupportedOperationException.)))

(defmethod outer-join! [KTable 3]
  [this table value-joiner]
  (throw (UnsupportedOperationException.)))

(defmethod outer-join! [KTable 4]
  [this table value-joiner materialized]
  (throw (UnsupportedOperationException.)))

;;; KGroupedTable

(defmethod count! [KGroupedTable 1]
  [table]
  (.count table))

(defmethod count! [KGroupedTable 2]
  [table materialized]
  (.count table materialized))

(defmethod reduce! [KGroupedTable 3]
  [table r-add r-sub]
  (.reduce table (u/->reducer r-add) (u/->reducer r-sub)))

(defmethod reduce! [KGroupedTable 4]
  [table r-add r-sub materialized]
  (.reduce table (u/->reducer r-add) (u/->reducer r-sub) materialized))

(defmethod aggregate! [KGroupedTable 4]
  [table init agg-add agg-sub]
  (.aggregate table (u/->initializer init) (u/->aggregator agg-add) (u/->aggregator agg-sub)))

(defmethod aggregate! [KGroupedTable 5]
  [table init agg-add agg-sub materialized]
  (.aggregate table (u/->initializer init) (u/->aggregator agg-add) (u/->aggregator agg-sub) materialized))