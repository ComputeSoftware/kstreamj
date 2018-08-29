(ns compute.kstreamj.dsl
  (:require [compute.kstreamj.util :as u])
  (:import (org.apache.kafka.streams StreamsBuilder)
           (org.apache.kafka.streams.kstream KStream Predicate Windows SessionWindows KGroupedStream
                                             TimeWindowedKStream SessionWindowedKStream KTable KGroupedTable)))

(defmulti add-state-store! (fn [this & args] (type this)))
(defmethod add-state-store! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti aggregate! (fn [this & args] (type this)))
(defmethod aggregate! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti branch! (fn [this & args] (type this)))
(defmethod branch! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti count! (fn [this & args] (type this)))
(defmethod count! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti filter! (fn [this & args] (type this)))
(defmethod filter! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti filter-not! (fn [this & args] (type this)))
(defmethod filter-not! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti flat-map! (fn [this & args] (type this)))
(defmethod flat-map! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti flat-map-values! (fn [this & args] (type this)))
(defmethod flat-map-values! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti foreach! (fn [this & args] (type this)))
(defmethod foreach! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti group-by! (fn [this & args] (type this)))
(defmethod group-by! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti group-by-key! (fn [this & args] (type this)))
(defmethod group-by-key! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti join! (fn [this & args] (type this)))
(defmethod join! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti left-join! (fn [this & args] (type this)))
(defmethod left-join! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti map! (fn [this & args] (type this)))
(defmethod map! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti map-values! (fn [this & args] (type this)))
(defmethod map-values! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti merge! (fn [this & args] (type this)))
(defmethod merge! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti outer-join! (fn [this & args] (type this)))
(defmethod outer-join! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti peek! (fn [this & args] (type this)))
(defmethod peek! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti print! (fn [this & args] (type this)))
(defmethod print! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti process! (fn [this & args] (type this)))
(defmethod process! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti reduce! (fn [this & args] (type this)))
(defmethod reduce! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti stream! (fn [this & args] (type this)))
(defmethod stream! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti through! (fn [this & args] (type this)))
(defmethod through! :default
  [this & args]
  (throw (UnsupportedOperationException.)))

(defmulti to! (fn [this & args] (type this)))
(defmethod to! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti to-stream! (fn [this & args] (type this)))
(defmethod to-stream! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti transform! (fn [this & args] (type this)))
(defmethod transform! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti transform-values! (fn [this & args] (type this)))
(defmethod transform-values! :default
  [& _]
  (throw (UnsupportedOperationException.)))

(defmulti windowed-by! (fn [this & args] (type this)))
(defmethod windowed-by! :default
  [& _]
  (throw (UnsupportedOperationException.)))

;;; StreamsBuilder

(defmethod stream! StreamsBuilder
  [builder topics]
  (.stream builder (if (string? topics) topics (vec topics))))

(defmethod add-state-store! StreamsBuilder
  [builder supplier]
  (.addStateStore builder supplier))

;;; KStream

(defmethod filter! KStream
  [stream pred]
  (.filter stream (u/->predicate pred)))

(defmethod filter-not! KStream
  [stream pred]
  (.filterNot stream (u/->predicate pred)))

(defmethod map! KStream
  [stream kvm]
  (.map stream (u/->key-value-mapper kvm)))

(defmethod map-values! KStream
  [stream vm]
  (.mapValues stream (u/->value-mapper vm)))

(defmethod flat-map! KStream
  [stream kvm]
  (.flatMap stream (u/->key-value-mapper kvm)))

(defmethod flat-map-values! KStream
  [stream vm]
  (.flatMapValues stream (u/->value-mapper vm)))

(defmethod print! KStream
  [stream printed]
  (.print stream printed))

(defmethod foreach! KStream
  [stream action]
  (.forEach stream (u/->for-each-action action)))

(defmethod peek! KStream
  [stream action]
  (.peek stream (u/->for-each-action action)))

(defmethod branch! KStream
  [stream preds]
  (vec (.branch stream (into-array Predicate (map u/->predicate preds)))))

(defmethod merge! KStream
  [stream s]
  (.merge stream s))

(defmethod through! KStream
  [stream topic]
  (.through stream topic))

(defmethod to! KStream
  [stream topic]
  (.to stream topic))

(defmethod to! KStream
  [stream topic produced]
  (.to stream topic produced))

(defmethod transform! KStream
  [stream ts]
  (transform! stream ts []))

(defmethod transform! KStream
  [stream ts store-names]
  (.transform stream (u/->transformer-supplier ts) store-names))

(defmethod transform-values! KStream
  [stream vts]
  (transform-values! stream vts []))

(defmethod transform-values! KStream
  [stream vts store-names]
  (.transformValues stream (u/->value-transformer-supplier vts) (into-array String store-names)))

(defmethod process! KStream
  [stream ps]
  (process! stream ps []))

(defmethod process! KStream
  [stream ps store-names]
  (.process stream (u/->processor-supplier ps) (into-array String store-names)))

(defmethod group-by-key! KStream
  [stream]
  (.groupByKey stream))

(defmethod group-by-key! KStream
  [stream serialized]
  (.groupByKey stream serialized))

(defmethod group-by! KStream
  [stream kvm]
  (.groupBy stream (u/->key-value-mapper kvm)))

(defmethod group-by! KStream
  [stream kvm serialized]
  (.groupBy stream (u/->key-value-mapper kvm) serialized))

(defmethod join! KStream
  [this stream-or-table value-joiner join-windows]
  (throw (UnsupportedOperationException. "Not implemented")))

(defmethod join! KStream
  [this stream-or-table value-joiner join-windows joined]
  (throw (UnsupportedOperationException. "Not implemented")))

(defmethod left-join! KStream
  [this stream-or-table value-joiner join-windows]
  (throw (UnsupportedOperationException. "Not implemented")))

(defmethod left-join! KStream
  [this stream-or-table value-joiner join-windows joined]
  (throw (UnsupportedOperationException. "Not implemented")))

(defmethod outer-join! KStream
  [this stream value-joiner join-windows]
  (throw (UnsupportedOperationException. "Not implemented")))

(defmethod outer-join! KStream
  [this stream value-joiner join-windows joined]
  (throw (UnsupportedOperationException. "Not implemented")))

;;; KGroupedStream

(defmethod count! KGroupedStream
  [stream]
  (.count stream))

(defmethod count! KGroupedStream
  [stream materialized]
  (.count stream materialized))

(defmethod reduce! KGroupedStream
  [stream r]
  (.reduce stream (u/->reducer r)))

(defmethod reduce! KGroupedStream
  [stream r materialized]
  (.reduce stream (u/->reducer r) materialized))

(defmethod aggregate! KGroupedStream
  [stream init agg]
  (.aggregate stream (u/->initializer init) (u/->aggregator agg)))

(defmethod aggregate! KGroupedStream
  [stream init agg materialized]
  (.aggregate stream (u/->initializer init) (u/->aggregator agg) materialized))

(defmethod windowed-by! KGroupedStream
  [stream windows]
  (.windowedBy stream windows))

;;; TimeWindowedKStream

(defmethod count! TimeWindowedKStream
  [stream]
  (.count stream))

(defmethod count! TimeWindowedKStream
  [stream materialized]
  (.count stream materialized))

(defmethod reduce! TimeWindowedKStream
  [stream r]
  (.reduce stream (u/->reducer r)))

(defmethod reduce! TimeWindowedKStream
  [stream r materialized]
  (.reduce stream (u/->reducer r) materialized))

(defmethod aggregate! TimeWindowedKStream
  [stream init agg]
  (.aggregate stream (u/->initializer init) (u/->aggregator agg)))

(defmethod aggregate! TimeWindowedKStream
  [stream init agg materialized]
  (.aggregate stream (u/->initializer init) (u/->aggregator agg) materialized))

;;; SessionWindowedKStream

(defmethod count! SessionWindowedKStream
  [stream]
  (.count stream))

(defmethod count! SessionWindowedKStream
  [stream materialized]
  (.count stream materialized))

(defmethod reduce! SessionWindowedKStream
  [stream r]
  (.reduce stream (u/->reducer r)))

(defmethod reduce! SessionWindowedKStream
  [stream r materialized]
  (.reduce stream (u/->reducer r) materialized))

(defmethod aggregate! SessionWindowedKStream
  [stream init merg agg]
  (.aggregate stream (u/->initializer init) (u/->aggregator agg) (u/->merger merg)))

(defmethod aggregate! SessionWindowedKStream
  [stream init agg merg materialized]
  (.aggregate stream (u/->initializer init) (u/->aggregator agg) (u/->merger merg) materialized))

;;; KTable

(defmethod filter! KTable
  [table pred]
  (.filter table (u/->predicate pred)))

(defmethod filter! KTable
  [table pred materialized]
  (.filter table (u/->predicate pred) materialized))

(defmethod filter-not! KTable
  [table pred]
  (.filterNot table (u/->predicate pred)))

(defmethod filter-not! KTable
  [table pred materialized]
  (.filterNot table (u/->predicate pred) materialized))

(defmethod map-values! KTable
  [table vm]
  (.mapValues table (u/->value-mapper vm)))

(defmethod map-values! KTable
  [table vm materialized]
  (.mapValues table (u/->value-mapper vm) materialized))

(defmethod to-stream! KTable
  [table]
  (.toStream table))

(defmethod to-stream! KTable
  [table kvm]
  (.toStream table (u/->key-value-mapper kvm)))

(defmethod transform-values! KTable
  [table vts store-names]
  (.transformValues table (u/->value-transformer-with-key-supplier vts) (into-array String store-names)))

(defmethod transform-values! KTable
  [table vts store-names materialized]
  (.transformValues table (u/->value-transformer-with-key-supplier vts) (into-array String store-names)))

(defmethod group-by! KTable
  [table kvm]
  (.groupBy table (u/->key-value-mapper kvm)))

(defmethod group-by! KTable
  [table kvm serialized]
  (.groupBy table (u/->key-value-mapper kvm) serialized))

(defmethod join! KTable
  [this table value-joiner]
  (throw (UnsupportedOperationException.)))

(defmethod join! KTable
  [this table value-joiner materialized]
  (throw (UnsupportedOperationException.)))

(defmethod left-join! KTable
  [this table value-joiner]
  (throw (UnsupportedOperationException.)))

(defmethod left-join! KTable
  [this table value-joiner materialized]
  (throw (UnsupportedOperationException.)))

(defmethod outer-join! KTable
  [this table value-joiner]
  (throw (UnsupportedOperationException.)))

(defmethod outer-join! KTable
  [this table value-joiner materialized]
  (throw (UnsupportedOperationException.)))

;;; KGroupedTable

(defmethod count! KGroupedTable
  [table]
  (.count table))

(defmethod count! KGroupedTable
  [table materialized]
  (.count table materialized))

(defmethod reduce! KGroupedTable
  [table r-add r-sub]
  (.reduce table (u/->reducer r-add) (u/->reducer r-sub)))

(defmethod reduce! KGroupedTable
  [table r-add r-sub materialized]
  (.reduce table (u/->reducer r-add) (u/->reducer r-sub) materialized))

(defmethod aggregate! KGroupedTable
  [table init agg-add agg-sub]
  (.aggregate table (u/->initializer init) (u/->aggregator agg-add) (u/->aggregator agg-sub)))

(defmethod aggregate! KGroupedTable
  [table init agg-add agg-sub materialized]
  (.aggregate table (u/->initializer init) (u/->aggregator agg-add) (u/->aggregator agg-sub) materialized))