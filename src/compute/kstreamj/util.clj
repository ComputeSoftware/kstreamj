(ns compute.kstreamj.util
  (:import (org.apache.kafka.streams.kstream KeyValueMapper ValueMapper Predicate ForeachAction TransformerSupplier ValueTransformerSupplier Transformer ValueTransformer Materialized Reducer Initializer Aggregator Merger)
           (org.apache.kafka.streams.processor Processor ProcessorSupplier)))

(defmacro reify-function-class
  [type args body]
  `(reify
     ~type
     (apply ~(vec (cons 'this# args))
       ~@body)))

(defmacro key-value-mapper
  ([f]
   `(reify
      KeyValueMapper
      (apply [this# k# v#] (~f k# v#))))
  ([args & body]
   (reify-function-class KeyValueMapper args body)))

(defmacro value-mapper
  ([f]
   `(reify
      ValueMapper
      (apply [this# v#] (~f v#))))
  ([args & body]
   (reify-function-class ValueMapper args body)))

(defmacro predicate
  ([f]
   `(reify
      Predicate
      (test [_ k v] (~f k v))))
  ([args & body]
   (reify-function-class Predicate args body)))

(defmacro for-each-action
  ([f]
   `(reify
      ForeachAction
      (apply [_ k v] (~f k v))))
  ([args & body]
   (reify-function-class ForeachAction args body)))

(defmacro reducer
  ([f]
   `(reify
      Reducer
      (apply [_ agg v] (~f agg v))))
  ([args & body]
   (reify-function-class Reducer args body)))

(defmacro initializer
  [f]
  `(reify
     Initializer
     (apply [_] (~f))))

(defmacro aggregator
  ([f]
   `(reify
      Aggregator
      (apply [_ k v agg] (~f k v agg))))
  ([args & body]
   (reify-function-class Aggregator args body)))

(defmacro merger
  ([f]
   `(reify
      Merger
      (apply [_ k v1 v2] (~f k v1 v2))))
  ([args & body]
   (reify-function-class Merger args body)))


(defmacro transformer-supplier
  [& body]
  (reify-function-class TransformerSupplier [] body))

(defmacro value-transformer-supplier
  [& body]
  (reify-function-class ValueTransformerSupplier [] body))

(defmacro processor-supplier
  [& body]
  (reify-function-class Processor [] body))

(defn- ->predicate
  [pred]
  (cond
    (ifn? pred) (predicate pred)
    (instance? Predicate pred) pred
    :else (throw (ex-info "Unknown predicate type" {:pred pred}))))

(defn ->key-value-mapper
  [kvm]
  (cond
    (ifn? kvm) (key-value-mapper kvm)
    (instance? KeyValueMapper kvm) kvm
    :else (throw (ex-info "Unknown key-value mapper type" {:kvm kvm}))))

(defn ->value-mapper
  [vm]
  (cond
    (ifn? vm) (value-mapper vm)
    (instance? ValueMapper vm) vm
    :else (throw (ex-info "Unknown value mapper type" {:vm vm}))))

(defn ->for-each-action
  [action]
  (cond
    (ifn? action) (for-each-action action)
    (instance? ForeachAction action) action
    :else (throw (ex-info "Unknown action type" {:action action}))))

(defn ->reducer
  [r]
  (cond
    (ifn? r) (reducer r)
    (instance? Reducer r) r
    :else (throw (ex-info "Unknown reducer type" {:r r}))))

(defn ->initializer
  [init]
  (cond
    (ifn? init) (initializer init)
    (instance? Initializer init) init
    :else (throw (ex-info "Unknown initializer type" {:init init}))))

(defn ->aggregator
  [agg]
  (cond
    (ifn? agg) (aggregator agg)
    (instance? Aggregator agg) agg
    :else (throw (ex-info "Unknown aggregator type" {:agg agg}))))

(defn ->merger
  [merg]
  (cond
    (ifn? merg) (merger merg)
    (instance? Merger merg) merg
    :else (throw (ex-info "Unknown merger type" {:merg merg}))))

(defn ->transformer-supplier
  [ts]
  (cond
    (ifn? ts) (transformer-supplier (ts))
    (instance? Transformer ts) (transformer-supplier ts)
    (instance? TransformerSupplier ts) ts
    :else (throw (ex-info "Unknown transformer supplier type" {:ts ts}))))

(defn ->value-transformer-supplier
  [vts]
  (cond
    (ifn? vts) (value-transformer-supplier (vts))
    (instance? ValueTransformer vts) (value-transformer-supplier vts)
    (instance? ValueTransformerSupplier vts) vts
    :else (throw (ex-info "Unknown value-transformer supplier type" {:vts vts}))))

(defn ->processor-supplier
  [ps]
  (cond
    (ifn? ps) (processor-supplier (ps))
    (instance? Processor ps) (processor-supplier ps)
    (instance? ProcessorSupplier ps) ps
    :else (throw (ex-info "Unknown processor supplier type" {:ps ps}))))

(defn ->materialized
  [m]
  (cond))
    
