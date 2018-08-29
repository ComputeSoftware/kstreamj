(ns compute.kstreamj.util
  (:import (org.apache.kafka.streams.kstream KeyValueMapper ValueMapper Predicate ForeachAction TransformerSupplier ValueTransformerSupplier Transformer ValueTransformer Materialized Reducer Initializer Aggregator Merger ValueMapperWithKey ValueTransformerWithKeySupplier ValueTransformerWithKey)
           (org.apache.kafka.streams.processor Processor ProcessorSupplier)))

(defn reify-function-class
  [type f-name args body]
  `(reify
     ~type
     (~f-name ~(vec (cons 'this# args))
       ~@body)))

(defmacro key-value-mapper
  ([f]
   `(reify
      KeyValueMapper
      (apply [this# k# v#] (~f k# v#))))
  ([args & body]
   (reify-function-class 'KeyValueMapper 'apply args body)))

(defmacro value-mapper
  ([f]
   `(reify
      ValueMapper
      (apply [this# v#] (~f v#))))
  ([args & body]
   (reify-function-class 'ValueMapper 'apply args body)))

(defmacro value-mapper-with-key
  ([f]
   `(reify
      ValueMapperWithKey
      (apply [this# k# v#] (~f k# v#))))
  ([args & body]
   (reify-function-class 'ValueMapperWithKey 'apply args body)))

(defmacro predicate
  ([f]
   `(reify
      Predicate
      (test [_this# k# v#] (~f k# v#))))
  ([args & body]
   (reify-function-class 'Predicate 'test args body)))

(defmacro for-each-action
  ([f]
   `(reify
      ForeachAction
      (apply [this# k# v#] (~f k# v#))))
  ([args & body]
   (reify-function-class 'ForeachAction 'apply args body)))

(defmacro reducer
  ([f]
   `(reify
      Reducer
      (apply [this# agg# v#] (~f agg# v#))))
  ([args & body]
   (reify-function-class 'Reducer 'apply args body)))

(defmacro initializer
  [f]
  `(reify
     Initializer
     (apply [this#] (~f))))

(defmacro aggregator
  ([f]
   `(reify
      Aggregator
      (apply [this# k# v# agg#] (~f k# v# agg#))))
  ([args & body]
   (reify-function-class 'Aggregator 'apply args body)))

(defmacro merger
  ([f]
   `(reify
      Merger
      (apply [this# k# v1# v2#] (~f k# v1# v2#))))
  ([args & body]
   (reify-function-class 'Merger 'apply args body)))

(defmacro transformer-supplier
  [& body]
  (reify-function-class 'TransformerSupplier 'get [] (vec body)))

(defmacro value-transformer-supplier
  [& body]
  (reify-function-class 'ValueTransformerSupplier 'get [] body))

(defmacro value-transformer-with-key-supplier
  [& body]
  (reify-function-class 'ValueTransformerWithKeySupplier 'get [] body))

(defmacro processor-supplier
  [& body]
  (reify-function-class 'ProcessorSupplier 'get [] body))

(defn ->predicate
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

(defn- arity
  "Returns the maximum arity of:
    - anonymous functions like `#()` and `(fn [])`.
    - defined functions like `map` or `+`.
    - macros, by passing a var like `#'->`.

  Returns `:variadic` if the function/macro is variadic."
  [f]
  (let [func (if (var? f) @f f)
        methods (->> func class .getDeclaredMethods
                     (map #(vector (.getName %)
                                   (count (.getParameterTypes %)))))
        var-args? (some #(-> % first #{"getRequiredArity"})
                        methods)]
    (if var-args?
      :variadic
      (let [max-arity (->> methods
                           (filter (comp #{"invoke"} first))
                           (sort-by second)
                           last
                           second)]
        (if (and (var? f) (-> f meta :macro))
          (- max-arity 2) ;; substract implicit &form and &env arguments
          max-arity)))))

(defn ->value-mapper
  [vm]
  (cond
    (ifn? vm) (condp = (arity vm)
                     1 (value-mapper vm)
                     2 (value-mapper-with-key vm))
    (instance? ValueMapper vm) vm
    (instance? ValueMapperWithKey vm) vm
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

(defn ->value-transformer-with-key-supplier
  [vts]
  (cond
    (ifn? vts) (value-transformer-with-key-supplier (vts))
    (instance? ValueTransformerWithKey vts) (value-transformer-with-key-supplier vts)
    (instance? ValueTransformerWithKeySupplier vts) vts
    :else (throw (ex-info "Unknown value-transformer-with-key supplier type" {:vts vts}))))

(defn ->processor-supplier
  [ps]
  (cond
    (ifn? ps) (processor-supplier (ps))
    (instance? Processor ps) (processor-supplier ps)
    (instance? ProcessorSupplier ps) ps
    :else (throw (ex-info "Unknown processor supplier type" {:ps ps}))))
    
