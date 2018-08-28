(ns compute.kstreamj.core
  (:require [com.stuartsierra.component :as component]
            [manifold.stream :as stream]
            [compute.kafka-components.core :as kafka-components]
            [compute.test-components.kafka :as kafka]
            [compute.config.core :as config]
            [taoensso.timbre :as log]
            [franzy.common.configuration.codec :as config-codec]
            [taoensso.nippy :as nippy])
  (:import (java.util Properties)
           (org.apache.kafka.streams.kstream Reducer KeyValueMapper ValueMapper Predicate Printed Transformer TransformerSupplier ValueTransformer TimeWindows Materialized Initializer Aggregator)
           (org.apache.kafka.streams StreamsConfig KafkaStreams KeyValue StreamsBuilder TopologyTestDriver)
           (org.apache.kafka.common.serialization Serde Serdes Serializer)
           (org.apache.kafka.clients.consumer ConsumerConfig ConsumerRecord)
           (org.apache.kafka.streams.state QueryableStoreTypes Stores StoreSupplier)
           (org.apache.kafka.streams.test ConsumerRecordFactory)
           (org.apache.kafka.streams.processor TimestampExtractor Processor Punctuator ProcessorSupplier PunctuationType)
           (java.util.concurrent TimeUnit)
           (kafka.server KafkaConfig)
           (org.apache.zookeeper.server ServerConfig)
           (clojure.lang IFn)))

(log/set-level! :error)

(Thread/setDefaultUncaughtExceptionHandler
  (reify Thread$UncaughtExceptionHandler
    (uncaughtException [_ thread ex]
      (println ex "Uncaught exception on" (.getName thread)))))

(def conf (config/read :test))

(defn map->properties
  "Converts a map to a Java Properties object.

  Notes:

  * Sequential collections will be joined as comma-delimited strings, ex: [1 2 3 4] -> \"1, 2, 3, 4\".
  * Anything else will be converted to a string.
  * Does not use the codec, and is here for the simple people that asked for a simple conversion function. I simply disapprove, but here it is."
  [m]
  (let [properties (Properties.)]
    (doseq [[k v] m] (.put properties (name k) v))
    properties))

(deftype EdnSerde []
  Serde
  (configure [this map b])
  (close [this])
  (serializer [this]
    (kafka-components/nippy-serializer))
  (deserializer [this]
    (kafka-components/nippy-deserializer)))

(defn get-state
  [context store-name k]
  (-> context
      (.getStateStore store-name)
      (.get k)))

(def state-store-name "state-store")
(def state-store-builder (Stores/keyValueStoreBuilder (Stores/inMemoryKeyValueStore state-store-name)
                                                      (EdnSerde.) (EdnSerde.)))

(deftype SimulationTimestampExtractor []
  TimestampExtractor
  (^long extract [_ ^ConsumerRecord record ^long previousTimeStamp]
    (let [v (.value record)]
      (long (:time v)))))

(deftype SimulationPunctuator [context]
  Punctuator
  (punctuate [_ timestamp]
    (let [state-store (.getStateStore context "doink")
          state (.all state-store)]
      (println "------------- PUNCTUATE")
      (doseq [s (iterator-seq state)]
        (println
          (.value s)
          (-> s (.key) (.window) (.end) (java.util.Date.)))))))

(deftype SimulationTransformer [^{:volatile-mutable true} context]
  Transformer
  (init [_ c]
    (set! context c))
    ;;; Schedule punctuation
  (transform [_ k v]
    (let [state-store (.getStateStore context state-store-name)
          state (or (.get state-store k) {:count 10})
          state' (assoc state :demand (rand-int 10))]
      (.put state-store k state')
      (KeyValue/pair k (merge state' v))))
  (close [_]))

(deftype ActionTransformer [^{:volatile-mutable true} context]
  Transformer
  (init [_ c]
    (set! context c))
  ;;; Schedule punctuation
  (transform [_ k v]
    (let [t (:time v)
          action (:action v)
          update-fn (condp = action :inc inc :dec dec)
          state-store (.getStateStore context state-store-name)
          state (or (.get state-store k) {})
          state' (update state :count update-fn)]
      (.put state-store k state')
      (KeyValue/pair k state')))
  (close [_]))

(deftype DoinkProcessor [^{:volatile-mutable true} context]
  Processor
  (init [_ c]
    (set! context c)
    (.schedule context (.toMillis TimeUnit/MINUTES 30) PunctuationType/STREAM_TIME
               (SimulationPunctuator. context)))
  (process [_ k v])
  (close [_]))


(def kafka-config
  {StreamsConfig/APPLICATION_ID_CONFIG                    "example-consumer"
   StreamsConfig/BOOTSTRAP_SERVERS_CONFIG                 "localhost:9092"
   StreamsConfig/CACHE_MAX_BYTES_BUFFERING_CONFIG         0
   StreamsConfig/COMMIT_INTERVAL_MS_CONFIG                100000
   StreamsConfig/DEFAULT_KEY_SERDE_CLASS_CONFIG           EdnSerde
   StreamsConfig/DEFAULT_VALUE_SERDE_CLASS_CONFIG         EdnSerde
   #_#_StreamsConfig/DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG SimulationTimestampExtractor
   StreamsConfig/STATE_DIR_CONFIG                         (kafka/dir (System/getProperty "java.io.tmpdir") "kafka-streams")
   ConsumerConfig/AUTO_OFFSET_RESET_CONFIG                "earliest"})

(def builder (StreamsBuilder.))

(defmacro value-mapper
  ([f]
   `(reify
      ValueMapper
      (apply [this# v#] (~f v#))))
  ([args & body]
   `(reify
      ValueMapper
      (apply ~(vec (cons 'this# args))
        ~@body))))

(defmacro transformer-supplier
  [& body]
  `(reify TransformerSupplier
     (get [_]
       ~@body)))

(defn add-state-store
  [^StreamsBuilder builder ^StoreSupplier supplier]
  (.addStateStore builder supplier)
  builder)

(defn stream
  [^StreamsBuilder builder & topics]
  [(.stream builder (vec topics)) builder])

(defn transform
  [[stream builder] transformer-supplier & store-names]
  [(.transform stream transformer-supplier (into-array String store-names)) builder])

(defn map-values
  [[stream-or-table builder] value-mapper]
  [(.mapValues stream-or-table value-mapper) builder])

(defn group-by-key
  [[stream builder]]
  [(.groupByKey stream) builder])

(defn hours-ms
  [n]
  (.toMillis TimeUnit/HOURS n))

(defn of
  [ms]
  (TimeWindows/of ms))

(defn windowed-by
  [[table builder] window]
  [(.windowedBy table window) builder])

(defn foo
  [v]
  (assoc v :action (if (> 5 (:demand v)) :dec :inc)))

(defn aggregate
  ([[stream builder] initializer aggregator]))

#_(def s
    (-> builder
        (add-state-store state-store-builder)
        (stream "simulation-ticks")
        (transform (transformer-supplier (SimulationTransformer. nil)) state-store-name)
        (map-values (value-mapper foo))
        (transform (transformer-supplier (ActionTransformer. nil)) state-store-name)
        (group-by-key)
        (windowed-by (of (hours-ms 1)))
        (.aggregate (reify Initializer (apply [_] 0))
                    (reify Aggregator (apply [_ _ v total]
                                        (if (:end v) total (inc total))))
                    (Materialized/as "doink"))
        (.toStream)
        (.process (reify ProcessorSupplier (get [_] (DoinkProcessor. nil))) (into-array String ["doink"]))))



#_(def test-driver (TopologyTestDriver. (.build builder) (map->properties kafka-config)))
#_(def crf (ConsumerRecordFactory. "topic-input" (nippy-serializers/nippy-serializer) (nippy-serializers/nippy-serializer)))

(def s
  (-> builder
      (.stream "simulation-ticks")
      (.mapValues (value-mapper (fn [x] (assoc x :foo :bar))))
      (.to "output-topic")))

(defn new-system-map
  [conf topology streams-config]
  {:kafka-broker   (kafka/new-embedded-kafka conf)
   :streams        (component/using (kafka/new-streams topology streams-config)
                                    [:kafka-broker])
   :kafka-producer (component/using (kafka-components/new-kafka-producer conf)
                                    [:kafka-broker])
   :kafka-consumer (component/using (kafka-components/new-kafka-consumer conf {:topics #{:response :error}})
                                    [#_:kafka-broker])})

(defn new-system
  [conf topology streams-config]
  (apply component/system-map (mapcat identity (new-system-map conf topology streams-config))))

(defn start-new-system
  [conf topology streams-config]
  (try
    (component/start (new-system conf topology streams-config))
    (catch Exception ex
      (println ex)
      (component/stop (:system (ex-data ex)))
      (throw ex))))

(comment
  (do
    (def consumer (component/start (kafka/new-kafka-consumer kafka-config #{"output-topic"})))
    (def cs (-> consumer ::kafka/consumer-stream))
    (stream/consume
      (fn [v]
        (println "OUTPUT--------------------------")
        (clojure.pprint/pprint v)
        (println "----------------------------------"))
      (->> cs
           #_(stream/filter #(= :response (:topic %)))
           (stream/map :value))))

  (do
    (def system (start-new-system conf (.build builder) kafka-config))
    (def producer-stream (get-in system [:kafka-producer ::kafka-components/producer-stream]))
    (def consumer-stream (get-in system [:kafka-producer ::kafka-components/producer-stream]))
    (stream/consume
      (fn [v]
        (println "OUTPUT--------------------------")
        (clojure.pprint/pprint v)
        (println "----------------------------------"))
      (->> consumer-stream
           #_(stream/filter #(= :response (:topic %)))
           (stream/map :value)))
    (stream/put! producer-stream {:topic :simulation-ticks
                                  :key :foo
                                  :value {:shit :burger}}))

  (do
    (def system (start-new-system conf (.build builder) kafka-config))
    (def producer-stream (get-in system [:kafka-producer ::kafka-components/producer-stream]))
    (def now (.getTime #inst"2018-04-30T00:00:00.000-00:00"))
    (def segs (conj (mapv #(assoc {} :topic :simulation-ticks :key :foo :value {:time %})
                          (range now
                                 (+ now (.toMillis TimeUnit/DAYS 1))
                                 (.toMillis TimeUnit/MINUTES 1)))
                    {:topic :simulation-ticks :key :foo :value {:time (.getTime #inst"3000-04-30T00:00:00.000-00:00") :end true}}))
    (stream/put-all! producer-stream segs))

          ; Make punctuation segment with "infinite" time



  (do
    (.pipeInput test-driver (.create crf :foo {:id 5 :n 100}))
    (.value (.readOutput test-driver "output-topic" (nippy-deserializers/nippy-deserializer) (nippy-deserializers/nippy-deserializer))))

  (do
    (component/stop system))

  identity)