(ns compute.kstreamj-test
  (:require [clojure.test :refer :all]
            [compute.kstreamj.core :as streams]
            [compute.kstreamj.dsl :as dsl]
            [franzy.serialization.nippy.serializers :as ns]
            [franzy.serialization.nippy.deserializers :as nd]
            [taoensso.timbre :as log])
  (:import (org.apache.kafka.common.serialization Serde)
           (org.apache.kafka.streams.state Stores)
           (org.apache.kafka.streams.processor Punctuator Processor TimestampExtractor PunctuationType)
           (org.apache.kafka.streams.kstream Transformer Materialized Windows TimeWindows)
           (org.apache.kafka.streams KeyValue StreamsConfig)
           (org.apache.kafka.clients.consumer ConsumerConfig ConsumerRecord)
           (java.util.concurrent TimeUnit)))

(log/set-level! :error)

(Thread/setDefaultUncaughtExceptionHandler
  (reify Thread$UncaughtExceptionHandler
    (uncaughtException [_ thread ex]
      (println ex "Uncaught exception on" (.getName thread)))))

(deftype NippySerde []
  Serde
  (configure [this map b])
  (close [this])
  (serializer [this]
    (ns/nippy-serializer))
  (deserializer [this]
    (nd/nippy-deserializer)))

(defn get-state
  [context store-name k]
  (-> context
      (.getStateStore store-name)
      (.get k)))

(def state-store-name "state-store")
(def state-store-builder (Stores/keyValueStoreBuilder (Stores/inMemoryKeyValueStore state-store-name)
                                                      (NippySerde.) (NippySerde.)))

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
  {StreamsConfig/APPLICATION_ID_CONFIG            "example-consumer"
   StreamsConfig/BOOTSTRAP_SERVERS_CONFIG         "localhost:9092"
   StreamsConfig/CACHE_MAX_BYTES_BUFFERING_CONFIG 0
   StreamsConfig/COMMIT_INTERVAL_MS_CONFIG        100000
   StreamsConfig/DEFAULT_KEY_SERDE_CLASS_CONFIG   NippySerde
   StreamsConfig/DEFAULT_VALUE_SERDE_CLASS_CONFIG NippySerde
   ConsumerConfig/AUTO_OFFSET_RESET_CONFIG        "earliest"})

(defn foo
  [v]
  (assoc v :action (if (> 5 (:demand v)) :dec :inc)))

(def builder (streams/builder))

(-> builder
    (dsl/add-state-store! state-store-builder)
    (dsl/stream! "simulation-ticks")
    (dsl/transform! (SimulationTransformer. nil) [state-store-name])
    (dsl/map-values! foo)
    (dsl/transform! (ActionTransformer. nil) [state-store-name])
    (dsl/group-by-key!)
    (dsl/windowed-by! (TimeWindows/of (.toMillis TimeUnit/HOURS 1)))
    (dsl/aggregate! (constantly 0)
                    (fn [k v total] (if (:end v) total (inc total)))
                    (Materialized/as "doink"))
    (dsl/to-stream!)
    (dsl/process! (DoinkProcessor. nil) ["doink"]))




#_(def test-driver (TopologyTestDriver. (.build builder) (map->properties kafka-config)))
#_(def crf (ConsumerRecordFactory. "topic-input" (nippy-serializers/nippy-serializer) (nippy-serializers/nippy-serializer)))