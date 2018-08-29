(ns compute.kstreamj.core
  (:import (java.util Properties)
           (org.apache.kafka.streams KafkaStreams StreamsBuilder)))

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

(defn builder
  []
  (StreamsBuilder.))

(defn build
  [builder]
  (.build builder))

(defn streams
  [builder config]
  (KafkaStreams. (build builder) (map->properties config)))