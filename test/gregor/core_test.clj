(ns gregor.core-test
  (:require [gregor.core :refer :all]
            [clojure.test :refer [is testing deftest]])
  (:import [org.apache.kafka.clients.consumer MockConsumer OffsetResetStrategy ConsumerRecords ConsumerRecord]
           [org.apache.kafka.clients.producer MockProducer]
           [org.apache.kafka.common TopicPartition]
           [org.apache.kafka.common.serialization StringSerializer]
           [java.util ArrayList])
  (:refer-clojure :exclude [flush send]))

(deftest producing
  (let [p (MockProducer. true (StringSerializer.) (StringSerializer.))]
    (send p "unittest" {:a 1 :b "two"})
    (send-then p "unittest" {:a 2 :b "three"} (fn [metadata ex]))
    (let [values (.history p)
          one (-> values first .value)
          two (-> values second .value)]
      (is (= {:a 1 :b "two"} one))
      (is (= {:a 2 :b "three"} two))
      (.close p))))

(deftest subscribing
  (let [c (consumer "localhost:9092" "unit-test" ["test-topic"])]
    (is (= #{"test-topic"} (subscription c)))
    (unsubscribe c)
    (is (= #{} (subscription c)))
    (close c)))

(deftest consuming
  (let [c (MockConsumer. (OffsetResetStrategy/EARLIEST))
        _ (assign! c "test-topic" 0)
        c (doto c
            (.updateBeginningOffsets {(topic-partition "test-topic" 0) 0})
            (.addRecord (ConsumerRecord. "test-topic" 0 0 0 {:a 1}))
            (.addRecord (ConsumerRecord. "test-topic" 0 0 0 {:b 2})))
        ms (records c)]
    (is (= {:a 1}
           (-> ms (first) (first) (:value))))
    (is (= {:b 2}
           (-> ms (first) (second) (:value))))
    (is (= #{(topic-partition "test-topic" 0)}
           (assignment c)))
    (.close c)))


(deftest commit
  (let [c (doto (MockConsumer. (OffsetResetStrategy/EARLIEST))
            (assign! "unittest" 0)
            (.updateBeginningOffsets {(topic-partition "unittest" 0) 0})
            (.addRecord (ConsumerRecord. "unittest" 0 1 0 {:key :a})))]
    (is (= nil (committed c "unittest" 0)))
    (poll c)
    (commit-offsets! c)
    (is (= {:offset 2 :metadata nil} (committed c "unittest" 0)))
        (.addRecord c (ConsumerRecord. "unittest" 0 2 0 {:key :b}))
    (poll c)
    (commit-offsets-async! c)
    (is (= {:offset 3 :metadata nil} (committed c "unittest" 0)))
    (.addRecord c (ConsumerRecord. "unittest" 0 3 0 {:key :c}))
    (poll c)
    (commit-offsets-async! c (fn [om ex]))
    (is (= {:offset 4 :metadata nil} (committed c "unittest" 0)))
    (.addRecord c (ConsumerRecord. "unittest" 0 4 0 {:key :c}))
    (poll c)
    (is (= 5 (position c "unittest" 0)))
    (commit-offsets-async! c [{:topic "unittest" :partition 0 :offset 5}] (fn [om ex]))
    (is (= {:offset 5 :metadata nil} (committed c "unittest" 0)))
    (seek! c "unittest" 0 2)
    (is (= 2 (position c "unittest" 0)))
    (seek-to! c :beginning "unittest" 0)
    (is (= 0 (position c "unittest" 0)))
    (is (thrown? IllegalStateException
                 (seek-to! c :end "unittest" 0)
                 (= 5 (position c "unittest" 0))))))
