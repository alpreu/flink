/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kafka.sink;

import org.apache.flink.util.TestLoggerExtension;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.flink.connector.kafka.testutils.KafkaUtil.createKafkaContainer;
import static org.apache.flink.util.DockerImageVersions.KAFKA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
@ExtendWith(TestLoggerExtension.class)
class FlinkKafkaInternalProducerITCase {

    private static final Logger LOG =
            LoggerFactory.getLogger(FlinkKafkaInternalProducerITCase.class);

    @Container
    private static final KafkaContainer KAFKA_CONTAINER =
            createKafkaContainer(KAFKA, LOG).withEmbeddedZookeeper();

    private static final String TRANSACTION_PREFIX = "test-transaction-";

    @Test
    void testLateArrivalOfLowerTransactionId() {
        final String topic = "test-late-arrival-lower-transaction-id";
        final int lateArrivalId = 2;

        try (FlinkKafkaInternalProducer<String, String> producer =
                new FlinkKafkaInternalProducer<>(getProperties(), "dummy")) {
            for (int i = 1; i <= lateArrivalId + 10; i++) {
                producer.initTransactionId(TRANSACTION_PREFIX + i);
                producer.beginTransaction();
                producer.send(new ProducerRecord<>(topic, 0, null, "test-value-" + i));
                producer.commitTransaction();
            }

            producer.initTransactionId(TRANSACTION_PREFIX + lateArrivalId);
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(topic, 0, null, "test-value-" + lateArrivalId));
            producer.commitTransaction();
        }
    }

    @Test
    void testLateArrivalOfHigherTransactionId() {
        final String topic = "test-late-arrival-higher-transaction-id";
        final int lateArrivalId = 20;

        try (FlinkKafkaInternalProducer<String, String> producer =
                new FlinkKafkaInternalProducer<>(getProperties(), "dummy")) {
            for (int i = 1; i <= lateArrivalId - 10; i++) {
                producer.initTransactionId(TRANSACTION_PREFIX + i);
                producer.beginTransaction();
                producer.send(new ProducerRecord<>(topic, 0, null, "test-value-" + i));
                producer.commitTransaction();
            }

            producer.initTransactionId(TRANSACTION_PREFIX + lateArrivalId);
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(topic, 0, null, "test-value-" + 4));
            producer.commitTransaction();
        }
    }

    @Test
    void testInterleavedProducers() {
        final String topic = "test-interleaved-producers";

        try (FlinkKafkaInternalProducer<String, String> producer =
                        new FlinkKafkaInternalProducer<>(getProperties(), "dummy");
                FlinkKafkaInternalProducer<String, String> other =
                        new FlinkKafkaInternalProducer<>(getProperties(), "dummy2")) {

            producer.initTransactionId(TRANSACTION_PREFIX + 1);
            producer.beginTransaction();

            producer.send(new ProducerRecord<>(topic, 0, null, "test-value-" + 1));
            producer.send(new ProducerRecord<>(topic, 0, null, "test-value-" + 2));
            producer.send(new ProducerRecord<>(topic, 0, null, "test-value-" + 3));
            producer.commitTransaction();
            producer.flush();

            producer.beginTransaction();
            producer.send(new ProducerRecord<>(topic, 0, null, "test-value" + 5));
            producer.flush();

            other.initTransactionId(TRANSACTION_PREFIX + 20);
            other.beginTransaction();
            other.send(new ProducerRecord<>(topic, 0, null, "test-value-" + 4));

            other.flush();
            other.commitTransaction();
            other.flush();

            producer.flush();
            producer.commitTransaction();
            producer.flush();
        }
    }

    @Test
    void testInterleavedProducersFlushLast() {
        final String topic = "test-interleaved-producers-flush-last";

        try (FlinkKafkaInternalProducer<String, String> producer =
                        new FlinkKafkaInternalProducer<>(getProperties(), "dummy");
                FlinkKafkaInternalProducer<String, String> other =
                        new FlinkKafkaInternalProducer<>(getProperties(), "dummy2")) {

            producer.initTransactionId(TRANSACTION_PREFIX + 1);
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(topic, 0, null, "test-value-" + 1));

            other.initTransactionId(TRANSACTION_PREFIX + 20);
            other.beginTransaction();
            other.send(new ProducerRecord<>(topic, 0, null, "test-value-" + 4));

            other.flush();
            other.commitTransaction();
            other.flush();

            producer.flush();
            producer.commitTransaction();
            producer.flush();
        }
    }

    @Test
    void testInterleavedProducersCommitLast() {
        final String topic = "test-interleaved-producers-commit-last";

        try (FlinkKafkaInternalProducer<String, String> producer =
                        new FlinkKafkaInternalProducer<>(getProperties(), "dummy");
                FlinkKafkaInternalProducer<String, String> other =
                        new FlinkKafkaInternalProducer<>(getProperties(), "dummy2")) {

            producer.initTransactionId(TRANSACTION_PREFIX + 1);
            producer.beginTransaction();

            producer.send(new ProducerRecord<>(topic, 0, null, "test-value-" + 1));
            producer.send(new ProducerRecord<>(topic, 0, null, "test-value-" + 2));
            producer.send(new ProducerRecord<>(topic, 0, null, "test-value-" + 3));

            producer.flush();

            other.initTransactionId(TRANSACTION_PREFIX + 20);
            other.beginTransaction();
            other.send(new ProducerRecord<>(topic, 0, null, "test-value-" + 4));

            other.flush();
            other.commitTransaction();

            producer.commitTransaction();
        }
    }

    @Test
    void testInterleavedProducersCommitWithoutFlush() {
        final String topic = "test-interleaved-producers-commit-without-flush";

        try (FlinkKafkaInternalProducer<String, String> producer =
                        new FlinkKafkaInternalProducer<>(getProperties(), "dummy");
                FlinkKafkaInternalProducer<String, String> other =
                        new FlinkKafkaInternalProducer<>(getProperties(), "dummy2")) {

            producer.initTransactionId(TRANSACTION_PREFIX + 1);
            producer.beginTransaction();

            producer.send(new ProducerRecord<>(topic, 0, null, "test-value-" + 1));
            producer.send(new ProducerRecord<>(topic, 0, null, "test-value-" + 2));
            producer.send(new ProducerRecord<>(topic, 0, null, "test-value-" + 3));

            producer.commitTransaction();

            other.initTransactionId(TRANSACTION_PREFIX + 20);
            other.beginTransaction();
            other.send(new ProducerRecord<>(topic, 0, null, "test-value-" + 4));

            other.commitTransaction();

            producer.flush();
            other.flush();
        }
    }

    @Test
    void testSequentialTransactions() {
        final String topic = "test-sequential-transactions";

        try (FlinkKafkaInternalProducer<String, String> producer =
                new FlinkKafkaInternalProducer<>(getProperties(), "dummy")) {
            for (int i = 1; i <= 10; i++) {
                producer.initTransactionId(TRANSACTION_PREFIX + i);
                producer.beginTransaction();
                producer.send(new ProducerRecord<>(topic, 0, null, "test-value-" + i));
                producer.commitTransaction();
                producer.beginTransaction();
                producer.send(new ProducerRecord<>(topic, 0, null, "test-value-" + i));
                producer.commitTransaction();
            }
        }
    }

    @Test
    void testTwoProducers() {
        final String topic = "test-two-producers";

        try (FlinkKafkaInternalProducer<String, String> producer =
                        new FlinkKafkaInternalProducer<>(getProperties(), "dummy");
                FlinkKafkaInternalProducer<String, String> other =
                        new FlinkKafkaInternalProducer<>(getProperties(), "dummy2")) {
            for (int i = 1; i <= 10; i++) {
                producer.initTransactionId(TRANSACTION_PREFIX + i);
                other.initTransactionId(TRANSACTION_PREFIX + i + 20);
                producer.beginTransaction();
                other.beginTransaction();
                producer.send(new ProducerRecord<>(topic, 0, null, "test-value-" + i));
                other.send(new ProducerRecord<>(topic, 0, null, "test-value-" + i));
                producer.commitTransaction();
                other.commitTransaction();
            }
        }
    }

    @Test
    void testInitTransactionId() {
        final String topic = "test-init-transactions";
        try (FlinkKafkaInternalProducer<String, String> reuse =
                new FlinkKafkaInternalProducer<>(getProperties(), "dummy")) {
            int numTransactions = 20;
            for (int i = 1; i <= numTransactions; i++) {
                reuse.initTransactionId(TRANSACTION_PREFIX + i);
                reuse.beginTransaction();
                reuse.send(new ProducerRecord<>(topic, "test-value-" + i));
                if (i % 2 == 0) {
                    reuse.commitTransaction();
                } else {
                    reuse.flush();
                    reuse.abortTransaction();
                }
                assertNumTransactions(i);
                assertThat(readRecords(topic).count()).isEqualTo(i / 2);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("provideTransactionsFinalizer")
    void testResetInnerTransactionIfFinalizingTransactionFailed(
            Consumer<FlinkKafkaInternalProducer<?, ?>> transactionFinalizer) {
        final String topic = "reset-producer-internal-state";
        try (FlinkKafkaInternalProducer<String, String> fenced =
                new FlinkKafkaInternalProducer<>(getProperties(), "dummy")) {
            fenced.initTransactions();
            fenced.beginTransaction();
            fenced.send(new ProducerRecord<>(topic, "test-value"));
            // Start a second producer that fences the first one
            try (FlinkKafkaInternalProducer<String, String> producer =
                    new FlinkKafkaInternalProducer<>(getProperties(), "dummy")) {
                producer.initTransactions();
                producer.beginTransaction();
                producer.send(new ProducerRecord<>(topic, "test-value"));
                producer.commitTransaction();
            }
            assertThatThrownBy(() -> transactionFinalizer.accept(fenced))
                    .isInstanceOf(ProducerFencedException.class);
            // Internal transaction should be reset and setting a new transactional id is possible
            fenced.setTransactionId("dummy2");
        }
    }

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.put(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                KAFKA_CONTAINER.getBootstrapServers());
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return properties;
    }

    private static List<Consumer<FlinkKafkaInternalProducer<?, ?>>> provideTransactionsFinalizer() {
        return Lists.newArrayList(
                FlinkKafkaInternalProducer::commitTransaction,
                FlinkKafkaInternalProducer::abortTransaction);
    }

    private void assertNumTransactions(int numTransactions) {
        List<KafkaTransactionLog.TransactionRecord> transactions =
                new KafkaTransactionLog(getProperties())
                        .getTransactions(id -> id.startsWith(TRANSACTION_PREFIX));
        assertThat(
                        transactions.stream()
                                .map(KafkaTransactionLog.TransactionRecord::getTransactionId)
                                .collect(Collectors.toSet()))
                .hasSize(numTransactions);
    }

    private ConsumerRecords<String, String> readRecords(String topic) {
        Properties properties = getProperties();
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.assign(
                consumer.partitionsFor(topic).stream()
                        .map(partitionInfo -> new TopicPartition(topic, partitionInfo.partition()))
                        .collect(Collectors.toSet()));
        consumer.seekToBeginning(consumer.assignment());
        return consumer.poll(Duration.ofMillis(1000));
    }
}
