package com.jackniu.kafka.clients.producer;

import com.jackniu.kafka.clients.consumer.OffsetAndMetadata;
import com.jackniu.kafka.common.Metric;
import com.jackniu.kafka.common.MetricName;
import com.jackniu.kafka.common.PartitionInfo;
import com.jackniu.kafka.common.TopicPartition;
import com.jackniu.kafka.common.errors.ProducerFencedException;

import java.io.Closeable;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public interface Producer<K,V> extends Closeable {
    /**
     * See {@link KafkaProducer#initTransactions()}
     */
    void initTransactions();

    /**
     * See {@link KafkaProducer#beginTransaction()}
     */
    void beginTransaction() throws ProducerFencedException;

    /**
     * See {@link KafkaProducer#sendOffsetsToTransaction(Map, String)}
     */
    void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                  String consumerGroupId) throws ProducerFencedException;

    /**
     * See {@link KafkaProducer#commitTransaction()}
     */
    void commitTransaction() throws ProducerFencedException;

    /**
     * See {@link KafkaProducer#abortTransaction()}
     */
    void abortTransaction() throws ProducerFencedException;

    /**
     * See {@link KafkaProducer#send(ProducerRecord)}
     */
    Future<RecordMetadata> send(ProducerRecord<K, V> record);

    /**
     * See {@link KafkaProducer#send(ProducerRecord, Callback)}
     */
    Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback);

    /**
     * See {@link KafkaProducer#flush()}
     */
    void flush();

    /**
     * See {@link KafkaProducer#partitionsFor(String)}
     */
    List<PartitionInfo> partitionsFor(String topic);

    /**
     * See {@link KafkaProducer#metrics()}
     */
    Map<MetricName, ? extends Metric> metrics();

    /**
     * See {@link KafkaProducer#close()}
     */
    void close();

    @Deprecated
    default void close(long timeout, TimeUnit unit) {
        close(Duration.ofMillis(unit.toMillis(timeout)));
    }

    /**
     * See {@link KafkaProducer#close(Duration)}
     */
    void close(Duration timeout);


}
