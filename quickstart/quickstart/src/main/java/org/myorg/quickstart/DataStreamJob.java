/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.myorg.quickstart;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.*;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.IntSummaryStatistics;

public class DataStreamJob {

    public static void main(String[] args) throws Exception {
        // Sets up the execution environment, which is the main entry point
        // to building Flink applications.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<SensorData> source = KafkaSource.<SensorData>builder()
//        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("sensors")
                .setGroupId("flink_sensor_consumers")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(new KafkaRecordDeserializationSchema<>() {
                    private final ObjectMapper objectMapper = new ObjectMapper();

                    @Override
                    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<SensorData> collector) throws IOException {
                        SensorData sensorData = objectMapper.readValue(consumerRecord.value(), SensorData.class);
                        sensorData.setTimestamp(consumerRecord.timestamp());
                        collector.collect(sensorData);
                    }

                    @Override
                    public TypeInformation<SensorData> getProducedType() {
                        return TypeInformation.of(SensorData.class);
                    }
                })
                .build();

        env.setParallelism(1);
        DataStream<SensorData> sensorDataStream = env.fromSource(source,
                WatermarkStrategy.<SensorData>forMonotonousTimestamps(), "Kafka Consumer");
//        sensorDataStream.print("KAFKA");

        var result1Stream = sensorDataStream
                .keyBy(SensorData::getKey)
                .window(SlidingEventTimeWindows.of(Duration.ofSeconds(5), Duration.ofSeconds(1)))
                .process(new ProcessWindowFunction<SensorData, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, ProcessWindowFunction<SensorData, String, String, TimeWindow>.Context context, Iterable<SensorData> iterable, Collector<String> collector) throws Exception {
                        int count = 0;
                        for (SensorData record : iterable) {
                            count++;
                        }
                        collector.collect(String.format("(%s)[%d - %d] counted %d", key, context.window().getStart(), context.window().getEnd(), count));
                    }
                });
//        result1Stream.print("RESULT1");

        KafkaSink<String> result1Sink = KafkaSink.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopic("result1")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE).build();

        result1Stream.sinkTo(result1Sink);

        var result2Stream = sensorDataStream
                .keyBy(SensorData::getKey)
                .window(SlidingEventTimeWindows.of(Duration.ofSeconds(5), Duration.ofSeconds(1)))
                .process(new ProcessWindowFunction<SensorData, SensorDataStatistics, String, TimeWindow>() {
                    @Override
                    public void process(String key, ProcessWindowFunction<SensorData, SensorDataStatistics, String, TimeWindow>.Context context, Iterable<SensorData> iterable, Collector<SensorDataStatistics> collector) throws Exception {
                        IntSummaryStatistics statistics = new IntSummaryStatistics();
                        for (SensorData record : iterable) {
                            statistics.accept(record.getValue());
                        }

// {"key":"B","window_start":1669748895000,"window_end":1669748896000,"min_value":10,"count":10,"average":55.55,"max_value":100}

                        SensorDataStatistics s = new SensorDataStatistics(statistics, key, context.window());
                        collector.collect(s);
                    }
                });
//        result2Stream.print("RESULT2");

        KafkaSink<SensorDataStatistics> result2Sink = KafkaSink.<SensorDataStatistics>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<SensorDataStatistics>builder()
                                .setTopic("result2")
                                .setValueSerializationSchema(new SensorDataStatisticsSchema())
                                .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE).build();

        result2Stream.sinkTo(result2Sink);

        env.execute(DataStreamJob.class.getSimpleName());
        return;
    }
}