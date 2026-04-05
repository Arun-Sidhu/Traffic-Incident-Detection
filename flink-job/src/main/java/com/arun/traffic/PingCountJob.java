// PingCountJob.java
package com.arun.traffic;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class PingCountJob {

  public static void main(String[] args) throws Exception {
    ParameterTool p = ParameterTool.fromArgs(args);

    String bootstrap = p.get("bootstrapServers", "kafka:9092");
    String inTopic = p.get("inTopic", "gps_pings_raw");
    String outTopic = p.get("outTopic", "gps_ping_counts");

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    KafkaSource<String> source =
        KafkaSource.<String>builder()
            .setBootstrapServers(bootstrap)
            .setTopics(inTopic)
            .setGroupId("traffic-demo-pingcount")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

    DataStream<String> in =
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-gps_pings_raw");

    DataStream<String> counts =
        in.map(x -> 1L)
            .returns(Types.LONG)
            .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
            .reduce(Long::sum)
            .map(c -> String.format("{\"window_end_ms\":%d,\"ping_count\":%d}", System.currentTimeMillis(), c))
            .returns(Types.STRING);

    KafkaSink<String> sink =
        KafkaSink.<String>builder()
            .setBootstrapServers(bootstrap)
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic(outTopic)
                    .setValueSerializationSchema(new SimpleStringSchema())
                    .build())
            .build();

    counts.sinkTo(sink).name("kafka-gps_ping_counts");
    env.execute("Traffic Ping Counter (10s)");
  }
}