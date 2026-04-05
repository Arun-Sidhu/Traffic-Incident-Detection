// SegmentMetricsJob.java
package com.arun.traffic;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class SegmentMetricsJob {

  public static class GpsPing {
    public String vehicleId;
    public String eventId;
    public long timestampMs;
    public double lat;
    public double lon;
    public double speedMps;
    public String segmentId;

    public GpsPing() {}
  }

  public static class SegmentMetric {
    public String segmentId;
    public long windowStartMs;
    public long windowEndMs;
    public long count;
    public double avgSpeedMps;
    public double minSpeedMps;
    public double maxSpeedMps;

    public SegmentMetric() {}
  }

  static String segmentId(double lat, double lon) {
    int a = (int) Math.floor(lat * 1000.0);
    int b = (int) Math.floor(lon * 1000.0);
    return "seg_" + a + "_" + b;
  }

  public static void main(String[] args) throws Exception {
    ParameterTool p = ParameterTool.fromArgs(args);

    String bootstrap = p.get("bootstrapServers", "kafka:9092");
    String inTopic = p.get("inTopic", "gps_pings_raw");
    String outTopic = p.get("outTopic", "segment_metrics");

    long checkpointIntervalMs = p.getLong("checkpointIntervalMs", 10_000L);
    long checkpointTimeoutMs = p.getLong("checkpointTimeoutMs", 60_000L);
    long minPauseBetweenCheckpointsMs = p.getLong("minPauseBetweenCheckpointsMs", 5_000L);
    String checkpointDir = p.get("checkpointDir", "file:///flink-checkpoints/segment-metrics");
    int maxCheckpointFailures = p.getInt("maxCheckpointFailures", 3);

    long dedupTtlMs = p.getLong("dedupTtlMs", Duration.ofMinutes(10).toMillis());
    int dedupMaxIdsPerVehicle = p.getInt("dedupMaxIdsPerVehicle", 512);
    long dedupCleanupIntervalMs = p.getLong("dedupCleanupIntervalMs", Duration.ofMinutes(1).toMillis());

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    configureEnvironment(
        env,
        checkpointIntervalMs,
        checkpointTimeoutMs,
        minPauseBetweenCheckpointsMs,
        checkpointDir,
        maxCheckpointFailures);

    KafkaSource<String> source =
        KafkaSource.<String>builder()
            .setBootstrapServers(bootstrap)
            .setTopics(inTopic)
            .setGroupId("traffic-segment-metrics-v2")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

    DataStream<String> raw =
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-gps_pings_raw");

    DataStream<GpsPing> parsed =
        raw.process(new ParseAndValidate()).name("parse-validate");

    DataStream<GpsPing> withWm =
        parsed.assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<GpsPing>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((e, ts) -> e.timestampMs))
            .name("event-time-watermarks");

    DataStream<GpsPing> deduped =
        withWm.keyBy(e -> e.vehicleId)
            .process(new DedupBySeenEventIdTtl(dedupTtlMs, dedupMaxIdsPerVehicle, dedupCleanupIntervalMs))
            .name("dedup-by-seen-eventId");

    DataStream<GpsPing> enriched =
        deduped.map(e -> {
              e.segmentId = segmentId(e.lat, e.lon);
              return e;
            })
            .returns(Types.POJO(GpsPing.class))
            .name("compute-segmentId");

    DataStream<SegmentMetric> metrics =
        enriched
            .keyBy(e -> e.segmentId)
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .aggregate(new SpeedAgg(), new AddWindowInfo())
            .name("window-segment-metrics");

    Properties producerProps = new Properties();
    producerProps.setProperty("transaction.timeout.ms", "900000");

    KafkaSink<String> sink =
        KafkaSink.<String>builder()
            .setBootstrapServers(bootstrap)
            .setKafkaProducerConfig(producerProps)
            .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
            .setTransactionalIdPrefix("traffic-segment-metrics")
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic(outTopic)
                    .setValueSerializationSchema(new SimpleStringSchema())
                    .build())
            .build();

    ObjectMapper om = new ObjectMapper();
    metrics
        .map(om::writeValueAsString)
        .returns(Types.STRING)
        .sinkTo(sink)
        .name("kafka-segment_metrics");

    env.execute("Traffic Segment Metrics (10s, event-time)");
  }

  private static void configureEnvironment(
      StreamExecutionEnvironment env,
      long checkpointIntervalMs,
      long checkpointTimeoutMs,
      long minPauseBetweenCheckpointsMs,
      String checkpointDir,
      int maxCheckpointFailures) {
    env.enableCheckpointing(checkpointIntervalMs, CheckpointingMode.EXACTLY_ONCE);
    env.getCheckpointConfig().setCheckpointTimeout(checkpointTimeoutMs);
    env.getCheckpointConfig().setMinPauseBetweenCheckpoints(minPauseBetweenCheckpointsMs);
    env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
    env.getCheckpointConfig().setTolerableCheckpointFailureNumber(maxCheckpointFailures);
    env.getCheckpointConfig().setExternalizedCheckpointCleanup(
        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    env.getCheckpointConfig().setCheckpointStorage(checkpointDir);
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, org.apache.flink.api.common.time.Time.seconds(10)));
  }

  static class ParseAndValidate extends ProcessFunction<String, GpsPing> {
    private transient ObjectMapper om;
    private transient Counter parseErrors;
    private transient Counter invalidDropped;

    @Override
    public void open(Configuration parameters) {
      om = new ObjectMapper();
      parseErrors = getRuntimeContext().getMetricGroup().counter("parse_errors");
      invalidDropped = getRuntimeContext().getMetricGroup().counter("invalid_dropped");
    }

    @Override
    public void processElement(String value, Context ctx, Collector<GpsPing> out) {
      try {
        JsonNode n = om.readTree(value);

        JsonNode vehicleId = n.get("vehicleId");
        JsonNode eventId = n.get("eventId");
        JsonNode ts = n.get("timestampMs");
        JsonNode lat = n.get("lat");
        JsonNode lon = n.get("lon");
        JsonNode speed = n.get("speedMps");

        if (vehicleId == null || eventId == null || ts == null || lat == null || lon == null || speed == null) {
          invalidDropped.inc();
          return;
        }

        GpsPing e = new GpsPing();
        e.vehicleId = vehicleId.asText();
        e.eventId = eventId.asText();
        e.timestampMs = ts.asLong();
        e.lat = lat.asDouble();
        e.lon = lon.asDouble();
        e.speedMps = speed.asDouble();

        if (e.vehicleId.isEmpty() || e.eventId.isEmpty() || e.timestampMs <= 0) {
          invalidDropped.inc();
          return;
        }

        out.collect(e);
      } catch (Exception ex) {
        parseErrors.inc();
      }
    }
  }

  static class DedupBySeenEventIdTtl extends KeyedProcessFunction<String, GpsPing, GpsPing> {
    private final long ttlMs;
    private final int maxTrackedEventIds;
    private final long cleanupIntervalMs;

    private transient MapState<String, Long> seenEventIds;
    private transient ValueState<Long> nextCleanupProcessingTs;
    private transient Counter dupDropped;

    DedupBySeenEventIdTtl(long ttlMs, int maxTrackedEventIds, long cleanupIntervalMs) {
      this.ttlMs = ttlMs;
      this.maxTrackedEventIds = maxTrackedEventIds;
      this.cleanupIntervalMs = cleanupIntervalMs;
    }

    @Override
    public void open(Configuration parameters) {
      MapStateDescriptor<String, Long> seenDesc =
          new MapStateDescriptor<>("seenEventIds", Types.STRING, Types.LONG);
      seenEventIds = getRuntimeContext().getMapState(seenDesc);

      nextCleanupProcessingTs =
          getRuntimeContext().getState(new ValueStateDescriptor<>("nextCleanupProcessingTs", Types.LONG));

      dupDropped = getRuntimeContext().getMetricGroup().counter("dedup_dropped");
    }

    @Override
    public void processElement(GpsPing e, Context ctx, Collector<GpsPing> out) throws Exception {
      long now = ctx.timerService().currentProcessingTime();
      maybeCleanup(now);

      Long seenAt = seenEventIds.get(e.eventId);
      if (seenAt != null && now - seenAt <= ttlMs) {
        dupDropped.inc();
        return;
      }

      seenEventIds.put(e.eventId, now);
      out.collect(e);
    }

    private void maybeCleanup(long now) throws Exception {
      Long nextCleanup = nextCleanupProcessingTs.value();
      if (nextCleanup != null && now < nextCleanup) {
        return;
      }

      List<Tuple2<String, Long>> liveEntries = new ArrayList<>();
      for (Map.Entry<String, Long> entry : seenEventIds.entries()) {
        Long seenAt = entry.getValue();
        if (seenAt == null || now - seenAt > ttlMs) {
          seenEventIds.remove(entry.getKey());
        } else {
          liveEntries.add(Tuple2.of(entry.getKey(), seenAt));
        }
      }

      if (liveEntries.size() > maxTrackedEventIds) {
        liveEntries.sort(Comparator.comparingLong(t -> t.f1));
        int removeCount = liveEntries.size() - maxTrackedEventIds;
        for (int i = 0; i < removeCount; i++) {
          seenEventIds.remove(liveEntries.get(i).f0);
        }
      }

      nextCleanupProcessingTs.update(now + cleanupIntervalMs);
    }
  }

  static class Acc {
    long count = 0;
    double sum = 0.0;
    double min = Double.POSITIVE_INFINITY;
    double max = Double.NEGATIVE_INFINITY;
  }

  static class SpeedAgg implements AggregateFunction<GpsPing, Acc, Acc> {
    @Override
    public Acc createAccumulator() {
      return new Acc();
    }

    @Override
    public Acc add(GpsPing value, Acc acc) {
      acc.count += 1;
      acc.sum += value.speedMps;
      acc.min = Math.min(acc.min, value.speedMps);
      acc.max = Math.max(acc.max, value.speedMps);
      return acc;
    }

    @Override
    public Acc getResult(Acc acc) {
      return acc;
    }

    @Override
    public Acc merge(Acc a, Acc b) {
      Acc m = new Acc();
      m.count = a.count + b.count;
      m.sum = a.sum + b.sum;
      m.min = Math.min(a.min, b.min);
      m.max = Math.max(a.max, b.max);
      return m;
    }
  }

  static class AddWindowInfo extends ProcessWindowFunction<Acc, SegmentMetric, String, TimeWindow> {
    @Override
    public void process(String segmentId, Context ctx, Iterable<Acc> elements, Collector<SegmentMetric> out) {
      Acc acc = elements.iterator().next();
      SegmentMetric m = new SegmentMetric();
      m.segmentId = segmentId;
      m.windowStartMs = ctx.window().getStart();
      m.windowEndMs = ctx.window().getEnd();
      m.count = acc.count;
      m.avgSpeedMps = acc.count == 0 ? 0.0 : (acc.sum / acc.count);
      m.minSpeedMps = acc.min == Double.POSITIVE_INFINITY ? 0.0 : acc.min;
      m.maxSpeedMps = acc.max == Double.NEGATIVE_INFINITY ? 0.0 : acc.max;
      out.collect(m);
    }
  }
}