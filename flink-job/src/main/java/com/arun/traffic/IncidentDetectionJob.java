// IncidentDetectionJob.java
package com.arun.traffic;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class IncidentDetectionJob {
  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  public static void main(String[] args) throws Exception {
    final ParameterTool p = ParameterTool.fromArgs(args);
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    final long checkpointIntervalMs =
        firstNonBlankLong(
            p.get("checkpointIntervalMs", null),
            System.getenv("CHECKPOINT_INTERVAL_MS"),
            "10000");
    final long checkpointTimeoutMs =
        firstNonBlankLong(
            p.get("checkpointTimeoutMs", null),
            System.getenv("CHECKPOINT_TIMEOUT_MS"),
            "60000");
    final long minPauseBetweenCheckpointsMs =
        firstNonBlankLong(
            p.get("minPauseBetweenCheckpointsMs", null),
            System.getenv("MIN_PAUSE_BETWEEN_CHECKPOINTS_MS"),
            "5000");
    final int maxCheckpointFailures =
        (int)
            firstNonBlankLong(
                p.get("maxCheckpointFailures", null),
                System.getenv("MAX_CHECKPOINT_FAILURES"),
                "3");
    final String checkpointDir =
        firstNonBlank(
            p.get("checkpointDir", null),
            System.getenv("CHECKPOINT_DIR"),
            System.getenv("CHECKPOINT_ROOT") == null
                ? null
                : System.getenv("CHECKPOINT_ROOT") + "/incident-detector",
            "file:///flink-checkpoints/incident-detector");

    env.enableCheckpointing(checkpointIntervalMs, CheckpointingMode.EXACTLY_ONCE);

    CheckpointConfig checkpointConfig = env.getCheckpointConfig();
    checkpointConfig.setCheckpointTimeout(checkpointTimeoutMs);
    checkpointConfig.setMinPauseBetweenCheckpoints(minPauseBetweenCheckpointsMs);
    checkpointConfig.setMaxConcurrentCheckpoints(1);
    checkpointConfig.setTolerableCheckpointFailureNumber(maxCheckpointFailures);
    checkpointConfig.setExternalizedCheckpointCleanup(
        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    checkpointConfig.setCheckpointStorage(checkpointDir);

    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10)));

    final String bootstrap =
        firstNonBlank(
            p.get("bootstrapServers", null),
            System.getenv("BOOTSTRAP_SERVERS"),
            System.getenv("KAFKA_BOOTSTRAP"),
            "kafka:9092");
    final String inputTopic =
        firstNonBlank(
            p.get("inTopic", null),
            System.getenv("IN_TOPIC"),
            System.getenv("INPUT_TOPIC"),
            "segment_metrics");
    final String outputTopic =
        firstNonBlank(
            p.get("outTopic", null),
            System.getenv("OUT_TOPIC"),
            System.getenv("OUTPUT_TOPIC"),
            "traffic_incidents");
    final String consumerGroup =
        firstNonBlank(
            p.get("consumerGroup", null),
            System.getenv("CONSUMER_GROUP"),
            "incident-detector-v1");

    final double alpha =
        firstNonBlankDouble(
            p.get("emaAlpha", null),
            System.getenv("EMA_ALPHA"),
            "0.20");
    final double openRatio =
        firstNonBlankDouble(
            p.get("openRatio", null),
            System.getenv("OPEN_RATIO"),
            "0.80");
    final double closeRatio =
        firstNonBlankDouble(
            p.get("closeRatio", null),
            System.getenv("CLOSE_RATIO"),
            "0.90");
    final long minCount =
        firstNonBlankLong(
            p.get("minCount", null),
            System.getenv("MIN_COUNT"),
            "3");
    final long closeHoldMs =
        firstNonBlankLong(
            p.get("closeHoldMs", null),
            System.getenv("CLOSE_HOLD_MS"),
            "10000");

    KafkaSource<String> source =
        KafkaSource.<String>builder()
            .setBootstrapServers(bootstrap)
            .setTopics(inputTopic)
            .setGroupId(consumerGroup)
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

    Properties producerProps = new Properties();
    producerProps.setProperty("transaction.timeout.ms", "900000");

    KafkaSink<String> sink =
        KafkaSink.<String>builder()
            .setBootstrapServers(bootstrap)
            .setKafkaProducerConfig(producerProps)
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic(outputTopic)
                    .setValueSerializationSchema(new SimpleStringSchema())
                    .build())
            .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
            .setTransactionalIdPrefix("traffic-incident-detector")
            .build();

    DataStream<SegmentMetric> metrics =
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "segment-metrics-source")
            .map(new SegmentMetricParser())
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<SegmentMetric>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner(
                        new SerializableTimestampAssigner<SegmentMetric>() {
                          @Override
                          public long extractTimestamp(
                              SegmentMetric element, long recordTimestamp) {
                            return element.windowEndMs;
                          }
                        }));

    DataStream<IncidentEvent> incidentEvents =
        metrics
            .keyBy(m -> m.segmentId)
            .process(new IncidentFSM(alpha, openRatio, closeRatio, minCount, closeHoldMs));

    DataStream<String> jsonEvents =
        incidentEvents.map(
            new MapFunction<IncidentEvent, String>() {
              @Override
              public String map(IncidentEvent value) throws Exception {
                return MAPPER.writeValueAsString(value);
              }
            });

    jsonEvents.sinkTo(sink);

    env.execute("Traffic Incident Detector (open/close)");
  }

  private static String firstNonBlank(String... values) {
    for (String value : values) {
      if (value != null && !value.isBlank()) {
        return value;
      }
    }
    return null;
  }

  private static long firstNonBlankLong(String first, String second, String fallback) {
    return Long.parseLong(firstNonBlank(first, second, fallback));
  }

  private static double firstNonBlankDouble(String first, String second, String fallback) {
    return Double.parseDouble(firstNonBlank(first, second, fallback));
  }

  public static final class SegmentMetricParser implements MapFunction<String, SegmentMetric> {
    @Override
    public SegmentMetric map(String value) throws Exception {
      return MAPPER.readValue(value, SegmentMetric.class);
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static final class SegmentMetric {
    public String segmentId;
    public long windowStartMs;
    public long windowEndMs;
    public long count;
    public double avgSpeedMps;
    public double minSpeedMps;
    public double maxSpeedMps;

    public SegmentMetric() {}

    public SegmentMetric(
        String segmentId,
        long windowStartMs,
        long windowEndMs,
        long count,
        double avgSpeedMps,
        double minSpeedMps,
        double maxSpeedMps) {
      this.segmentId = segmentId;
      this.windowStartMs = windowStartMs;
      this.windowEndMs = windowEndMs;
      this.count = count;
      this.avgSpeedMps = avgSpeedMps;
      this.minSpeedMps = minSpeedMps;
      this.maxSpeedMps = maxSpeedMps;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static final class IncidentEvent {
    public String incidentId;
    public String segmentId;
    public String status;
    public long startTs;
    public long endTs;
    public double severity;
    public double baselineSpeedMps;
    public double observedSpeedMps;
    public long observedCount;

    public IncidentEvent() {}

    public IncidentEvent(
        String incidentId,
        String segmentId,
        String status,
        long startTs,
        long endTs,
        double severity,
        double baselineSpeedMps,
        double observedSpeedMps,
        long observedCount) {
      this.incidentId = incidentId;
      this.segmentId = segmentId;
      this.status = status;
      this.startTs = startTs;
      this.endTs = endTs;
      this.severity = severity;
      this.baselineSpeedMps = baselineSpeedMps;
      this.observedSpeedMps = observedSpeedMps;
      this.observedCount = observedCount;
    }
  }

  public static final class IncidentFSM
      extends KeyedProcessFunction<String, SegmentMetric, IncidentEvent> {
    private final double alpha;
    private final double openRatio;
    private final double closeRatio;
    private final long minCount;
    private final long closeHoldMs;

    private transient ValueState<Double> baseline;
    private transient ValueState<Boolean> isOpen;
    private transient ValueState<String> incidentId;
    private transient ValueState<Long> incidentStart;
    private transient ValueState<Long> closeTimerTs;
    private transient ValueState<Long> closeProcessingTimerTs;
    private transient ValueState<SegmentMetric> lastMetric;

    public IncidentFSM(
        double alpha,
        double openRatio,
        double closeRatio,
        long minCount,
        long closeHoldMs) {
      this.alpha = alpha;
      this.openRatio = openRatio;
      this.closeRatio = closeRatio;
      this.minCount = minCount;
      this.closeHoldMs = closeHoldMs;
    }

    @Override
    public void open(Configuration parameters) {
      baseline =
          getRuntimeContext().getState(new ValueStateDescriptor<>("baseline", Double.class));
      isOpen = getRuntimeContext().getState(new ValueStateDescriptor<>("isOpen", Boolean.class));
      incidentId =
          getRuntimeContext().getState(new ValueStateDescriptor<>("incidentId", String.class));
      incidentStart =
          getRuntimeContext().getState(new ValueStateDescriptor<>("incidentStart", Long.class));
      closeTimerTs =
          getRuntimeContext().getState(new ValueStateDescriptor<>("closeTimerTs", Long.class));
      closeProcessingTimerTs =
          getRuntimeContext()
              .getState(new ValueStateDescriptor<>("closeProcessingTimerTs", Long.class));
      lastMetric =
          getRuntimeContext()
              .getState(new ValueStateDescriptor<>("lastMetric", SegmentMetric.class));
    }

    @Override
    public void processElement(SegmentMetric value, Context ctx, Collector<IncidentEvent> out)
        throws Exception {

      IncidentDetectorLogic.State state = readState();
      IncidentDetectorLogic.Metric metric = toLogicMetric(value);

      IncidentDetectorLogic.TransitionResult result =
          IncidentDetectorLogic.onMetric(
              state, metric, alpha, openRatio, closeRatio, minCount, closeHoldMs);

      if (result.cancelTimerAt != null) {
        ctx.timerService().deleteEventTimeTimer(result.cancelTimerAt);
        clearProcessingFallbackTimer(ctx);
      }

      if (result.registerTimerAt != null) {
        ctx.timerService().registerEventTimeTimer(result.registerTimerAt);

        Long existingProcessingTimerTs = closeProcessingTimerTs.value();
        if (existingProcessingTimerTs != null && existingProcessingTimerTs != 0L) {
          ctx.timerService().deleteProcessingTimeTimer(existingProcessingTimerTs);
        }

        long processingFallbackAt = ctx.timerService().currentProcessingTime() + closeHoldMs;
        ctx.timerService().registerProcessingTimeTimer(processingFallbackAt);
        closeProcessingTimerTs.update(processingFallbackAt);
      }

      if (result.emittedEvent != null) {
        out.collect(toIncidentEvent(result.emittedEvent));
      }

      writeState((String) ctx.getCurrentKey(), result.state);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<IncidentEvent> out)
        throws Exception {

      IncidentDetectorLogic.State state = readState();
      String segmentIdKey = (String) ctx.getCurrentKey();

      if (ctx.timeDomain() == TimeDomain.PROCESSING_TIME) {
        Long processingTimerTs = closeProcessingTimerTs.value();
        if (processingTimerTs == null || !processingTimerTs.equals(timestamp)) {
          return;
        }

        closeProcessingTimerTs.clear();

        Long eventTimerTs = state.closeTimerTs;
        if (eventTimerTs == null || eventTimerTs == 0L) {
          writeState(segmentIdKey, state);
          return;
        }

        ctx.timerService().deleteEventTimeTimer(eventTimerTs);

        IncidentDetectorLogic.TransitionResult result =
            IncidentDetectorLogic.onTimer(state, segmentIdKey, eventTimerTs);

        if (result.emittedEvent != null) {
          out.collect(toIncidentEvent(result.emittedEvent));
        }

        writeState(segmentIdKey, result.state);
        return;
      }

      IncidentDetectorLogic.TransitionResult result =
          IncidentDetectorLogic.onTimer(state, segmentIdKey, timestamp);

      if (result.emittedEvent != null) {
        out.collect(toIncidentEvent(result.emittedEvent));
        clearProcessingFallbackTimer(ctx);
      }

      writeState(segmentIdKey, result.state);
    }

    private IncidentDetectorLogic.State readState() throws Exception {
      IncidentDetectorLogic.State state = new IncidentDetectorLogic.State();
      state.baseline = baseline.value();
      state.open = Boolean.TRUE.equals(isOpen.value());
      state.incidentId = incidentId.value();
      state.incidentStart = incidentStart.value();
      state.closeTimerTs = closeTimerTs.value();

      SegmentMetric metric = lastMetric.value();
      if (metric != null) {
        state.lastMetric = new IncidentDetectorLogic.Snapshot(toLogicMetric(metric));
      }

      return state;
    }

    private void writeState(String segmentIdKey, IncidentDetectorLogic.State state)
        throws Exception {
      baseline.update(state.baseline);
      isOpen.update(state.open);
      incidentId.update(state.incidentId);
      incidentStart.update(state.incidentStart);
      closeTimerTs.update(state.closeTimerTs);

      if (state.lastMetric == null) {
        lastMetric.clear();
      } else {
        lastMetric.update(
            new SegmentMetric(
                segmentIdKey,
                state.lastMetric.windowStartMs,
                state.lastMetric.windowEndMs,
                state.lastMetric.count,
                state.lastMetric.avgSpeedMps,
                state.lastMetric.minSpeedMps,
                state.lastMetric.maxSpeedMps));
      }
    }

    private void clearProcessingFallbackTimer(OnTimerContext ctx) throws Exception {
      Long processingTimerTs = closeProcessingTimerTs.value();
      if (processingTimerTs != null && processingTimerTs != 0L) {
        ctx.timerService().deleteProcessingTimeTimer(processingTimerTs);
      }
      closeProcessingTimerTs.clear();
    }

    private void clearProcessingFallbackTimer(Context ctx) throws Exception {
      Long processingTimerTs = closeProcessingTimerTs.value();
      if (processingTimerTs != null && processingTimerTs != 0L) {
        ctx.timerService().deleteProcessingTimeTimer(processingTimerTs);
      }
      closeProcessingTimerTs.clear();
    }

    private IncidentDetectorLogic.Metric toLogicMetric(SegmentMetric metric) {
      return new IncidentDetectorLogic.Metric(
          metric.segmentId,
          metric.windowStartMs,
          metric.windowEndMs,
          metric.count,
          metric.avgSpeedMps,
          metric.minSpeedMps,
          metric.maxSpeedMps);
    }

    private IncidentEvent toIncidentEvent(IncidentDetectorLogic.Event event) {
      return new IncidentEvent(
          event.incidentId,
          event.segmentId,
          event.status,
          event.startTs,
          event.endTs,
          event.severity,
          event.baselineSpeedMps,
          event.observedSpeedMps,
          event.observedCount);
    }
  }
}