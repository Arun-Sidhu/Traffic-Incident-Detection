// IncidentDetectorLogic.java
package com.arun.traffic;

import java.util.Objects;
import java.util.UUID;

public final class IncidentDetectorLogic {
  private IncidentDetectorLogic() {}

  public static final class Metric {
    public final String segmentId;
    public final long windowStartMs;
    public final long windowEndMs;
    public final long count;
    public final double avgSpeedMps;
    public final double minSpeedMps;
    public final double maxSpeedMps;

    public Metric(
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

  public static final class Snapshot {
    public final long windowStartMs;
    public final long windowEndMs;
    public final long count;
    public final double avgSpeedMps;
    public final double minSpeedMps;
    public final double maxSpeedMps;

    public Snapshot(Metric metric) {
      this.windowStartMs = metric.windowStartMs;
      this.windowEndMs = metric.windowEndMs;
      this.count = metric.count;
      this.avgSpeedMps = metric.avgSpeedMps;
      this.minSpeedMps = metric.minSpeedMps;
      this.maxSpeedMps = metric.maxSpeedMps;
    }
  }

  public static final class State {
    public Double baseline;
    public boolean open;
    public String incidentId;
    public Long incidentStart;
    public Long closeTimerTs;
    public Snapshot lastMetric;
  }

  public static final class Event {
    public final String incidentId;
    public final String segmentId;
    public final String status;
    public final long startTs;
    public final long endTs;
    public final double severity;
    public final double baselineSpeedMps;
    public final double observedSpeedMps;
    public final long observedCount;

    public Event(
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

  public static final class TransitionResult {
    public final State state;
    public final Event emittedEvent;
    public final Long registerTimerAt;
    public final Long cancelTimerAt;

    public TransitionResult(
        State state,
        Event emittedEvent,
        Long registerTimerAt,
        Long cancelTimerAt) {
      this.state = state;
      this.emittedEvent = emittedEvent;
      this.registerTimerAt = registerTimerAt;
      this.cancelTimerAt = cancelTimerAt;
    }
  }

  public static TransitionResult onMetric(
      State state,
      Metric metric,
      double alpha,
      double openRatio,
      double closeRatio,
      long minCount,
      long closeHoldMs) {
    State next = state == null ? new State() : state;
    next.lastMetric = new Snapshot(metric);

    double baseline = next.baseline == null ? metric.avgSpeedMps : next.baseline;
    if (!next.open) {
      baseline = alpha * metric.avgSpeedMps + (1.0 - alpha) * baseline;
    }
    next.baseline = baseline;

    boolean shouldOpen = metric.count >= minCount && metric.avgSpeedMps < baseline * openRatio;
    boolean recovered = metric.avgSpeedMps >= baseline * closeRatio;

    if (!next.open) {
      if (shouldOpen) {
        String id = UUID.randomUUID().toString();
        next.open = true;
        next.incidentId = id;
        next.incidentStart = metric.windowEndMs;
        next.closeTimerTs = null;

        return new TransitionResult(
            next,
            makeEvent("open", id, metric.segmentId, next.lastMetric, baseline, metric.windowEndMs, 0L),
            null,
            null);
      }
      return new TransitionResult(next, null, null, null);
    }

    if (recovered) {
      if (next.closeTimerTs == null || next.closeTimerTs == 0L) {
        long fireAt = metric.windowEndMs + closeHoldMs;
        next.closeTimerTs = fireAt;
        return new TransitionResult(next, null, fireAt, null);
      }
      return new TransitionResult(next, null, null, null);
    }

    if (next.closeTimerTs != null && next.closeTimerTs != 0L) {
      Long cancelled = next.closeTimerTs;
      next.closeTimerTs = null;
      return new TransitionResult(next, null, null, cancelled);
    }

    return new TransitionResult(next, null, null, null);
  }

  public static TransitionResult onTimer(State state, String segmentId, long timestamp) {
    Objects.requireNonNull(state, "state");

    if (state.closeTimerTs == null || !state.closeTimerTs.equals(timestamp)) {
      return new TransitionResult(state, null, null, null);
    }

    String id = state.incidentId;
    long startTs = state.incidentStart == null ? 0L : state.incidentStart;
    double baseline = state.baseline == null ? 0.0 : state.baseline;
    Snapshot snapshot = state.lastMetric;

    Event event = makeEvent("closed", id, segmentId, snapshot, baseline, startTs, timestamp);

    state.open = false;
    state.incidentId = null;
    state.incidentStart = null;
    state.closeTimerTs = null;
    state.lastMetric = null;

    return new TransitionResult(state, event, null, null);
  }

  public static double computeSeverity(double baselineSpeed, double observedSpeed, long observedCount) {
    if (baselineSpeed <= 0.0) {
      return 0.0;
    }

    double normalizedDrop =
        Math.max(0.0, Math.min(1.0, (baselineSpeed - observedSpeed) / baselineSpeed));
    double confidence = Math.min(1.0, observedCount / 10.0);
    double score = normalizedDrop * confidence * 10.0;

    return Math.round(Math.max(0.0, Math.min(10.0, score)) * 10.0) / 10.0;
  }

  private static Event makeEvent(
      String status,
      String incidentId,
      String segmentId,
      Snapshot snapshot,
      double baselineSpeed,
      long startTs,
      long endTs) {
    double observedSpeed = snapshot == null ? 0.0 : snapshot.avgSpeedMps;
    long observedCount = snapshot == null ? 0L : snapshot.count;
    double severity = computeSeverity(baselineSpeed, observedSpeed, observedCount);

    return new Event(
        incidentId,
        segmentId,
        status,
        startTs,
        endTs,
        severity,
        baselineSpeed,
        observedSpeed,
        observedCount);
  }
}