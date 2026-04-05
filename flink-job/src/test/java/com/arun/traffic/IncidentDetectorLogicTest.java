// IncidentDetectorLogicTest.java
package com.arun.traffic;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class IncidentDetectorLogicTest {
  private static final double ALPHA = 0.20;
  private static final double OPEN_RATIO = 0.80;
  private static final double CLOSE_RATIO = 0.90;
  private static final long MIN_COUNT = 3L;
  private static final long CLOSE_HOLD_MS = 10_000L;

  @Test
  void opensWhenSpeedDropsBelowThreshold() {
    IncidentDetectorLogic.State state = new IncidentDetectorLogic.State();

    IncidentDetectorLogic.Metric normal =
        new IncidentDetectorLogic.Metric("seg_a", 0L, 10_000L, 8L, 24.0, 22.0, 25.0);
    IncidentDetectorLogic.TransitionResult first =
        IncidentDetectorLogic.onMetric(
            state, normal, ALPHA, OPEN_RATIO, CLOSE_RATIO, MIN_COUNT, CLOSE_HOLD_MS);

    assertNull(first.emittedEvent);
    assertFalse(first.state.open);
    assertEquals(24.0, first.state.baseline, 0.0001);

    IncidentDetectorLogic.Metric slowdown =
        new IncidentDetectorLogic.Metric("seg_a", 10_000L, 20_000L, 9L, 14.0, 12.0, 16.0);
    IncidentDetectorLogic.TransitionResult second =
        IncidentDetectorLogic.onMetric(
            state, slowdown, ALPHA, OPEN_RATIO, CLOSE_RATIO, MIN_COUNT, CLOSE_HOLD_MS);

    assertNotNull(second.emittedEvent);
    assertTrue(second.state.open);
    assertEquals("open", second.emittedEvent.status);
    assertEquals("seg_a", second.emittedEvent.segmentId);
    assertEquals(20_000L, second.emittedEvent.startTs);
    assertTrue(second.emittedEvent.severity > 0.0);
  }

  @Test
  void doesNotOpenWhenObservedCountIsTooLow() {
    IncidentDetectorLogic.State state = new IncidentDetectorLogic.State();

    IncidentDetectorLogic.Metric metric =
        new IncidentDetectorLogic.Metric("seg_a", 0L, 10_000L, 2L, 14.0, 12.0, 16.0);
    IncidentDetectorLogic.TransitionResult result =
        IncidentDetectorLogic.onMetric(
            state, metric, ALPHA, OPEN_RATIO, CLOSE_RATIO, MIN_COUNT, CLOSE_HOLD_MS);

    assertNull(result.emittedEvent);
    assertFalse(result.state.open);
  }

  @Test
  void startsCloseTimerThenEmitsCloseEvent() {
    IncidentDetectorLogic.State state = new IncidentDetectorLogic.State();

    IncidentDetectorLogic.onMetric(
        state,
        new IncidentDetectorLogic.Metric("seg_a", 0L, 10_000L, 8L, 24.0, 22.0, 25.0),
        ALPHA,
        OPEN_RATIO,
        CLOSE_RATIO,
        MIN_COUNT,
        CLOSE_HOLD_MS);

    IncidentDetectorLogic.TransitionResult openResult =
        IncidentDetectorLogic.onMetric(
            state,
            new IncidentDetectorLogic.Metric("seg_a", 10_000L, 20_000L, 9L, 14.0, 12.0, 16.0),
            ALPHA,
            OPEN_RATIO,
            CLOSE_RATIO,
            MIN_COUNT,
            CLOSE_HOLD_MS);

    assertNotNull(openResult.emittedEvent);
    assertTrue(state.open);

    IncidentDetectorLogic.TransitionResult recoveryResult =
        IncidentDetectorLogic.onMetric(
            state,
            new IncidentDetectorLogic.Metric("seg_a", 20_000L, 30_000L, 8L, 21.0, 20.0, 22.0),
            ALPHA,
            OPEN_RATIO,
            CLOSE_RATIO,
            MIN_COUNT,
            CLOSE_HOLD_MS);

    assertNull(recoveryResult.emittedEvent);
    assertEquals(40_000L, recoveryResult.registerTimerAt);
    assertEquals(40_000L, state.closeTimerTs);

    IncidentDetectorLogic.TransitionResult closeResult =
        IncidentDetectorLogic.onTimer(state, "seg_a", 40_000L);

    assertNotNull(closeResult.emittedEvent);
    assertEquals("closed", closeResult.emittedEvent.status);
    assertFalse(state.open);
    assertNull(state.closeTimerTs);
  }

  @Test
  void cancelsPendingCloseWhenSpeedDropsAgain() {
    IncidentDetectorLogic.State state = new IncidentDetectorLogic.State();

    IncidentDetectorLogic.onMetric(
        state,
        new IncidentDetectorLogic.Metric("seg_a", 0L, 10_000L, 8L, 24.0, 22.0, 25.0),
        ALPHA,
        OPEN_RATIO,
        CLOSE_RATIO,
        MIN_COUNT,
        CLOSE_HOLD_MS);

    IncidentDetectorLogic.onMetric(
        state,
        new IncidentDetectorLogic.Metric("seg_a", 10_000L, 20_000L, 9L, 14.0, 12.0, 16.0),
        ALPHA,
        OPEN_RATIO,
        CLOSE_RATIO,
        MIN_COUNT,
        CLOSE_HOLD_MS);

    IncidentDetectorLogic.onMetric(
        state,
        new IncidentDetectorLogic.Metric("seg_a", 20_000L, 30_000L, 8L, 21.0, 20.0, 22.0),
        ALPHA,
        OPEN_RATIO,
        CLOSE_RATIO,
        MIN_COUNT,
        CLOSE_HOLD_MS);

    IncidentDetectorLogic.TransitionResult cancelResult =
        IncidentDetectorLogic.onMetric(
            state,
            new IncidentDetectorLogic.Metric("seg_a", 30_000L, 40_000L, 8L, 15.0, 14.0, 16.0),
            ALPHA,
            OPEN_RATIO,
            CLOSE_RATIO,
            MIN_COUNT,
            CLOSE_HOLD_MS);

    assertNull(cancelResult.emittedEvent);
    assertEquals(40_000L, cancelResult.cancelTimerAt);
    assertNull(state.closeTimerTs);
    assertTrue(state.open);
  }

  @Test
  void computeSeverityMatchesCurrentFsmFormula() {
    assertEquals(3.6, IncidentDetectorLogic.computeSeverity(20.0, 14.0, 12L));
    assertEquals(0.0, IncidentDetectorLogic.computeSeverity(0.0, 14.0, 12L));
  }
}