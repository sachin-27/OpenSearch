/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.profile;

import org.opensearch.common.Booleans;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;

/**
 * Node-local, lock-free phase timers for the DSL execution pipeline. Measurement aid for
 * benchmark runs: nothing is logged or serialized on the request path — timings accumulate into
 * striped counters and are read out of band via {@code GET /_dsl/stats}.
 *
 * <p>Overhead by construction: a recorded phase costs two {@link System#nanoTime()} calls plus
 * striped adds (tens of nanoseconds against millisecond phases). {@link #DETAIL} is a
 * {@code static final} read at class initialization, so when it is off the JIT eliminates every
 * guarded detail block — including its {@code nanoTime} calls — rather than merely skipping it.
 */
public final class DslPhaseStats {

    /**
     * Enables the per-invocation response-side phases ({@link Phase#MATERIALIZE},
     * {@link Phase#GROUP}, {@link Phase#PACKAGE}) and the row and bucket counters. Off by default:
     * those sites run once per bucket, so at high cardinality their timer calls would themselves
     * be measurable. Enable with {@code -Ddsl.profile.detail=true} for targeted runs only.
     */
    public static final boolean DETAIL = Booleans.parseBoolean(System.getProperty("dsl.profile.detail"), false);

    /** A timed stage of the pipeline. */
    public enum Phase {
        /** DSL to Calcite plan conversion, including per-request planning infrastructure setup. */
        CONVERT,
        /** Handing plans to the analytics engine until results arrive. */
        EXECUTE,
        /** Rebuilding the SearchResponse from execution results. */
        BUILD,
        /** Draining one granularity's rows into a list (detail only). */
        MATERIALIZE,
        /** Grouping rows into per-bucket piles (detail only). */
        GROUP,
        /** Translator construction of the vanilla aggregation object (detail only). */
        PACKAGE
    }

    /** A monotonically increasing count reported alongside the timers. */
    public enum Counter {
        /** Completed DSL requests. */
        REQUESTS,
        /** Result rows materialized (detail only). */
        ROWS,
        /** Bucket entries handed to translators (detail only). */
        BUCKETS
    }

    private static final LongAdder[] INVOCATIONS = adders(Phase.values().length);
    private static final LongAdder[] TOTAL_NANOS = adders(Phase.values().length);
    private static final LongAccumulator[] MAX_NANOS = maxAccumulators(Phase.values().length);
    private static final LongAdder[] COUNTERS = adders(Counter.values().length);

    private DslPhaseStats() {}

    /**
     * Records one occurrence of a phase.
     *
     * @param phase the phase measured
     * @param nanos elapsed nanoseconds
     */
    public static void record(Phase phase, long nanos) {
        int i = phase.ordinal();
        INVOCATIONS[i].increment();
        TOTAL_NANOS[i].add(nanos);
        MAX_NANOS[i].accumulate(nanos);
    }

    /**
     * Adds to a counter.
     *
     * @param counter the counter to advance
     * @param delta the amount to add
     */
    public static void add(Counter counter, long delta) {
        COUNTERS[counter.ordinal()].add(delta);
    }

    /** Clears all timers and counters. */
    public static void reset() {
        for (int i = 0; i < INVOCATIONS.length; i++) {
            INVOCATIONS[i].reset();
            TOTAL_NANOS[i].reset();
            MAX_NANOS[i].reset();
        }
        for (LongAdder counter : COUNTERS) {
            counter.reset();
        }
    }

    /**
     * Reads the current values. Called off the request path by the stats endpoint; the striped
     * sums are not atomic with respect to each other, which is immaterial for aggregate timings.
     *
     * @return phase timings and counters, keyed by lowercase name
     */
    public static Map<String, Object> snapshot() {
        Map<String, Object> phases = new LinkedHashMap<>();
        for (Phase phase : Phase.values()) {
            int i = phase.ordinal();
            long invocations = INVOCATIONS[i].sum();
            long totalNanos = TOTAL_NANOS[i].sum();
            Map<String, Object> stats = new LinkedHashMap<>();
            stats.put("invocations", invocations);
            stats.put("total_millis", totalNanos / 1_000_000.0);
            stats.put("mean_micros", invocations == 0 ? 0.0 : totalNanos / (invocations * 1000.0));
            stats.put("max_micros", MAX_NANOS[i].get() / 1000.0);
            phases.put(phase.name().toLowerCase(java.util.Locale.ROOT), stats);
        }

        Map<String, Object> counters = new LinkedHashMap<>();
        for (Counter counter : Counter.values()) {
            counters.put(counter.name().toLowerCase(java.util.Locale.ROOT), COUNTERS[counter.ordinal()].sum());
        }

        Map<String, Object> snapshot = new LinkedHashMap<>();
        snapshot.put("detail_enabled", DETAIL);
        snapshot.put("phases", phases);
        snapshot.put("counters", counters);
        return snapshot;
    }

    private static LongAdder[] adders(int size) {
        LongAdder[] adders = new LongAdder[size];
        for (int i = 0; i < size; i++) {
            adders[i] = new LongAdder();
        }
        return adders;
    }

    private static LongAccumulator[] maxAccumulators(int size) {
        LongAccumulator[] accumulators = new LongAccumulator[size];
        for (int i = 0; i < size; i++) {
            accumulators[i] = new LongAccumulator(Math::max, 0L);
        }
        return accumulators;
    }
}
