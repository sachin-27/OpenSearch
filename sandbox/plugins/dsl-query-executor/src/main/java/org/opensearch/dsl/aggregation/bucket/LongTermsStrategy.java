/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.bucket;

import org.opensearch.dsl.aggregation.AggregationTranslator;
import org.opensearch.dsl.result.BucketEntry;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.terms.LongTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;

import java.util.ArrayList;
import java.util.List;

/**
 * Builds a {@link LongTerms} response. Handles all integral numeric types (long, integer,
 * short, byte, date, date_nanos, unsigned_long) and booleans (as 0/1).
 *
 * <p>The {@link DocValueFormat} controls key rendering: dates produce formatted strings
 * via {@code DocValueFormat.DateTime}, booleans produce "true"/"false" via
 * {@code DocValueFormat.BOOLEAN}, plain numerics use RAW (just the number).
 */
public final class LongTermsStrategy implements TermsResponseStrategy {

    /** Standard instance for numeric/date fields. */
    public static final LongTermsStrategy INSTANCE = new LongTermsStrategy(false);

    /** Instance for boolean fields — converts true/false to 1/0. */
    public static final LongTermsStrategy BOOLEAN_INSTANCE = new LongTermsStrategy(true);

    private final boolean isBoolean;

    private LongTermsStrategy(boolean isBoolean) {
        this.isBoolean = isBoolean;
    }

    @Override
    public InternalAggregation build(TermsAggregationBuilder agg, List<BucketEntry> entries, DocValueFormat format) {
        // Boolean fields use DocValueFormat.BOOLEAN regardless of what was resolved
        DocValueFormat effectiveFormat = isBoolean ? DocValueFormat.BOOLEAN : format;

        List<LongTerms.Bucket> termBuckets = new ArrayList<>(entries.size());
        for (BucketEntry entry : entries) {
            Object key = entry.keys().get(0);
            long term = toLong(key);
            termBuckets.add(new LongTerms.Bucket(term, entry.docCount(), entry.subAggs(), false, 0, effectiveFormat));
        }
        TermsBucketTranslator.Truncated<LongTerms.Bucket> visible = TermsBucketTranslator.sortAndTruncate(termBuckets, agg);
        BucketOrder order = agg.order();
        return new LongTerms(
            agg.getName(),
            order,
            order,
            AggregationTranslator.userMetadata(agg),
            effectiveFormat,
            agg.shardSize(),
            false,
            visible.otherDocCount(),
            visible.buckets(),
            0,
            TermsBucketTranslator.thresholds(agg)
        );
    }

    private long toLong(Object key) {
        if (key instanceof Boolean bool) {
            return bool ? 1L : 0L;
        }
        return ((Number) key).longValue();
    }
}
