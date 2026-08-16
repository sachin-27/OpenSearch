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
import org.opensearch.search.aggregations.bucket.terms.DoubleTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;

import java.util.ArrayList;
import java.util.List;

/**
 * Builds a {@link DoubleTerms} response. Handles floating-point field types
 * (float, double, half_float, scaled_float).
 *
 * <p>The {@link DocValueFormat} is typically RAW for plain floating-point fields;
 * scaled_float may carry a scaling factor format in future.
 */
public final class DoubleTermsStrategy implements TermsResponseStrategy {

    public static final DoubleTermsStrategy INSTANCE = new DoubleTermsStrategy();

    private DoubleTermsStrategy() {}

    @Override
    public InternalAggregation build(TermsAggregationBuilder agg, List<BucketEntry> entries, DocValueFormat format) {
        List<DoubleTerms.Bucket> termBuckets = new ArrayList<>(entries.size());
        for (BucketEntry entry : entries) {
            double term = ((Number) entry.keys().get(0)).doubleValue();
            termBuckets.add(new DoubleTerms.Bucket(term, entry.docCount(), entry.subAggs(), false, 0, format));
        }
        TermsBucketTranslator.Truncated<DoubleTerms.Bucket> visible = TermsBucketTranslator.sortAndTruncate(termBuckets, agg);
        BucketOrder order = agg.order();
        return new DoubleTerms(
            agg.getName(),
            order,
            order,
            AggregationTranslator.userMetadata(agg),
            format,
            agg.shardSize(),
            false,
            visible.otherDocCount(),
            visible.buckets(),
            0,
            TermsBucketTranslator.thresholds(agg)
        );
    }
}
