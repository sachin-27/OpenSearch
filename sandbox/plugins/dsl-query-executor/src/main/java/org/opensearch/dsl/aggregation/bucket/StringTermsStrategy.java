/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.bucket;

import org.apache.lucene.util.BytesRef;
import org.opensearch.dsl.aggregation.AggregationTranslator;
import org.opensearch.dsl.result.BucketEntry;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;

import java.util.ArrayList;
import java.util.List;

/**
 * Builds a {@link StringTerms} response. Handles keyword, ip, text, and any field
 * type whose bucket keys are naturally string-representable.
 *
 * <p>Keys are converted to {@link BytesRef} using the resolved {@link DocValueFormat}
 * for rendering. For IP fields the format produces the dotted-quad/colon-hex string;
 * for keywords it is identity.
 */
public final class StringTermsStrategy implements TermsResponseStrategy {

    public static final StringTermsStrategy INSTANCE = new StringTermsStrategy();

    private StringTermsStrategy() {}

    @Override
    public InternalAggregation build(TermsAggregationBuilder agg, List<BucketEntry> entries, DocValueFormat format) {
        List<StringTerms.Bucket> termBuckets = new ArrayList<>(entries.size());
        for (BucketEntry entry : entries) {
            Object key = entry.keys().get(0);
            // Use the DocValueFormat to render the key string, then wrap as BytesRef
            String rendered = formatKey(key, format);
            BytesRef term = new BytesRef(rendered);
            termBuckets.add(new StringTerms.Bucket(term, entry.docCount(), entry.subAggs(), false, 0, format));
        }
        TermsBucketTranslator.Truncated<StringTerms.Bucket> visible = TermsBucketTranslator.sortAndTruncate(termBuckets, agg);
        BucketOrder order = agg.order();
        return new StringTerms(
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

    /**
     * Formats a key value using the resolved DocValueFormat.
     * Handles BytesRef (ip), byte[] (raw ip from Arrow), and falls back to toString.
     */
    private static String formatKey(Object key, DocValueFormat format) {
        if (key instanceof BytesRef ref) {
            return format.format(ref);
        }
        if (key instanceof byte[] bytes) {
            return format.format(new BytesRef(bytes));
        }
        return key.toString();
    }
}
