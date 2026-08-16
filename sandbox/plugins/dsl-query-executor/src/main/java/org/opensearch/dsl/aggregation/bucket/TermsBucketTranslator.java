/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.bucket;

import org.opensearch.dsl.aggregation.AggregationTranslator;
import org.opensearch.dsl.aggregation.FieldGrouping;
import org.opensearch.dsl.aggregation.GroupingInfo;
import org.opensearch.dsl.result.BucketEntry;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

/**
 * Translates a {@link TermsAggregationBuilder} — single-field GROUP BY.
 * {@code {"aggs": {"by_brand": {"terms": {"field": "brand"}}}}} becomes {@code GROUP BY brand}.
 *
 * <p>Type dispatch is handled by the {@link TermsResponseStrategy} registry — each field type
 * maps to a strategy that builds the correct {@code InternalTerms} subclass. This mirrors the
 * {@code ValuesSourceRegistry} pattern from classic search: additive, not a growing switch.
 *
 * <p>The field's type name and {@link DocValueFormat} are resolved from the index mapping via
 * {@code MappedFieldType}. See the {@code TODO} in {@link #toBucketAggregation} for the
 * remaining wiring needed to pass these through from the coordinator's {@code MapperService}.
 */
public class TermsBucketTranslator implements BucketTranslator<TermsAggregationBuilder> {

    private final Supplier<MapperService> mapperServiceSupplier;

    /** Creates a terms bucket translator. MapperService supplier is required. */
    public TermsBucketTranslator(Supplier<MapperService> mapperServiceSupplier) {
        this.mapperServiceSupplier = mapperServiceSupplier;
    }

    @Override
    public Class<TermsAggregationBuilder> getAggregationType() {
        return TermsAggregationBuilder.class;
    }

    @Override
    public GroupingInfo getGrouping(TermsAggregationBuilder agg) {
        return new FieldGrouping(List.of(agg.field()));
    }

    @Override
    public Collection<AggregationBuilder> getSubAggregations(TermsAggregationBuilder agg) {
        return agg.getSubAggregations();
    }

    @Override
    public BucketOrder getBucketOrder(TermsAggregationBuilder agg) {
        return agg.order();
    }

    /**
     * Builds the terms response using registry-based type dispatch.
     *
     * <p>The field's {@code typeName} and {@link DocValueFormat} determine which
     * {@link TermsResponseStrategy} builds the response — no instanceof sampling needed.
     * Buckets are filtered by {@code min_doc_count} before dispatch; sorting and truncation
     * to {@code size} happen inside the strategy.
     */
    @Override
    public InternalAggregation toBucketAggregation(TermsAggregationBuilder agg, Iterable<BucketEntry> buckets) {
        List<BucketEntry> kept = filterBuckets(agg, buckets);

        // Resolve typeName and DocValueFormat from MapperService.
        MapperService mapperService = mapperServiceSupplier.get();
        MappedFieldType fieldType = mapperService.fieldType(agg.field());
        String typeName = fieldType.typeName();
        DocValueFormat format = fieldType.docValueFormat(null, null);

        TermsResponseStrategy strategy = TermsResponseStrategy.forType(typeName);
        return strategy.build(agg, kept, format);
    }

    /**
     * Filters out null keys and buckets below min_doc_count.
     */
    private static List<BucketEntry> filterBuckets(TermsAggregationBuilder agg, Iterable<BucketEntry> buckets) {
        List<BucketEntry> kept = new ArrayList<>();
        for (BucketEntry entry : buckets) {
            if (entry.keys().get(0) == null) {
                continue;
            }
            if (entry.docCount() < agg.minDocCount()) {
                continue;
            }
            kept.add(entry);
        }
        return kept;
    }

    /** Result of {@link #sortAndTruncate}: the visible buckets and the truncated tail's doc count. */
    record Truncated<B extends MultiBucketsAggregation.Bucket>(List<B> buckets, long otherDocCount) {
    }

    /**
     * Sorts buckets per this aggregation's own order and truncates to {@code size}. The re-sort is
     * required because sibling aggregations sharing a granularity share one plan-level sort, which
     * cannot satisfy two different requested orders.
     */
    static <B extends MultiBucketsAggregation.Bucket> Truncated<B> sortAndTruncate(
        List<B> termBuckets,
        TermsAggregationBuilder agg
    ) {
        termBuckets.sort(agg.order().comparator());
        long otherDocCount = 0;
        List<B> visible = termBuckets;
        if (termBuckets.size() > agg.size()) {
            for (int i = agg.size(); i < termBuckets.size(); i++) {
                otherDocCount += termBuckets.get(i).getDocCount();
            }
            visible = new ArrayList<>(termBuckets.subList(0, agg.size()));
        }
        return new Truncated<>(visible, otherDocCount);
    }

    /** Bundles the request's bucket-count knobs for the result constructors. */
    static TermsAggregator.BucketCountThresholds thresholds(TermsAggregationBuilder agg) {
        return new TermsAggregator.BucketCountThresholds(agg.minDocCount(), agg.shardMinDocCount(), agg.size(), agg.shardSize());
    }
}
