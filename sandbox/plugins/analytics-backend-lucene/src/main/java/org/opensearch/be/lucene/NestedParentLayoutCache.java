/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Segment-lifetime cache of {@link NestedParentLayout} instances, shared across
 * queries and reader reopens.
 *
 * <p>Follows the {@code BitsetFilterCache} pattern from the server (the cache vanilla
 * OpenSearch keeps for nested parent bitsets): entries are keyed by the leaf's
 * <em>core cache key</em> ({@link IndexReader.CacheHelper#getKey()}), which identifies
 * the segment's immutable data and is stable across {@code DirectoryReader} reopens —
 * a refresh that adds one segment reuses every existing segment's layout and builds
 * only the new one. Eviction is driven by Lucene itself: a closed listener registered
 * on the core removes the entry when the segment's data is closed (merged away, shard
 * closed). No TTL, no LRU, no explicit invalidation — the key cannot outlive the data
 * it describes.
 *
 * <p>Flat segments cache a {@code null} layout (wrapped in a slot) so the per-segment
 * attribute check also runs at most once.
 *
 * <p>Intended ownership: one instance on the backend plugin (node lifetime); handles
 * look layouts up here instead of building their own. Leaves whose reader exposes no
 * core cache helper (exotic wrappers) fall back to an uncached build.
 *
 * @opensearch.internal
 */
final class NestedParentLayoutCache {

    private final ConcurrentHashMap<IndexReader.CacheKey, Slot> byCoreKey = new ConcurrentHashMap<>();

    /**
     * Returns the leaf's layout ({@code null} for flat segments), building and caching
     * it on first access for the segment's core.
     *
     * @param leaf the segment reader
     * @throws IOException if building the layout fails
     */
    NestedParentLayout get(LeafReader leaf) throws IOException {
        IndexReader.CacheHelper cacheHelper = leaf.getCoreCacheHelper();
        if (cacheHelper == null) {
            // No stable identity to key on — build uncached rather than risk a leak.
            return NestedParentLayout.of(leaf);
        }
        try {
            // computeIfAbsent (not racy putIfAbsent): the mapping function runs exactly once
            // per key, so the closed listener is registered exactly once and cannot interleave
            // with a concurrent close leaving a stale entry behind.
            Slot slot = byCoreKey.computeIfAbsent(cacheHelper.getKey(), key -> {
                try {
                    NestedParentLayout layout = NestedParentLayout.of(leaf);
                    cacheHelper.addClosedListener(byCoreKey::remove);
                    return new Slot(layout);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            return slot.layout;
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    /** Number of cached segments (flat and nested alike). Exposed for tests/stats. */
    int size() {
        return byCoreKey.size();
    }

    /** Nullable-value wrapper so flat leaves (layout == null) are cached too. */
    private static final class Slot {
        final NestedParentLayout layout;

        Slot(NestedParentLayout layout) {
            this.layout = layout;
        }
    }
}
