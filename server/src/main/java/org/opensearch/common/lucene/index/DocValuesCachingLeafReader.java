/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.lucene.index;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A {@link LeafReader} view that memoizes doc-values iterators per field, for consumers that
 * read the same fields for many documents in ascending doc ID order — most importantly the
 * derived-source path, which rebuilds {@code _source} field by field for every fetched hit.
 *
 * <p><b>Why this exists.</b> The derived-source fetchers ({@code FieldValueFetcher.fetch})
 * request a field's doc values once per {@code (field, document)} rather than once per
 * {@code (field, leaf)}. On the Lucene codec a {@code get*DocValues} call is a cheap object
 * allocation over already-open segment files, so the pattern was historically harmless. On
 * columnar codecs (e.g. the Parquet DocValues codec) every call opens a fresh native reader —
 * file open plus column-metadata parse — so an N-hit fetch over an M-field mapping performs
 * N×M expensive opens where M would do. Wrapping the leaf handed to the derive path in this
 * view collapses the cost back to one iterator per field per thread.
 *
 * <p><b>Why caching is safe here and not below.</b> Doc-values iterators are stateful and
 * forward-only, so they must never be shared across consumers that interleave doc IDs —
 * which is exactly why the codec-level producer deliberately hands out a dedicated reader per
 * call (concurrent query-phase slices would otherwise corrupt each other's cursors). This
 * view is therefore scoped to a single consumer path with a known access pattern:
 * <ul>
 *   <li>the fetch phase visits hits in ascending doc ID order (see {@code FetchPhase}), and
 *       every fetcher advances only forward within a leaf;</li>
 *   <li>iterators are cached <b>per thread</b>, so if the derive path is ever invoked from
 *       concurrent contexts (e.g. {@code top_hits} under concurrent segment search, or merge
 *       threads via the sequential stored-fields reader), each thread still owns its
 *       iterators exclusively.</li>
 * </ul>
 *
 * <p>Absent fields are memoized as well ({@code null} results), so repeated lookups of a
 * field with no doc values stay cheap.
 *
 * @opensearch.internal
 */
public final class DocValuesCachingLeafReader extends FilterLeafReader {

    /** Per-thread, per-field iterator cache. Values may be {@code null} (field absent). */
    private final ThreadLocal<Map<String, Object>> cache = ThreadLocal.withInitial(HashMap::new);

    private static final Object NULL_SENTINEL = new Object();

    public DocValuesCachingLeafReader(LeafReader in) {
        super(in);
    }

    @SuppressWarnings("unchecked")
    private <T> T cached(String kind, String field, CheckedIOSupplier<T> supplier) throws IOException {
        final Map<String, Object> perThread = cache.get();
        final String key = kind + ":" + field;
        Object value = perThread.get(key);
        if (value == null) {
            T created = supplier.get();
            perThread.put(key, created == null ? NULL_SENTINEL : created);
            return created;
        }
        return value == NULL_SENTINEL ? null : (T) value;
    }

    @Override
    public NumericDocValues getNumericDocValues(String field) throws IOException {
        return cached("n", field, () -> in.getNumericDocValues(field));
    }

    @Override
    public BinaryDocValues getBinaryDocValues(String field) throws IOException {
        return cached("b", field, () -> in.getBinaryDocValues(field));
    }

    @Override
    public SortedDocValues getSortedDocValues(String field) throws IOException {
        return cached("s", field, () -> in.getSortedDocValues(field));
    }

    @Override
    public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
        return cached("sn", field, () -> in.getSortedNumericDocValues(field));
    }

    @Override
    public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
        return cached("ss", field, () -> in.getSortedSetDocValues(field));
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
        return in.getCoreCacheHelper();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return in.getReaderCacheHelper();
    }

    /** A supplier that may perform I/O. */
    @FunctionalInterface
    private interface CheckedIOSupplier<T> {
        T get() throws IOException;
    }
}
