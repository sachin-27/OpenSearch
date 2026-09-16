/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec.iter;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.parquet.bridge.BinaryPageReader;
import org.opensearch.parquet.codec.cache.ColumnPageIndex;
import org.opensearch.parquet.codec.cache.PageCache;

import java.io.IOException;

/**
 * Cache-aware {@link BinaryDocValues} over a single-valued Parquet {@code BYTE_ARRAY} column.
 *
 * <p><b>Presence-lazy:</b> iteration ({@code advanceExact}/{@code nextDoc}/{@code advance}) answers
 * "does this doc have a value?" from the column's per-page statistics whenever they decide it —
 * a fully dense page ({@code nullCount == 0}) is present without decoding anything, and a fully
 * null page is absent without decoding anything (and {@link #advance(int)} leaps it wholesale).
 * Only mixed pages, or columns without statistics, decode during iteration. This matters because
 * presence-only consumers exist: Lucene's {@code FieldExistsQuery} drives the iterator and never
 * calls {@link #binaryValue()}, so an exists query over a dense column touches zero pages.
 *
 * <p>Value bytes are materialised lazily: the page load is deferred to the first
 * {@link #binaryValue()} call after a stats-answered advance. Value-reading consumers
 * (aggregations, derived source) see identical bytes to the eager implementation, at the cost of
 * moving the page load from advance time to first value access.
 *
 * <p>Hot path for values: a presence bit-test plus a CSR-offset slice into the cached page's
 * backing byte buffer. The returned {@link BytesRef} is a single reused instance whose
 * {@code bytes}/{@code offset}/{@code length} are repointed each call (Layer 5). UTF-8 bytes are
 * preserved byte-for-byte.
 */
public final class ParquetBinaryDocValues extends BinaryDocValues {

    /** Page classification, memoized for the page containing the most recent target. */
    private static final byte PAGE_UNKNOWN = 0;
    private static final byte PAGE_DENSE = 1;      // nullCount == 0: every row present
    private static final byte PAGE_ALL_NULL = 2;   // nullCount == rows: every row absent
    private static final byte PAGE_MIXED = 3;      // anything else, incl. unknown stats

    private final BinaryPageReader reader;
    private final int maxDoc;
    /** Per-page stats, or {@code null} when unavailable — then every advance decodes. */
    private final ColumnPageIndex pageIndex;
    /**
     * Column-wide density verdict from the footer's page stats: every row present. A dense
     * iterator matches every doc, so iteration degenerates to arithmetic — advances always
     * succeed without consulting stats, {@link #docIDRunEnd()} declares the whole segment as
     * one run, and {@link #intoBitSet} sets the range in one call. This is how Lucene's own
     * fully-dense {@code DenseBinaryDocValues} lets {@code DenseConjunctionBulkScorer} count
     * matches as ranges instead of stepping every doc. Value reads stay lazy and unchanged.
     */
    private final boolean dense;
    /** Number of matching docs ({@code nonNullRowCount}); {@code maxDoc} when unknown. */
    private final long cost;
    private final BytesRef scratch = new BytesRef();

    private int doc = -1;
    private boolean currentPresent;
    /** Whether {@link #scratch} currently points at {@link #doc}'s bytes. */
    private boolean valueSliced;

    // Memoized page range [pageFirstRow, pageLastRow] and its classification. Starts empty
    // (first lookup always misses). Rows outside the stats' range memoize a single-row MIXED
    // "page" so they take the decode path without consulting stats again.
    private long pageFirstRow = 0;
    private long pageLastRow = -1;
    private byte pageKind = PAGE_UNKNOWN;

    public ParquetBinaryDocValues(BinaryPageReader reader, int maxDoc) {
        this(reader, maxDoc, false, maxDoc);
    }

    /**
     * @param dense the producer's footer-stats verdict that every row of the column is present;
     *              never pass {@code true} for physically repeated columns (their page stats
     *              count leaf values, not docs)
     * @param cost  the number of matching docs ({@code nonNullRowCount}), or {@code maxDoc}
     *              when the footer cannot say — Lucene uses it to elect conjunction leads
     */
    public ParquetBinaryDocValues(BinaryPageReader reader, int maxDoc, boolean dense, long cost) {
        this.reader = reader;
        this.maxDoc = maxDoc;
        this.dense = dense;
        this.cost = cost;
        this.pageIndex = pageIndexOrNull(reader);
    }

    /**
     * Stats are an optimization, never a requirement: a column whose page index cannot be
     * loaded still reads correctly through page decodes, so the failure degrades to the
     * eager path instead of failing the reader open.
     */
    private static ColumnPageIndex pageIndexOrNull(BinaryPageReader reader) {
        try {
            return reader.pageIndex();
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        if (target >= maxDoc) {
            doc = NO_MORE_DOCS;
            currentPresent = false;
            valueSliced = false;
            return false;
        }
        doc = target;
        valueSliced = false;
        if (dense) {
            currentPresent = true;
            return true;
        }
        if (pageIndex != null) {
            if (target < pageFirstRow || target > pageLastRow) {
                classifyPageContaining(target);
            }
            if (pageKind == PAGE_DENSE) {
                currentPresent = true;
                return true;
            }
            if (pageKind == PAGE_ALL_NULL) {
                currentPresent = false;
                return false;
            }
        }
        return decodeAndCheck(target);
    }

    /** Memoizes the row range and classification of the page containing {@code row}. */
    private void classifyPageContaining(long row) {
        int p = pageIndex.pageForRow(row);
        if (p < 0) {
            // Row beyond the stats' row count: treat just this row as mixed so it decodes.
            pageFirstRow = row;
            pageLastRow = row;
            pageKind = PAGE_MIXED;
            return;
        }
        pageFirstRow = pageIndex.firstRowOf(p);
        pageLastRow = pageFirstRow + pageIndex.numRowsOf(p) - 1;
        long nulls = pageIndex.nullCountOf(p);
        if (nulls == 0) {
            pageKind = PAGE_DENSE;
        } else if (pageIndex.isAllNulls(p)) {
            pageKind = PAGE_ALL_NULL;
        } else {
            // Mixed page, or unknown null count (-1): only the presence bits can answer.
            pageKind = PAGE_MIXED;
        }
    }

    /** The pre-stats behavior: ensure the page is resident, test the presence bit, slice. */
    private boolean decodeAndCheck(int target) throws IOException {
        PageCache cache = reader.cache();
        if (cache == null || target < cache.firstRow || target > cache.lastRow) {
            reader.loadPageContaining(target);
            cache = reader.cache();
            if (cache == null) { // Layer 4: page is all-nulls.
                currentPresent = false;
                return false;
            }
        }
        currentPresent = cache.isPresent(target);
        if (currentPresent) {
            slice(cache, target);
        }
        return currentPresent;
    }

    private void slice(PageCache cache, long row) {
        int rel = (int) (row - cache.firstRow);
        int start = cache.byteOffsets[rel];
        int end = cache.byteOffsets[rel + 1];
        scratch.bytes = cache.byteBuf;
        scratch.offset = start;
        scratch.length = end - start;
        valueSliced = true;
    }

    @Override
    public BytesRef binaryValue() throws IOException {
        if (valueSliced == false) {
            // Presence came from page stats; materialise the value now.
            PageCache cache = reader.cache();
            if (cache == null || doc < cache.firstRow || doc > cache.lastRow) {
                reader.loadPageContaining(doc);
                cache = reader.cache();
            }
            if (cache == null) {
                // Stats declared this page dense yet the decode produced nothing — corrupt
                // stats or a reader bug; fail loudly rather than hand back a stale slice.
                throw new IllegalStateException("page stats declared doc " + doc + " present but its page decoded to no values");
            }
            slice(cache, doc);
        }
        return scratch;
    }

    @Override
    public int docID() {
        return doc;
    }

    @Override
    public int nextDoc() throws IOException {
        return advance(doc + 1);
    }

    @Override
    public int advance(int target) throws IOException {
        int d = target;
        while (d < maxDoc) {
            if (advanceExact(d)) {
                return d; // advanceExact already set doc.
            }
            if (pageIndex != null && pageKind == PAGE_ALL_NULL && d >= pageFirstRow && d <= pageLastRow) {
                // The whole page is null: leap to the first row past it instead of walking it.
                d = (int) Math.min(maxDoc, pageLastRow + 1);
            } else {
                d++;
            }
        }
        doc = NO_MORE_DOCS;
        return NO_MORE_DOCS;
    }

    @Override
    public long cost() {
        return cost;
    }

    /**
     * Declares runs of consecutive matching docs so bulk scorers can count ranges arithmetically
     * instead of stepping every doc (the mechanism behind vanilla's ~2 ms binary exists). A dense
     * column is one segment-wide run; otherwise a stats-classified dense page is a page-wide run.
     * The contract only requires a lower bound, so the single-doc default remains correct for
     * mixed pages.
     */
    @Override
    public int docIDRunEnd() throws IOException {
        if (dense) {
            return maxDoc;
        }
        if (pageKind == PAGE_DENSE && doc >= pageFirstRow && doc <= pageLastRow) {
            return (int) Math.min(maxDoc, pageLastRow + 1);
        }
        return super.docIDRunEnd();
    }

    @Override
    public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
        if (dense == false) {
            super.intoBitSet(upTo, bitSet, offset);
            return;
        }
        assert offset <= doc;
        upTo = Math.min(upTo, maxDoc);
        if (upTo > doc) {
            bitSet.set(doc - offset, upTo - offset);
            advance(upTo);
        }
    }
}
