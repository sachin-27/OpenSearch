/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec.iter;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.opensearch.parquet.bridge.BinaryPageReader;
import org.opensearch.parquet.codec.cache.ColumnPageIndex;
import org.opensearch.parquet.codec.cache.PageCache;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;

/**
 * Unit tests for the presence-lazy {@link ParquetBinaryDocValues}: iteration answers presence
 * from per-page statistics without decoding pages (the exists fast path), values decode lazily
 * on first {@link ParquetBinaryDocValues#binaryValue()}, and columns without statistics keep
 * the eager decode behavior.
 */
public class ParquetBinaryDocValuesTests extends OpenSearchTestCase {

    /** A page of test data: its first global row and one entry per row ({@code null} = absent). */
    private record PageDef(long firstRow, byte[][] rows) {
        long lastRow() {
            return firstRow + rows.length - 1;
        }

        long nullCount() {
            long n = 0;
            for (byte[] row : rows) {
                if (row == null) {
                    n++;
                }
            }
            return n;
        }

        boolean allNull() {
            return nullCount() == rows.length;
        }
    }

    /**
     * Serves {@link PageDef}s the way {@code DataFusionColumnReader} serves decoded batches,
     * counting loads. An all-null page loads to a {@code null} cache, mirroring the real
     * reader's "Layer 4" behavior. Statistics are served separately so tests can withhold
     * them or falsify null counts.
     */
    private static final class FakePageReader implements BinaryPageReader {
        private final List<PageDef> pages;
        private final ColumnPageIndex stats;
        private final IOException statsFailure;
        private PageCache current;
        int loads;

        FakePageReader(List<PageDef> pages, ColumnPageIndex stats) {
            this(pages, stats, null);
        }

        FakePageReader(List<PageDef> pages, ColumnPageIndex stats, IOException statsFailure) {
            this.pages = pages;
            this.stats = stats;
            this.statsFailure = statsFailure;
        }

        @Override
        public PageCache cache() {
            return current;
        }

        @Override
        public ColumnPageIndex pageIndex() throws IOException {
            if (statsFailure != null) {
                throw statsFailure;
            }
            return stats;
        }

        @Override
        public void loadPageContaining(long row) {
            loads++;
            for (PageDef page : pages) {
                if (row >= page.firstRow() && row <= page.lastRow()) {
                    current = page.allNull() ? null : toCache(page);
                    return;
                }
            }
            throw new AssertionError("no page defined for row " + row);
        }

        @Override
        public byte[][] readRepeatedBytesAtRow(long row) {
            throw new UnsupportedOperationException("single-valued tests");
        }

        private static PageCache toCache(PageDef page) {
            PageCache cache = new PageCache();
            cache.firstRow = page.firstRow();
            cache.lastRow = page.lastRow();
            int rows = page.rows().length;
            cache.byteOffsets = new int[rows + 1];
            int total = 0;
            for (int i = 0; i < rows; i++) {
                cache.byteOffsets[i] = total;
                if (page.rows()[i] != null) {
                    total += page.rows()[i].length;
                }
            }
            cache.byteOffsets[rows] = total;
            cache.byteBuf = new byte[total];
            int pos = 0;
            for (byte[] row : page.rows()) {
                if (row != null) {
                    System.arraycopy(row, 0, cache.byteBuf, pos, row.length);
                    pos += row.length;
                }
            }
            if (page.nullCount() > 0) {
                long[] bits = new long[(rows + 63) / 64];
                for (int i = 0; i < rows; i++) {
                    if (page.rows()[i] != null) {
                        bits[i >>> 6] |= 1L << (i & 63);
                    }
                }
                cache.presenceBits = MemorySegment.ofArray(bits);
                cache.presenceBitOffset = 0;
            }
            return cache;
        }
    }

    /** Builds the stats the native page-index load would report, with optional null-count override. */
    private static ColumnPageIndex statsFor(List<PageDef> pages, long... nullCountOverride) {
        int n = pages.size();
        long[] firstRows = new long[n];
        long[] nulls = new long[n];
        long totalRows = 0;
        for (int i = 0; i < n; i++) {
            firstRows[i] = pages.get(i).firstRow();
            nulls[i] = nullCountOverride.length > 0 ? nullCountOverride[i] : pages.get(i).nullCount();
            totalRows = pages.get(i).lastRow() + 1;
        }
        return new ColumnPageIndex(firstRows, new long[n], new int[n], nulls, new long[n], new long[n], totalRows);
    }

    private static byte[] value(int seed, int len) {
        byte[] bytes = new byte[len];
        for (int i = 0; i < len; i++) {
            bytes[i] = (byte) (seed + i);
        }
        return bytes;
    }

    private static PageDef densePage(long firstRow, int rows) {
        byte[][] values = new byte[rows][];
        for (int i = 0; i < rows; i++) {
            values[i] = value((int) firstRow + i, 1 + ((int) firstRow + i) % 5);
        }
        return new PageDef(firstRow, values);
    }

    private static PageDef allNullPage(long firstRow, int rows) {
        return new PageDef(firstRow, new byte[rows][]);
    }

    private static void assertValue(ParquetBinaryDocValues dv, int doc) throws IOException {
        BytesRef actual = dv.binaryValue();
        byte[] expected = value(doc, 1 + doc % 5);
        assertEquals("value length for doc " + doc, expected.length, actual.length);
        for (int i = 0; i < expected.length; i++) {
            assertEquals("byte " + i + " of doc " + doc, expected[i], actual.bytes[actual.offset + i]);
        }
    }

    /** The exists fast path: presence over dense pages costs zero page decodes. */
    public void testDensePagesAnswerPresenceWithoutDecoding() throws IOException {
        List<PageDef> pages = List.of(densePage(0, 4), densePage(4, 4));
        FakePageReader reader = new FakePageReader(pages, statsFor(pages));
        ParquetBinaryDocValues dv = new ParquetBinaryDocValues(reader, 8);

        for (int doc = 0; doc < 8; doc++) {
            assertTrue("doc " + doc + " must be present", dv.advanceExact(doc));
        }
        assertEquals("presence over dense pages must not decode any page", 0, reader.loads);
    }

    /** All-null pages answer absent with zero decodes — including repeated probes into the same page. */
    public void testAllNullPageAnswersAbsentWithoutDecoding() throws IOException {
        List<PageDef> pages = List.of(allNullPage(0, 4));
        FakePageReader reader = new FakePageReader(pages, statsFor(pages));
        ParquetBinaryDocValues dv = new ParquetBinaryDocValues(reader, 4);

        for (int doc = 0; doc < 4; doc++) {
            assertFalse("doc " + doc + " must be absent", dv.advanceExact(doc));
        }
        assertEquals("absence over an all-null page must not decode", 0, reader.loads);
    }

    /** Values materialise on first {@code binaryValue()}, and a resident page is not reloaded. */
    public void testBinaryValueLazilyDecodesOnce() throws IOException {
        List<PageDef> pages = List.of(densePage(0, 4));
        FakePageReader reader = new FakePageReader(pages, statsFor(pages));
        ParquetBinaryDocValues dv = new ParquetBinaryDocValues(reader, 4);

        assertTrue(dv.advanceExact(2));
        assertEquals("advance alone must not decode", 0, reader.loads);

        assertValue(dv, 2);
        assertEquals("first value read decodes the page", 1, reader.loads);
        assertValue(dv, 2);
        assertEquals("repeated reads reuse the slice", 1, reader.loads);

        // Another doc in the now-resident page: no further decode even for its value.
        assertTrue(dv.advanceExact(3));
        assertValue(dv, 3);
        assertEquals("resident page serves subsequent values", 1, reader.loads);
    }

    /** {@code advance} leaps a fully-null page instead of probing it row by row. */
    public void testAdvanceLeapsAllNullPage() throws IOException {
        List<PageDef> pages = List.of(densePage(0, 4), allNullPage(4, 4), densePage(8, 4));
        FakePageReader reader = new FakePageReader(pages, statsFor(pages));
        ParquetBinaryDocValues dv = new ParquetBinaryDocValues(reader, 12);

        assertEquals("advance(4) must land on the first doc past the null page", 8, dv.advance(4));
        assertEquals(8, dv.docID());
        assertEquals("the leap must not decode anything", 0, reader.loads);
    }

    /** An exists-style full scan (nextDoc loop) over mixed page kinds decodes nothing. */
    public void testExistsStyleScanTouchesNoPages() throws IOException {
        List<PageDef> pages = List.of(densePage(0, 4), allNullPage(4, 4), densePage(8, 4));
        FakePageReader reader = new FakePageReader(pages, statsFor(pages));
        ParquetBinaryDocValues dv = new ParquetBinaryDocValues(reader, 12);

        List<Integer> matches = new ArrayList<>();
        for (int doc = dv.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = dv.nextDoc()) {
            matches.add(doc);
        }
        assertEquals(List.of(0, 1, 2, 3, 8, 9, 10, 11), matches);
        assertEquals("presence-only scan must not decode any page", 0, reader.loads);
    }

    /** Mixed pages decode (statistics cannot answer) and the presence bits are honoured. */
    public void testMixedPageDecodesAndHonoursPresence() throws IOException {
        byte[][] rows = new byte[][] { value(0, 1), null, value(2, 3), null };
        List<PageDef> pages = List.of(new PageDef(0, rows));
        FakePageReader reader = new FakePageReader(pages, statsFor(pages));
        ParquetBinaryDocValues dv = new ParquetBinaryDocValues(reader, 4);

        assertTrue(dv.advanceExact(0));
        assertEquals("mixed page must decode to answer presence", 1, reader.loads);
        assertValue(dv, 0);

        assertFalse(dv.advanceExact(1));
        assertTrue(dv.advanceExact(2));
        assertValue(dv, 2);
        assertFalse(dv.advanceExact(3));
        assertEquals("one decode serves the whole resident page", 1, reader.loads);
    }

    /** Unknown null counts (-1) must be treated as mixed: decode rather than trust. */
    public void testUnknownNullCountFallsBackToDecode() throws IOException {
        List<PageDef> pages = List.of(densePage(0, 4));
        FakePageReader reader = new FakePageReader(pages, statsFor(pages, -1L));
        ParquetBinaryDocValues dv = new ParquetBinaryDocValues(reader, 4);

        assertTrue(dv.advanceExact(0));
        assertEquals("unknown stats must decode to answer presence", 1, reader.loads);
        assertValue(dv, 0);
    }

    /** Without statistics the reader keeps the original eager-decode behavior. */
    public void testNoStatsFallsBackToEagerDecode() throws IOException {
        List<PageDef> pages = List.of(densePage(0, 4));
        FakePageReader reader = new FakePageReader(pages, null);
        ParquetBinaryDocValues dv = new ParquetBinaryDocValues(reader, 4);

        assertTrue(dv.advanceExact(1));
        assertEquals("without stats, presence requires the decode", 1, reader.loads);
        assertValue(dv, 1);
    }

    /** A failing statistics load degrades to the eager path instead of failing the reader. */
    public void testStatsLoadFailureDegradesToEagerPath() throws IOException {
        List<PageDef> pages = List.of(densePage(0, 4));
        FakePageReader reader = new FakePageReader(pages, null, new IOException("page index unavailable"));
        ParquetBinaryDocValues dv = new ParquetBinaryDocValues(reader, 4);

        assertTrue(dv.advanceExact(0));
        assertEquals(1, reader.loads);
        assertValue(dv, 0);
    }

    /** Rows past the statistics' row count take the decode path (defensive, not silent). */
    public void testRowBeyondStatsDecodes() throws IOException {
        // Stats only describe rows [0,3]; the fake still serves a page for rows [4,7].
        List<PageDef> statsPages = List.of(densePage(0, 4));
        List<PageDef> allPages = List.of(densePage(0, 4), densePage(4, 4));
        FakePageReader reader = new FakePageReader(allPages, statsFor(statsPages));
        ParquetBinaryDocValues dv = new ParquetBinaryDocValues(reader, 8);

        assertTrue(dv.advanceExact(5));
        assertEquals("rows beyond stats must decode", 1, reader.loads);
        assertValue(dv, 5);
    }

    /** Past {@code maxDoc} the iterator is exhausted regardless of stats. */
    public void testExhaustion() throws IOException {
        List<PageDef> pages = List.of(densePage(0, 4));
        FakePageReader reader = new FakePageReader(pages, statsFor(pages));
        ParquetBinaryDocValues dv = new ParquetBinaryDocValues(reader, 4);

        assertFalse(dv.advanceExact(4));
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.docID());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.advance(4));
        assertEquals(0, reader.loads);
    }
}
