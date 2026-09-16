/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec;

import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.opensearch.parquet.codec.cache.ColumnPageIndex;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link ParquetDocValuesSkipper}: the DocValuesSkipper contract (advance
 * semantics, sentinel doc IDs) and range-driven page skipping over a synthetic page index.
 */
public class ParquetDocValuesSkipperTests extends OpenSearchTestCase {

    /**
     * Three 100-row pages with disjoint value ranges:
     * page 0 rows [0,99] values [10,20], page 1 rows [100,199] values [50,60],
     * page 2 rows [200,299] values [90,100]. No nulls.
     */
    private static ColumnPageIndex threePages() {
        return new ColumnPageIndex(
            new long[] { 0, 100, 200 },
            new long[] { 0, 0, 0 },
            new int[] { 0, 0, 0 },
            new long[] { 0, 0, 0 },
            new long[] { 10, 50, 90 },
            new long[] { 20, 60, 100 },
            300
        );
    }

    /**
     * Lucene validates the (DocValuesType, DocValuesSkipIndexType) pair in the {@link FieldInfo}
     * constructor: a RANGE skip index records min/max and so is only defined for the ordered
     * doc-values kinds (NUMERIC, SORTED_NUMERIC, SORTED, SORTED_SET). {@code binary} and
     * {@code text} map to {@link DocValuesType#BINARY}, which has no ordering, so declaring
     * RANGE for them makes Lucene throw and every query touching the field fail at search time.
     *
     * <p>This walks every mapping type the codec supports and actually constructs the FieldInfo
     * the reader would synthesise, so an incompatible pair fails here instead of at search time.
     */
    public void testSyntheticFieldInfosNeverDeclareAnIncompatibleSkipIndex() {
        List<String> mappingTypes = List.of(
            "boolean",
            "byte",
            "short",
            "integer",
            "long",
            "float",
            "double",
            "date",
            "date_nanos",
            "keyword",
            "ip",
            "text",
            "binary"
        );

        for (String mappingType : mappingTypes) {
            FieldTypeMapping.Mapping mapping = FieldTypeMapping.forType(mappingType);
            DocValuesSkipIndexType declared = ParquetDocValuesLeafReader.skipIndexTypeFor(mapping, false);

            // Must not throw: Lucene rejects an incompatible (DV type, skip index type) pair.
            FieldInfo info = syntheticFieldInfo(mappingType, mapping.singleValued(), declared);
            assertEquals(declared, info.docValuesSkipIndexType());

            if (mapping.singleValued() == DocValuesType.BINARY) {
                assertEquals(
                    "unordered BINARY doc values cannot carry a skip index (" + mappingType + ")",
                    DocValuesSkipIndexType.NONE,
                    declared
                );
            }
        }
    }

    /** The keyword/ip shape does get a skip index: BYTE_ARRAY stats plus an ordered DV type. */
    public void testOrderedByteArrayColumnsStillDeclareRange() {
        for (String mappingType : List.of("keyword", "ip")) {
            FieldTypeMapping.Mapping mapping = FieldTypeMapping.forType(mappingType);
            assertEquals(
                mappingType + " should keep its existence-only skip index",
                DocValuesSkipIndexType.RANGE,
                ParquetDocValuesLeafReader.skipIndexTypeFor(mapping, false)
            );
        }
    }

    /**
     * Multi-valued declarations forfeit the skipper regardless of type: repeated values can span
     * Parquet pages, so OffsetIndex page rows no longer bound Lucene documents and per-page stats
     * would be unsafe. Must hold even for types whose single-valued form declares RANGE.
     */
    public void testMultiValuedDeclarationsNeverDeclareASkipIndex() {
        for (String mappingType : List.of("long", "integer", "keyword", "ip", "boolean")) {
            FieldTypeMapping.Mapping mapping = FieldTypeMapping.forType(mappingType);
            assertEquals(
                mappingType + " multi-valued must not declare a skip index",
                DocValuesSkipIndexType.NONE,
                ParquetDocValuesLeafReader.skipIndexTypeFor(mapping, true)
            );
        }
    }

    private static FieldInfo syntheticFieldInfo(String name, DocValuesType dvType, DocValuesSkipIndexType skipIndexType) {
        return new FieldInfo(
            name,
            0,
            false,
            false,
            true,
            IndexOptions.NONE,
            dvType,
            skipIndexType,
            -1,
            Map.of(),
            0,
            0,
            0,
            0,
            VectorEncoding.FLOAT32,
            VectorSimilarityFunction.EUCLIDEAN,
            false,
            false
        );
    }

    public void testInitialStateAndAdvance() throws Exception {
        DocValuesSkipper skipper = new ParquetDocValuesSkipper(threePages(), 300);
        assertEquals(-1, skipper.minDocID(0));
        assertEquals(-1, skipper.maxDocID(0));
        assertEquals(1, skipper.numLevels());

        skipper.advance(0);
        assertEquals(0, skipper.minDocID(0));
        assertEquals(99, skipper.maxDocID(0));
        assertEquals(10L, skipper.minValue(0));
        assertEquals(20L, skipper.maxValue(0));
        assertEquals(100, skipper.docCount(0));

        skipper.advance(150);
        assertEquals(100, skipper.minDocID(0));
        assertEquals(199, skipper.maxDocID(0));
        assertEquals(50L, skipper.minValue(0));
        assertEquals(60L, skipper.maxValue(0));
    }

    /**
     * The BYTE_ARRAY shape (keyword/ip columns — see
     * {@link #testSyntheticFieldInfosNeverDeclareAnIncompatibleSkipIndex} for why {@code binary}
     * and {@code text} are excluded): the native page-index load reports the unknown sentinel
     * (Long.MIN_VALUE, Long.MAX_VALUE) for min/max — which must intersect every conceivable
     * range so no page is ever wrongly skipped — while null counts are exact, so existence
     * consumers get correct per-page and global doc counts. Page 1 is all-null and must report
     * zero docs with a value.
     */
    public void testExistenceOnlyStatsWithSentinelMinMax() throws Exception {
        ColumnPageIndex byteArrayPages = new ColumnPageIndex(
            new long[] { 0, 100, 200 },
            new long[] { 0, 0, 0 },
            new int[] { 0, 0, 0 },
            new long[] { 5, 100, 0 },                                            // null counts: sparse, all-null, dense
            new long[] { Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE },       // sentinel mins
            new long[] { Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE },       // sentinel maxes
            300
        );
        DocValuesSkipper skipper = new ParquetDocValuesSkipper(byteArrayPages, 300);

        // Global stats: sentinels for values, exact count of docs with a value (95 + 0 + 100).
        assertEquals(Long.MIN_VALUE, skipper.minValue());
        assertEquals(Long.MAX_VALUE, skipper.maxValue());
        assertEquals(195, skipper.docCount());

        // Per-page: sentinel min/max intersect any range (never wrongly skipped)...
        skipper.advance(0);
        assertEquals(Long.MIN_VALUE, skipper.minValue(0));
        assertEquals(Long.MAX_VALUE, skipper.maxValue(0));
        assertEquals(95, skipper.docCount(0));

        // ...and the all-null page reports zero docs with a value, so existence consumers
        // can dismiss it without decoding.
        skipper.advance(150);
        assertEquals(0, skipper.docCount(0));

        skipper.advance(250);
        assertEquals(100, skipper.docCount(0));
    }

    /**
     * Unknown null counts (-1, written by the native load when the ColumnIndex lacks them)
     * must under-claim as zero rather than over-claim: docCount is used for
     * all-docs-have-values fast paths, where overclaiming produces wrong results and
     * underclaiming merely disables an optimization.
     */
    public void testUnknownNullCountUnderclaims() throws Exception {
        ColumnPageIndex unknownNulls = new ColumnPageIndex(
            new long[] { 0 },
            new long[] { 0 },
            new int[] { 0 },
            new long[] { -1 },
            new long[] { Long.MIN_VALUE },
            new long[] { Long.MAX_VALUE },
            100
        );
        DocValuesSkipper skipper = new ParquetDocValuesSkipper(unknownNulls, 100);
        assertEquals(0, skipper.docCount());
        skipper.advance(0);
        assertEquals(0, skipper.docCount(0));
    }

    public void testExhaustion() throws Exception {
        DocValuesSkipper skipper = new ParquetDocValuesSkipper(threePages(), 300);
        skipper.advance(300);
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, skipper.minDocID(0));
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, skipper.maxDocID(0));
    }

    public void testGlobalStats() throws Exception {
        DocValuesSkipper skipper = new ParquetDocValuesSkipper(threePages(), 300);
        assertEquals(10L, skipper.minValue());
        assertEquals(100L, skipper.maxValue());
        assertEquals(300, skipper.docCount());
    }

    /** The base-class range advance must land on the first page intersecting the value range. */
    public void testRangeAdvanceSkipsNonIntersectingPages() throws Exception {
        DocValuesSkipper skipper = new ParquetDocValuesSkipper(threePages(), 300);
        // [55, 58] intersects only page 1.
        skipper.advance(55L, 58L);
        assertEquals(100, skipper.minDocID(0));
        assertEquals(199, skipper.maxDocID(0));

        // A range beyond every page exhausts the skipper.
        DocValuesSkipper skipper2 = new ParquetDocValuesSkipper(threePages(), 300);
        skipper2.advance(200L, 300L);
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, skipper2.minDocID(0));
    }

    /** Pages with the unknown-stats sentinel must intersect every range (never wrongly skipped). */
    public void testUnknownStatsPageIsNeverSkipped() throws Exception {
        ColumnPageIndex idx = new ColumnPageIndex(
            new long[] { 0, 100 },
            new long[] { 0, 0 },
            new int[] { 0, 0 },
            new long[] { 0, -1 },
            new long[] { 10, Long.MIN_VALUE },
            new long[] { 20, Long.MAX_VALUE },
            200
        );
        DocValuesSkipper skipper = new ParquetDocValuesSkipper(idx, 200);
        // Range [500, 600] excludes page 0 (max 20) but must land on the unknown-stats page.
        skipper.advance(500L, 600L);
        assertEquals(100, skipper.minDocID(0));
        assertEquals(199, skipper.maxDocID(0));
        // Unknown null count → docCount under-claims as 0 rather than overclaiming.
        assertEquals(0, skipper.docCount(0));
    }

    public void testDocCountSubtractsNulls() throws Exception {
        ColumnPageIndex idx = new ColumnPageIndex(
            new long[] { 0 },
            new long[] { 0 },
            new int[] { 0 },
            new long[] { 30 },
            new long[] { 1 },
            new long[] { 9 },
            100
        );
        DocValuesSkipper skipper = new ParquetDocValuesSkipper(idx, 100);
        skipper.advance(0);
        assertEquals(70, skipper.docCount(0));
        assertEquals(70, skipper.docCount());
    }
}
