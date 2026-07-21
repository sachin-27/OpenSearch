/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.analytics.spi.DelegatedExpression;
import org.opensearch.be.lucene.index.LuceneDocumentInput;
import org.opensearch.be.lucene.index.LucenePluginBaseTests;
import org.opensearch.be.lucene.index.LuceneWriter;
import org.opensearch.be.lucene.stats.LuceneShardStatsTracker;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.index.engine.dataformat.FlushInput;
import org.opensearch.index.engine.dataformat.WriteResult;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.TermQueryBuilder;

import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * End-to-end tests for the nested docId→logical-row translation in
 * {@link LuceneFilterDelegationHandle} — the read-side half of the cross-format
 * contract (design doc §4.1.2.2).
 *
 * <p>Exercises the REAL delegation flow: a segment written by the nested-mode
 * {@link LuceneWriter}, delegated expressions deserialized from real
 * {@link TermQueryBuilder} bytes, {@code createProvider → createCollector →
 * collectDocs} over the FFM {@link MemorySegment} boundary — asserting the emitted
 * bitset is in LOGICAL ROW space (Parquet rows), regardless of whether the Lucene
 * matches were child docs or root docs.
 *
 * <p>Segment layout (4 logical rows, 9 physical docs):
 * <pre>
 * docId | doc               | row
 *   0   | child alice       |  0
 *   1   | child dave        |  0
 *   2   | root "First"      |  0
 *   3   | root "Second"     |  1   (childless)
 *   4   | child erin        |  2
 *   5   | root "Third"      |  2
 *   6   | child zoe         |  3
 *   7   | child zoe         |  3   (duplicate author — distinct-parent semantics)
 *   8   | root "Fourth"     |  3
 * </pre>
 */
public class LuceneFilterDelegationHandleNestedTests extends LucenePluginBaseTests {

    private static final int AUTHOR_ALICE = 1;
    private static final int TITLE_SECOND = 2;
    private static final int AUTHOR_ZOE = 3;

    private final LuceneDataFormat luceneFormat = new LuceneDataFormat();
    private NIOFSDirectory directory;
    private DirectoryReader reader;
    private LuceneFilterDelegationHandle handle;
    private LuceneReader luceneReader;
    private QueryShardContext context;
    private NamedWriteableRegistry registry;
    private List<DelegatedExpression> expressions;
    private NestedParentLayoutCache sharedCache;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        MappedFieldType author = mockKeywordField("comments.author");
        MappedFieldType title = mockKeywordField("title");

        // --- Write the nested segment through the real writer ---
        Path baseDir = createTempDir();
        try (
            LuceneWriter writer = new LuceneWriter(
                1L,
                0L,
                luceneFormat,
                baseDir,
                null,
                Codec.getDefault(),
                null,
                ConcurrentHashMap.newKeySet(),
                new LuceneShardStatsTracker(),
                true
            )
        ) {
            addLogicalDoc(writer, author, title, 0L, "First", "alice", "dave");
            addLogicalDoc(writer, author, title, 1L, "Second");
            addLogicalDoc(writer, author, title, 2L, "Third", "erin");
            addLogicalDoc(writer, author, title, 3L, "Fourth", "zoe", "zoe");
            WriterFileSet wfs = writer.flush(FlushInput.EMPTY).getWriterFileSet(luceneFormat).orElseThrow();
            directory = new NIOFSDirectory(Path.of(wfs.directory()));
            reader = DirectoryReader.open(directory);
        }

        // --- Build the real handle around it ---
        String segmentName = ((SegmentReader) reader.leaves().get(0).reader()).getSegmentInfo().info.name;
        luceneReader = new LuceneReader(reader, Map.of(1L, segmentName));

        context = mock(QueryShardContext.class);
        when(context.searcher()).thenReturn(new IndexSearcher(reader));
        when(context.fieldMapper("comments.author")).thenReturn(author);
        when(context.fieldMapper("title")).thenReturn(title);

        registry = new NamedWriteableRegistry(
            List.of(new NamedWriteableRegistry.Entry(QueryBuilder.class, TermQueryBuilder.NAME, TermQueryBuilder::new))
        );

        expressions = List.of(
            expression(AUTHOR_ALICE, new TermQueryBuilder("comments.author", "alice")),
            expression(TITLE_SECOND, new TermQueryBuilder("title", "Second")),
            expression(AUTHOR_ZOE, new TermQueryBuilder("comments.author", "zoe"))
        );

        sharedCache = new NestedParentLayoutCache();
        handle = newHandle();
    }

    private LuceneFilterDelegationHandle newHandle() {
        return new LuceneFilterDelegationHandle(
            expressions,
            context,
            luceneReader,
            mock(CatalogSnapshot.class),
            registry,
            () -> false,
            sharedCache
        );
    }

    @Override
    public void tearDown() throws Exception {
        handle.close();
        reader.close();
        directory.close();
        super.tearDown();
    }

    /** A child-field match (alice, docId 0) must surface as its PARENT's logical row (0). */
    public void testChildMatchEmitsLogicalRowBit() {
        long[] bits = collect(AUTHOR_ALICE, 0, 4);
        assertEquals("only row 0 set", 0b0001L, bits[0]);
    }

    /** A root-field match ("Second", docId 3) must surface as its logical row (1), not its docId. */
    public void testRootMatchEmitsLogicalRowBit() {
        long[] bits = collect(TITLE_SECOND, 0, 4);
        assertEquals("only row 1 set — docId 3 would be bit 3", 0b0010L, bits[0]);
    }

    /** Two child matches in one block (zoe × 2) must set ONE bit — distinct-parent semantics. */
    public void testDistinctParentSemantics() {
        long[] bits = collect(AUTHOR_ZOE, 0, 4);
        assertEquals("both zoe docs collapse to row 3", 0b1000L, bits[0]);
    }

    /**
     * Windows are row-space: collecting rows [2,4) must exclude alice's block (row 0)
     * and place zoe's block at bit (3 − 2) = 1.
     */
    public void testRowSpaceWindow() {
        assertEquals("alice's row 0 outside window [2,4)", 0L, collect(AUTHOR_ALICE, 2, 4)[0]);
        assertEquals("zoe's row 3 at window-relative bit 1", 0b10L, collect(AUTHOR_ZOE, 2, 4)[0]);
    }

    /**
     * Production topology: per-query handles share the plugin-owned layout cache. A second
     * handle must produce identical row bits, served from the single cached layout entry
     * built by the first handle (same segment core → one entry, before and after).
     */
    public void testSharedCacheAcrossHandles() throws Exception {
        assertEquals("row bits via first handle", 0b0001L, collect(handle, AUTHOR_ALICE, 0, 4)[0]);
        assertEquals("first handle's collect populated the shared cache", 1, sharedCache.size());

        LuceneFilterDelegationHandle second = newHandle();
        try {
            assertEquals("child match via second handle", 0b0001L, collect(second, AUTHOR_ALICE, 0, 4)[0]);
            assertEquals("root match via second handle", 0b0010L, collect(second, TITLE_SECOND, 0, 4)[0]);
            assertEquals("distinct-parent via second handle", 0b1000L, collect(second, AUTHOR_ZOE, 0, 4)[0]);
            assertEquals("same segment core — still exactly one cached layout", 1, sharedCache.size());
        } finally {
            second.close();
        }
    }

    /** createCollector must validate bounds against the ROW count (4), not maxDoc (9). */
    public void testCollectorBoundsAreRowSpace() {
        int providerKey = handle.createProvider(AUTHOR_ALICE);
        assertTrue(providerKey > 0);
        // Rows [0,4) is the full valid row range — would already exceed nothing; the
        // point is it does NOT allow docId-space bounds like [0,9).
        assertTrue(handle.createCollector(providerKey, 1L, 0, 4) > 0);
        expectThrows(AssertionError.class, () -> handle.createCollector(providerKey, 1L, 0, 9));
    }

    // ---------- helpers ----------

    private void addLogicalDoc(
        LuceneWriter writer,
        MappedFieldType author,
        MappedFieldType title,
        long rowId,
        String titleValue,
        String... authors
    ) throws Exception {
        LuceneDocumentInput input = new LuceneDocumentInput(new LuceneFieldFactoryRegistry(), true);
        input.addField(title, titleValue);
        for (String a : authors) {
            input.beginChild("comments");
            input.addField(author, a);
            input.endChild();
        }
        input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, rowId);
        assertTrue(writer.addDoc(input) instanceof WriteResult.Success);
    }

    private static DelegatedExpression expression(int annotationId, QueryBuilder builder) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeNamedWriteable(builder);
            return new DelegatedExpression(annotationId, "lucene", org.opensearch.core.common.bytes.BytesReference.toBytes(out.bytes()));
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    /** Runs the full provider→collector→collectDocs flow and returns the raw bitset words. */
    private long[] collect(int annotationId, int minRow, int maxRow) {
        return collect(handle, annotationId, minRow, maxRow);
    }

    private long[] collect(LuceneFilterDelegationHandle target, int annotationId, int minRow, int maxRow) {
        int providerKey = target.createProvider(annotationId);
        assertTrue("provider must be created", providerKey > 0);
        int collectorKey = target.createCollector(providerKey, 1L, minRow, maxRow);
        assertTrue("collector must be created", collectorKey > 0);
        int span = maxRow - minRow;
        long[] words = new long[(span + 63) >>> 6];
        MemorySegment out = MemorySegment.ofArray(words);
        int wordCount = target.collectDocs(collectorKey, minRow, maxRow, out);
        assertEquals((span + 63) >>> 6, wordCount);
        target.releaseCollector(collectorKey);
        target.releaseProvider(providerKey);
        return words;
    }
}
