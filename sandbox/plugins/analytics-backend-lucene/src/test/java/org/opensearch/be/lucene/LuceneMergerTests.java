/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MergeIndexWriter;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.opensearch.be.lucene.merge.LuceneMerger;
import org.opensearch.be.lucene.stats.LuceneShardStatsTracker;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.MergeInput;
import org.opensearch.index.engine.dataformat.MergeResult;
import org.opensearch.index.engine.dataformat.PackedRowIdMapping;
import org.opensearch.index.engine.dataformat.RowIdMapping;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.mapper.NestedPathFieldMapper;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.opensearch.be.lucene.index.LuceneWriter.NESTED_BLOCKS_ATTRIBUTE;
import static org.opensearch.be.lucene.index.LuceneWriter.NESTED_PARENT_FIELD;
import static org.opensearch.be.lucene.index.LuceneWriter.WRITER_GENERATION_ATTRIBUTE;

/**
 * End-to-end tests for {@link LuceneMerger}.
 *
 * <p>These tests create real Lucene segments with {@code writer_generation} attributes
 * and {@code ___row_id} doc values, then exercise the merge path and validate the output.
 */
public class LuceneMergerTests extends OpenSearchTestCase {

    private static final String ROW_ID_FIELD = DocumentInput.ROW_ID_FIELD;

    private MergeIndexWriter writer;
    private Directory directory;
    private Path dataPath;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        dataPath = createTempDir();
        directory = NIOFSDirectory.open(dataPath);
        IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
        iwc.setMergeScheduler(new SerialMergeScheduler());
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        iwc.setIndexSort(new Sort(new SortedNumericSortField(ROW_ID_FIELD, SortField.Type.LONG)));
        writer = new MergeIndexWriter(directory, iwc);
    }

    @Override
    public void tearDown() throws Exception {
        if (writer != null) {
            writer.close();
        }
        if (directory != null) {
            directory.close();
        }
        super.tearDown();
    }

    // ========== Test Cases ==========

    /**
     * Merge with empty input returns empty result without error.
     */
    public void testMergeWithEmptyInput() throws IOException {
        LuceneMerger merger = new LuceneMerger(writer, new LuceneDataFormat(), dataPath, new LuceneShardStatsTracker());
        MergeInput input = MergeInput.builder().segments(List.of()).newWriterGeneration(99L).build();

        MergeResult result = merger.merge(input);
        assertNotNull(result);
        assertTrue(result.getMergedWriterFileSet().isEmpty());
    }

    /**
     * Merge with RowIdMapping remaps ___row_id doc values AND reorders documents.
     * Verifies that the merged segment has documents sorted by remapped row IDs
     * and that stored fields follow the documents to their new positions.
     *
     * The mapping preserves within-segment order (ascending remapped values within
     * each generation), matching real Parquet merge behavior where rows within each
     * source file maintain their relative order in the merged output.
     */
    public void testMergeWithRowIdMappingRemapsRowIds() throws IOException {
        // gen=1: doc_0 (rowId=0), doc_1 (rowId=1), doc_2 (rowId=2)
        // gen=2: doc_3 (rowId=0), doc_4 (rowId=1)
        writeSegment(writer, 1L, 0, 3);
        writeSegment(writer, 2L, 3, 2);
        writer.commit();

        assertEquals(5, writer.getDocStats().numDocs);

        // Mapping interleaves segments but preserves within-segment order:
        // gen=1: 0→0, 1→2, 2→4 (ascending within gen=1)
        // gen=2: 0→1, 1→3 (ascending within gen=2)
        //
        // This simulates a Parquet merge that interleaves rows from two files:
        // merged output: gen1-row0, gen2-row0, gen1-row1, gen2-row1, gen1-row2
        //
        // Expected sorted order by remapped rowId:
        // position 0: rowId=0 → doc_0 (gen=1, original rowId=0)
        // position 1: rowId=1 → doc_3 (gen=2, original rowId=0)
        // position 2: rowId=2 → doc_1 (gen=1, original rowId=1)
        // position 3: rowId=3 → doc_4 (gen=2, original rowId=1)
        // Build a PackedRowIdMapping for the interleaved merge:
        // gen=1 has 3 rows (offsets 0,1,2), gen=2 has 2 rows (offsets 3,4)
        // position 0: rowId=0 → doc_0 (gen=1, original rowId=0)
        // position 1: rowId=1 → doc_a (gen=2, original rowId=0)
        // position 2: rowId=2 → doc_1 (gen=1, original rowId=1)
        // position 3: rowId=3 → doc_b (gen=2, original rowId=1)
        // position 4: rowId=4 → doc_2 (gen=1, original rowId=2)
        long[] mappingArray = new long[] { 0, 2, 4, 1, 3 };
        Map<Long, Integer> genOffsets = Map.of(1L, 0, 2L, 3);
        Map<Long, Integer> genSizes = Map.of(1L, 3, 2L, 2);
        RowIdMapping rowIdMapping = new PackedRowIdMapping(mappingArray, genOffsets, genSizes);

        LuceneMerger merger = new LuceneMerger(writer, new LuceneDataFormat(), dataPath, new LuceneShardStatsTracker());
        SegmentInfos infos = getSegmentInfos(writer);
        List<Segment> segments = buildSegments(infos);

        MergeInput input = MergeInput.builder().segments(segments).rowIdMapping(rowIdMapping).newWriterGeneration(10L).build();

        MergeResult result = merger.merge(input);
        assertNotNull(result);
        assertTrue(result.rowIdMapping().isPresent());

        writer.commit();

        // Expected: documents sorted by remapped rowId, with correct stored fields
        String[] expectedIds = { "doc_0", "doc_3", "doc_1", "doc_4", "doc_2" };
        long[] expectedRowIds = { 0, 1, 2, 3, 4 };

        try (DirectoryReader reader = DirectoryReader.open(writer)) {
            // Find the merged segment (should be the largest leaf after old segments are deleted)
            LeafReaderContext mergedLeaf = null;
            for (LeafReaderContext ctx : reader.leaves()) {
                if (mergedLeaf == null || ctx.reader().maxDoc() > mergedLeaf.reader().maxDoc()) {
                    mergedLeaf = ctx;
                }
            }
            assertNotNull("Should have at least one leaf", mergedLeaf);
            assertEquals("Merged segment should have 5 docs", 5, mergedLeaf.reader().maxDoc());

            SortedNumericDocValues rowIdDV = mergedLeaf.reader().getSortedNumericDocValues(ROW_ID_FIELD);
            assertNotNull("___row_id doc values should exist", rowIdDV);

            for (int i = 0; i < 5; i++) {
                // Verify ___row_id value
                assertTrue("Should have doc values for doc " + i, rowIdDV.advanceExact(i));
                long actualRowId = rowIdDV.nextValue();
                assertEquals("Doc at position " + i + " should have ___row_id=" + expectedRowIds[i], expectedRowIds[i], actualRowId);

                // Verify stored field follows the document
                Document doc = mergedLeaf.reader().storedFields().document(i);
                assertEquals("Doc at position " + i + " should be " + expectedIds[i], expectedIds[i], doc.get("id"));
            }
        }
    }

    /**
     * Merge preserves keyword, numeric, and stored field data integrity.
     *
     * <p>Uses an identity {@link RowIdMapping} so the merge exercises the real
     * secondary-format path; the assertions focus on field-data survival rather
     * than on row-id remapping (which is covered by
     * {@link #testMergeWithRowIdMappingRemapsRowIds()}).
     */
    public void testMergePreservesFieldDataIntegrity() throws IOException {
        writeSegmentWithRichFields(writer, 1L, 0, 3);
        writeSegmentWithRichFields(writer, 2L, 3, 2);
        writer.commit();

        LuceneMerger merger = new LuceneMerger(writer, new LuceneDataFormat(), dataPath, new LuceneShardStatsTracker());
        SegmentInfos infos = getSegmentInfos(writer);
        List<Segment> segments = buildSegments(infos);

        // Identity mapping — writeSegmentWithRichFields already writes globally-unique row IDs
        // (0,1,2 in gen=1 and 3,4 in gen=2), so returning the original row ID is well-formed.
        long[] identityArray = new long[] { 0, 1, 2, 3, 4 };
        Map<Long, Integer> identityOffsets = Map.of(1L, 0, 2L, 3);
        Map<Long, Integer> identitySizes = Map.of(1L, 3, 2L, 2);
        RowIdMapping identityMapping = new PackedRowIdMapping(identityArray, identityOffsets, identitySizes);

        MergeInput input = MergeInput.builder().segments(segments).rowIdMapping(identityMapping).newWriterGeneration(10L).build();
        merger.merge(input);
        writer.commit();

        try (DirectoryReader reader = DirectoryReader.open(writer)) {
            assertTrue("Should have at least 5 docs after merge", reader.numDocs() >= 5);
            for (LeafReaderContext ctx : reader.leaves()) {
                for (int i = 0; i < ctx.reader().maxDoc(); i++) {
                    Document doc = ctx.reader().storedFields().document(i);
                    String id = doc.get("id");
                    assertNotNull("id field missing", id);
                    String storedData = doc.get("data");
                    assertNotNull("stored data field missing for " + id, storedData);
                    assertTrue("data should contain the doc id", storedData.contains(id));
                    String numericStr = doc.get("score");
                    assertNotNull("stored numeric field missing for " + id, numericStr);
                }
            }
        }
    }

    /**
     * Constructor with null IndexWriter throws IllegalArgumentException.
     */
    public void testConstructorWithNullIndexWriterThrows() {
        expectThrows(
            IllegalArgumentException.class,
            () -> new LuceneMerger(null, new LuceneDataFormat(), Path.of("."), new LuceneShardStatsTracker())
        );
    }

    /**
     * Regression guard for the {@code writer_generation} stamping path. Verifies two things:
     * <ol>
     *   <li>The merged segment carries the {@code writer_generation} attribute in the live
     *       {@link SegmentInfos} immediately after the merge completes — catches regressions
     *       where the {@link org.opensearch.be.lucene.merge.RowIdRemappingOneMerge#setMergeInfo}
     *       override stops running.</li>
     *   <li>The attribute is <em>persisted</em> to the {@code .si} file and survives a writer
     *       reopen — catches regressions that would revert to an in-memory-only stamp (e.g.
     *       moving the {@code putAttribute} call back to {@code LuceneMerger#merge} after
     *       {@code executeMerge}, which runs too late to influence the codec write).</li>
     * </ol>
     */
    public void testMergedSegmentWriterGenerationIsPersisted() throws IOException {
        long newGeneration = 99L;

        writeSegment(writer, 1L, 0, 3);
        writeSegment(writer, 2L, 3, 2);
        writer.commit();

        LuceneMerger merger = new LuceneMerger(writer, new LuceneDataFormat(), dataPath, new LuceneShardStatsTracker());
        SegmentInfos infos = getSegmentInfos(writer);
        List<Segment> segments = buildSegments(infos);

        RowIdMapping identity = new RowIdMapping() {
            @Override
            public long getNewRowId(long oldId, long oldGeneration) {
                return oldId;
            }

            @Override
            public long getOldRowId(long newId) {
                return newId;
            }

            @Override
            public boolean isNewToOldSupported() {
                return true;
            }

            @Override
            public int size() {
                return 0;
            }
        };
        MergeInput input = MergeInput.builder().segments(segments).rowIdMapping(identity).newWriterGeneration(newGeneration).build();

        merger.merge(input);

        // Assertion 1: attribute is set on the live (in-memory) merged SegmentCommitInfo
        SegmentCommitInfo mergedInMemory = findSegmentWithGeneration(getSegmentInfos(writer), newGeneration);
        assertNotNull("Merged segment must carry writer_generation=" + newGeneration + " in the live SegmentInfos", mergedInMemory);

        // Persist to disk, close the writer, and reopen against the same directory to verify
        // the attribute survives — i.e. it was stamped before Lucene wrote the .si file.
        writer.commit();
        writer.close();

        SegmentInfos onDisk = SegmentInfos.readLatestCommit(directory);
        SegmentCommitInfo mergedAfterReopen = findSegmentWithGeneration(onDisk, newGeneration);
        assertNotNull(
            "Merged segment must carry writer_generation="
                + newGeneration
                + " after reopen — the attribute must be written to the .si file during the merge, "
                + "not stamped in-memory after executeMerge returns",
            mergedAfterReopen
        );
        assertEquals(
            "Persisted writer_generation attribute must match the one passed to MergeInput#newWriterGeneration",
            String.valueOf(newGeneration),
            mergedAfterReopen.info.getAttribute(WRITER_GENERATION_ATTRIBUTE)
        );

        // Reopen for the tearDown close() to be a no-op on an already-closed writer.
        IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
        iwc.setMergeScheduler(new SerialMergeScheduler());
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        iwc.setIndexSort(new Sort(new SortedNumericSortField(ROW_ID_FIELD, SortField.Type.LONG)));
        writer = new MergeIndexWriter(directory, iwc);
    }

    /**
     * End-to-end merge of nested-blocks segments under Scheme C (sequential row ids):
     * verifies that the merge recovers each source segment's block structure from
     * {@code _nested_path}, expands the primary's logical-row mapping into final
     * per-doc positions, that the index sort lays merged blocks out contiguously
     * (children first, parent last), that stored fields follow their documents, that
     * the merged segment's row ids come out sequential 0..maxDoc-1 (I1 restored), that
     * the merged segment inherits the {@code nested_blocks} attribute, and that the
     * merged file set reports LOGICAL rows (one per block), not physical docs.
     *
     * <p>Sources (both with {@code __row_id__ == docId}):
     * <ul>
     *   <li>gen=1: one block (2 children + parent, logical row 0) + one childless
     *       parent (logical row 1) — 4 physical docs, 2 logical rows</li>
     *   <li>gen=2: one block (1 child + parent, logical row 0) — 2 physical docs, 1 logical row</li>
     * </ul>
     *
     * <p>Interleaving logical mapping: gen1 {0→0, 1→2}, gen2 {0→1}. Block sizes by new
     * logical row: {0:3, 1:2, 2:1} → block starts {0, 3, 5}. Expected merged doc order:
     * [g1b_c0, g1b_c1, g1b_p, g2b_c0, g2b_p, g1_p1] with row ids [0..5] — the gen2
     * block lands between gen1's block and gen1's childless parent.
     */
    public void testMergeExpandsLogicalMappingAndPreservesBlocks() throws IOException {
        // The shared harness writer has no parent field configured; nested blocks + index
        // sort require IndexWriterConfig#setParentField, so this test uses its own writer.
        Path nestedPath = createTempDir();
        try (Directory nestedDir = NIOFSDirectory.open(nestedPath)) {
            IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
            iwc.setMergeScheduler(new SerialMergeScheduler());
            iwc.setMergePolicy(NoMergePolicy.INSTANCE);
            iwc.setIndexSort(new Sort(new SortedNumericSortField(ROW_ID_FIELD, SortField.Type.LONG)));
            iwc.setParentField(NESTED_PARENT_FIELD);
            try (MergeIndexWriter nestedWriter = new MergeIndexWriter(nestedDir, iwc)) {
                // gen=1: block [g1b_c0=0, g1b_c1=1, g1b_p=2] + childless g1_p1=3.
                // Sequential ids (== docId); children marked by _nested_path, which is how
                // the merge recovers the block structure.
                List<Document> gen1Block = new ArrayList<>();
                gen1Block.add(childDoc("g1b_c0", 0));
                gen1Block.add(childDoc("g1b_c1", 1));
                gen1Block.add(parentDoc("g1b_p", 2));
                nestedWriter.addDocuments(gen1Block);
                nestedWriter.addDocument(parentDoc("g1_p1", 3));
                nestedWriter.flush();
                stampLatestSegment(nestedWriter, 1L);

                // gen=2: block [g2b_c0=0, g2b_p=1]
                List<Document> gen2Block = new ArrayList<>();
                gen2Block.add(childDoc("g2b_c0", 0));
                gen2Block.add(parentDoc("g2b_p", 1));
                nestedWriter.addDocuments(gen2Block);
                nestedWriter.flush();
                stampLatestSegment(nestedWriter, 2L);

                nestedWriter.commit();

                // Logical-row mapping (children never appear): gen1 {0→0, 1→2}, gen2 {0→1}
                long[] mappingArray = new long[] { 0, 2, 1 };
                Map<Long, Integer> genOffsets = Map.of(1L, 0, 2L, 2);
                Map<Long, Integer> genSizes = Map.of(1L, 2, 2L, 1);
                RowIdMapping rowIdMapping = new PackedRowIdMapping(mappingArray, genOffsets, genSizes);

                LuceneMerger merger = new LuceneMerger(nestedWriter, new LuceneDataFormat(), nestedPath, new LuceneShardStatsTracker());
                List<Segment> segments = buildSegments(getSegmentInfos(nestedWriter));
                MergeInput input = MergeInput.builder().segments(segments).rowIdMapping(rowIdMapping).newWriterGeneration(10L).build();

                MergeResult result = merger.merge(input);

                // The merged file set must report LOGICAL rows (3 blocks), not physical docs (6).
                WriterFileSet mergedFileSet = result.getMergedWriterFileSet().values().iterator().next();
                assertEquals("Merged file set must count logical rows, not physical docs", 3L, mergedFileSet.numRows());

                // The merged segment must inherit the nested-blocks marker for future merges.
                SegmentCommitInfo merged = findSegmentWithGeneration(getSegmentInfos(nestedWriter), 10L);
                assertNotNull("Merged segment must carry writer_generation=10", merged);
                assertEquals(
                    "Merged segment must inherit the nested_blocks attribute",
                    "true",
                    merged.info.getAttribute(NESTED_BLOCKS_ATTRIBUTE)
                );

                nestedWriter.commit();

                // Expected doc order after block-aware expansion + index sort; row ids come
                // out sequential 0..5 == final docIds (I1 restored in the merged segment).
                String[] expectedIds = { "g1b_c0", "g1b_c1", "g1b_p", "g2b_c0", "g2b_p", "g1_p1" };
                long[] expectedKeys = { 0, 1, 2, 3, 4, 5 };

                try (DirectoryReader reader = DirectoryReader.open(nestedWriter)) {
                    LeafReaderContext mergedLeaf = null;
                    for (LeafReaderContext ctx : reader.leaves()) {
                        if (mergedLeaf == null || ctx.reader().maxDoc() > mergedLeaf.reader().maxDoc()) {
                            mergedLeaf = ctx;
                        }
                    }
                    assertNotNull("Should have at least one leaf", mergedLeaf);
                    assertEquals("Merged segment should have 6 physical docs", 6, mergedLeaf.reader().maxDoc());

                    SortedNumericDocValues rowIdDV = mergedLeaf.reader().getSortedNumericDocValues(ROW_ID_FIELD);
                    assertNotNull(ROW_ID_FIELD + " doc values should exist", rowIdDV);

                    for (int i = 0; i < expectedIds.length; i++) {
                        assertTrue("Should have doc values for doc " + i, rowIdDV.advanceExact(i));
                        assertEquals("Sequential row id at position " + i, expectedKeys[i], rowIdDV.nextValue());
                        Document doc = mergedLeaf.reader().storedFields().document(i);
                        assertEquals("Stored doc at position " + i, expectedIds[i], doc.get("id"));
                    }
                }
            }
        }
    }

    /** A nested child doc: sequential row id + {@code _nested_path} (how the merge identifies children). */
    private Document childDoc(String id, long sequentialRowId) {
        Document doc = parentDoc(id, sequentialRowId);
        doc.add(new StringField(NestedPathFieldMapper.NAME, "comments", Field.Store.NO));
        return doc;
    }

    /** A parent (root) doc: sequential row id, no {@code _nested_path}. */
    private Document parentDoc(String id, long sequentialRowId) {
        Document doc = new Document();
        doc.add(new StringField("id", id, Field.Store.YES));
        doc.add(new SortedNumericDocValuesField(ROW_ID_FIELD, sequentialRowId));
        return doc;
    }

    /**
     * Stamps both the writer-generation and nested-blocks attributes on the newest
     * segment, mimicking what {@code LuceneWriter#flush} does for nested-index segments.
     */
    @SuppressForbidden(reason = "Need reflection to stamp segment attributes for testing")
    private void stampLatestSegment(IndexWriter w, long generation) throws IOException {
        setWriterGenerationOnLatestSegment(w, generation);
        try {
            java.lang.reflect.Field segInfosField = IndexWriter.class.getDeclaredField("segmentInfos");
            segInfosField.setAccessible(true);
            SegmentInfos segInfos = (SegmentInfos) segInfosField.get(w);
            SegmentCommitInfo lastSegment = segInfos.asList().get(segInfos.size() - 1);
            lastSegment.info.putAttribute(NESTED_BLOCKS_ATTRIBUTE, "true");
        } catch (ReflectiveOperationException e) {
            throw new IOException("Failed to set nested_blocks attribute via reflection", e);
        }
    }

    private SegmentCommitInfo findSegmentWithGeneration(SegmentInfos infos, long generation) {
        String target = String.valueOf(generation);
        for (SegmentCommitInfo sci : infos.asList()) {
            if (target.equals(sci.info.getAttribute(WRITER_GENERATION_ATTRIBUTE))) {
                return sci;
            }
        }
        return null;
    }

    // ========== Helper Methods ==========

    private void writeSegment(IndexWriter w, long generation, int startRowId, int numDocs) throws IOException {
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();
            doc.add(new StringField("id", "doc_" + (startRowId + i), Field.Store.YES));
            doc.add(new StoredField("data", "value_for_doc_" + (startRowId + i)));
            // ___row_id is local to the segment: 0, 1, 2, ... (matches how the real system works)
            doc.add(new SortedNumericDocValuesField(ROW_ID_FIELD, i));
            w.addDocument(doc);
        }
        w.flush();
        setWriterGenerationOnLatestSegment(w, generation);
    }

    private void writeSegmentWithRichFields(IndexWriter w, long generation, int startRowId, int numDocs) throws IOException {
        for (int i = 0; i < numDocs; i++) {
            int docIdx = startRowId + i;
            Document doc = new Document();
            doc.add(new StringField("id", "doc_" + docIdx, Field.Store.YES));
            doc.add(new StoredField("data", "rich_data_for_doc_" + docIdx));
            doc.add(new StoredField("score", String.valueOf(docIdx * 10)));
            doc.add(new SortedNumericDocValuesField(ROW_ID_FIELD, docIdx));
            doc.add(new SortedNumericDocValuesField("score_dv", docIdx * 10));
            w.addDocument(doc);
        }
        w.flush();
        setWriterGenerationOnLatestSegment(w, generation);
    }

    @SuppressForbidden(reason = "Need reflection to stamp writer_generation on segments for testing")
    private void setWriterGenerationOnLatestSegment(IndexWriter w, long generation) throws IOException {
        try {
            java.lang.reflect.Field segInfosField = IndexWriter.class.getDeclaredField("segmentInfos");
            segInfosField.setAccessible(true);
            SegmentInfos segInfos = (SegmentInfos) segInfosField.get(w);
            if (segInfos.size() > 0) {
                SegmentCommitInfo lastSegment = segInfos.asList().get(segInfos.size() - 1);
                if (lastSegment.info.getAttribute(WRITER_GENERATION_ATTRIBUTE) == null) {
                    lastSegment.info.putAttribute(WRITER_GENERATION_ATTRIBUTE, String.valueOf(generation));
                }
            }
        } catch (ReflectiveOperationException e) {
            throw new IOException("Failed to set writer_generation attribute via reflection", e);
        }
    }

    @SuppressForbidden(reason = "Need reflection to access live SegmentInfos for test assertions")
    private SegmentInfos getSegmentInfos(IndexWriter w) throws IOException {
        try {
            java.lang.reflect.Field segInfosField = IndexWriter.class.getDeclaredField("segmentInfos");
            segInfosField.setAccessible(true);
            return (SegmentInfos) segInfosField.get(w);
        } catch (ReflectiveOperationException e) {
            throw new IOException("Failed to access segmentInfos via reflection", e);
        }
    }

    private List<Segment> buildSegments(SegmentInfos infos) {
        List<Segment> segments = new ArrayList<>();
        for (SegmentCommitInfo sci : infos.asList()) {
            String genAttr = sci.info.getAttribute(WRITER_GENERATION_ATTRIBUTE);
            if (genAttr != null) {
                long generation = Long.parseLong(genAttr);
                segments.add(Segment.builder(generation).build());
            }
        }
        return segments;
    }
}
