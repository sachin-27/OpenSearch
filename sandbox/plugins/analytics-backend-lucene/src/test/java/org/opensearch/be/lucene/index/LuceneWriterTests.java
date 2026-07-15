/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.index;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.be.lucene.LuceneDataFormat;
import org.opensearch.be.lucene.stats.LuceneShardStatsTracker;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DeleteInput;
import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.engine.dataformat.FlushInput;
import org.opensearch.index.engine.dataformat.WriteResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.mapper.MappedFieldType;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link LuceneWriter} — the per-generation Lucene writer that creates
 * segments in isolated temp directories with force-merge to 1 segment on flush.
 */
public class LuceneWriterTests extends LucenePluginBaseTests {

    private LuceneDataFormat dataFormat;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        dataFormat = new LuceneDataFormat();
    }

    public void testAddDocAndFlushProducesSingleSegment() throws IOException {
        Path baseDir = createTempDir();
        try (
            LuceneWriter writer = new LuceneWriter(
                1L,
                0L,
                dataFormat,
                baseDir,
                null,
                Codec.getDefault(),
                null,
                ConcurrentHashMap.newKeySet(),
                new LuceneShardStatsTracker()
            )
        ) {
            int numDocs = randomIntBetween(5, 20);
            MappedFieldType textField = mockTextField("content");
            for (int i = 0; i < numDocs; i++) {
                LuceneDocumentInput input = new LuceneDocumentInput();
                input.addField(textField, "value " + i);
                input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, i);
                WriteResult result = writer.addDoc(input);
                assertTrue(result instanceof WriteResult.Success);
            }

            FileInfos fileInfos = writer.flush(FlushInput.EMPTY);
            assertTrue(fileInfos.getWriterFileSet(dataFormat).isPresent());

            WriterFileSet wfs = fileInfos.getWriterFileSet(dataFormat).get();
            assertThat(wfs.numRows(), equalTo((long) numDocs));
            assertThat(wfs.writerGeneration(), equalTo(1L));
            assertFalse(wfs.files().isEmpty());

            // Verify the segment is readable and has exactly numDocs documents
            try (NIOFSDirectory dir = new NIOFSDirectory(Path.of(wfs.directory())); IndexReader reader = DirectoryReader.open(dir)) {
                assertThat(reader.numDocs(), equalTo(numDocs));
                assertThat(reader.leaves().size(), equalTo(1));
            }
        }
    }

    /**
     * End-to-end nested block write: a nested-blocks writer ingests a two-comment
     * document plus a childless document, flushes, and the segment must show the
     * block-join layout — children before parent — with plain sequential row ids
     * ({@code __row_id__ == docId}, invariant I1) on every physical doc, children included.
     */
    public void testNestedBlockWriteAndFlush() throws IOException {
        Path baseDir = createTempDir();
        MappedFieldType author = mockKeywordField("comments.author");
        MappedFieldType title = mockKeywordField("title");
        try (
            LuceneWriter writer = new LuceneWriter(
                1L,
                0L,
                dataFormat,
                baseDir,
                null,
                Codec.getDefault(),
                null,
                ConcurrentHashMap.newKeySet(),
                new LuceneShardStatsTracker(),
                true // nested-blocks mode
            )
        ) {
            // Logical doc 0: two comments → block of 3.
            LuceneDocumentInput nested = new LuceneDocumentInput(new org.opensearch.be.lucene.LuceneFieldFactoryRegistry(), true);
            nested.addField(title, "First post");
            nested.beginChild("comments");
            nested.addField(author, "alice");
            nested.endChild();
            nested.beginChild("comments");
            nested.addField(author, "dave");
            nested.endChild();
            nested.setRowId(LuceneDocumentInput.ROW_ID_FIELD, 0L);
            WriteResult first = writer.addDoc(nested);
            assertTrue(first instanceof WriteResult.Success);
            // The block's identity is the parent docId — last of the 3 consumed.
            assertThat(((WriteResult.Success) first).seqNo(), equalTo(2L));

            // Logical doc 1: childless → block of 1.
            LuceneDocumentInput flat = new LuceneDocumentInput(new org.opensearch.be.lucene.LuceneFieldFactoryRegistry(), true);
            flat.addField(title, "Second post");
            flat.setRowId(LuceneDocumentInput.ROW_ID_FIELD, 1L);
            assertTrue(writer.addDoc(flat) instanceof WriteResult.Success);

            FileInfos fileInfos = writer.flush(FlushInput.EMPTY);
            WriterFileSet wfs = fileInfos.getWriterFileSet(dataFormat).get();
            // numRows is the cross-format LOGICAL row count (2 documents), matching what
            // Parquet reports for the same generation — not the 4 physical Lucene docs.
            assertThat(wfs.numRows(), equalTo(2L));

            try (NIOFSDirectory dir = new NIOFSDirectory(Path.of(wfs.directory())); IndexReader reader = DirectoryReader.open(dir)) {
                assertThat(reader.numDocs(), equalTo(4));
                LeafReader leaf = reader.leaves().get(0).reader();
                SortedNumericDocValues rowIds = leaf.getSortedNumericDocValues(LuceneDocumentInput.ROW_ID_FIELD);

                // Scheme C: every physical doc — alice, dave, doc-0 parent, childless doc-1 —
                // carries __row_id__ == docId. Block↔row correspondence is positional
                // (Kth parent == Kth Parquet row), not encoded in the values.
                for (int docId = 0; docId < 4; docId++) {
                    assertTrue(rowIds.advanceExact(docId));
                    assertThat("sequential row id at docId " + docId, rowIds.nextValue(), equalTo((long) docId));
                }

                // Children are addressable by nested path; parents are not.
                IndexSearcher searcher = new IndexSearcher(reader);
                assertThat(
                    searcher.count(new TermQuery(new Term(org.opensearch.index.mapper.NestedPathFieldMapper.NAME, "comments"))),
                    equalTo(2)
                );
            }
        }
    }

    /**
     * Refresh-path simulation: two nested generations flushed by composite-mode writers,
     * incorporated via {@code addIndexes} into a committer-like shared writer (index sort
     * on the row-id key + the nested parent field), then force-merged. Blocks must remain
     * contiguous with children before parents, in global logical-row order — the property
     * every block-join query depends on after refresh and merge.
     */
    public void testNestedBlocksSurviveAddIndexesAndSortedMerge() throws IOException {
        MappedFieldType author = mockKeywordField("comments.author");
        MappedFieldType title = mockKeywordField("title");

        java.util.List<Path> segmentDirs = new java.util.ArrayList<>();

        // Segment 1 — produced by the real composite-mode LuceneWriter (logical row 0,
        // two comments). Proves flushed segments are addIndexes-compatible with a
        // parent-field committer (the parent field lands in this segment's FieldInfos).
        try (
            LuceneWriter writer = new LuceneWriter(
                1L,
                0L,
                dataFormat,
                createTempDir(),
                null,
                Codec.getDefault(),
                null,
                ConcurrentHashMap.newKeySet(),
                new LuceneShardStatsTracker(),
                true
            )
        ) {
            LuceneDocumentInput input = new LuceneDocumentInput(new org.opensearch.be.lucene.LuceneFieldFactoryRegistry(), true);
            input.addField(title, "post-0");
            for (String a : new String[] { "alice", "dave" }) {
                input.beginChild("comments");
                input.addField(author, a);
                input.endChild();
            }
            input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, 0L);
            assertTrue(writer.addDoc(input) instanceof WriteResult.Success);
            WriterFileSet wfs = writer.flush(FlushInput.EMPTY).getWriterFileSet(dataFormat).get();
            segmentDirs.add(Path.of(wfs.directory()));
        }

        // Segment 2 — a post-remap-shaped segment (global logical row 1, one comment),
        // written directly with the same parent-field config. Simulates the state the
        // remapping merge produces: global sequential row ids continuing after gen1's
        // 3 docs (ids 3 and 4), block layout, parent field present.
        Path seg2Dir = createTempDir();
        try (NIOFSDirectory dir2 = new NIOFSDirectory(seg2Dir)) {
            org.apache.lucene.index.IndexWriterConfig rawIwc = new org.apache.lucene.index.IndexWriterConfig();
            rawIwc.setParentField(LuceneWriter.NESTED_PARENT_FIELD);
            rawIwc.setIndexSort(
                new org.apache.lucene.search.Sort(
                    new org.apache.lucene.search.SortedNumericSortField(
                        LuceneDocumentInput.ROW_ID_FIELD,
                        org.apache.lucene.search.SortField.Type.LONG
                    )
                )
            );
            try (org.apache.lucene.index.IndexWriter rawWriter = new org.apache.lucene.index.IndexWriter(dir2, rawIwc)) {
                org.apache.lucene.document.Document child = new org.apache.lucene.document.Document();
                child.add(new org.apache.lucene.document.StringField("comments.author", "eve", org.apache.lucene.document.Field.Store.NO));
                child.add(new org.apache.lucene.document.SortedNumericDocValuesField(LuceneDocumentInput.ROW_ID_FIELD, 3L));
                org.apache.lucene.document.Document root = new org.apache.lucene.document.Document();
                root.add(new org.apache.lucene.document.StringField("title", "post-1", org.apache.lucene.document.Field.Store.NO));
                root.add(new org.apache.lucene.document.SortedNumericDocValuesField(LuceneDocumentInput.ROW_ID_FIELD, 4L));
                rawWriter.addDocuments(java.util.List.of(child, root));
                rawWriter.commit();
            }
        }
        segmentDirs.add(seg2Dir);

        // Committer-like shared writer: index sort on the row id + parent field.
        Path sharedDir = createTempDir();
        org.apache.lucene.index.IndexWriterConfig iwc = new org.apache.lucene.index.IndexWriterConfig();
        iwc.setIndexSort(
            new org.apache.lucene.search.Sort(
                new org.apache.lucene.search.SortedNumericSortField(
                    LuceneDocumentInput.ROW_ID_FIELD,
                    org.apache.lucene.search.SortField.Type.LONG
                )
            )
        );
        iwc.setParentField(LuceneWriter.NESTED_PARENT_FIELD);
        try (NIOFSDirectory shared = new NIOFSDirectory(sharedDir)) {
            try (org.apache.lucene.index.IndexWriter sharedWriter = new org.apache.lucene.index.IndexWriter(shared, iwc)) {
                // Incorporate gen segments in REVERSE order so the sorted merge has real
                // reordering work (gen2's block must sort after gen1's despite arriving first).
                sharedWriter.addIndexes(new NIOFSDirectory(segmentDirs.get(1)), new NIOFSDirectory(segmentDirs.get(0)));
                sharedWriter.forceMerge(1);
                sharedWriter.commit();
            }

            try (IndexReader reader = DirectoryReader.open(shared)) {
                assertThat(reader.numDocs(), equalTo(5)); // (2 children + parent) + (1 child + parent)
                LeafReader leaf = reader.leaves().get(0).reader();
                SortedNumericDocValues keys = leaf.getSortedNumericDocValues(LuceneDocumentInput.ROW_ID_FIELD);

                // Expected order: gen1's block [alice=0, dave=1, parent=2] then gen2's block
                // [eve=3, parent=4] — sequential row ids equal final docIds (I1 restored).
                for (int docId = 0; docId < 5; docId++) {
                    assertTrue(keys.advanceExact(docId));
                    assertThat("block layout at docId " + docId, keys.nextValue(), equalTo((long) docId));
                }
            }
        }
    }

    public void testRowIdMatchesLuceneDocId() throws IOException {
        Path baseDir = createTempDir();
        int numDocs = randomIntBetween(10, 50);
        MappedFieldType textField = mockTextField("content");
        try (
            LuceneWriter writer = new LuceneWriter(
                1L,
                0L,
                dataFormat,
                baseDir,
                null,
                Codec.getDefault(),
                null,
                ConcurrentHashMap.newKeySet(),
                new LuceneShardStatsTracker()
            )
        ) {
            for (int i = 0; i < numDocs; i++) {
                LuceneDocumentInput input = new LuceneDocumentInput();
                input.addField(textField, "doc " + i);
                input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, i);
                writer.addDoc(input);
            }

            FileInfos fileInfos = writer.flush(FlushInput.EMPTY);
            WriterFileSet wfs = fileInfos.getWriterFileSet(dataFormat).get();

            try (NIOFSDirectory dir = new NIOFSDirectory(Path.of(wfs.directory())); IndexReader reader = DirectoryReader.open(dir)) {
                for (LeafReaderContext ctx : reader.leaves()) {
                    LeafReader leafReader = ctx.reader();
                    SortedNumericDocValues rowIdValues = leafReader.getSortedNumericDocValues(LuceneDocumentInput.ROW_ID_FIELD);
                    assertNotNull("row_id doc values should exist", rowIdValues);
                    for (int docId = 0; docId < leafReader.maxDoc(); docId++) {
                        assertTrue(rowIdValues.advanceExact(docId));
                        assertThat("row ID should equal Lucene doc ID", rowIdValues.nextValue(), equalTo((long) docId));
                    }
                }
            }
        }
    }

    public void testFlushWithNoDocsReturnsEmpty() throws IOException {
        Path baseDir = createTempDir();
        try (
            LuceneWriter writer = new LuceneWriter(
                1L,
                0L,
                dataFormat,
                baseDir,
                null,
                Codec.getDefault(),
                null,
                ConcurrentHashMap.newKeySet(),
                new LuceneShardStatsTracker()
            )
        ) {
            FileInfos fileInfos = writer.flush(FlushInput.EMPTY);
            assertTrue(fileInfos.writerFilesMap().isEmpty());
        }
    }

    public void testWriterGenerationIsPreserved() throws IOException {
        Path baseDir = createTempDir();
        long gen = randomLongBetween(1, 100);
        MappedFieldType textField = mockTextField("content");
        try (
            LuceneWriter writer = new LuceneWriter(
                gen,
                0L,
                dataFormat,
                baseDir,
                null,
                Codec.getDefault(),
                null,
                ConcurrentHashMap.newKeySet(),
                new LuceneShardStatsTracker()
            )
        ) {
            assertThat(writer.generation(), equalTo(gen));

            LuceneDocumentInput input = new LuceneDocumentInput();
            input.addField(textField, "test value");
            input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, 0);
            writer.addDoc(input);

            FileInfos fileInfos = writer.flush(FlushInput.EMPTY);
            WriterFileSet wfs = fileInfos.getWriterFileSet(dataFormat).get();
            assertThat(wfs.writerGeneration(), equalTo(gen));
        }
    }

    public void testKeywordFieldsAreIndexed() throws IOException {
        Path baseDir = createTempDir();
        MappedFieldType keywordField = mockKeywordField("status");
        try (
            LuceneWriter writer = new LuceneWriter(
                1L,
                0L,
                dataFormat,
                baseDir,
                null,
                Codec.getDefault(),
                null,
                ConcurrentHashMap.newKeySet(),
                new LuceneShardStatsTracker()
            )
        ) {
            LuceneDocumentInput input = new LuceneDocumentInput();
            input.addField(keywordField, "active");
            input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, 0);
            writer.addDoc(input);

            FileInfos fileInfos = writer.flush(FlushInput.EMPTY);
            WriterFileSet wfs = fileInfos.getWriterFileSet(dataFormat).get();

            try (NIOFSDirectory dir = new NIOFSDirectory(Path.of(wfs.directory())); IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                assertThat(searcher.count(new MatchAllDocsQuery()), equalTo(1));
            }
        }
    }

    public void testUnsupportedFieldTypeIsSilentlySkipped() throws IOException {
        Path baseDir = createTempDir();
        MappedFieldType numericField = mock(MappedFieldType.class);
        when(numericField.typeName()).thenReturn("integer");
        when(numericField.name()).thenReturn("count");
        // Empty capability map → no format owns this field; should be silently skipped
        when(numericField.getCapabilityMap()).thenReturn(java.util.Map.of());

        try (
            LuceneWriter writer = new LuceneWriter(
                1L,
                0L,
                dataFormat,
                baseDir,
                null,
                Codec.getDefault(),
                null,
                ConcurrentHashMap.newKeySet(),
                new LuceneShardStatsTracker()
            )
        ) {
            LuceneDocumentInput input = new LuceneDocumentInput();
            // Should not throw — unsupported types are silently skipped (handled by other formats)
            input.addField(numericField, 42);
            // The document should have no fields for the unsupported type
            assertEquals(0, input.getFinalInput().get(0).getFields().size());
        }
    }

    public void testFieldOwnedByAnotherFormatIsSilentlySkipped() throws IOException {
        Path baseDir = createTempDir();
        DataFormat otherFormat = mock(DataFormat.class);
        when(otherFormat.name()).thenReturn("parquet");

        MappedFieldType fieldOwnedByOther = mock(MappedFieldType.class);
        when(fieldOwnedByOther.typeName()).thenReturn("integer");
        when(fieldOwnedByOther.name()).thenReturn("count");
        when(fieldOwnedByOther.getCapabilityMap()).thenReturn(
            java.util.Map.of(
                otherFormat,
                java.util.Set.of(org.opensearch.index.engine.dataformat.FieldTypeCapabilities.Capability.COLUMNAR_STORAGE)
            )
        );

        try (
            LuceneWriter writer = new LuceneWriter(
                1L,
                0L,
                dataFormat,
                baseDir,
                null,
                Codec.getDefault(),
                null,
                ConcurrentHashMap.newKeySet(),
                new LuceneShardStatsTracker()
            )
        ) {
            LuceneDocumentInput input = new LuceneDocumentInput();
            input.addField(fieldOwnedByOther, 42);
            assertEquals(0, input.getFinalInput().get(0).getFields().size());
        }
    }

    public void testMixedTextAndKeywordFields() throws IOException {
        Path baseDir = createTempDir();
        MappedFieldType textField = mockTextField("title");
        MappedFieldType keywordField = mockKeywordField("category");

        try (
            LuceneWriter writer = new LuceneWriter(
                1L,
                0L,
                dataFormat,
                baseDir,
                null,
                Codec.getDefault(),
                null,
                ConcurrentHashMap.newKeySet(),
                new LuceneShardStatsTracker()
            )
        ) {
            int numDocs = randomIntBetween(5, 15);
            for (int i = 0; i < numDocs; i++) {
                LuceneDocumentInput input = new LuceneDocumentInput();
                input.addField(textField, "document title " + i);
                input.addField(keywordField, "cat_" + (i % 3));
                input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, i);
                writer.addDoc(input);
            }

            FileInfos fileInfos = writer.flush(FlushInput.EMPTY);
            WriterFileSet wfs = fileInfos.getWriterFileSet(dataFormat).get();
            assertThat(wfs.numRows(), equalTo((long) numDocs));

            try (NIOFSDirectory dir = new NIOFSDirectory(Path.of(wfs.directory())); IndexReader reader = DirectoryReader.open(dir)) {
                assertThat(reader.numDocs(), equalTo(numDocs));
                assertThat(reader.leaves().size(), equalTo(1));
            }
        }
    }

    public void testWriteAndFlushEndToEndWithTextAndKeyword() throws IOException {
        Path baseDir = createTempDir();
        MappedFieldType textField = mockTextField("body");
        MappedFieldType keywordField = mockKeywordField("status");
        int numDocs = randomIntBetween(5, 20);

        try (
            LuceneWriter writer = new LuceneWriter(
                1L,
                0L,
                dataFormat,
                baseDir,
                null,
                Codec.getDefault(),
                null,
                ConcurrentHashMap.newKeySet(),
                new LuceneShardStatsTracker()
            )
        ) {
            for (int i = 0; i < numDocs; i++) {
                LuceneDocumentInput input = new LuceneDocumentInput();
                input.addField(textField, "hello world " + i);
                input.addField(keywordField, "active");
                input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, i);
                writer.addDoc(input);
            }

            FileInfos fileInfos = writer.flush(FlushInput.EMPTY);
            WriterFileSet wfs = fileInfos.getWriterFileSet(dataFormat).get();

            try (NIOFSDirectory dir = new NIOFSDirectory(Path.of(wfs.directory())); IndexReader reader = DirectoryReader.open(dir)) {
                // Verify exactly 1 segment
                assertThat(reader.leaves().size(), equalTo(1));
                // Verify correct doc count
                assertThat(reader.numDocs(), equalTo(numDocs));

                // Verify row IDs match doc IDs
                LeafReader leafReader = reader.leaves().get(0).reader();
                SortedNumericDocValues rowIdValues = leafReader.getSortedNumericDocValues(LuceneDocumentInput.ROW_ID_FIELD);
                assertNotNull(rowIdValues);
                for (int docId = 0; docId < numDocs; docId++) {
                    assertTrue(rowIdValues.advanceExact(docId));
                    assertThat(rowIdValues.nextValue(), equalTo((long) docId));
                }

                // Verify text field is searchable via TermQuery
                IndexSearcher searcher = new IndexSearcher(reader);
                assertTrue(searcher.count(new TermQuery(new Term("body", "hello"))) > 0);

                // Verify keyword field is searchable
                assertTrue(searcher.count(new TermQuery(new Term("status", "active"))) > 0);
            }
        }
    }

    public void testMultipleWriterGenerationsProduceIsolatedSegments() throws IOException {
        Path baseDir = createTempDir();
        MappedFieldType textField = mockTextField("content");

        long gen1 = 1L;
        long gen2 = 2L;
        int numDocs1 = randomIntBetween(3, 10);
        int numDocs2 = randomIntBetween(3, 10);

        FileInfos fileInfos1;
        FileInfos fileInfos2;

        // Create both writers without closing them until after verification,
        // because close() deletes the temp directory.
        LuceneWriter writer1 = new LuceneWriter(
            gen1,
            0L,
            dataFormat,
            baseDir,
            null,
            Codec.getDefault(),
            null,
            ConcurrentHashMap.newKeySet(),
            new LuceneShardStatsTracker()
        );
        LuceneWriter writer2 = new LuceneWriter(
            gen2,
            0L,
            dataFormat,
            baseDir,
            null,
            Codec.getDefault(),
            null,
            ConcurrentHashMap.newKeySet(),
            new LuceneShardStatsTracker()
        );
        try {
            for (int i = 0; i < numDocs1; i++) {
                LuceneDocumentInput input = new LuceneDocumentInput();
                input.addField(textField, "gen1 doc " + i);
                input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, i);
                writer1.addDoc(input);
            }
            fileInfos1 = writer1.flush(FlushInput.EMPTY);

            for (int i = 0; i < numDocs2; i++) {
                LuceneDocumentInput input = new LuceneDocumentInput();
                input.addField(textField, "gen2 doc " + i);
                input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, i);
                writer2.addDoc(input);
            }
            fileInfos2 = writer2.flush(FlushInput.EMPTY);

            // Verify each produces its own independent segment
            WriterFileSet wfs1 = fileInfos1.getWriterFileSet(dataFormat).get();
            WriterFileSet wfs2 = fileInfos2.getWriterFileSet(dataFormat).get();

            // Different directories
            assertNotEquals("Writers should have different directories", wfs1.directory(), wfs2.directory());

            // Correct generations
            assertThat(wfs1.writerGeneration(), equalTo(gen1));
            assertThat(wfs2.writerGeneration(), equalTo(gen2));

            // Correct doc counts
            assertThat(wfs1.numRows(), equalTo((long) numDocs1));
            assertThat(wfs2.numRows(), equalTo((long) numDocs2));

            // Each is independently readable with correct content
            try (NIOFSDirectory dir1 = new NIOFSDirectory(Path.of(wfs1.directory())); IndexReader reader1 = DirectoryReader.open(dir1)) {
                assertThat(reader1.numDocs(), equalTo(numDocs1));
                assertThat(reader1.leaves().size(), equalTo(1));
            }

            try (NIOFSDirectory dir2 = new NIOFSDirectory(Path.of(wfs2.directory())); IndexReader reader2 = DirectoryReader.open(dir2)) {
                assertThat(reader2.numDocs(), equalTo(numDocs2));
                assertThat(reader2.leaves().size(), equalTo(1));
            }
        } finally {
            writer1.close();
            writer2.close();
        }
    }

    public void testGetWriterForFormatReturnsItselfForLucene() throws IOException {
        Path baseDir = createTempDir();
        try (
            LuceneWriter writer = new LuceneWriter(
                1L,
                0L,
                dataFormat,
                baseDir,
                null,
                Codec.getDefault(),
                null,
                ConcurrentHashMap.newKeySet(),
                new LuceneShardStatsTracker()
            )
        ) {
            Optional<Writer<?>> result = writer.getWriterForFormat("lucene");

            assertTrue("Should return present for 'lucene'", result.isPresent());
            assertSame("Should return itself", writer, result.get());
        }
    }

    public void testGetWriterForFormatReturnsEmptyForOtherFormats() throws IOException {
        Path baseDir = createTempDir();
        try (
            LuceneWriter writer = new LuceneWriter(
                1L,
                0L,
                dataFormat,
                baseDir,
                null,
                Codec.getDefault(),
                null,
                ConcurrentHashMap.newKeySet(),
                new LuceneShardStatsTracker()
            )
        ) {
            Optional<Writer<?>> parquetResult = writer.getWriterForFormat("parquet");
            Optional<Writer<?>> nullResult = writer.getWriterForFormat(null);

            assertFalse("Should return empty for non-lucene format", parquetResult.isPresent());
            assertFalse("Should return empty for null format", nullResult.isPresent());
        }
    }

    public void testWriterDefaultGetWriterForFormatReturnsEmpty() {
        Writer<?> writer = mock(Writer.class, org.mockito.Mockito.CALLS_REAL_METHODS);
        Optional<Writer<?>> result = writer.getWriterForFormat("any");
        assertFalse(result.isPresent());
    }

    public void testWriterDefaultDeleteDocumentThrowsUnsupported() {
        Writer<?> writer = mock(Writer.class, org.mockito.Mockito.CALLS_REAL_METHODS);
        UnsupportedOperationException e = expectThrows(
            UnsupportedOperationException.class,
            () -> writer.deleteDocument(new DeleteInput("_id", "1", 1L))
        );
        assertTrue(e.getMessage().contains("deleteDocument is not supported"));
    }

    /**
     * Rollback path with the indexSort branch (LogByteSizeMergePolicy + IndexSort). Here
     * forceMerge actually rewrites the segment, so the tombstone is physically expunged
     * and maxDoc equals the live row count.
     */
    public void testRollbackInIndexSortBranchExpungesTombstone() throws IOException {
        Path baseDir = createTempDir();
        Sort indexSort = new Sort(new SortedNumericSortField(LuceneDocumentInput.ROW_ID_FIELD, SortField.Type.LONG));
        try (
            LuceneWriter writer = new LuceneWriter(
                1L,
                0L,
                dataFormat,
                baseDir,
                null,
                Codec.getDefault(),
                indexSort,
                ConcurrentHashMap.newKeySet(),
                new LuceneShardStatsTracker()
            )
        ) {
            MappedFieldType textField = mockTextField("content");
            for (int i = 0; i < 5; i++) {
                LuceneDocumentInput input = new LuceneDocumentInput();
                input.addField(textField, "value " + i);
                input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, i);
                writer.addDoc(input);
            }
            LuceneDocumentInput rollback = new LuceneDocumentInput();
            rollback.addField(textField, "to-rollback");
            rollback.setRowId(LuceneDocumentInput.ROW_ID_FIELD, 5);
            writer.addDoc(rollback);
            writer.rollbackTo(5);

            FileInfos fileInfos = writer.flush(FlushInput.EMPTY);
            WriterFileSet wfs = fileInfos.getWriterFileSet(dataFormat).get();
            assertThat("WriterFileSet must report live row count only", wfs.numRows(), equalTo(5L));

            try (NIOFSDirectory dir = new NIOFSDirectory(Path.of(wfs.directory())); IndexReader reader = DirectoryReader.open(dir)) {
                assertThat(
                    "forceMerge under real merge policy must expunge tombstone",
                    reader.leaves().get(0).reader().maxDoc(),
                    equalTo(5)
                );
                assertThat(reader.numDocs(), equalTo(5));
            }
        }
    }

    /**
     * After rollbackLastDoc the writer must transition to RETIRED_FLUSHABLE — further addDoc
     * calls are rejected. Holds for both no-sort and indexSort branches.
     */
    public void testRollbackRetiresWriterInBothBranches() throws IOException {
        Path baseDir = createTempDir();
        Sort indexSort = new Sort(new SortedNumericSortField(LuceneDocumentInput.ROW_ID_FIELD, SortField.Type.LONG));
        for (Sort sort : new Sort[] { null, indexSort }) {
            try (
                LuceneWriter writer = new LuceneWriter(
                    1L,
                    0L,
                    dataFormat,
                    createTempDir(),
                    null,
                    Codec.getDefault(),
                    sort,
                    ConcurrentHashMap.newKeySet(),
                    new LuceneShardStatsTracker()
                )
            ) {
                MappedFieldType textField = mockTextField("content");
                LuceneDocumentInput input = new LuceneDocumentInput();
                input.addField(textField, "v");
                input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, 0);
                writer.addDoc(input);
                writer.rollbackTo(0);

                assertThat("rollback must retire the writer (sort=" + sort + ")", writer.state().toString(), equalTo("RETIRED_FLUSHABLE"));

                LuceneDocumentInput nextDoc = new LuceneDocumentInput();
                nextDoc.addField(textField, "should-fail");
                nextDoc.setRowId(LuceneDocumentInput.ROW_ID_FIELD, 0);
                expectThrows(IllegalStateException.class, () -> writer.addDoc(nextDoc));
            }
        }
    }

    public void testGetHeapBytesUsedPositiveAfterIndexing() throws IOException {
        Path baseDir = createTempDir();
        MappedFieldType textField = mockTextField("content");
        try (
            LuceneWriter writer = new LuceneWriter(
                1L,
                0L,
                dataFormat,
                baseDir,
                null,
                Codec.getDefault(),
                null,
                ConcurrentHashMap.newKeySet(),
                new LuceneShardStatsTracker()
            )
        ) {
            LuceneDocumentInput input = new LuceneDocumentInput();
            input.addField(textField, "hello world");
            input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, 0);
            // add document so that it consumes heap memory
            writer.addDoc(input);
            assertTrue("getHeapBytesUsed should be > 0 after indexing", writer.getHeapBytesUsed() > 0);
            writer.flush(FlushInput.EMPTY);
            assertEquals("getHeapBytesUsed should be 0 after flush", 0L, writer.getHeapBytesUsed());
        }
        // try-with-resources calls close(); getHeapBytesUsed verified as 0 after flush (which closes IndexWriter)
    }

    public void testGetHeapBytesUsedZeroAfterCloseWithoutFlush() throws IOException {
        Path baseDir = createTempDir();
        MappedFieldType textField = mockTextField("content");
        LuceneWriter writer = new LuceneWriter(
            1L,
            0L,
            dataFormat,
            baseDir,
            null,
            Codec.getDefault(),
            null,
            ConcurrentHashMap.newKeySet(),
            new LuceneShardStatsTracker()
        );
        LuceneDocumentInput input = new LuceneDocumentInput();
        input.addField(textField, "hello world");
        input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, 0);
        writer.addDoc(input);
        assertTrue("getHeapBytesUsed should be > 0 before close", writer.getHeapBytesUsed() > 0);
        writer.close();
        assertEquals("getHeapBytesUsed should be 0 after close", 0L, writer.getHeapBytesUsed());
    }
}
