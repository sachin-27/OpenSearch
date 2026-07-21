/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.be.lucene.index.LuceneDocumentInput;
import org.opensearch.be.lucene.index.LucenePluginBaseTests;
import org.opensearch.be.lucene.index.LuceneWriter;
import org.opensearch.be.lucene.stats.LuceneShardStatsTracker;
import org.opensearch.index.engine.dataformat.FlushInput;
import org.opensearch.index.engine.dataformat.WriteResult;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NestedPathFieldMapper;

import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tests for {@link NestedParentLayout} — the read-side docId↔logical-row translator.
 *
 * <p>Layout under test (built through the real nested-mode {@link LuceneWriter}):
 * <pre>
 * docId | doc              | row
 *   0   | child alice      |  0
 *   1   | child dave       |  0
 *   2   | root "First"     |  0
 *   3   | root "Second"    |  1   (childless)
 *   4   | child erin       |  2
 *   5   | root "Third"     |  2
 * </pre>
 */
public class NestedParentLayoutTests extends LucenePluginBaseTests {

    private final LuceneDataFormat luceneFormat = new LuceneDataFormat();
    private NIOFSDirectory directory;
    private DirectoryReader reader;
    private LeafReader leaf;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        MappedFieldType author = mockKeywordField("comments.author");
        MappedFieldType title = mockKeywordField("title");
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
            LuceneDocumentInput doc0 = new LuceneDocumentInput(new LuceneFieldFactoryRegistry(), true);
            doc0.addField(title, "First");
            for (String a : new String[] { "alice", "dave" }) {
                doc0.beginChild("comments");
                doc0.addField(author, a);
                doc0.endChild();
            }
            doc0.setRowId(LuceneDocumentInput.ROW_ID_FIELD, 0L);
            assertTrue(writer.addDoc(doc0) instanceof WriteResult.Success);

            LuceneDocumentInput doc1 = new LuceneDocumentInput(new LuceneFieldFactoryRegistry(), true);
            doc1.addField(title, "Second");
            doc1.setRowId(LuceneDocumentInput.ROW_ID_FIELD, 1L);
            assertTrue(writer.addDoc(doc1) instanceof WriteResult.Success);

            LuceneDocumentInput doc2 = new LuceneDocumentInput(new LuceneFieldFactoryRegistry(), true);
            doc2.addField(title, "Third");
            doc2.beginChild("comments");
            doc2.addField(author, "erin");
            doc2.endChild();
            doc2.setRowId(LuceneDocumentInput.ROW_ID_FIELD, 2L);
            assertTrue(writer.addDoc(doc2) instanceof WriteResult.Success);

            WriterFileSet wfs = writer.flush(FlushInput.EMPTY).getWriterFileSet(luceneFormat).orElseThrow();
            directory = new NIOFSDirectory(Path.of(wfs.directory()));
            reader = DirectoryReader.open(directory);
            leaf = reader.leaves().get(0).reader();
        }
    }

    @Override
    public void tearDown() throws Exception {
        reader.close();
        directory.close();
        super.tearDown();
    }

    /** Detection: nested-mode segments (with the .si attribute) yield a layout. */
    public void testOfDetectsNestedSegment() throws Exception {
        NestedParentLayout layout = NestedParentLayout.of(leaf);
        assertNotNull("flushed nested-mode segment must carry the nested_blocks attribute", layout);
        assertEquals(3, layout.rowCount());
    }

    /** Detection: plain segments (no attribute) yield null — flat, no translation. */
    public void testOfReturnsNullForFlatSegment() throws Exception {
        try (ByteBuffersDirectory flatDir = new ByteBuffersDirectory(); IndexWriter w = new IndexWriter(flatDir, new IndexWriterConfig())) {
            Document doc = new Document();
            doc.add(new StringField("f", "v", Field.Store.NO));
            w.addDocument(doc);
            w.commit();
            try (DirectoryReader flatReader = DirectoryReader.open(w)) {
                assertNull(NestedParentLayout.of(flatReader.leaves().get(0).reader()));
            }
        }
    }

    /** rowOf: children and parents alike map to their block's logical row. */
    public void testRowOf() throws Exception {
        NestedParentLayout layout = NestedParentLayout.build(leaf);
        int[] expectedRowByDocId = { 0, 0, 0, 1, 2, 2 };
        for (int docId = 0; docId < expectedRowByDocId.length; docId++) {
            assertEquals("rowOf(" + docId + ")", expectedRowByDocId[docId], layout.rowOf(docId));
        }
    }

    /** Select direction: parent docIds and block start docIds per row. */
    public void testParentAndBlockStart() throws Exception {
        NestedParentLayout layout = NestedParentLayout.build(leaf);
        assertEquals(2, layout.parentDocId(0));
        assertEquals(3, layout.parentDocId(1));
        assertEquals(5, layout.parentDocId(2));
        assertEquals(0, layout.blockStartDocId(0));
        assertEquals(3, layout.blockStartDocId(1));
        assertEquals(4, layout.blockStartDocId(2));
    }

    /** A segment ending with a child doc (torn block) must fail loudly at build. */
    public void testTrailingChildThrows() throws Exception {
        try (ByteBuffersDirectory rawDir = new ByteBuffersDirectory(); IndexWriter w = new IndexWriter(rawDir, new IndexWriterConfig())) {
            Document parent = new Document();
            parent.add(new StringField("title", "p", Field.Store.NO));
            w.addDocument(parent);
            Document trailingChild = new Document();
            trailingChild.add(new StringField(NestedPathFieldMapper.NAME, "comments", Field.Store.NO));
            w.addDocument(trailingChild);
            w.commit();
            try (DirectoryReader rawReader = DirectoryReader.open(w)) {
                expectThrows(IllegalStateException.class, () -> NestedParentLayout.build(rawReader.leaves().get(0).reader()));
            }
        }
    }
}
