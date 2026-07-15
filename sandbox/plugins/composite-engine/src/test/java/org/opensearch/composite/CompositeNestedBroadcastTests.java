/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.be.lucene.LuceneDataFormat;
import org.opensearch.be.lucene.LuceneFieldFactoryRegistry;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.be.lucene.index.LuceneDocumentInput;
import org.opensearch.be.lucene.index.LuceneWriter;
import org.opensearch.be.lucene.stats.LuceneShardStatsTracker;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.engine.dataformat.FlushInput;
import org.opensearch.index.engine.dataformat.NestedScope;
import org.opensearch.index.engine.dataformat.WriteResult;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NestedPathFieldMapper;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.parquet.writer.FieldValuePair;
import org.opensearch.parquet.writer.ParquetDocumentInput;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.opensearch.index.engine.dataformat.FieldTypeCapabilities.Capability.COLUMNAR_STORAGE;
import static org.opensearch.index.engine.dataformat.FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH;

/**
 * Composite-level nested write tests: one parse script broadcast through
 * {@link CompositeDocumentInput} must produce <b>identical child identity</b> in the
 * Parquet buffer (scope ordinals → list positions) and the Lucene block (block
 * positions → composite row-id keys). This is the cross-format identity contract at
 * the seam where it is established.
 *
 * <p>Also covers the post-parse metadata placement rule: fields the engine adds after
 * parsing ({@code _seq_no}, version, primary term) must land on the root scope even
 * though child scopes were opened and closed earlier in the script.
 */
public class CompositeNestedBroadcastTests extends OpenSearchTestCase {

    private final LuceneDataFormat luceneFormat = new LuceneDataFormat();
    private MappedFieldType titleField;
    private MappedFieldType authorField;
    private MappedFieldType scoreField;
    private MappedFieldType seqNoField;
    private MappedFieldType versionField;
    private MappedFieldType primaryTermField;
    private MappedFieldType idField;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        titleField = dualFormatKeyword("title");
        authorField = dualFormatKeyword("comments.author");
        scoreField = new NumberFieldMapper.NumberFieldType("comments.score", NumberFieldMapper.NumberType.INTEGER);
        scoreField.setCapabilityMap(Map.of(ParquetDataFormatPlugin.PARQUET_DATA_FORMAT, Set.of(COLUMNAR_STORAGE)));
        // Metadata: Parquet-only capabilities, mirroring how the engine routes them today.
        seqNoField = parquetOnlyLong(SeqNoFieldMapper.NAME);
        versionField = parquetOnlyLong(org.opensearch.index.mapper.VersionFieldMapper.NAME);
        primaryTermField = parquetOnlyLong(SeqNoFieldMapper.PRIMARY_TERM_NAME);
        idField = new MappedFieldType(
            org.opensearch.index.mapper.IdFieldMapper.CONTENT_TYPE,
            true,
            true,
            true,
            org.opensearch.index.mapper.TextSearchInfo.SIMPLE_MATCH_ONLY,
            Map.of()
        ) {
            @Override
            public org.opensearch.index.mapper.ValueFetcher valueFetcher(
                org.opensearch.index.query.QueryShardContext context,
                org.opensearch.search.lookup.SearchLookup searchLookup,
                String format
            ) {
                return null;
            }

            @Override
            public String typeName() {
                return org.opensearch.index.mapper.IdFieldMapper.CONTENT_TYPE;
            }

            @Override
            public org.apache.lucene.search.Query termQuery(Object value, org.opensearch.index.query.QueryShardContext context) {
                return null;
            }
        };
        idField.setCapabilityMap(Map.of(ParquetDataFormatPlugin.PARQUET_DATA_FORMAT, Set.of(COLUMNAR_STORAGE)));
    }

    private static MappedFieldType parquetOnlyLong(String name) {
        NumberFieldMapper.NumberFieldType ft = new NumberFieldMapper.NumberFieldType(name, NumberFieldMapper.NumberType.LONG);
        ft.setCapabilityMap(Map.of(ParquetDataFormatPlugin.PARQUET_DATA_FORMAT, Set.of(COLUMNAR_STORAGE)));
        return ft;
    }

    private static MappedFieldType dualFormatKeyword(String name) {
        // Indexed, untokenized keyword — the Lucene field factory requires an indexed
        // (or stored) Lucene FieldType, matching the shape real KeywordFieldMapper produces.
        org.apache.lucene.document.FieldType luceneType = new org.apache.lucene.document.FieldType();
        luceneType.setTokenized(false);
        luceneType.setStored(false);
        luceneType.setOmitNorms(true);
        luceneType.setIndexOptions(org.apache.lucene.index.IndexOptions.DOCS);
        luceneType.freeze();
        KeywordFieldMapper.KeywordFieldType ft = new KeywordFieldMapper.KeywordFieldType(name, luceneType);
        ft.setCapabilityMap(
            Map.of(
                ParquetDataFormatPlugin.PARQUET_DATA_FORMAT,
                Set.of(COLUMNAR_STORAGE),
                LucenePlugin.DATA_FORMAT,
                Set.of(FULL_TEXT_SEARCH)
            )
        );
        return ft;
    }

    /**
     * Drives the two-comment running example through the composite broadcast, mimicking
     * the engine's real call order: parse fields (with child scopes), then post-parse
     * metadata, then setRowId.
     */
    private CompositeDocumentInput driveScript(ParquetDocumentInput parquet, LuceneDocumentInput lucene) {
        CompositeDocumentInput composite = new CompositeDocumentInput(
            ParquetDataFormatPlugin.PARQUET_DATA_FORMAT,
            parquet,
            Map.of(LucenePlugin.DATA_FORMAT, lucene)
        );

        // Parse phase.
        composite.addField(titleField, "First post");
        composite.beginChild("comments");
        composite.addField(authorField, "alice");
        composite.addField(scoreField, 5);
        composite.endChild();
        composite.beginChild("comments");
        composite.addField(authorField, "dave");
        composite.endChild();

        // Post-parse phase (DataFormatAwareEngine adds these after the parser returns).
        composite.addField(idField, "doc-7".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        composite.addField(versionField, 1L);
        composite.addField(seqNoField, 100L);
        composite.addField(primaryTermField, 1L);
        composite.setRowId(DocumentInput.ROW_ID_FIELD, 0L);
        return composite;
    }

    /** One broadcast script → identical child identity in both formats' buffers. */
    public void testBroadcastProducesIdenticalIdentityInBothFormats() {
        ParquetDocumentInput parquet = new ParquetDocumentInput();
        LuceneDocumentInput lucene = new LuceneDocumentInput(new LuceneFieldFactoryRegistry(), true);
        driveScript(parquet, lucene);

        // Parquet side: two scopes with ordinals 0/1; alice's values at comments[0], dave's at comments[1].
        List<NestedScope> scopes = parquet.getNestedScopes();
        assertEquals(2, scopes.size());
        FieldValuePair alicePair = pairFor(parquet, "alice");
        FieldValuePair davePair = pairFor(parquet, "dave");
        assertEquals(0, alicePair.getScope().ordinal());
        assertEquals(1, davePair.getScope().ordinal());

        // Lucene side: block [alice, dave, root] in emission order. Under Scheme C the
        // identity contract is POSITIONAL: a child's index in the Lucene block equals its
        // Parquet list position (both were assigned by the same parse-time scope ordinal).
        List<Document> block = lucene.getFinalInput();
        assertEquals(3, block.size());
        assertEquals("alice", block.get(alicePair.getScope().ordinal()).getField("comments.author").stringValue());
        assertEquals("dave", block.get(davePair.getScope().ordinal()).getField("comments.author").stringValue());
        // Root is last (the block's parent).
        assertEquals("First post", block.get(2).getField("title").stringValue());

        // Row-id stamping is deferred to LuceneWriter (sequential docId-space ids); the
        // input records the logical rowId only.
        assertEquals(parquet.getRowId(), lucene.getRowId());
        for (Document doc : block) {
            assertNull("sequential ids are stamped by the writer, not the input", doc.getField(DocumentInput.ROW_ID_FIELD));
        }
    }

    /** Post-parse metadata lands on the root scope (Parquet) and the root doc (Lucene). */
    public void testPostParseMetadataLandsOnRoot() {
        ParquetDocumentInput parquet = new ParquetDocumentInput();
        LuceneDocumentInput lucene = new LuceneDocumentInput(new LuceneFieldFactoryRegistry(), true);
        driveScript(parquet, lucene);

        // Parquet: the seq_no pair must carry no scope even though children were parsed first.
        FieldValuePair seqNoPair = parquet.getFinalInput().stream().filter(p -> p.getFieldType() == seqNoField).findFirst().orElseThrow();
        assertFalse("post-parse metadata must belong to the root document", seqNoPair.isNested());
        assertNull(seqNoPair.getScope());

        // Lucene: title (root field) on the root doc only; children carry only their own fields.
        List<Document> block = lucene.getFinalInput();
        assertNull(block.get(0).getField("title"));
        assertNull(block.get(1).getField("title"));
        assertEquals("First post", block.get(2).getField("title").stringValue());
    }

    /**
     * Segment-level cross-check: flush the broadcast-built block through a composite-mode
     * {@link LuceneWriter} and verify, via real index queries, that each child's docId
     * position matches the Parquet-side scope ordinal — identity intact on disk.
     */
    public void testFlushedSegmentMatchesParquetOrdinals() throws Exception {
        ParquetDocumentInput parquet = new ParquetDocumentInput();
        LuceneDocumentInput lucene = new LuceneDocumentInput(new LuceneFieldFactoryRegistry(), true);
        driveScript(parquet, lucene);

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
            assertTrue(writer.addDoc(lucene) instanceof WriteResult.Success);
            FileInfos fileInfos = writer.flush(FlushInput.EMPTY);
            WriterFileSet wfs = fileInfos.getWriterFileSet(luceneFormat).orElseThrow();
            // numRows is the cross-format LOGICAL row count: 1 logical document, agreeing
            // with the 1 Parquet row for the same generation (the engine's cross-format
            // parity assertions depend on this), even though the segment holds 3 docs.
            assertEquals(1L, wfs.numRows());

            try (NIOFSDirectory dir = new NIOFSDirectory(Path.of(wfs.directory())); IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                LeafReader leaf = reader.leaves().get(0).reader();
                SortedNumericDocValues rowIds = leaf.getSortedNumericDocValues(DocumentInput.ROW_ID_FIELD);

                // Scheme C: every physical doc carries __row_id__ == docId, and child
                // identity is positional — a child's docId offset from its block start
                // equals its Parquet list position. The single block starts at docId 0,
                // so each author's docId IS her Parquet scope ordinal.
                ScoreDoc aliceHit = searcher.search(new TermQuery(new Term("comments.author", "alice")), 1).scoreDocs[0];
                assertTrue(rowIds.advanceExact(aliceHit.doc));
                assertEquals("I1: row id equals docId", aliceHit.doc, rowIds.nextValue());
                assertEquals(pairFor(parquet, "alice").getScope().ordinal(), aliceHit.doc);

                ScoreDoc daveHit = searcher.search(new TermQuery(new Term("comments.author", "dave")), 1).scoreDocs[0];
                assertTrue(rowIds.advanceExact(daveHit.doc));
                assertEquals("I1: row id equals docId", daveHit.doc, rowIds.nextValue());
                assertEquals(pairFor(parquet, "dave").getScope().ordinal(), daveHit.doc);

                // Child count agrees with the Parquet scope count.
                int childDocs = searcher.count(new TermQuery(new Term(NestedPathFieldMapper.NAME, "comments")));
                assertEquals(parquet.getNestedScopes().size(), childDocs);
            }
        }
    }

    private static FieldValuePair pairFor(ParquetDocumentInput parquet, String authorValue) {
        return parquet.getFinalInput()
            .stream()
            .filter(p -> "comments.author".equals(p.getFieldType().name()) && authorValue.equals(p.getValue()))
            .findFirst()
            .orElseThrow();
    }
}
