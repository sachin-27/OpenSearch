/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.Version;
import org.opensearch.arrow.allocator.ArrowNativeAllocator;
import org.opensearch.arrow.spi.NativeAllocatorPoolConfig;
import org.opensearch.be.lucene.LuceneDataFormat;
import org.opensearch.be.lucene.LuceneFieldFactoryRegistry;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.be.lucene.index.LuceneDocumentInput;
import org.opensearch.be.lucene.index.LuceneWriter;
import org.opensearch.be.lucene.stats.LuceneShardStatsTracker;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.FlushInput;
import org.opensearch.index.engine.dataformat.WriteResult;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.TextSearchInfo;
import org.opensearch.index.mapper.VersionFieldMapper;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.parquet.engine.ParquetDataFormat;
import org.opensearch.parquet.fields.ArrowFieldRegistry;
import org.opensearch.parquet.fields.ParquetField;
import org.opensearch.parquet.memory.ArrowBufferPool;
import org.opensearch.parquet.writer.ParquetDocumentInput;
import org.opensearch.parquet.writer.ParquetWriter;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.opensearch.index.engine.dataformat.FieldTypeCapabilities.Capability.COLUMNAR_STORAGE;
import static org.opensearch.index.engine.dataformat.FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH;

/**
 * Produces a matched <b>golden file pair</b> — one Lucene segment (composite block
 * keys, nested blocks) and one Parquet file ({@code LIST<STRUCT>} column) — for the
 * same deterministic nested dataset, written through the real composite broadcast and
 * both real writers, and verifies the cross-format identity contract on the artifacts:
 *
 * <ul>
 *   <li>Parquet row count == Lucene logical row count (parent blocks)</li>
 *   <li>each Lucene parent key's decoded logical row == the Parquet row index</li>
 *   <li>each Lucene child key's decoded ordinal == the child's Parquet list position</li>
 * </ul>
 *
 * <p><b>Dataset</b> (fixed, no randomization — the pair must be reproducible):
 * <pre>
 * row 0: { title: "First post",  comments: [ {author: alice, score: 5}, {author: dave} ] }
 * row 1: { title: "Second post" }                                        (no nested)
 * row 2: { title: "Third post",  comments: [ {author: erin, score: 9} ] }
 * </pre>
 * Lucene physical layout (6 docs): [c(0,0)=alice, c(0,1)=dave, p(0), p(1), c(2,0)=erin, p(2)].
 *
 * <p><b>Exporting the pair for the query-side POC:</b> the test security policy forbids
 * writing outside the test sandbox, so the export path is the test framework's own
 * temp-dir retention flag — run:
 * <pre>
 * ./gradlew -Dsandbox.enabled=true :sandbox:plugins:composite-engine:test \
 *   --tests "*.CompositeNestedGoldenFilesTests" -Dtests.leaveTemporary=true
 * </pre>
 * and the test logs the on-disk locations of the segment directory and the Parquet
 * file (under {@code build/testrun/test/temp/...}), which survive the run and can be
 * copied out by hand.
 */
public class CompositeNestedGoldenFilesTests extends OpenSearchTestCase {

    private final LuceneDataFormat luceneFormat = new LuceneDataFormat();
    private final ParquetDataFormat parquetFormat = new ParquetDataFormat();

    private ArrowNativeAllocator nativeAllocator;
    private ArrowBufferPool bufferPool;
    private ThreadPool threadPool;
    private IndexSettings indexSettings;

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
        RustBridge.initLogger();
        nativeAllocator = new ArrowNativeAllocator();
        nativeAllocator.getOrCreatePool(NativeAllocatorPoolConfig.POOL_INGEST, 0L, Long.MAX_VALUE, null);
        bufferPool = new ArrowBufferPool(Settings.EMPTY, nativeAllocator);
        Settings settings = Settings.builder().put("node.name", "nested-golden-test").build();
        threadPool = new ThreadPool(
            settings,
            new FixedExecutorBuilder(
                settings,
                ParquetDataFormatPlugin.PARQUET_THREAD_POOL_NAME,
                1,
                -1,
                "thread_pool." + ParquetDataFormatPlugin.PARQUET_THREAD_POOL_NAME
            )
        );
        Settings indexSettingsBuilder = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        indexSettings = new IndexSettings(IndexMetadata.builder("golden-index").settings(indexSettingsBuilder).build(), Settings.EMPTY);

        titleField = dualFormatKeyword("title");
        authorField = dualFormatKeyword("comments.author");
        scoreField = parquetOnly(new NumberFieldMapper.NumberFieldType("comments.score", NumberFieldMapper.NumberType.INTEGER));
        seqNoField = parquetOnly(new NumberFieldMapper.NumberFieldType(SeqNoFieldMapper.NAME, NumberFieldMapper.NumberType.LONG));
        versionField = parquetOnly(new NumberFieldMapper.NumberFieldType(VersionFieldMapper.NAME, NumberFieldMapper.NumberType.LONG));
        primaryTermField = parquetOnly(
            new NumberFieldMapper.NumberFieldType(SeqNoFieldMapper.PRIMARY_TERM_NAME, NumberFieldMapper.NumberType.LONG)
        );
        idField = parquetOnly(idFieldType());
    }

    @Override
    public void tearDown() throws Exception {
        terminate(threadPool);
        bufferPool.close();
        if (nativeAllocator != null) {
            nativeAllocator.close();
            nativeAllocator = null;
        }
        super.tearDown();
    }

    /**
     * Writes the deterministic dataset through both real writers, verifies the
     * cross-format identity contract on the flushed artifacts, and (optionally)
     * exports the pair.
     */
    public void testGoldenSegmentParquetPair() throws Exception {
        Path luceneDir = createTempDir();
        Path parquetDir = createTempDir();
        String parquetPath = parquetDir.resolve("nested.parquet").toString();

        Schema schema = buildNestedSchema();
        ParquetWriter parquetWriter = new ParquetWriter(
            parquetPath,
            1L,
            1L,
            parquetFormat,
            schema,
            () -> schema,
            bufferPool,
            indexSettings,
            threadPool,
            null
        );

        WriterFileSet luceneFiles;
        try (
            LuceneWriter luceneWriter = new LuceneWriter(
                1L,
                0L,
                luceneFormat,
                luceneDir,
                null,
                Codec.getDefault(),
                null,
                ConcurrentHashMap.newKeySet(),
                new LuceneShardStatsTracker(),
                true
            )
        ) {
            // row 0: two comments
            writeDoc(parquetWriter, luceneWriter, 0, "First post", "doc-0", new String[] { "alice", "dave" }, new Integer[] { 5, null });
            // row 1: no nested content — childless parent / null Parquet list
            writeDoc(parquetWriter, luceneWriter, 1, "Second post", "doc-1", new String[0], new Integer[0]);
            // row 2: one comment
            writeDoc(parquetWriter, luceneWriter, 2, "Third post", "doc-2", new String[] { "erin" }, new Integer[] { 9 });

            parquetWriter.flush(FlushInput.EMPTY);
            luceneFiles = luceneWriter.flush(FlushInput.EMPTY).getWriterFileSet(luceneFormat).orElseThrow();
        }

        // --- Cross-format contract on the artifacts ---

        // Row parity: 3 Parquet rows == 3 Lucene LOGICAL rows (the segment holds 6 docs).
        assertEquals(3, RustBridge.getFileMetadata(parquetPath).numRows());
        assertEquals(3L, luceneFiles.numRows());

        // Parquet content shape: titles present; row 1's comments column is null (not empty list).
        String json = RustBridge.readAsJson(parquetPath);
        assertTrue("all rows present: " + json, json.contains("First post") && json.contains("Second post") && json.contains("Third post"));
        assertTrue("nested rows carry LIST<STRUCT>: " + json, json.contains("List(Struct("));
        assertTrue("flat row must have null comments: " + json, json.contains("\"comments\":null"));

        // Lucene block layout + sequential row ids (Scheme C: __row_id__ == docId).
        // Physical layout: [alice=0, dave=1, p(row0)=2, p(row1)=3, erin=4, p(row2)=5].
        // The block↔Parquet-row correspondence is positional: Kth parent == Parquet row K.
        try (NIOFSDirectory dir = new NIOFSDirectory(Path.of(luceneFiles.directory())); IndexReader reader = DirectoryReader.open(dir)) {
            LeafReader leaf = reader.leaves().get(0).reader();
            assertEquals("6 physical docs (3 children + 3 parents)", 6, leaf.maxDoc());
            SortedNumericDocValues rowIds = leaf.getSortedNumericDocValues(DocumentInput.ROW_ID_FIELD);
            for (int docId = 0; docId < 6; docId++) {
                assertTrue(rowIds.advanceExact(docId));
                assertEquals("sequential row id at docId=" + docId, docId, rowIds.nextValue());
            }

            // Identity contract by query: each author's derived (logicalRow, listPosition)
            // — parent rank and offset from block start — must equal its Parquet
            // (row index, list position). Blocks: row0=[0..2], row1=[3], row2=[4..5].
            IndexSearcher searcher = new IndexSearcher(reader);
            assertAuthorAt(searcher, leaf, "alice", 0);  // block 0, element 0
            assertAuthorAt(searcher, leaf, "dave", 1);   // block 0, element 1
            assertAuthorAt(searcher, leaf, "erin", 4);   // block 2, element 0
        }

        // --- Handoff breadcrumbs for the query-side POC ---
        // The security policy forbids copying outside the test sandbox; instead, run with
        // -Dtests.leaveTemporary=true and grab the pair from these logged locations. The
        // segment directory includes its segments_N commit file, so it opens standalone
        // with a plain DirectoryReader.
        logger.info("Nested golden pair: Lucene segment dir = {}", luceneFiles.directory());
        logger.info("Nested golden pair: Parquet file = {}", parquetPath);
    }

    // ========== Dataset driver ==========

    /**
     * Drives one logical document through the composite broadcast in the engine's real
     * call order (parse fields with child scopes → post-parse metadata → setRowId) and
     * hands the result to both writers.
     */
    private void writeDoc(
        ParquetWriter parquetWriter,
        LuceneWriter luceneWriter,
        long rowId,
        String title,
        String docId,
        String[] authors,
        Integer[] scores
    ) throws Exception {
        ParquetDocumentInput parquet = new ParquetDocumentInput();
        LuceneDocumentInput lucene = new LuceneDocumentInput(new LuceneFieldFactoryRegistry(), true);
        CompositeDocumentInput composite = new CompositeDocumentInput(
            ParquetDataFormatPlugin.PARQUET_DATA_FORMAT,
            parquet,
            Map.of(LucenePlugin.DATA_FORMAT, lucene)
        );

        composite.addField(titleField, title);
        for (int i = 0; i < authors.length; i++) {
            composite.beginChild("comments");
            composite.addField(authorField, authors[i]);
            if (scores[i] != null) {
                composite.addField(scoreField, scores[i]);
            }
            composite.endChild();
        }

        composite.addField(idField, docId.getBytes(StandardCharsets.UTF_8));
        composite.addField(versionField, 1L);
        composite.addField(seqNoField, 100L + rowId);
        composite.addField(primaryTermField, 1L);
        composite.setRowId(DocumentInput.ROW_ID_FIELD, rowId);

        assertTrue(parquetWriter.addDoc(parquet) instanceof WriteResult.Success);
        assertTrue(luceneWriter.addDoc(lucene) instanceof WriteResult.Success);
        parquet.close();
    }

    // ========== Assertions ==========

    /**
     * Asserts the author's doc landed at the expected physical position and carries a
     * sequential row id (I1). Under Scheme C the child's Parquet list position is not
     * stored — it is derivable as (docId − blockStart) — so the position check pins the
     * physical layout the derivation depends on.
     */
    private void assertAuthorAt(IndexSearcher searcher, LeafReader leaf, String author, int expectedDocId) throws Exception {
        ScoreDoc hit = searcher.search(new TermQuery(new Term("comments.author", author)), 1).scoreDocs[0];
        assertEquals(author + " must sit at the expected block position", expectedDocId, hit.doc);
        SortedNumericDocValues rowIds = leaf.getSortedNumericDocValues(DocumentInput.ROW_ID_FIELD);
        assertTrue(rowIds.advanceExact(hit.doc));
        assertEquals(author + "'s row id must equal her docId (I1)", hit.doc, rowIds.nextValue());
    }

    // ========== Field/schema fixtures (mirroring CompositeNestedBroadcastTests) ==========

    private Schema buildNestedSchema() {
        ParquetField keywordPf = ArrowFieldRegistry.getParquetField("keyword");
        ParquetField intPf = ArrowFieldRegistry.getParquetField("integer");
        Field author = new Field("author", keywordPf.getFieldType(), null);
        Field score = new Field("score", intPf.getFieldType(), null);
        Field element = new Field("element", FieldType.nullable(ArrowType.Struct.INSTANCE), List.of(author, score));
        Field comments = new Field("comments", FieldType.nullable(new ArrowType.List()), List.of(element));

        List<Field> fields = new ArrayList<>();
        fields.add(new Field("title", keywordPf.getFieldType(), null));
        fields.add(comments);
        fields.add(new Field(VersionFieldMapper.NAME, FieldType.notNullable(new ArrowType.Int(64, true)), null));
        fields.add(new Field(SeqNoFieldMapper.NAME, FieldType.notNullable(new ArrowType.Int(64, true)), null));
        fields.add(new Field(SeqNoFieldMapper.PRIMARY_TERM_NAME, FieldType.notNullable(new ArrowType.Int(64, true)), null));
        fields.add(new Field(IdFieldMapper.NAME, FieldType.notNullable(new ArrowType.Binary()), null));
        return new Schema(fields);
    }

    private static MappedFieldType parquetOnly(MappedFieldType ft) {
        ft.setCapabilityMap(Map.of(ParquetDataFormatPlugin.PARQUET_DATA_FORMAT, Set.of(COLUMNAR_STORAGE)));
        return ft;
    }

    private static MappedFieldType dualFormatKeyword(String name) {
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

    private static MappedFieldType idFieldType() {
        return new MappedFieldType(IdFieldMapper.CONTENT_TYPE, true, true, true, TextSearchInfo.SIMPLE_MATCH_ONLY, Map.of()) {
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
                return IdFieldMapper.CONTENT_TYPE;
            }

            @Override
            public org.apache.lucene.search.Query termQuery(Object value, org.opensearch.index.query.QueryShardContext context) {
                return null;
            }
        };
    }
}
