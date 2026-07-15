/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.writer;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.Version;
import org.opensearch.arrow.allocator.ArrowNativeAllocator;
import org.opensearch.arrow.spi.NativeAllocatorPoolConfig;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.engine.dataformat.FlushInput;
import org.opensearch.index.engine.dataformat.WriteResult;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.parquet.ParquetBaseTests;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.parquet.engine.ParquetDataFormat;
import org.opensearch.parquet.fields.ArrowFieldRegistry;
import org.opensearch.parquet.fields.ParquetField;
import org.opensearch.parquet.memory.ArrowBufferPool;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class ParquetWriterTests extends ParquetBaseTests {

    private final ParquetDataFormat parquetFormat = new ParquetDataFormat();
    private ArrowNativeAllocator nativeAllocator;
    private ArrowBufferPool bufferPool;
    private MappedFieldType idField;
    private MappedFieldType nameField;
    private MappedFieldType scoreField;
    private Schema schema;
    private ThreadPool threadPool;
    private IndexSettings indexSettings;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        RustBridge.initLogger();
        nativeAllocator = new ArrowNativeAllocator();
        nativeAllocator.getOrCreatePool(NativeAllocatorPoolConfig.POOL_INGEST, 0L, Long.MAX_VALUE, null);
        bufferPool = new ArrowBufferPool(Settings.EMPTY, nativeAllocator);
        idField = new NumberFieldMapper.NumberFieldType("id", NumberFieldMapper.NumberType.INTEGER);
        nameField = new KeywordFieldMapper.KeywordFieldType("name");
        scoreField = new NumberFieldMapper.NumberFieldType("score", NumberFieldMapper.NumberType.LONG);
        assignTestCapabilities(idField, parquetFormat);
        assignTestCapabilities(nameField, parquetFormat);
        assignTestCapabilities(scoreField, parquetFormat);
        schema = buildSchema(List.of(idField, nameField, scoreField));
        Settings indexSettingsBuilder = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder("test-index").settings(indexSettingsBuilder).build();
        indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);
        Settings settings = Settings.builder().put("node.name", "parquetwriter-test").build();
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

    public void testAddDocReturnsSuccess() throws Exception {
        String filePath = createTempDir().resolve("success.parquet").toString();
        ParquetWriter writer = new ParquetWriter(
            filePath,
            1L,
            1L,
            new ParquetDataFormat(),
            schema,
            () -> schema,
            bufferPool,
            indexSettings,
            threadPool,
            null
        );

        ParquetDocumentInput doc = new ParquetDocumentInput();
        populateMetadataFields(doc);
        doc.addField(idField, 1);
        doc.addField(nameField, "alice");
        doc.addField(scoreField, 100L);
        doc.setRowId(DocumentInput.ROW_ID_FIELD, 0);
        WriteResult result = writer.addDoc(doc);
        assertTrue(result instanceof WriteResult.Success);
        doc.close();
        writer.flush(FlushInput.EMPTY);
    }

    public void testSingleDocumentFlush() throws Exception {
        String filePath = createTempDir().resolve("single.parquet").toString();
        ParquetWriter writer = new ParquetWriter(
            filePath,
            1L,
            1L,
            new ParquetDataFormat(),
            schema,
            () -> schema,
            bufferPool,
            indexSettings,
            threadPool,
            null
        );

        ParquetDocumentInput doc = new ParquetDocumentInput();
        populateMetadataFields(doc);
        doc.addField(idField, 42);
        doc.addField(nameField, "bob");
        doc.addField(scoreField, 500L);
        doc.setRowId(DocumentInput.ROW_ID_FIELD, 0);
        writer.addDoc(doc);
        doc.close();

        writer.flush(FlushInput.EMPTY);
        assertEquals(1, RustBridge.getFileMetadata(filePath).numRows());
    }

    public void testMultipleDocumentsFlush() throws Exception {
        String filePath = createTempDir().resolve("multi.parquet").toString();
        ParquetWriter writer = new ParquetWriter(
            filePath,
            1L,
            1L,
            new ParquetDataFormat(),
            schema,
            () -> schema,
            bufferPool,
            indexSettings,
            threadPool,
            null
        );

        for (int i = 0; i < 10; i++) {
            ParquetDocumentInput doc = new ParquetDocumentInput();
            populateMetadataFields(doc);
            doc.addField(idField, i);
            doc.addField(nameField, "user_" + i);
            doc.addField(scoreField, (long) (i * 100));
            doc.setRowId("__row_id__", i);
            writer.addDoc(doc);
            doc.close();
        }

        FileInfos fileInfos = writer.flush(FlushInput.EMPTY);
        assertNotNull(fileInfos);
        assertTrue(Files.exists(Path.of(filePath)));
        assertEquals(10, RustBridge.getFileMetadata(filePath).numRows());
    }

    public void testFlushWithNoDocuments() throws Exception {
        String filePath = createTempDir().resolve("empty.parquet").toString();
        ParquetWriter writer = new ParquetWriter(
            filePath,
            1L,
            1L,
            new ParquetDataFormat(),
            schema,
            () -> schema,
            bufferPool,
            indexSettings,
            threadPool,
            null
        );
        assertEquals(FileInfos.empty(), writer.flush(FlushInput.EMPTY));
    }

    private Schema buildSchema(List<MappedFieldType> fieldTypes) {
        List<Field> fields = new ArrayList<>();
        for (MappedFieldType ft : fieldTypes) {
            ParquetField pf = ArrowFieldRegistry.getParquetField(ft.typeName());
            assertNotNull("No ParquetField registered for type: " + ft.typeName(), pf);
            fields.add(new Field(ft.name(), pf.getFieldType(), null));
        }
        fields.addAll(metadataFields());
        return new Schema(fields);
    }

    /**
     * End-to-end nested round trip through the native Rust writer: a two-comment doc plus
     * a flat doc flushed to a real Parquet file, read back and verified structurally —
     * proving LIST&lt;STRUCT&gt; vectors survive the Arrow C Data export and Dremel encoding.
     */
    public void testNestedListStructRoundTrip() throws Exception {
        // Schema: title (flat keyword) + comments: LIST<STRUCT<author, score>> + metadata.
        MappedFieldType titleField = new KeywordFieldMapper.KeywordFieldType("title");
        MappedFieldType authorField = new KeywordFieldMapper.KeywordFieldType("comments.author");
        MappedFieldType commentScoreField = new NumberFieldMapper.NumberFieldType("comments.score", NumberFieldMapper.NumberType.INTEGER);
        assignTestCapabilities(titleField, parquetFormat);
        assignTestCapabilities(authorField, parquetFormat);
        assignTestCapabilities(commentScoreField, parquetFormat);

        ParquetField keywordPf = ArrowFieldRegistry.getParquetField("keyword");
        ParquetField intPf = ArrowFieldRegistry.getParquetField("integer");
        Field author = new Field("author", keywordPf.getFieldType(), null);
        Field score = new Field("score", intPf.getFieldType(), null);
        Field element = new Field(
            "element",
            org.apache.arrow.vector.types.pojo.FieldType.nullable(org.apache.arrow.vector.types.pojo.ArrowType.Struct.INSTANCE),
            List.of(author, score)
        );
        Field comments = new Field(
            "comments",
            org.apache.arrow.vector.types.pojo.FieldType.nullable(new org.apache.arrow.vector.types.pojo.ArrowType.List()),
            List.of(element)
        );
        List<Field> fields = new ArrayList<>();
        fields.add(new Field("title", keywordPf.getFieldType(), null));
        fields.add(comments);
        fields.addAll(metadataFields());
        Schema nestedSchema = new Schema(fields);

        String filePath = createTempDir().resolve("nested.parquet").toString();
        ParquetWriter writer = new ParquetWriter(
            filePath,
            1L,
            1L,
            new ParquetDataFormat(),
            nestedSchema,
            () -> nestedSchema,
            bufferPool,
            indexSettings,
            threadPool,
            null
        );

        // Doc 0: two comments (dave has no score). Doc 1: flat, no comments.
        ParquetDocumentInput nestedDoc = new ParquetDocumentInput();
        populateMetadataFields(nestedDoc);
        nestedDoc.addField(titleField, "First post");
        nestedDoc.beginChild("comments");
        nestedDoc.addField(authorField, "alice");
        nestedDoc.addField(commentScoreField, 5);
        nestedDoc.endChild();
        nestedDoc.beginChild("comments");
        nestedDoc.addField(authorField, "dave");
        nestedDoc.endChild();
        nestedDoc.setRowId(DocumentInput.ROW_ID_FIELD, 0);
        assertTrue(writer.addDoc(nestedDoc) instanceof WriteResult.Success);
        nestedDoc.close();

        ParquetDocumentInput flatDoc = new ParquetDocumentInput();
        populateMetadataFields(flatDoc);
        flatDoc.addField(titleField, "Second post");
        flatDoc.setRowId(DocumentInput.ROW_ID_FIELD, 1);
        assertTrue(writer.addDoc(flatDoc) instanceof WriteResult.Success);
        flatDoc.close();

        writer.flush(FlushInput.EMPTY);
        assertEquals(2, RustBridge.getFileMetadata(filePath).numRows());

        // Read the file back through the native debug reader. Its JSON serializer cannot
        // render list values (prints "<unsupported:List(...)>"), so element-level values
        // are verified at the vector layer (NestedVectorWriterTests); here we verify the
        // file-level structure: the LIST<STRUCT> column round-tripped with the right
        // shape, the nested row holds a NON-NULL list, and the flat row holds null.
        String json = RustBridge.readAsJson(filePath);
        assertTrue("both rows present: " + json, json.contains("First post") && json.contains("Second post"));
        // Row 0's comments value is non-null and typed LIST<STRUCT<author: Utf8, score: Int32>>.
        assertTrue("row 0 comments must be a present list value: " + json, json.contains("List(Struct("));
        assertTrue("struct children must round-trip: " + json, json.contains("author") && json.contains("score"));
        // Row 1 (flat doc) must have a null comments column, not an empty list.
        assertTrue("flat row must have null comments: " + json, json.contains("\"comments\":null"));
    }
}
