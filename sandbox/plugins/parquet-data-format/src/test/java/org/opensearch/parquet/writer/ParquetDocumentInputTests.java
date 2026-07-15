/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.writer;

import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.parquet.ParquetBaseTests;
import org.opensearch.parquet.engine.ParquetDataFormat;

import java.util.List;

public class ParquetDocumentInputTests extends ParquetBaseTests {

    private static final DataFormat PARQUET_FORMAT = new ParquetDataFormat();

    public void testAddFieldAndGetFinalInput() {
        ParquetDocumentInput input = new ParquetDocumentInput();
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType("age", NumberFieldMapper.NumberType.INTEGER);
        assignTestCapabilities(ft, PARQUET_FORMAT);
        input.addField(ft, 25);
        input.setRowId(DocumentInput.ROW_ID_FIELD, 0L);
        populateMetadataFields(input);
        List<FieldValuePair> result = input.getFinalInput();
        assertEquals(5, result.size());
        assertSame(ft, result.getFirst().getFieldType());
        assertEquals(25, result.getFirst().getValue());
    }

    public void testMultipleFields() {
        ParquetDocumentInput input = new ParquetDocumentInput();
        MappedFieldType ft1 = new NumberFieldMapper.NumberFieldType("a", NumberFieldMapper.NumberType.INTEGER);
        MappedFieldType ft2 = new KeywordFieldMapper.KeywordFieldType("b");
        assignTestCapabilities(ft1, PARQUET_FORMAT);
        assignTestCapabilities(ft2, PARQUET_FORMAT);
        input.addField(ft1, 1);
        input.addField(ft2, "val");
        input.setRowId(DocumentInput.ROW_ID_FIELD, 0L);
        populateMetadataFields(input);
        assertEquals(6, input.getFinalInput().size());
    }

    public void testEmptyInput() {
        ParquetDocumentInput input = new ParquetDocumentInput();
        populateMetadataFields(input);
        input.setRowId(DocumentInput.ROW_ID_FIELD, 0L);
        assertEquals(4, input.getFinalInput().size());
    }

    public void testSetRowId() {
        ParquetDocumentInput input = new ParquetDocumentInput();
        populateMetadataFields(input);
        input.setRowId(DocumentInput.ROW_ID_FIELD, 42L);
        assertEquals(42L, input.getRowId());
    }

    public void testCloseClearsState() {
        ParquetDocumentInput input = new ParquetDocumentInput();
        populateMetadataFields(input);
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType("age", NumberFieldMapper.NumberType.INTEGER);
        assignTestCapabilities(ft, PARQUET_FORMAT);
        input.addField(ft, 25);
        input.setRowId(DocumentInput.ROW_ID_FIELD, 0L);
        assertEquals(5, input.getFinalInput().size());

        input.close();
        assertTrue(input.getFinalInput().isEmpty());
    }

    public void testRejectsDuplicateFieldInSingleDocument() throws Exception {
        ParquetDocumentInput input = new ParquetDocumentInput();
        populateMetadataFields(input);

        NumberFieldMapper.NumberFieldType valField = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.INTEGER);
        assignTestCapabilities(valField, PARQUET_FORMAT);

        input.addField(valField, 10);
        expectThrows(MapperParsingException.class, () -> input.addField(valField, 20));
    }

    /**
     * The two-comment scenario: the same nested field appearing once per child must be
     * accepted (one scope each), where the flat dedup used to reject the second value.
     */
    public void testSameFieldAcceptedAcrossNestedSiblings() {
        ParquetDocumentInput input = new ParquetDocumentInput();
        populateMetadataFields(input);
        input.setRowId(DocumentInput.ROW_ID_FIELD, 0L);
        MappedFieldType author = new KeywordFieldMapper.KeywordFieldType("comments.author");
        assignTestCapabilities(author, PARQUET_FORMAT);

        input.beginChild("comments");
        input.addField(author, "alice");
        input.endChild();

        input.beginChild("comments");
        input.addField(author, "dave");
        input.endChild();

        List<FieldValuePair> pairs = nestedPairs(input);
        assertEquals(2, pairs.size());
        assertEquals("comments[0]", pairs.get(0).getScope().positionalPath());
        assertEquals("alice", pairs.get(0).getValue());
        assertEquals("comments[1]", pairs.get(1).getScope().positionalPath());
        assertEquals("dave", pairs.get(1).getValue());
    }

    /** Duplicate values within the SAME child are still rejected. */
    public void testRejectsDuplicateFieldWithinOneNestedChild() {
        ParquetDocumentInput input = new ParquetDocumentInput();
        MappedFieldType author = new KeywordFieldMapper.KeywordFieldType("comments.author");
        assignTestCapabilities(author, PARQUET_FORMAT);

        input.beginChild("comments");
        input.addField(author, "alice");
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> input.addField(author, "alice-again"));
        assertTrue(e.getMessage().contains("comments[0]"));
    }

    /** Root fields and nested fields dedup independently; root pairs carry no scope. */
    public void testRootAndNestedScopesAreIndependent() {
        ParquetDocumentInput input = new ParquetDocumentInput();
        populateMetadataFields(input);
        input.setRowId(DocumentInput.ROW_ID_FIELD, 0L);
        MappedFieldType title = new KeywordFieldMapper.KeywordFieldType("title");
        MappedFieldType author = new KeywordFieldMapper.KeywordFieldType("comments.author");
        assignTestCapabilities(title, PARQUET_FORMAT);
        assignTestCapabilities(author, PARQUET_FORMAT);

        input.addField(title, "First post");
        input.beginChild("comments");
        input.addField(author, "alice");
        input.endChild();

        List<FieldValuePair> all = input.getFinalInput();
        FieldValuePair titlePair = all.stream().filter(p -> p.getFieldType() == title).findFirst().orElseThrow();
        FieldValuePair authorPair = all.stream().filter(p -> p.getFieldType() == author).findFirst().orElseThrow();
        assertFalse(titlePair.isNested());
        assertNull(titlePair.getScope());
        assertTrue(authorPair.isNested());
    }

    /** Two-level nesting: replies within comments carry the full positional path. */
    public void testMultiLevelNestedScopes() {
        ParquetDocumentInput input = new ParquetDocumentInput();
        populateMetadataFields(input);
        input.setRowId(DocumentInput.ROW_ID_FIELD, 0L);
        MappedFieldType user = new KeywordFieldMapper.KeywordFieldType("comments.replies.user");
        assignTestCapabilities(user, PARQUET_FORMAT);

        input.beginChild("comments");
        input.beginChild("comments.replies");
        input.addField(user, "bob");
        input.endChild();
        input.beginChild("comments.replies");
        input.addField(user, "carol");
        input.endChild();
        input.endChild();

        List<FieldValuePair> pairs = nestedPairs(input);
        assertEquals("comments[0].replies[0]", pairs.get(0).getScope().positionalPath());
        assertEquals("comments[0].replies[1]", pairs.get(1).getScope().positionalPath());
    }

    /** Returns only the nested-scoped pairs, in insertion order (metadata/root pairs filtered out). */
    private static List<FieldValuePair> nestedPairs(ParquetDocumentInput input) {
        return input.getFinalInput().stream().filter(FieldValuePair::isNested).toList();
    }
}
