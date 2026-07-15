/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NestedPathFieldMapper;
import org.opensearch.index.mapper.SeqNoFieldMapper;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;

import static org.opensearch.index.engine.dataformat.FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies that each field type registered in {@link org.opensearch.be.lucene.LuceneFieldFactoryRegistry}
 * produces Lucene fields with the expected storage properties.
 */
public class LuceneDocumentInputTests extends LucenePluginBaseTests {

    public void testIdFieldProperties() {
        MappedFieldType idField = mockIdField();
        LuceneDocumentInput input = new LuceneDocumentInput();
        input.addField(idField, "test-id".getBytes(StandardCharsets.UTF_8));

        Document doc = input.getFinalInput().get(0);
        IndexableField field = doc.getField(IdFieldMapper.NAME);
        assertNotNull("_id field should be present in document", field);

        IndexableFieldType ft = field.fieldType();
        assertFalse("_id: should not be stored", ft.stored());
        assertNotEquals("_id: should be indexed", IndexOptions.NONE, ft.indexOptions());
    }

    public void testTextFieldProperties() {
        MappedFieldType textField = mockTextField("content");
        LuceneDocumentInput input = new LuceneDocumentInput();
        input.addField(textField, "hello world");

        Document doc = input.getFinalInput().get(0);
        IndexableField field = doc.getField("content");
        assertNotNull("text field should be present in document", field);

        IndexableFieldType ft = field.fieldType();
        assertFalse("text: should not be stored", ft.stored());
        assertTrue("text: should omit norms", ft.omitNorms());
        assertEquals("text: should have no doc values", DocValuesType.NONE, ft.docValuesType());
        assertNotEquals("text: should be indexed", IndexOptions.NONE, ft.indexOptions());
    }

    public void testKeywordFieldProperties() {
        MappedFieldType keywordField = mockKeywordField("status");

        LuceneDocumentInput input = new LuceneDocumentInput();
        input.addField(keywordField, "active");

        Document doc = input.getFinalInput().get(0);
        IndexableField field = doc.getField("status");
        assertNotNull("keyword field should be present in document", field);

        IndexableFieldType ft = field.fieldType();
        assertFalse("keyword: should not be stored", ft.stored());
        assertTrue("keyword: should omit norms", ft.omitNorms());
        assertEquals("keyword: should have no doc values", DocValuesType.NONE, ft.docValuesType());
        assertNotEquals("keyword: should be indexed", IndexOptions.NONE, ft.indexOptions());
    }

    public void testMatchOnlyTextFieldProperties() {
        MappedFieldType matchOnlyField = mockMatchOnlyTextField("body");

        LuceneDocumentInput input = new LuceneDocumentInput();
        input.addField(matchOnlyField, "some text");

        Document doc = input.getFinalInput().get(0);
        IndexableField field = doc.getField("body");
        assertNotNull("match_only_text field should be present in document", field);

        IndexableFieldType ft = field.fieldType();
        assertFalse("match_only_text: should not be stored", ft.stored());
        assertTrue("match_only_text: should omit norms", ft.omitNorms());
        assertEquals("match_only_text: should have no doc values", DocValuesType.NONE, ft.docValuesType());
        assertNotEquals("match_only_text: should be indexed", IndexOptions.DOCS, ft.indexOptions());
    }

    public void testSeqNoFieldProperties() {
        MappedFieldType seqNoField = mockSeqNoField();
        LuceneDocumentInput input = new LuceneDocumentInput();
        input.addField(seqNoField, 42L);

        Document doc = input.getFinalInput().get(0);
        IndexableField field = doc.getField(SeqNoFieldMapper.NAME);
        assertNull("_seq_no field should be present in document", field);
    }

    private static MappedFieldType mockIdField() {
        MappedFieldType idField = mock(MappedFieldType.class);
        when(idField.typeName()).thenReturn(IdFieldMapper.CONTENT_TYPE);
        when(idField.name()).thenReturn(IdFieldMapper.NAME);
        when(idField.getCapabilityMap()).thenReturn(Map.of(LucenePlugin.DATA_FORMAT, Set.of(FULL_TEXT_SEARCH)));
        return idField;
    }

    private static MappedFieldType mockSeqNoField() {
        MappedFieldType seqNoField = mock(MappedFieldType.class);
        when(seqNoField.typeName()).thenReturn(SeqNoFieldMapper.CONTENT_TYPE);
        when(seqNoField.name()).thenReturn(SeqNoFieldMapper.NAME);
        return seqNoField;
    }

    // ----- nested block building (nested-blocks mode) -----

    /** The two-comment running example: block = [alice, dave, root]; row-id stamping deferred to the writer. */
    public void testNestedChildrenBuildBlockInLayoutOrder() {
        LuceneDocumentInput input = new LuceneDocumentInput(new org.opensearch.be.lucene.LuceneFieldFactoryRegistry(), true);
        MappedFieldType author = mockKeywordField("comments.author");
        MappedFieldType title = mockKeywordField("title");

        input.addField(title, "First post");
        input.beginChild("comments");
        input.addField(author, "alice");
        input.endChild();
        input.beginChild("comments");
        input.addField(author, "dave");
        input.endChild();
        input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, 5L);

        assertTrue(input.hasChildren());
        assertEquals(3, input.blockSize());

        java.util.List<Document> block = input.getFinalInput();
        assertEquals(3, block.size());

        // Children first (emission order), root last.
        assertEquals("alice", block.get(0).getField("comments.author").stringValue());
        assertEquals("dave", block.get(1).getField("comments.author").stringValue());
        assertEquals("First post", block.get(2).getField("title").stringValue());

        // Child docs carry the nested path; root does not.
        assertEquals("comments", block.get(0).getField(NestedPathFieldMapper.NAME).stringValue());
        assertEquals("comments", block.get(1).getField(NestedPathFieldMapper.NAME).stringValue());
        assertNull(block.get(2).getField(NestedPathFieldMapper.NAME));

        // Scheme C: the input records the logical rowId but stamps nothing — sequential
        // docId-space row ids are assigned by LuceneWriter.addDoc, which owns the global
        // physical doc counter this input cannot know at parse time.
        assertEquals(5L, input.getRowId());
        for (Document doc : block) {
            assertNull("row-id stamping is the writer's job in nested-blocks mode", doc.getField(LuceneDocumentInput.ROW_ID_FIELD));
        }
    }

    /** Two-level nesting emits deepest-first: replies precede their comment (vanilla layout). */
    public void testMultiLevelBlockLayoutIsDeepestFirst() {
        LuceneDocumentInput input = new LuceneDocumentInput(new org.opensearch.be.lucene.LuceneFieldFactoryRegistry(), true);
        MappedFieldType user = mockKeywordField("comments.replies.user");
        MappedFieldType author = mockKeywordField("comments.author");

        input.beginChild("comments");
        input.addField(author, "alice");
        input.beginChild("comments.replies");
        input.addField(user, "bob");
        input.endChild();
        input.beginChild("comments.replies");
        input.addField(user, "carol");
        input.endChild();
        input.endChild();
        input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, 0L);

        java.util.List<Document> block = input.getFinalInput();
        assertEquals(4, block.size());
        // Emission (endChild) order: bob, carol, then their enclosing comment, then root.
        assertEquals("bob", block.get(0).getField("comments.replies.user").stringValue());
        assertEquals("carol", block.get(1).getField("comments.replies.user").stringValue());
        assertEquals("alice", block.get(2).getField("comments.author").stringValue());
        assertEquals("comments.replies", block.get(0).getField(NestedPathFieldMapper.NAME).stringValue());
        assertEquals("comments", block.get(2).getField(NestedPathFieldMapper.NAME).stringValue());
    }

    /** Childless documents in nested-blocks mode form a block of one; stamping is still deferred. */
    public void testChildlessNestedModeDocumentDefersStamping() {
        LuceneDocumentInput input = new LuceneDocumentInput(new org.opensearch.be.lucene.LuceneFieldFactoryRegistry(), true);
        input.addField(mockKeywordField("title"), "no comments here");
        input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, 3L);

        java.util.List<Document> block = input.getFinalInput();
        assertEquals(1, block.size());
        assertFalse(input.hasChildren());
        assertEquals(3L, input.getRowId());
        assertNull(block.get(0).getField(LuceneDocumentInput.ROW_ID_FIELD));
    }

    /** Plain mode (no nested mappings): unchanged single-doc behavior, raw row id. */
    public void testPlainModeKeepsRawRowId() {
        LuceneDocumentInput input = new LuceneDocumentInput();
        input.addField(mockKeywordField("title"), "flat doc");
        input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, 7L);

        java.util.List<Document> block = input.getFinalInput();
        assertEquals(1, block.size());
        assertEquals(7L, block.get(0).getField(LuceneDocumentInput.ROW_ID_FIELD).numericValue().longValue());
    }

    /** Plain mode rejects child scopes — nested docs must never slip in without composite keys. */
    public void testPlainModeRejectsChildScopes() {
        LuceneDocumentInput input = new LuceneDocumentInput();
        expectThrows(IllegalStateException.class, () -> input.beginChild("comments"));
    }

    /** Finalizing with an unclosed child scope is a bug in the parser wiring — must throw. */
    public void testUnclosedChildScopeFailsFinalization() {
        LuceneDocumentInput input = new LuceneDocumentInput(new org.opensearch.be.lucene.LuceneFieldFactoryRegistry(), true);
        input.beginChild("comments");
        expectThrows(IllegalStateException.class, input::getFinalInput);
        expectThrows(IllegalStateException.class, () -> input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, 0L));
    }
}
