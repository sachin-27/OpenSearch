/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.common.Explicit;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.ObjectMapper;
import org.opensearch.parquet.ParquetBaseTests;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link ArrowSchemaBuilder}, focused on nested mappings folding into
 * {@code LIST<STRUCT>} columns. Uses the running example mapping:
 * {@code comments} (nested: author, score) containing {@code comments.replies}
 * (nested: user), plus a sibling {@code reviews} nested array.
 */
public class ArrowSchemaBuilderTests extends ParquetBaseTests {

    public void testFlatMappingProducesFlatColumns() {
        Schema schema = buildSchema(List.of(mockFieldMapper("title", "keyword"), mockFieldMapper("views", "integer")), Map.of());

        assertNotNull(schema.findField("title"));
        assertNotNull(schema.findField("views"));
        assertNotNull(schema.findField(DocumentInput.ROW_ID_FIELD));
        // No list columns anywhere.
        for (Field field : schema.getFields()) {
            assertFalse(field.getName() + " must not be a LIST", field.getType() instanceof ArrowType.List);
        }
    }

    public void testSingleLevelNestedBecomesListOfStruct() {
        Schema schema = buildSchema(
            List.of(
                mockFieldMapper("title", "keyword"),
                mockFieldMapper("comments.author", "keyword"),
                mockFieldMapper("comments.score", "integer")
            ),
            Map.of("comments", mockNestedObjectMapper())
        );

        // Root field stays flat; nested leaves must NOT appear as flat columns.
        assertNotNull(schema.findField("title"));
        assertNull(findFieldOrNull(schema, "comments.author"));
        assertNull(findFieldOrNull(schema, "comments.score"));

        Field comments = schema.findField("comments");
        assertTrue("comments must be a LIST", comments.getType() instanceof ArrowType.List);
        Field element = comments.getChildren().get(0);
        assertTrue("list element must be a STRUCT", element.getType() instanceof ArrowType.Struct);
        // Struct children carry names relative to the nested path.
        assertEquals(List.of("author", "score"), element.getChildren().stream().map(Field::getName).toList());
    }

    public void testTwoLevelNestingProducesNestedList() {
        Schema schema = buildSchema(
            List.of(mockFieldMapper("comments.author", "keyword"), mockFieldMapper("comments.replies.user", "keyword")),
            Map.of("comments", mockNestedObjectMapper(), "comments.replies", mockNestedObjectMapper())
        );

        Field comments = schema.findField("comments");
        Field struct = comments.getChildren().get(0);
        assertEquals(2, struct.getChildren().size());
        assertEquals("author", struct.getChildren().get(0).getName());

        Field replies = struct.getChildren().get(1);
        assertEquals("replies", replies.getName());
        assertTrue("replies must be a nested LIST", replies.getType() instanceof ArrowType.List);
        Field repliesStruct = replies.getChildren().get(0);
        assertEquals(List.of("user"), repliesStruct.getChildren().stream().map(Field::getName).toList());

        // The deep path must not surface as a top-level column.
        assertNull(findFieldOrNull(schema, "comments.replies"));
    }

    public void testSiblingNestedArraysGetIndependentColumns() {
        Schema schema = buildSchema(
            List.of(mockFieldMapper("comments.author", "keyword"), mockFieldMapper("reviews.date", "keyword")),
            Map.of("comments", mockNestedObjectMapper(), "reviews", mockNestedObjectMapper())
        );

        assertTrue(schema.findField("comments").getType() instanceof ArrowType.List);
        assertTrue(schema.findField("reviews").getType() instanceof ArrowType.List);
    }

    public void testNestedPathWithNoSupportedFieldsIsOmitted() {
        // "attachments" is nested but its only field has an unregistered type.
        Schema schema = buildSchema(
            List.of(mockFieldMapper("title", "keyword"), mockFieldMapper("attachments.blob", "unsupported_type")),
            Map.of("attachments", mockNestedObjectMapper())
        );

        assertNull(findFieldOrNull(schema, "attachments"));
        assertNotNull(schema.findField("title"));
    }

    public void testNonNestedObjectFieldsStayFlatWithDottedNames() {
        // A plain (non-nested) object: its subfields remain flat dotted columns.
        Schema schema = buildSchema(List.of(mockFieldMapper("meta.owner", "keyword")), Map.of());

        assertNotNull(schema.findField("meta.owner"));
        for (Field field : schema.getFields()) {
            assertFalse(field.getType() instanceof ArrowType.List);
        }
    }

    // ----- mock helpers -----

    private static Field findFieldOrNull(Schema schema, String name) {
        return schema.getFields().stream().filter(f -> f.getName().equals(name)).findFirst().orElse(null);
    }

    private static Mapper mockFieldMapper(String name, String typeName) {
        Mapper mapper = mock(Mapper.class);
        when(mapper.name()).thenReturn(name);
        when(mapper.typeName()).thenReturn(typeName);
        return mapper;
    }

    private static ObjectMapper mockNestedObjectMapper() {
        ObjectMapper om = mock(ObjectMapper.class);
        when(om.nested()).thenReturn(ObjectMapper.Nested.newNested(new Explicit<>(false, false), new Explicit<>(false, false)));
        return om;
    }

    /** Builds the schema through the testable core (no DocumentMapper — MappingLookup is final/unmockable). */
    private static Schema buildSchema(List<Mapper> fieldMappers, Map<String, ObjectMapper> nestedObjectMappers) {
        return ArrowSchemaBuilder.buildSchema(fieldMappers, new LinkedHashMap<>(nestedObjectMappers), null);
    }
}
