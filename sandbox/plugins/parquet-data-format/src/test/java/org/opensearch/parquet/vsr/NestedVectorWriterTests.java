/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.vsr;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.engine.dataformat.NestedScope;
import org.opensearch.index.engine.dataformat.NestedScopeTracker;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.parquet.ParquetBaseTests;
import org.opensearch.parquet.writer.FieldValuePair;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Tests for {@link NestedVectorWriter}: scope-labeled pairs written into
 * {@code LIST<STRUCT>} vectors with element positions equal to parse-time ordinals.
 */
public class NestedVectorWriterTests extends ParquetBaseTests {

    private static final MappedFieldType AUTHOR = new KeywordFieldMapper.KeywordFieldType("comments.author");
    private static final MappedFieldType SCORE = new NumberFieldMapper.NumberFieldType(
        "comments.score",
        NumberFieldMapper.NumberType.INTEGER
    );
    private static final MappedFieldType REPLY_USER = new KeywordFieldMapper.KeywordFieldType("comments.replies.user");

    /** Builds the comments {@code LIST<STRUCT<author,score>>} vector (single-level shape). */
    private static ListVector newCommentsVector(BufferAllocator allocator) {
        Field author = new Field("author", FieldType.nullable(new ArrowType.Utf8()), null);
        Field score = new Field("score", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field element = new Field("element", FieldType.nullable(ArrowType.Struct.INSTANCE), List.of(author, score));
        Field comments = new Field("comments", FieldType.nullable(new ArrowType.List()), List.of(element));
        ListVector vector = (ListVector) comments.createVector(allocator);
        vector.allocateNewSafe();
        return vector;
    }

    /** Builds comments {@code LIST<STRUCT<author, replies: LIST<STRUCT<user>>>>} (two-level shape). */
    private static ListVector newCommentsWithRepliesVector(BufferAllocator allocator) {
        Field user = new Field("user", FieldType.nullable(new ArrowType.Utf8()), null);
        Field replyElement = new Field("element", FieldType.nullable(ArrowType.Struct.INSTANCE), List.of(user));
        Field replies = new Field("replies", FieldType.nullable(new ArrowType.List()), List.of(replyElement));
        Field author = new Field("author", FieldType.nullable(new ArrowType.Utf8()), null);
        Field element = new Field("element", FieldType.nullable(ArrowType.Struct.INSTANCE), List.of(author, replies));
        Field comments = new Field("comments", FieldType.nullable(new ArrowType.List()), List.of(element));
        ListVector vector = (ListVector) comments.createVector(allocator);
        vector.allocateNewSafe();
        return vector;
    }

    private static Function<String, org.apache.arrow.vector.FieldVector> lookup(ListVector comments) {
        return name -> "comments".equals(name) ? comments : null;
    }

    /** The running example: two comments, dave has no score → null leaf, positions by ordinal. */
    public void testTwoCommentsWithMissingLeaf() {
        try (BufferAllocator allocator = new RootAllocator(); ListVector comments = newCommentsVector(allocator)) {
            NestedScopeTracker tracker = new NestedScopeTracker();
            List<FieldValuePair> pairs = new ArrayList<>();

            NestedScope alice = tracker.beginChild("comments");
            pairs.add(new FieldValuePair(AUTHOR, "alice", alice));
            pairs.add(new FieldValuePair(SCORE, 5, alice));
            tracker.endChild();
            NestedScope dave = tracker.beginChild("comments");
            pairs.add(new FieldValuePair(AUTHOR, "dave", dave));
            tracker.endChild();

            NestedVectorWriter.write(lookup(comments), 0, tracker.scopesInParseOrder(), pairs);
            comments.setValueCount(1);

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> row = (List<Map<String, Object>>) comments.getObject(0);
            assertEquals(2, row.size());
            assertEquals("alice", row.get(0).get("author").toString());
            assertEquals(5, ((Number) row.get(0).get("score")).intValue());
            assertEquals("dave", row.get(1).get("author").toString());
            assertNull("missing leaf must be null, not defaulted", row.get(1).get("score"));
        }
    }

    /** Two-level nesting: replies land inside their own comment, positions by per-level ordinal. */
    public void testTwoLevelNesting() {
        try (BufferAllocator allocator = new RootAllocator(); ListVector comments = newCommentsWithRepliesVector(allocator)) {
            NestedScopeTracker tracker = new NestedScopeTracker();
            List<FieldValuePair> pairs = new ArrayList<>();

            NestedScope comment0 = tracker.beginChild("comments");
            pairs.add(new FieldValuePair(AUTHOR, "alice", comment0));
            NestedScope reply0 = tracker.beginChild("comments.replies");
            pairs.add(new FieldValuePair(REPLY_USER, "bob", reply0));
            tracker.endChild();
            NestedScope reply1 = tracker.beginChild("comments.replies");
            pairs.add(new FieldValuePair(REPLY_USER, "carol", reply1));
            tracker.endChild();
            tracker.endChild();
            NestedScope comment1 = tracker.beginChild("comments");
            pairs.add(new FieldValuePair(AUTHOR, "dave", comment1));
            tracker.endChild();

            NestedVectorWriter.write(lookup(comments), 0, tracker.scopesInParseOrder(), pairs);
            comments.setValueCount(1);

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> row = (List<Map<String, Object>>) comments.getObject(0);
            assertEquals(2, row.size());
            assertEquals("alice", row.get(0).get("author").toString());
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> aliceReplies = (List<Map<String, Object>>) row.get(0).get("replies");
            assertEquals(2, aliceReplies.size());
            assertEquals("bob", aliceReplies.get(0).get("user").toString());
            assertEquals("carol", aliceReplies.get(1).get("user").toString());
            // dave has no replies → null (never started), not empty.
            assertEquals("dave", row.get(1).get("author").toString());
            assertNull(row.get(1).get("replies"));
        }
    }

    /** Rows without nested data leave the list null; later rows still write correctly. */
    public void testSparseRowsLeaveNullLists() {
        try (BufferAllocator allocator = new RootAllocator(); ListVector comments = newCommentsVector(allocator)) {
            // Row 0: flat document — no nested scopes at all.
            NestedVectorWriter.write(lookup(comments), 0, List.of(), List.of());

            // Row 1: one comment.
            NestedScopeTracker tracker = new NestedScopeTracker();
            NestedScope scope = tracker.beginChild("comments");
            tracker.endChild();
            NestedVectorWriter.write(
                lookup(comments),
                1,
                tracker.scopesInParseOrder(),
                List.of(new FieldValuePair(AUTHOR, "alice", scope))
            );
            comments.setValueCount(2);

            assertTrue("row without nested values must be null", comments.isNull(0));
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> row1 = (List<Map<String, Object>>) comments.getObject(1);
            assertEquals("alice", row1.get(0).get("author").toString());
        }
    }

    /** A child whose fields are all Lucene-only still occupies its list position. */
    public void testFieldlessChildKeepsItsPosition() {
        try (BufferAllocator allocator = new RootAllocator(); ListVector comments = newCommentsVector(allocator)) {
            NestedScopeTracker tracker = new NestedScopeTracker();
            List<FieldValuePair> pairs = new ArrayList<>();

            NestedScope alice = tracker.beginChild("comments");
            pairs.add(new FieldValuePair(AUTHOR, "alice", alice));
            tracker.endChild();
            tracker.beginChild("comments"); // middle child: no Parquet-supported fields
            tracker.endChild();
            NestedScope dave = tracker.beginChild("comments");
            pairs.add(new FieldValuePair(AUTHOR, "dave", dave));
            tracker.endChild();

            NestedVectorWriter.write(lookup(comments), 0, tracker.scopesInParseOrder(), pairs);
            comments.setValueCount(1);

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> row = (List<Map<String, Object>>) comments.getObject(0);
            assertEquals("ordinal alignment requires 3 elements", 3, row.size());
            assertEquals("alice", row.get(0).get("author").toString());
            assertNull("fieldless element present but empty", row.get(1).get("author"));
            assertEquals("dave must stay at ordinal 2", "dave", row.get(2).get("author").toString());
        }
    }

    /** byte[] values into text vectors must be written as UTF-8, never toString()'d. */
    public void testByteArrayValueWrittenAsUtf8() {
        try (BufferAllocator allocator = new RootAllocator(); ListVector comments = newCommentsVector(allocator)) {
            NestedScopeTracker tracker = new NestedScopeTracker();
            NestedScope scope = tracker.beginChild("comments");
            tracker.endChild();
            byte[] utf8 = "raw-bytes-value".getBytes(StandardCharsets.UTF_8);

            NestedVectorWriter.write(lookup(comments), 0, tracker.scopesInParseOrder(), List.of(new FieldValuePair(AUTHOR, utf8, scope)));
            comments.setValueCount(1);

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> row = (List<Map<String, Object>>) comments.getObject(0);
            assertEquals("raw-bytes-value", row.get(0).get("author").toString());
        }
    }

    /** Nested path with no vector in the schema (all fields unsupported) is skipped silently. */
    public void testMissingPathVectorIsSkipped() {
        try (BufferAllocator allocator = new RootAllocator(); ListVector comments = newCommentsVector(allocator)) {
            NestedScopeTracker tracker = new NestedScopeTracker();
            NestedScope scope = tracker.beginChild("attachments"); // no vector for this path
            tracker.endChild();

            // Must not throw; comments vector untouched.
            NestedVectorWriter.write(
                lookup(comments),
                0,
                tracker.scopesInParseOrder(),
                List.of(new FieldValuePair(AUTHOR, "ignored", scope))
            );
            comments.setValueCount(1);
            assertTrue(comments.isNull(0));
        }
    }
}
