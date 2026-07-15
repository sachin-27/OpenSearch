/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.vsr;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.engine.dataformat.NestedScope;
import org.opensearch.parquet.writer.FieldValuePair;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Writes one document's nested (scope-qualified) field values into the document's
 * {@code LIST<STRUCT>} column vectors at a given row index.
 *
 * <p>Input is the flat, scope-labeled buffer produced by
 * {@link org.opensearch.parquet.writer.ParquetDocumentInput}: every nested
 * {@link FieldValuePair} carries its {@link NestedScope} (which nested path, which
 * sibling ordinal, which enclosing parent). This class reassembles the scope tree and
 * writes it out so that <b>list element positions equal the per-level scope ordinals</b>
 * assigned at parse time — the cross-format identity contract: element {@code i} of a
 * Parquet list is the same child as scope ordinal {@code i} on the Lucene side.
 *
 * <p>Structural rules:
 * <ul>
 *   <li>A row with no nested values leaves its list columns <b>null</b> (never started) —
 *       "field absent" and "no children" are both encoded as null lists for now.</li>
 *   <li>A child scope with no Parquet-supported fields still occupies its list position
 *       (an empty-but-defined struct element), so positions stay aligned with Lucene
 *       ordinals even when all of a child's fields are Lucene-only.</li>
 *   <li>Nested paths with no corresponding vector (omitted from the schema because no
 *       field is Parquet-supported) are skipped silently, mirroring the capability
 *       filtering on flat fields.</li>
 * </ul>
 *
 * <p>Leaf struct-children are named relative to their owning nested path, matching
 * {@link org.opensearch.parquet.fields.ArrowSchemaBuilder} ({@code comments.author}
 * → struct child {@code author}; sub-lists {@code comments.replies} → child
 * {@code replies}).
 *
 * <p>Not thread-safe in the same sense as the VSR itself: callers must hold the same
 * single-writer discipline as {@link VSRManager#addDocument}.
 */
final class NestedVectorWriter {

    private static final Logger logger = LogManager.getLogger(NestedVectorWriter.class);

    private NestedVectorWriter() {}

    /** One nested child element: its scope, its leaf values, and its sub-elements grouped by path. */
    private static final class ScopeNode {
        final NestedScope scope;
        final List<FieldValuePair> fields = new ArrayList<>();
        final Map<String, List<ScopeNode>> childrenByPath = new LinkedHashMap<>();

        ScopeNode(NestedScope scope) {
            this.scope = scope;
        }
    }

    /**
     * Writes the document's nested values into the row's list vectors.
     *
     * @param vectorLookup resolves a top-level field name to its vector (typically
     *                     {@code activeVSR::getVector})
     * @param rowIndex     the row being written
     * @param scopes       all nested scopes of the document, in parse order (parents
     *                     before their descendants) — from
     *                     {@code ParquetDocumentInput#getNestedScopes()}
     * @param pairs        the document's buffered field pairs; only nested-scoped pairs
     *                     are consumed here
     */
    static void write(Function<String, FieldVector> vectorLookup, int rowIndex, List<NestedScope> scopes, List<FieldValuePair> pairs) {
        if (scopes.isEmpty()) {
            return;
        }

        // Rebuild the scope tree. Parse order guarantees a parent scope appears before
        // its descendants, so the parent node always exists when a child arrives.
        Map<NestedScope, ScopeNode> nodes = new IdentityHashMap<>();
        Map<String, List<ScopeNode>> topLevelByPath = new LinkedHashMap<>();
        for (NestedScope scope : scopes) {
            ScopeNode node = new ScopeNode(scope);
            nodes.put(scope, node);
            if (scope.parent() == null) {
                topLevelByPath.computeIfAbsent(scope.nestedPath(), k -> new ArrayList<>()).add(node);
            } else {
                ScopeNode parent = nodes.get(scope.parent());
                assert parent != null : "parse order must yield parent scopes before children: " + scope.positionalPath();
                parent.childrenByPath.computeIfAbsent(scope.nestedPath(), k -> new ArrayList<>()).add(node);
            }
        }
        for (FieldValuePair pair : pairs) {
            if (pair.isNested()) {
                nodes.get(pair.getScope()).fields.add(pair);
            }
        }

        for (Map.Entry<String, List<ScopeNode>> entry : topLevelByPath.entrySet()) {
            FieldVector vector = vectorLookup.apply(entry.getKey());
            if (vector instanceof ListVector listVector) {
                writeList(listVector, rowIndex, entry.getKey(), entry.getValue());
            } else {
                // Schema omitted this path (no Parquet-supported fields anywhere under it).
                logger.trace("No LIST vector for nested path [{}] — values are Lucene-only", entry.getKey());
            }
        }
    }

    /**
     * Writes one list value (all sibling elements of one nested path under one parent)
     * at {@code index} of {@code listVector}, recursing into sub-lists.
     *
     * <p>Element positions are the scopes' per-level ordinals. The tracker assigns
     * sibling ordinals densely (0..n-1 in parse order), asserted here.
     */
    private static void writeList(ListVector listVector, int index, String path, List<ScopeNode> elements) {
        int startOffset = listVector.startNewValue(index);
        StructVector structVector = (StructVector) listVector.getDataVector();
        for (int i = 0; i < elements.size(); i++) {
            ScopeNode element = elements.get(i);
            assert element.scope.ordinal() == i : "sibling ordinals must be dense: expected "
                + i
                + " but was "
                + element.scope.ordinal()
                + " at "
                + element.scope.positionalPath();
            int elemIndex = startOffset + i;
            // Mark the element present even if it contributes no Parquet values, so list
            // positions stay aligned with Lucene block ordinals.
            structVector.setIndexDefined(elemIndex);

            for (FieldValuePair pair : element.fields) {
                String leafName = pair.getFieldType().name().substring(path.length() + 1);
                FieldVector leafVector = structVector.getChild(leafName);
                if (leafVector == null) {
                    // Field not in the struct (capability-filtered at schema build time).
                    logger.trace("Struct [{}] has no child vector [{}] — value is Lucene-only", path, leafName);
                    continue;
                }
                setLeafValue(leafVector, elemIndex, pair.getValue(), pair.getFieldType().name());
            }

            for (Map.Entry<String, List<ScopeNode>> sub : element.childrenByPath.entrySet()) {
                String subLeafName = sub.getKey().substring(path.length() + 1);
                FieldVector subVector = structVector.getChild(subLeafName);
                if (subVector instanceof ListVector subList) {
                    writeList(subList, elemIndex, sub.getKey(), sub.getValue());
                } else {
                    logger.trace("Struct [{}] has no sub-list [{}] — values are Lucene-only", path, subLeafName);
                }
            }
        }
        listVector.endValue(index, elements.size());
    }

    /**
     * Writes a single scalar into a struct-child vector at the given element index.
     * Absent values are left null (validity bit unset). Unsupported vector types throw,
     * loudly, rather than writing a stringified representation.
     */
    private static void setLeafValue(FieldVector vector, int index, Object value, String fieldName) {
        if (value == null) {
            return; // leave null
        }
        if (vector instanceof VarCharVector v) {
            v.setSafe(index, toUtf8Bytes(value, fieldName));
        } else if (vector instanceof VarBinaryVector v) {
            if (value instanceof byte[] bytes) {
                v.setSafe(index, bytes);
            } else {
                throw unsupportedValue(vector, value, fieldName);
            }
        } else if (vector instanceof IntVector v) {
            v.setSafe(index, ((Number) value).intValue());
        } else if (vector instanceof BigIntVector v) {
            v.setSafe(index, ((Number) value).longValue());
        } else if (vector instanceof TimeStampVector v) {
            v.setSafe(index, ((Number) value).longValue());
        } else if (vector instanceof Float8Vector v) {
            v.setSafe(index, ((Number) value).doubleValue());
        } else if (vector instanceof Float4Vector v) {
            v.setSafe(index, ((Number) value).floatValue());
        } else if (vector instanceof BitVector v) {
            v.setSafe(index, Boolean.TRUE.equals(value) || "true".equals(value) ? 1 : 0);
        } else {
            throw unsupportedValue(vector, value, fieldName);
        }
    }

    /**
     * Converts a text-ish value to UTF-8 bytes. {@code byte[]} is treated as
     * already-encoded UTF-8 (never {@code toString()}'d, which would write the array's
     * identity hash); anything else uses its string form.
     */
    private static byte[] toUtf8Bytes(Object value, String fieldName) {
        if (value instanceof byte[] bytes) {
            return bytes;
        }
        if (value instanceof String s) {
            return s.getBytes(StandardCharsets.UTF_8);
        }
        if (value instanceof org.apache.lucene.util.BytesRef bytesRef) {
            byte[] out = new byte[bytesRef.length];
            System.arraycopy(bytesRef.bytes, bytesRef.offset, out, 0, bytesRef.length);
            return out;
        }
        return value.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static IllegalArgumentException unsupportedValue(FieldVector vector, Object value, String fieldName) {
        return new IllegalArgumentException(
            "Unsupported nested leaf write: field ["
                + fieldName
                + "] value type ["
                + value.getClass().getSimpleName()
                + "] into vector ["
                + vector.getClass().getSimpleName()
                + "]"
        );
    }
}
