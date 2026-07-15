/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.writer;

import org.opensearch.index.engine.dataformat.NestedScope;
import org.opensearch.index.mapper.MappedFieldType;

/**
 * Immutable pair of an OpenSearch {@link MappedFieldType} and its parsed value,
 * optionally qualified by the {@link NestedScope} (nested child) it belongs to.
 *
 * <p>Represents a single field entry collected by {@link ParquetDocumentInput} during
 * document indexing. The field type is used to resolve the corresponding Arrow vector
 * type via {@link org.opensearch.parquet.fields.ArrowFieldRegistry}, and the value is
 * written into that vector during document transfer to the VSR.
 *
 * <p>A null scope means the value belongs to the root (parent) document and maps to a
 * flat column. A non-null scope means the value belongs to one child of a {@code nested}
 * field (e.g. {@code comments[1]}) and maps into the corresponding element of a
 * {@code LIST<STRUCT>} column.
 *
 * <p>The field type must not be null (enforced by constructor); the value may be null
 * for nullable fields.
 */
public class FieldValuePair {

    private final MappedFieldType fieldType;
    private final Object value;
    private final NestedScope scope;

    /**
     * Creates a new FieldValuePair belonging to the root document.
     *
     * @param fieldType the mapped field type
     * @param value the parsed field value
     */
    public FieldValuePair(MappedFieldType fieldType, Object value) {
        this(fieldType, value, null);
    }

    /**
     * Creates a new FieldValuePair qualified by a nested child scope.
     *
     * @param fieldType the mapped field type
     * @param value the parsed field value
     * @param scope the nested child this value belongs to, or null for the root document
     */
    public FieldValuePair(MappedFieldType fieldType, Object value, NestedScope scope) {
        if (fieldType == null) {
            throw new IllegalArgumentException("fieldType cannot be null");
        }
        this.fieldType = fieldType;
        this.value = value;
        this.scope = scope;
    }

    /**
     * Returns the nested child scope this value belongs to, or null if it belongs
     * to the root document.
     *
     * @return the nested scope or null
     */
    public NestedScope getScope() {
        return scope;
    }

    /** Returns {@code true} if this value belongs to a nested child rather than the root document. */
    public boolean isNested() {
        return scope != null;
    }

    /**
     * Returns the field type.
     *
     * @return the mapped field type
     */
    public MappedFieldType getFieldType() {
        return fieldType;
    }

    /**
     * Returns the value.
     *
     * @return the parsed field value
     */
    public Object getValue() {
        return value;
    }
}
