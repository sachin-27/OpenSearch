/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.mapper.MappedFieldType;

/**
 * Represents a document input for adding fields and metadata to a writer.
 *
 * @param <T> the type of the final input representation
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface DocumentInput<T> extends AutoCloseable {

    /** Standard field name for the row ID used to correlate documents across data formats. */
    String ROW_ID_FIELD = "__row_id__";

    /**
     * Gets the final input representation.
     *
     * @return the final input of type T
     */
    T getFinalInput();

    /**
     * Adds a field to the document.
     *
     * <p>While a child scope opened by {@link #beginChild(String)} is active, the value
     * belongs to that nested child rather than the root document; implementations that
     * support {@code nested} fields must qualify the buffered value accordingly.
     *
     * @param fieldType the mapped field type
     * @param value the field value
     */
    void addField(MappedFieldType fieldType, Object value);

    /**
     * Signals that the parser is entering one child object of a {@code nested} field.
     * All subsequent {@link #addField} calls until the matching {@link #endChild()}
     * belong to this child. Calls may nest for multi-level nested mappings; each
     * invocation for the same path under the same parent addresses the next sibling
     * (array element) of that path.
     *
     * <p>The ordinal implied by the invocation order is the child's identity across
     * data formats (Parquet list position, Lucene intra-block ordinal), so every
     * implementation must observe the same parse traversal.
     *
     * <p>The default implementation is a no-op so implementations that do not support
     * nested fields are unaffected.
     *
     * @param nestedPath the full mapper path of the nested field, e.g. {@code comments.replies}
     */
    default void beginChild(String nestedPath) {}

    /**
     * Signals that the parser finished the child object opened by the matching
     * {@link #beginChild(String)}, restoring the enclosing scope.
     *
     * <p>The default implementation is a no-op.
     */
    default void endChild() {}

    /**
     * Adds a row ID field to the document.
     *
     * @param rowIdFieldName the name of the row ID field
     * @param rowId the row ID value
     */
    void setRowId(String rowIdFieldName, long rowId);

    /**
     * Given a field name, returns the number of values associated with that field in the document.
     * @param fieldName name of the field to lookup
     * @return count of field values
     */
    long getFieldCount(String fieldName);
}
