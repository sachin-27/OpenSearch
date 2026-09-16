/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields.core.data;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.parquet.fields.ParquetField;
import org.opensearch.parquet.vsr.ManagedVSR;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * Parquet field for binary data using {@link VarBinaryVector}.
 *
 * <p>Multi-valued documents are stored as a Parquet {@code LIST<BINARY>} whose per-document
 * values are sorted (unsigned lexicographic) and de-duplicated at write time — the same
 * normalization the classic Lucene path applies when packing
 * {@code CustomBinaryDocValuesField}'s framed encoding. Storing the normalized form means the
 * columnar values and the classic doc values are identical, and readers can frame values
 * straight off the column without re-sorting.
 */
public class BinaryParquetField extends ParquetField {

    /** Creates a new BinaryParquetField. */
    public BinaryParquetField() {}

    @Override
    protected void addToGroup(MappedFieldType mappedFieldType, ManagedVSR managedVSR, Object parseValue) {
        addToVector(managedVSR.getVector(mappedFieldType.name()), managedVSR.getRowCount(), parseValue);
    }

    @Override
    protected void addToVector(FieldVector vector, int index, Object parseValue) {
        ((VarBinaryVector) vector).setSafe(index, (byte[]) parseValue);
    }

    @Override
    public boolean supportsMultiValue() {
        return true;
    }

    /**
     * Classic-parity normalization: sorted ascending by unsigned byte comparison, duplicates
     * removed. Nulls inside the array never reach this point (the mapper drops explicit nulls
     * before adding values), but are tolerated by sorting them first and keeping one.
     */
    @Override
    protected List<?> normalizeListValues(List<?> values) {
        if (values.size() < 2) {
            return values;
        }
        List<byte[]> sorted = new ArrayList<>(values.size());
        for (Object value : values) {
            sorted.add((byte[]) value);
        }
        sorted.sort((a, b) -> {
            if (a == null || b == null) {
                return a == null ? (b == null ? 0 : -1) : 1;
            }
            return Arrays.compareUnsigned(a, b);
        });
        List<byte[]> deduped = new ArrayList<>(sorted.size());
        byte[] previous = null;
        boolean first = true;
        for (byte[] value : sorted) {
            if (first || Arrays.equals(previous, value) == false) {
                deduped.add(value);
            }
            previous = value;
            first = false;
        }
        return deduped;
    }

    @Override
    public ArrowType getArrowType() {
        return new ArrowType.Binary();
    }

    @Override
    public FieldType getFieldType() {
        return FieldType.nullable(getArrowType());
    }

    @Override
    public Set<FieldTypeCapabilities.Capability> supportedCapabilities() {
        return Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE, FieldTypeCapabilities.Capability.STORED_FIELDS);
    }
}
