/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields.core.data;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.index.mapper.BinaryFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.parquet.vsr.ManagedVSR;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for {@link BinaryParquetField}: Arrow type mapping, vector writes, and multi-value
 * support — classic-parity normalization (sorted unsigned-lexicographic, de-duplicated) applied
 * to each document's values before they are written into a LIST column, the same transformation
 * the classic Lucene path applies when packing {@code CustomBinaryDocValuesField}'s framed
 * encoding.
 */
public class BinaryParquetFieldTests extends OpenSearchTestCase {

    private BufferAllocator allocator;

    private final BinaryParquetField field = new BinaryParquetField();

    @Override
    public void setUp() throws Exception {
        super.setUp();
        allocator = new RootAllocator();
    }

    @Override
    public void tearDown() throws Exception {
        allocator.close();
        super.tearDown();
    }

    public void testArrowType() {
        assertTrue(field.getArrowType() instanceof ArrowType.Binary);
        assertTrue(field.getFieldType().isNullable());
    }

    public void testAddToGroup() {
        MappedFieldType ft = new BinaryFieldMapper.BinaryFieldType("val");
        Schema schema = new Schema(List.of(new Field("val", field.getFieldType(), null)));
        BufferAllocator child = allocator.newChildAllocator("bin-test", 0, Long.MAX_VALUE);
        ManagedVSR vsr = new ManagedVSR("bin-test", schema, child);
        // BinaryParquetField uses set() not setSafe(), so allocate capacity first
        ((VarBinaryVector) vsr.getVector("val")).allocateNew(64, 1);
        byte[] data = new byte[] { 1, 2, 3, 4 };
        field.createField(ft, vsr, data);
        vsr.setRowCount(1);
        byte[] result = ((VarBinaryVector) vsr.getVector("val")).get(0);
        assertArrayEquals(data, result);
        vsr.moveToFrozen();
        vsr.close();
    }

    @SuppressWarnings("unchecked")
    private List<byte[]> normalize(List<byte[]> values) {
        return (List<byte[]>) field.normalizeListValues(values);
    }

    public void testSupportsMultiValue() {
        assertTrue(field.supportsMultiValue());
    }

    public void testSortsUnsignedLexicographic() {
        byte[] high = new byte[] { (byte) 0xFF };          // unsigned 255: must sort AFTER 0x01
        byte[] low = new byte[] { 0x01 };
        byte[] mid = new byte[] { 0x7F };
        List<byte[]> normalized = normalize(List.of(high, low, mid));

        assertEquals(3, normalized.size());
        assertArrayEquals(low, normalized.get(0));
        assertArrayEquals(mid, normalized.get(1));
        assertArrayEquals("0xFF must compare as 255, not -1", high, normalized.get(2));
    }

    public void testPrefixSortsBeforeLongerValue() {
        byte[] prefix = new byte[] { 1, 2 };
        byte[] longer = new byte[] { 1, 2, 3 };
        List<byte[]> normalized = normalize(List.of(longer, prefix));

        assertArrayEquals(prefix, normalized.get(0));
        assertArrayEquals(longer, normalized.get(1));
    }

    public void testDeduplicatesEqualValues() {
        byte[] a1 = new byte[] { 5, 6 };
        byte[] a2 = new byte[] { 5, 6 };      // equal content, distinct instance
        byte[] b = new byte[] { 7 };
        List<byte[]> normalized = normalize(List.of(a1, b, a2));

        assertEquals("equal values must collapse to one", 2, normalized.size());
        assertArrayEquals(a1, normalized.get(0));
        assertArrayEquals(b, normalized.get(1));
    }

    public void testSingleValueListUntouched() {
        byte[] only = new byte[] { 1 };
        List<byte[]> values = List.of(only);
        assertSame("singleton lists skip normalization", values, field.normalizeListValues(values));
    }
}
