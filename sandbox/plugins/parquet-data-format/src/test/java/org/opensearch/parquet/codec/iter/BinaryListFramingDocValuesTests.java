/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec.iter;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.parquet.bridge.BinaryPageReader;
import org.opensearch.parquet.codec.cache.PageCache;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Unit tests for {@link BinaryListFramingDocValues}: repeated (LIST) columns served through the
 * classic framed encoding — vInt value count, then each value's vInt length + raw bytes — with
 * empty lists reported as missing documents and frames built lazily per document.
 */
public class BinaryListFramingDocValuesTests extends OpenSearchTestCase {

    /** Serves per-row value lists; counts value reads to observe framing laziness. */
    private static final class FakeRepeatedReader implements BinaryPageReader {
        private final byte[][][] rows;
        int reads;

        FakeRepeatedReader(byte[][][] rows) {
            this.rows = rows;
        }

        @Override
        public PageCache cache() {
            return null;
        }

        @Override
        public void loadPageContaining(long row) {
            throw new UnsupportedOperationException("repeated tests read whole rows");
        }

        @Override
        public byte[][] readRepeatedBytesAtRow(long row) {
            reads++;
            return rows[(int) row];
        }
    }

    /** Decodes the classic framing: vInt count, then per value vInt length + bytes. */
    private static List<byte[]> decode(BytesRef framed) throws IOException {
        BytesStreamInput in = new BytesStreamInput();
        in.reset(framed.bytes, framed.offset, framed.length);
        int count = in.readVInt();
        List<byte[]> values = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int length = in.readVInt();
            byte[] value = new byte[length];
            in.readBytes(value, 0, length);
            values.add(value);
        }
        assertEquals("frame must contain exactly the declared values", framed.offset + framed.length, in.getPosition());
        return values;
    }

    public void testFramesAllValuesOfADocument() throws IOException {
        byte[] a = new byte[] { 1 };
        byte[] b = new byte[] { 2, 3 };
        byte[] c = new byte[] { 4, 5, 6 };
        FakeRepeatedReader reader = new FakeRepeatedReader(new byte[][][] { { a, b, c } });
        BinaryListFramingDocValues dv = new BinaryListFramingDocValues(reader, 1);

        assertTrue(dv.advanceExact(0));
        List<byte[]> decoded = decode(dv.binaryValue());
        assertEquals(3, decoded.size());
        assertArrayEquals(a, decoded.get(0));
        assertArrayEquals(b, decoded.get(1));
        assertArrayEquals(c, decoded.get(2));
    }

    public void testSingleValueListFramesWithCountOne() throws IOException {
        byte[] only = new byte[] { 9, 9 };
        FakeRepeatedReader reader = new FakeRepeatedReader(new byte[][][] { { only } });
        BinaryListFramingDocValues dv = new BinaryListFramingDocValues(reader, 1);

        assertTrue(dv.advanceExact(0));
        List<byte[]> decoded = decode(dv.binaryValue());
        assertEquals(1, decoded.size());
        assertArrayEquals(only, decoded.get(0));
    }

    /** An empty list is a missing document, matching the classic path where no values = no entry. */
    public void testEmptyListIsMissing() throws IOException {
        FakeRepeatedReader reader = new FakeRepeatedReader(new byte[][][] { {}, { new byte[] { 7 } } });
        BinaryListFramingDocValues dv = new BinaryListFramingDocValues(reader, 2);

        assertFalse(dv.advanceExact(0));
        assertTrue(dv.advanceExact(1));
    }

    public void testIterationSkipsMissingDocs() throws IOException {
        byte[] v1 = new byte[] { 1 };
        byte[] v3 = new byte[] { 3 };
        FakeRepeatedReader reader = new FakeRepeatedReader(new byte[][][] { {}, { v1 }, {}, { v3 } });
        BinaryListFramingDocValues dv = new BinaryListFramingDocValues(reader, 4);

        assertEquals(1, dv.nextDoc());
        assertArrayEquals(v1, decode(dv.binaryValue()).get(0));
        assertEquals(3, dv.nextDoc());
        assertArrayEquals(v3, decode(dv.binaryValue()).get(0));
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.nextDoc());
    }

    /** Frames build lazily on first read and are reused for repeated reads of the same doc. */
    public void testFramingIsLazyPerDocument() throws IOException {
        byte[] a = new byte[] { 1 };
        byte[] b = new byte[] { 2 };
        FakeRepeatedReader reader = new FakeRepeatedReader(new byte[][][] { { a }, { b } });
        BinaryListFramingDocValues dv = new BinaryListFramingDocValues(reader, 2);

        assertTrue(dv.advanceExact(0));
        BytesRef first = dv.binaryValue();
        List<byte[]> firstDecoded = decode(first);
        assertArrayEquals(a, firstDecoded.get(0));
        assertSame("repeated reads reuse the frame instance", first, dv.binaryValue());

        assertTrue(dv.advanceExact(1));
        assertArrayEquals(b, decode(dv.binaryValue()).get(0));
    }

    public void testExhaustion() throws IOException {
        FakeRepeatedReader reader = new FakeRepeatedReader(new byte[][][] { { new byte[] { 1 } } });
        BinaryListFramingDocValues dv = new BinaryListFramingDocValues(reader, 1);

        assertFalse(dv.advanceExact(1));
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.docID());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.advance(1));
    }

    /** Wide values force the frame buffer to grow across documents without corruption. */
    public void testBufferGrowthAcrossDocuments() throws IOException {
        byte[] small = new byte[] { 5 };
        byte[] large = new byte[300];
        for (int i = 0; i < large.length; i++) {
            large[i] = (byte) i;
        }
        FakeRepeatedReader reader = new FakeRepeatedReader(new byte[][][] { { small }, { large, large } });
        BinaryListFramingDocValues dv = new BinaryListFramingDocValues(reader, 2);

        assertTrue(dv.advanceExact(0));
        assertArrayEquals(small, decode(dv.binaryValue()).get(0));

        assertTrue(dv.advanceExact(1));
        List<byte[]> decoded = decode(dv.binaryValue());
        assertEquals(2, decoded.size());
        assertArrayEquals(large, decoded.get(0));
        assertArrayEquals(large, decoded.get(1));
    }
}
