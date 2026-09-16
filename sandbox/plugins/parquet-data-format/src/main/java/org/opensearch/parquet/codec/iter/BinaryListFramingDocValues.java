/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec.iter;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.opensearch.parquet.bridge.BinaryPageReader;

import java.io.IOException;

/**
 * Presents a repeated (Parquet {@code LIST<BINARY>}) column as the doc values encoding that
 * OpenSearch {@code binary} fields use: one {@link BytesRef} per document holding a vInt value
 * count followed by each value's vInt length and raw bytes — the encoding
 * {@code BinaryFieldMapper.CustomBinaryDocValuesField#binaryValue()} produces on the classic
 * path and every binary doc-values consumer decodes.
 *
 * <p>The multi-valued counterpart of {@link BinaryFramingDocValues} (which frames scalar
 * columns with a fixed count of one). Values arrive already sorted and de-duplicated:
 * {@code BinaryParquetField} normalizes each document's values at write time to match the
 * classic encoding's semantics, so framing is a straight concatenation.
 *
 * <p>Presence requires the page decode — a repeated column's per-page leaf null counts do not
 * bound documents, so there is no statistics fast path here (unlike the scalar iterator): an
 * empty or null list is a missing document, which only the decoded list offsets can reveal.
 * The frame itself is built lazily on the first {@link #binaryValue()} read per document, so
 * presence-only consumers (exists) pay the decode but not the copy.
 */
public final class BinaryListFramingDocValues extends BinaryDocValues {

    /** Widest vInt encoding of a non-negative int, i.e. the most bytes a length prefix can take. */
    private static final int MAX_VINT_BYTES = 5;

    private final BinaryPageReader reader;
    private final int maxDoc;
    private final BytesRef framed = new BytesRef();

    private byte[] buffer = new byte[0];
    private int doc = -1;
    /** The current doc's values, or {@code null} when the doc has none (absent). */
    private byte[][] values;
    /** Whether {@link #framed} holds the frame for the current doc. */
    private boolean framedValid;

    public BinaryListFramingDocValues(BinaryPageReader reader, int maxDoc) {
        this.reader = reader;
        this.maxDoc = maxDoc;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        framedValid = false;
        if (target >= maxDoc) {
            doc = NO_MORE_DOCS;
            values = null;
            return false;
        }
        doc = target;
        byte[][] rowValues = reader.readRepeatedBytesAtRow(target);
        if (rowValues.length == 0) {
            values = null; // empty list == missing, matching the classic path's absent doc.
            return false;
        }
        values = rowValues;
        return true;
    }

    @Override
    public BytesRef binaryValue() throws IOException {
        if (framedValid == false) {
            frame(values);
            framedValid = true;
        }
        return framed;
    }

    /** Packs {@code docValues} as vInt(count) then per value vInt(length) + bytes. */
    private void frame(byte[][] docValues) {
        int upperBound = MAX_VINT_BYTES;
        for (byte[] value : docValues) {
            upperBound += MAX_VINT_BYTES + value.length;
        }
        if (buffer.length < upperBound) {
            buffer = ArrayUtil.grow(buffer, upperBound);
        }
        int pos = writeVInt(buffer, 0, docValues.length);
        for (byte[] value : docValues) {
            pos = writeVInt(buffer, pos, value.length);
            System.arraycopy(value, 0, buffer, pos, value.length);
            pos += value.length;
        }
        framed.bytes = buffer;
        framed.offset = 0;
        framed.length = pos;
    }

    /**
     * Writes {@code value} at {@code pos} in the variable length encoding
     * {@code StreamOutput#writeVInt(int)} uses — seven bits per byte, least significant group
     * first, high bit set on every byte but the last — and returns the position just past it.
     */
    private static int writeVInt(byte[] dst, int pos, int value) {
        while ((value & ~0x7F) != 0) {
            dst[pos++] = (byte) ((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        dst[pos++] = (byte) value;
        return pos;
    }

    @Override
    public int docID() {
        return doc;
    }

    @Override
    public int nextDoc() throws IOException {
        return advance(doc + 1);
    }

    @Override
    public int advance(int target) throws IOException {
        for (int d = target; d < maxDoc; d++) {
            if (advanceExact(d)) {
                return d; // advanceExact already set doc.
            }
        }
        doc = NO_MORE_DOCS;
        return NO_MORE_DOCS;
    }

    @Override
    public long cost() {
        return maxDoc;
    }
}
