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

import java.io.IOException;

/**
 * Presents a raw single-valued Parquet {@code BYTE_ARRAY} column as the doc values encoding that
 * OpenSearch {@code binary} fields use.
 *
 * <p>Lucene's BINARY doc values type carries one opaque {@link BytesRef} per document and imposes no
 * structure on it. The structure is imposed by the field type: for a {@code binary} field,
 * {@code BinaryFieldMapper.CustomBinaryDocValuesField#binaryValue()} packs the document's values as a
 * vInt value count followed by a vInt length and the raw bytes of each value. Every consumer of a
 * {@code binary} field's doc values decodes that framing — {@code AbstractBinaryDVLeafFieldData}
 * (aggregations, scripts) and {@code BinaryDocValuesFetcher} (derived source) both do.
 *
 * <p>Parquet stores the value itself, unframed, so that the column stays meaningful to readers that
 * are not OpenSearch (native scans, column statistics). This wrapper adds the framing on read.
 *
 * <p>{@code ParquetDocValuesProducer#getBinary} applies it only to fields the mapping types as
 * {@code binary}. Other field types whose doc values are BINARY — notably {@code text}, whose fielddata
 * reads the value through {@code FieldData.singleton(...)} — expect the value raw and must not be
 * wrapped.
 *
 * <p>Only scalar columns reach this class: {@code ParquetDocValuesProducer#getBinary} routes
 * physically repeated (LIST) columns to {@link BinaryListFramingDocValues}, which frames the
 * document's whole value list. Here every framed document holds exactly one value.
 */
public final class BinaryFramingDocValues extends BinaryDocValues {

    /** Widest vInt encoding of a non-negative int, i.e. the most bytes the length prefix can take. */
    private static final int MAX_VINT_BYTES = 5;

    private final BinaryDocValues in;
    private final BytesRef framed = new BytesRef();

    private byte[] buffer = new byte[0];
    /** Whether {@link #framed} holds the frame for the current doc. Framing is lazy: reading
     * the delegate's value can force a page decode, and presence-only consumers (notably
     * {@code FieldExistsQuery}) never ask for the value — so the frame is built on the first
     * {@link #binaryValue()} call after a positive advance, not during the advance itself. */
    private boolean framedValid;

    public BinaryFramingDocValues(BinaryDocValues in) {
        this.in = in;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        framedValid = false;
        return in.advanceExact(target);
    }

    /**
     * Copies {@code value} into {@link #buffer} behind a {@code vInt(1), vInt(value.length)} header and
     * repoints {@link #framed} at it. The copy is required and not merely convenient: the delegate hands
     * back a slice of the resident Parquet page, which has no room for a header and is reused across
     * documents.
     */
    private void frame(BytesRef value) {
        // 1 byte for the value count (always 1), up to MAX_VINT_BYTES for the length, then the value.
        final int upperBound = 1 + MAX_VINT_BYTES + value.length;
        if (buffer.length < upperBound) {
            buffer = ArrayUtil.grow(buffer, upperBound);
        }
        int pos = writeVInt(buffer, 0, 1);
        pos = writeVInt(buffer, pos, value.length);
        System.arraycopy(value.bytes, value.offset, buffer, pos, value.length);
        framed.bytes = buffer;
        framed.offset = 0;
        framed.length = pos + value.length;
    }

    /**
     * Writes {@code value} at {@code pos} in the variable length encoding
     * {@code StreamOutput#writeVInt(int)} uses — seven bits per byte, least significant group first,
     * high bit set on every byte but the last — and returns the position just past it.
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
    public BytesRef binaryValue() throws IOException {
        if (framedValid == false) {
            frame(in.binaryValue());
            framedValid = true;
        }
        return framed;
    }

    @Override
    public int docID() {
        return in.docID();
    }

    @Override
    public int nextDoc() throws IOException {
        framedValid = false;
        return in.nextDoc();
    }

    @Override
    public int advance(int target) throws IOException {
        framedValid = false;
        return in.advance(target);
    }

    @Override
    public long cost() {
        return in.cost();
    }

    /**
     * Run-API delegation: without these two overrides the wrapper would mask the delegate's
     * run declarations behind the single-doc defaults, and presence-only consumers (exists
     * queries) would fall back to stepping every doc. {@code docIDRunEnd} is a pure query and
     * needs no framing invalidation; {@code intoBitSet} moves the delegate's position, so the
     * frame is invalidated like every other repositioning call.
     */
    @Override
    public int docIDRunEnd() throws IOException {
        return in.docIDRunEnd();
    }

    @Override
    public void intoBitSet(int upTo, org.apache.lucene.util.FixedBitSet bitSet, int offset) throws IOException {
        framedValid = false;
        in.intoBitSet(upTo, bitSet, offset);
    }
}
