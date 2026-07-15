/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.merge;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.RowIdMapping;

import java.io.IOException;

/**
 * {@link DocValuesProducer} that intercepts the {@code ___row_id} field and returns
 * remapped row ID values from a {@link RowIdMapping}. All other fields are delegated
 * unchanged to the wrapped producer.
 *
 * <p>This ensures the merged segment's {@code ___row_id} doc values contain the new
 * global row IDs (0..n-1) rather than the original per-segment local values.
 *
 * <p><b>Nested block expansion:</b> for merges involving nested-blocks segments, the
 * primary's mapping is in <em>logical-row</em> space — one entry per parent, children
 * never appear in it — while every physical doc carries a plain sequential row id
 * ({@code __row_id__ == docId}, Scheme C). A {@link NestedBlockExpansion.SegmentExpansion}
 * supplies each doc's final merged position (its block's start offset in the new logical
 * order plus its intra-block index), which serves simultaneously as the index-sort value
 * (laying blocks out contiguously) and the stored value (keeping I1 sequential in the
 * merged segment) — one number, one mechanism.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
class RowIdRemappingDocValuesProducer extends DocValuesProducer {

    private final DocValuesProducer delegate;
    private final RowIdMapping rowIdMapping;
    private final long generation;
    private final int maxDoc;
    private final int rowIdOffset;
    private final NestedBlockExpansion.SegmentExpansion expansion;

    /**
     * @param delegate     the original doc values producer
     * @param rowIdMapping the mapping from old to new row IDs, or null for sequential assignment
     * @param generation   the writer generation of the source segment
     * @param maxDoc       the maximum document count in the source segment
     * @param rowIdOffset  the starting row ID offset for sequential assignment (used when rowIdMapping is null)
     * @param expansion    this segment's slice of the block-aware mapping expansion, or null
     *                     for flat merges (no nested-blocks segment among the sources)
     */
    RowIdRemappingDocValuesProducer(
        DocValuesProducer delegate,
        RowIdMapping rowIdMapping,
        long generation,
        int maxDoc,
        int rowIdOffset,
        NestedBlockExpansion.SegmentExpansion expansion
    ) {
        this.delegate = delegate;
        this.rowIdMapping = rowIdMapping;
        this.generation = generation;
        this.maxDoc = maxDoc;
        this.rowIdOffset = rowIdOffset;
        this.expansion = expansion;
    }

    @Override
    public NumericDocValues getNumeric(FieldInfo field) throws IOException {
        return delegate.getNumeric(field);
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
        if (DocumentInput.ROW_ID_FIELD.equals(field.name)) {
            if (rowIdMapping != null) {
                return new MappedRowIdDocValues(delegate.getSortedNumeric(field), rowIdMapping, generation, expansion);
            } else {
                // https://github.com/opensearch-project/OpenSearch/issues/21508
                // TODO check how this will work for primary engine when rowIdMapping will be null.
                throw new UnsupportedOperationException("Lucene as Primary Format is not supported yet");
            }
        }
        return delegate.getSortedNumeric(field);
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) throws IOException {
        return delegate.getBinary(field);
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) throws IOException {
        return delegate.getSorted(field);
    }

    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
        return delegate.getSortedSet(field);
    }

    @Override
    public DocValuesSkipper getSkipper(FieldInfo field) throws IOException {
        return delegate.getSkipper(field);
    }

    @Override
    public void checkIntegrity() throws IOException {
        delegate.checkIntegrity();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    /**
     * Reads the original {@code ___row_id} and maps it through the {@link RowIdMapping}.
     *
     * <p>Block-aware mode (nested indices): the mapping speaks logical rows only, so the
     * value comes from the pre-computed {@link NestedBlockExpansion.SegmentExpansion}
     * instead — the doc's final merged position. The stored old value (== docId, invariant
     * I1 on the source segment) is consumed for iterator consistency and asserted.
     */
    private static class MappedRowIdDocValues extends SortedNumericDocValues {

        private final SortedNumericDocValues delegate;
        private final RowIdMapping rowIdMapping;
        private final long generation;
        private final NestedBlockExpansion.SegmentExpansion expansion;

        MappedRowIdDocValues(
            SortedNumericDocValues delegate,
            RowIdMapping rowIdMapping,
            long generation,
            NestedBlockExpansion.SegmentExpansion expansion
        ) {
            this.delegate = delegate;
            this.rowIdMapping = rowIdMapping;
            this.generation = generation;
            this.expansion = expansion;
        }

        @Override
        public long nextValue() throws IOException {
            long oldRowId = delegate.nextValue();
            if (expansion != null) {
                // I1 on the source segment: stored row id must equal the docId.
                assert oldRowId == delegate.docID() : "source segment violates __row_id__ == docId: "
                    + oldRowId
                    + " at docId "
                    + delegate.docID();
                return expansion.finalRowId(delegate.docID());
            }
            return rowIdMapping.getNewRowId(oldRowId, generation);
        }

        @Override
        public int docValueCount() {
            return delegate.docValueCount();
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            return delegate.advanceExact(target);
        }

        @Override
        public int docID() {
            return delegate.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            return delegate.nextDoc();
        }

        @Override
        public int advance(int target) throws IOException {
            return delegate.advance(target);
        }

        @Override
        public long cost() {
            return delegate.cost();
        }
    }
}
