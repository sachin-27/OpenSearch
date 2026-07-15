/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.merge;

import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.index.engine.dataformat.RowIdMapping;
import org.opensearch.index.mapper.NestedPathFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Expands the primary's <em>logical-row</em> merge mapping into per-physical-doc
 * sequential row ids for indices with nested document blocks (design doc §5.4,
 * Scheme C — {@code __row_id__ == docId}).
 *
 * <h2>The problem</h2>
 * Parquet's merge mapping has one entry per logical document (parent); children never
 * appear in it. But every Lucene physical doc needs (a) a sort value that lays merged
 * blocks out contiguously in the new logical order, and (b) a stored {@code __row_id__}
 * equal to its final merged position (so I1 holds in the merged segment). Because
 * blocks are contiguous, both are the <em>same number</em>: a doc's final position is
 * the start offset of its block in the merged segment plus its intra-block index.
 *
 * <h2>How it works</h2>
 * <ol>
 *   <li><b>Per-segment scan</b> ({@link #register}): recover the block structure from
 *       {@code _nested_path} — child docs carry the field, parents do not (complement).
 *       Walking docs in order yields each doc's old logical row (running parent count)
 *       and intra-block index; each block's old logical row is remapped through the
 *       mapping to its new logical row, and the block's size is recorded under it.
 *       Segments without nested blocks scan as all-parents (blocks of one), so flat and
 *       nested segments mix freely in one merge.</li>
 *   <li><b>Global finalize</b> (lazy, on first value read — all merge readers are
 *       wrapped before Lucene reads any values): prefix-sum the block sizes over new
 *       logical rows {@code 0..mapping.size()-1} to get each block's start offset in
 *       the merged segment.</li>
 *   <li><b>Per-doc answer</b> ({@link SegmentExpansion#finalRowId}):
 *       {@code blockStart[newLogical(doc)] + intra(doc)}. Children of a block share its
 *       start and ascend by intra index; the parent (last of the block) lands highest —
 *       the index sort on these values reconstructs every block contiguously with zero
 *       block-splitting.</li>
 * </ol>
 *
 * @opensearch.experimental
 */
final class NestedBlockExpansion {

    private final RowIdMapping mapping;
    private final int expectedSegments;
    /** Block size per NEW logical row; filled during per-segment scans. */
    private final long[] blockSizeByNewLogical;
    /** Block start offset per NEW logical row in the merged segment; computed at finalize. */
    private long[] blockStartByNewLogical;
    private final List<SegmentExpansion> registered = new ArrayList<>();
    private boolean finalized;

    NestedBlockExpansion(RowIdMapping mapping, int expectedSegments) {
        this.mapping = mapping;
        this.expectedSegments = expectedSegments;
        this.blockSizeByNewLogical = new long[mapping.size()];
    }

    /**
     * Scans one source segment's block structure and registers it. Must be called once
     * per merge segment (from {@code wrapForMerge}) before any row-id values are read.
     */
    synchronized SegmentExpansion register(CodecReader reader, long generation) throws IOException {
        if (finalized) {
            throw new IllegalStateException("Cannot register a segment after the expansion has been finalized");
        }
        if (reader.getLiveDocs() != null) {
            throw new IllegalStateException(
                "Nested block expansion requires deletion-free source segments (flush force-merge expunges tombstones)"
            );
        }
        int maxDoc = reader.maxDoc();
        FixedBitSet childDocs = childDocs(reader, maxDoc);
        int[] newLogical = new int[maxDoc];
        int[] intra = new int[maxDoc];
        long oldLogical = 0;
        int blockStart = 0;
        for (int docId = 0; docId < maxDoc; docId++) {
            long newRow = mapping.getNewRowId(oldLogical, generation);
            newLogical[docId] = Math.toIntExact(newRow);
            intra[docId] = docId - blockStart;
            if (childDocs.get(docId) == false) {
                // Parent (root) doc — block [blockStart..docId] is complete.
                blockSizeByNewLogical[Math.toIntExact(newRow)] = docId - blockStart + 1L;
                oldLogical++;
                blockStart = docId + 1;
            }
        }
        if (blockStart != maxDoc) {
            throw new IllegalStateException("Segment ends mid-block: trailing child docs without a parent (generation " + generation + ")");
        }
        SegmentExpansion expansion = new SegmentExpansion(this, newLogical, intra);
        registered.add(expansion);
        return expansion;
    }

    private synchronized void ensureFinalized() {
        if (finalized) {
            return;
        }
        if (registered.size() != expectedSegments) {
            throw new IllegalStateException(
                "Row-id values requested before all merge segments were registered: " + registered.size() + " of " + expectedSegments
            );
        }
        blockStartByNewLogical = new long[blockSizeByNewLogical.length];
        long offset = 0;
        for (int row = 0; row < blockSizeByNewLogical.length; row++) {
            if (blockSizeByNewLogical[row] == 0) {
                throw new IllegalStateException("New logical row [" + row + "] has no source block — mapping/segments mismatch");
            }
            blockStartByNewLogical[row] = offset;
            offset += blockSizeByNewLogical[row];
        }
        finalized = true;
    }

    private long finalRowId(int[] newLogical, int[] intra, int docId) {
        ensureFinalized();
        return blockStartByNewLogical[newLogical[docId]] + intra[docId];
    }

    /**
     * Builds the child-doc bitset: the union of postings over all terms of
     * {@code _nested_path}. Each child doc carries exactly one path term; docs without
     * the field are parents (roots) — including every doc of a blocks-free segment.
     */
    private static FixedBitSet childDocs(CodecReader reader, int maxDoc) throws IOException {
        FixedBitSet bits = new FixedBitSet(maxDoc);
        Terms terms = reader.terms(NestedPathFieldMapper.NAME);
        if (terms == null) {
            return bits;
        }
        TermsEnum termsEnum = terms.iterator();
        PostingsEnum postings = null;
        while (termsEnum.next() != null) {
            postings = termsEnum.postings(postings, PostingsEnum.NONE);
            int doc;
            while ((doc = postings.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                bits.set(doc);
            }
        }
        return bits;
    }

    /** One source segment's slice of the expansion: per-doc new-logical-row and intra-block index. */
    static final class SegmentExpansion {
        private final NestedBlockExpansion shared;
        private final int[] newLogical;
        private final int[] intra;

        private SegmentExpansion(NestedBlockExpansion shared, int[] newLogical, int[] intra) {
            this.shared = shared;
            this.newLogical = newLogical;
            this.intra = intra;
        }

        /** Returns the doc's final sequential row id in the merged segment (== its final position). */
        long finalRowId(int docId) {
            return shared.finalRowId(newLogical, intra, docId);
        }
    }
}
