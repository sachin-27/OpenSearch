/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.be.lucene.index.LuceneWriter;
import org.opensearch.index.mapper.NestedPathFieldMapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * Per-leaf translator between Lucene's <em>physical docId space</em> and the
 * cross-format <em>logical row space</em> (Parquet row numbers) for segments with
 * nested document blocks.
 *
 * <h2>Why this exists</h2>
 * Under the nested design (Scheme C, HLD §5.4) every physical doc keeps a sequential
 * {@code __row_id__ == docId}, so nothing stored on a doc names its logical row. The
 * correspondence is positional — the Kth parent (in docId order) is Parquet row K —
 * and this class materializes that correspondence for the read path: given any docId
 * (child or parent), which logical row does it belong to; given a logical row, where
 * does its block start and end.
 *
 * <h2>How it works</h2>
 * One array: {@code parentDocIds[row] = docId of row's parent (root) doc}, ascending.
 * Parents are identified by complement — child docs carry {@link NestedPathFieldMapper#NAME},
 * roots do not (the same convention the flush invariant and the merge expansion use).
 * Then:
 * <ul>
 *   <li>{@link #rowOf}: binary search for the first parent at or after the docId —
 *       every doc of a block (children and parent alike) maps to the block's row.</li>
 *   <li>{@link #parentDocId}/{@link #blockStartDocId}: direct array lookups — the
 *       "select" direction, used to convert row-space scan windows into docId ranges.</li>
 * </ul>
 *
 * <p>Instances are immutable and safe to share across threads; build cost is one pass
 * over the {@code _nested_path} postings plus one pass over maxDoc bits. Intended to be
 * built once per leaf per reader (callers should cache, e.g. per delegation handle).
 *
 * <p><b>Deletions:</b> positions are physical, so derivation must NOT skip deleted docs.
 * The build reads postings without liveDocs filtering, which is correct pre- and
 * post-compaction as long as both formats compact atomically (the engine's merge
 * contract). Child-level delete overlays are future work tracked in the design notes.
 *
 * @opensearch.internal
 */
public final class NestedParentLayout {

    /** Ascending docIds of parent (root) docs; index == logical row. */
    private final int[] parentDocIds;

    private NestedParentLayout(int[] parentDocIds) {
        this.parentDocIds = parentDocIds;
    }

    /**
     * Returns the layout for the leaf, or {@code null} if the segment has no nested
     * blocks (flat segment — docId space and row space coincide, no translation needed).
     * Detection uses the {@link LuceneWriter#NESTED_BLOCKS_ATTRIBUTE} persisted in the
     * segment's {@code .si} at flush/merge time.
     */
    public static NestedParentLayout of(LeafReader leaf) throws IOException {
        LeafReader unwrapped = leaf;
        while (unwrapped instanceof FilterLeafReader flr) {
            unwrapped = flr.getDelegate();
        }
        if (unwrapped instanceof SegmentReader segmentReader) {
            String attr = segmentReader.getSegmentInfo().info.getAttribute(LuceneWriter.NESTED_BLOCKS_ATTRIBUTE);
            if (Boolean.parseBoolean(attr)) {
                return build(leaf);
            }
        }
        return null;
    }

    /**
     * Builds the layout unconditionally from the leaf's {@code _nested_path} postings
     * (parents = complement). Exposed for tests and for callers that already know the
     * segment is nested.
     */
    static NestedParentLayout build(LeafReader leaf) throws IOException {
        int maxDoc = leaf.maxDoc();
        FixedBitSet childDocs = new FixedBitSet(maxDoc);
        Terms terms = leaf.terms(NestedPathFieldMapper.NAME);
        if (terms != null) {
            TermsEnum termsEnum = terms.iterator();
            PostingsEnum postings = null;
            while (termsEnum.next() != null) {
                postings = termsEnum.postings(postings, PostingsEnum.NONE);
                int doc;
                while ((doc = postings.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                    childDocs.set(doc);
                }
            }
        }
        int parentCount = maxDoc - (int) childDocs.cardinality();
        int[] parents = new int[parentCount];
        int row = 0;
        for (int docId = 0; docId < maxDoc; docId++) {
            if (childDocs.get(docId) == false) {
                parents[row++] = docId;
            }
        }
        assert row == parentCount;
        if (parentCount > 0 && parents[parentCount - 1] != maxDoc - 1) {
            throw new IllegalStateException(
                "Segment ends mid-block: last doc " + (maxDoc - 1) + " is a child (last parent at " + parents[parentCount - 1] + ")"
            );
        }
        return new NestedParentLayout(parents);
    }

    /** Number of logical rows (parents) in the leaf — the segment's row-space size. */
    public int rowCount() {
        return parentDocIds.length;
    }

    /**
     * Returns the logical row the doc belongs to: the row of the first parent at or
     * after {@code docId}. Works uniformly for parents (their own row) and children
     * (their block's row).
     */
    public int rowOf(int docId) {
        int idx = Arrays.binarySearch(parentDocIds, docId);
        // Exact hit: docId IS a parent → its row. Miss: insertion point is the index of
        // the first parent greater than docId → the enclosing block's row.
        return idx >= 0 ? idx : -idx - 1;
    }

    /** DocId of the row's parent (root) doc — the block's last, highest docId. */
    public int parentDocId(int row) {
        return parentDocIds[row];
    }

    /** First docId of the row's block (its earliest child, or the parent itself if childless). */
    public int blockStartDocId(int row) {
        return row == 0 ? 0 : parentDocIds[row - 1] + 1;
    }
}
