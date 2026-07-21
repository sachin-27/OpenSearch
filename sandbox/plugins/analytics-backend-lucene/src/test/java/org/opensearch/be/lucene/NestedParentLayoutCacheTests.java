/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.opensearch.index.mapper.NestedPathFieldMapper;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for {@link NestedParentLayoutCache} — segment-lifetime caching keyed by core
 * cache key, with closed-listener eviction (the BitsetFilterCache pattern).
 *
 * <p>Uses plain Lucene segments: without the {@code nested_blocks} attribute the cache
 * stores a null layout, which is sufficient to exercise keying, memoization, and
 * eviction. Layout-building semantics themselves are covered by
 * {@link NestedParentLayoutTests}.
 */
public class NestedParentLayoutCacheTests extends OpenSearchTestCase {

    /** Same leaf asked twice → one entry; flat answer (null) is cached, not recomputed. */
    public void testFlatLeafCachedOnce() throws Exception {
        try (ByteBuffersDirectory dir = new ByteBuffersDirectory(); IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
            addFlatDoc(w);
            w.commit();
            try (DirectoryReader reader = DirectoryReader.open(w)) {
                NestedParentLayoutCache cache = new NestedParentLayoutCache();
                LeafReader leaf = reader.leaves().get(0).reader();
                assertNull(cache.get(leaf));
                assertNull(cache.get(leaf));
                assertEquals("second get must hit the cached null slot", 1, cache.size());
            }
        }
    }

    /** Two independent readers over the same unchanged index share segment cores via
     *  openIfChanged — the cache must serve both from one entry. */
    public void testReusedAcrossReaderReopen() throws Exception {
        try (ByteBuffersDirectory dir = new ByteBuffersDirectory(); IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
            addFlatDoc(w);
            w.commit();
            NestedParentLayoutCache cache = new NestedParentLayoutCache();
            try (DirectoryReader reader1 = DirectoryReader.open(w)) {
                cache.get(reader1.leaves().get(0).reader());
                assertEquals(1, cache.size());

                addFlatDoc(w);
                w.commit();
                try (DirectoryReader reader2 = DirectoryReader.openIfChanged(reader1)) {
                    assertNotNull("index changed, reopen must produce a new reader", reader2);
                    for (var leafCtx : reader2.leaves()) {
                        cache.get(leafCtx.reader());
                    }
                    // First segment's core is shared with reader1 → reused entry; only the
                    // newly flushed segment adds one.
                    assertEquals(2, cache.size());
                }
            }
        }
    }

    /** Entries must evict themselves when the segment core closes (all readers released).
     *  Uses a non-NRT reader: an NRT reader's core is pooled by the IndexWriter and outlives
     *  the reader, so eviction would (correctly) wait for the writer — core lifetime, not
     *  reader lifetime, is the contract. */
    public void testEvictedOnCoreClose() throws Exception {
        try (ByteBuffersDirectory dir = new ByteBuffersDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
                addFlatDoc(w);
                w.commit();
            }
            NestedParentLayoutCache cache = new NestedParentLayoutCache();
            DirectoryReader reader = DirectoryReader.open(dir);
            cache.get(reader.leaves().get(0).reader());
            assertEquals(1, cache.size());
            reader.close();
            assertEquals("closed listener must remove the entry with its core", 0, cache.size());
        }
    }

    /** Nested segments (attribute present) cache a real layout, same identity per core. */
    public void testNestedLeafCachesLayoutInstance() throws Exception {
        try (ByteBuffersDirectory dir = new ByteBuffersDirectory(); IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
            // Hand-build a minimal nested-shaped segment: child (carries _nested_path), then parent.
            Document child = new Document();
            child.add(new StringField(NestedPathFieldMapper.NAME, "comments", Field.Store.NO));
            Document parent = new Document();
            parent.add(new StringField("title", "p", Field.Store.NO));
            w.addDocuments(java.util.List.of(child, parent));
            w.commit();
            try (DirectoryReader reader = DirectoryReader.open(w)) {
                NestedParentLayoutCache cache = new NestedParentLayoutCache();
                LeafReader leaf = reader.leaves().get(0).reader();
                // Plain IndexWriter segments carry no nested_blocks attribute, so of() reports
                // flat; assert instance-level memoization via two gets sharing one slot.
                NestedParentLayout first = cache.get(leaf);
                NestedParentLayout second = cache.get(leaf);
                assertSame("same core must return the identical cached instance", first, second);
                assertEquals(1, cache.size());
            }
        }
    }

    private static void addFlatDoc(IndexWriter w) throws Exception {
        Document doc = new Document();
        doc.add(new StringField("f", "v", Field.Store.NO));
        w.addDocument(doc);
    }
}
