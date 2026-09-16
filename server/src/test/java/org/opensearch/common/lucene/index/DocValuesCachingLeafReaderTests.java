/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.lucene.index;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Unit tests for {@link DocValuesCachingLeafReader}: repeated doc-values lookups of the same
 * field return the same iterator instance within a thread (the whole point — one expensive
 * codec-level open per field instead of one per document), absent fields are memoized as
 * null, values read through the cached iterator are correct in ascending-docID access, and
 * different threads never share an iterator.
 */
public class DocValuesCachingLeafReaderTests extends OpenSearchTestCase {

    private Directory dir;
    private IndexWriter writer;
    private DirectoryReader directoryReader;
    private LeafReader leaf;
    private DocValuesCachingLeafReader caching;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        dir = newDirectory();
        writer = new IndexWriter(dir, new IndexWriterConfig());
        for (int i = 0; i < 3; i++) {
            Document doc = new Document();
            doc.add(new NumericDocValuesField("num", i * 10));
            doc.add(new SortedNumericDocValuesField("snum", i * 100));
            doc.add(new BinaryDocValuesField("bin", new BytesRef(new byte[] { (byte) i })));
            doc.add(new SortedDocValuesField("sorted", new BytesRef("v" + i)));
            doc.add(new SortedSetDocValuesField("sset", new BytesRef("s" + i)));
            writer.addDocument(doc);
        }
        writer.commit();
        directoryReader = DirectoryReader.open(writer);
        assertEquals(1, directoryReader.leaves().size());
        leaf = directoryReader.leaves().get(0).reader();
        caching = new DocValuesCachingLeafReader(leaf);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        try {
            IOUtils.close(directoryReader, writer, dir);
        } finally {
            super.tearDown();
        }
    }

    public void testSameInstanceReturnedPerFieldWithinThread() throws Exception {
        assertSame(caching.getNumericDocValues("num"), caching.getNumericDocValues("num"));
        assertSame(caching.getSortedNumericDocValues("snum"), caching.getSortedNumericDocValues("snum"));
        assertSame(caching.getBinaryDocValues("bin"), caching.getBinaryDocValues("bin"));
        assertSame(caching.getSortedDocValues("sorted"), caching.getSortedDocValues("sorted"));
        assertSame(caching.getSortedSetDocValues("sset"), caching.getSortedSetDocValues("sset"));
    }

    public void testDifferentFieldsGetDifferentIterators() throws Exception {
        assertNotSame(caching.getNumericDocValues("num"), caching.getSortedNumericDocValues("snum"));
    }

    /** The uncached delegate hands out a fresh iterator per call — the contrast this class exists for. */
    public void testDelegateReturnsFreshInstancePerCall() throws Exception {
        assertNotSame(leaf.getSortedNumericDocValues("snum"), leaf.getSortedNumericDocValues("snum"));
    }

    public void testAbsentFieldMemoizedAsNull() throws Exception {
        assertNull(caching.getSortedNumericDocValues("no_such_field"));
        assertNull(caching.getSortedNumericDocValues("no_such_field"));
        assertNull(caching.getBinaryDocValues("no_such_field"));
    }

    /**
     * The derive-path access pattern: many documents in ascending order, same field, one
     * cached iterator. Values must be correct for every document.
     */
    public void testAscendingReadsThroughCachedIteratorAreCorrect() throws Exception {
        for (int docId = 0; docId < 3; docId++) {
            SortedNumericDocValues dv = caching.getSortedNumericDocValues("snum");
            assertTrue(dv.advanceExact(docId));
            assertEquals(docId * 100L, dv.nextValue());

            BinaryDocValues bdv = caching.getBinaryDocValues("bin");
            assertTrue(bdv.advanceExact(docId));
            BytesRef value = bdv.binaryValue();
            assertEquals(1, value.length);
            assertEquals((byte) docId, value.bytes[value.offset]);
        }
    }

    /** Iterators are per-thread: another thread must get its own instance, never this thread's. */
    public void testThreadsDoNotShareIterators() throws Exception {
        SortedNumericDocValues mine = caching.getSortedNumericDocValues("snum");
        AtomicReference<SortedNumericDocValues> theirs = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        Thread other = new Thread(() -> {
            try {
                theirs.set(caching.getSortedNumericDocValues("snum"));
            } catch (Exception e) {
                failure.set(e);
            } finally {
                done.countDown();
            }
        });
        other.start();
        done.await();
        assertNull(failure.get());
        assertNotNull(theirs.get());
        assertNotSame(mine, theirs.get());
    }
}
