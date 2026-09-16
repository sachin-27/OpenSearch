/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.opensearch.test.OpenSearchTestCase;

import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

public class UninvertedOrdinalsTests extends OpenSearchTestCase {

    public void testReloadUsesPersistedCheckpointsWithoutCheckpointRebuild() throws Exception {
        Path ordsDir = createTempDir();
        String fileKey = "reload";

        try (Directory dir = newDirectory(); RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
            // The checkpoint interval moved from a class constant to the dynamic node setting
            // surfaced by ParquetDocValuesProducer.checkpointInterval().
            final int checkpointInterval = ParquetDocValuesProducer.checkpointInterval();
            final int termCount = checkpointInterval + 128;
            for (int i = 0; i < termCount; i++) {
                Document doc = new Document();
                doc.add(new StringField("f", termValue(i), Field.Store.NO));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);

            try (DirectoryReader reader = writer.getReader()) {
                LeafReader leaf = reader.leaves().get(0).reader();
                Terms baseTerms = leaf.terms("f");
                assertNotNull(baseTerms);

                try (
                    UninvertedOrdinals built = UninvertedOrdinals.build(ordsDir, fileKey, baseTerms, leaf.maxDoc(), termCount, () -> false)
                ) {
                    assertEquals(termCount, built.valueCount());
                    assertEquals(0, built.ordinal(0));
                    assertEquals(termCount - 1, built.ordinal(termCount - 1));
                }

                AtomicInteger iteratorCalls = new AtomicInteger();
                Terms countingTerms = countingTerms(baseTerms, iteratorCalls);
                try (
                    UninvertedOrdinals reloaded = UninvertedOrdinals.build(
                        ordsDir,
                        fileKey,
                        countingTerms,
                        leaf.maxDoc(),
                        termCount,
                        () -> false
                    )
                ) {
                    assertEquals("existing .ord should load without rebuilding checkpoints", 0, iteratorCalls.get());
                    assertEquals(termCount, reloaded.valueCount());
                    assertEquals(termCount - 1, reloaded.ordinal(termCount - 1));
                    assertEquals(termValue(checkpointInterval + 5), reloaded.term(checkpointInterval + 5).utf8ToString());
                    assertEquals(1, iteratorCalls.get());
                    assertEquals(checkpointInterval + 5, reloaded.rank(new BytesRef(termValue(checkpointInterval + 5))));
                }
            }
        }
    }

    public void testCorruptAssignedDocsMetadataTriggersRebuild() throws Exception {
        Path ordsDir = createTempDir();
        String fileKey = "assigned-docs";
        Path ordFile = ordsDir.resolve("parquet-ords-" + fileKey + ".ord");

        try (Directory dir = newDirectory(); RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
            addDoc(writer, "alpha");
            addDoc(writer, "beta");
            addDoc(writer, "gamma");
            writer.forceMerge(1);

            try (DirectoryReader reader = writer.getReader()) {
                LeafReader leaf = reader.leaves().get(0).reader();
                Terms baseTerms = leaf.terms("f");
                assertNotNull(baseTerms);

                try (UninvertedOrdinals ignored = UninvertedOrdinals.build(ordsDir, fileKey, baseTerms, leaf.maxDoc(), 3, () -> false)) {
                    assertTrue(Files.exists(ordFile));
                }

                try (RandomAccessFile raf = new RandomAccessFile(ordFile.toFile(), "rw")) {
                    raf.seek(20L); // magic, version, maxDoc, termCount
                    raf.writeLong(2L);
                }

                AtomicInteger iteratorCalls = new AtomicInteger();
                Terms countingTerms = countingTerms(baseTerms, iteratorCalls);
                try (
                    UninvertedOrdinals rebuilt = UninvertedOrdinals.build(ordsDir, fileKey, countingTerms, leaf.maxDoc(), 3, () -> false)
                ) {
                    assertTrue("corrupt assignedDocs metadata should force rebuild", iteratorCalls.get() > 0);
                    assertEquals("beta", rebuilt.term(1).utf8ToString());
                    assertEquals(2, rebuilt.rank(new BytesRef("gamma")));
                }
            }
        }
    }

    public void testCoverageMismatchFailsBeforePublishingOrdFile() throws Exception {
        Path ordsDir = createTempDir();
        String fileKey = "coverage-mismatch";
        Path ordFile = ordsDir.resolve("parquet-ords-" + fileKey + ".ord");

        try (Directory dir = newDirectory(); RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
            addDoc(writer, "alpha");
            addDoc(writer, "beta");
            writer.addDocument(new Document());
            writer.forceMerge(1);

            try (DirectoryReader reader = writer.getReader()) {
                LeafReader leaf = reader.leaves().get(0).reader();
                Terms terms = leaf.terms("f");
                assertNotNull(terms);

                IllegalStateException e = expectThrows(
                    IllegalStateException.class,
                    () -> UninvertedOrdinals.build(ordsDir, fileKey, terms, leaf.maxDoc(), 3, () -> false)
                );
                assertTrue(e.getMessage().contains("ordinal coverage mismatch"));
                assertFalse("coverage mismatch should fail before publishing the ord file", Files.exists(ordFile));
            }
        }
    }

    private static Terms countingTerms(Terms delegate, AtomicInteger iteratorCalls) {
        return new FilterLeafReader.FilterTerms(delegate) {
            @Override
            public TermsEnum iterator() throws java.io.IOException {
                iteratorCalls.incrementAndGet();
                return in.iterator();
            }
        };
    }

    private static void addDoc(RandomIndexWriter writer, String value) throws Exception {
        Document doc = new Document();
        doc.add(new StringField("f", value, Field.Store.NO));
        writer.addDocument(doc);
    }

    private static String termValue(int ord) {
        return String.format(java.util.Locale.ROOT, "term-%04d", ord);
    }
}
