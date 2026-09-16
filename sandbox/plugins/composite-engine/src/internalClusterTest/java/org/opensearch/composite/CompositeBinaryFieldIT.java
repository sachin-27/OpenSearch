/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.apache.lucene.util.BytesRef;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.document.DocumentField;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * End-to-end integration tests for {@code binary} fields on composite (parquet-primary) indices.
 *
 * <p>Covers the three behaviours that the binary work on this branch depends on:
 * <ul>
 *   <li><b>Framing</b> — parquet stores raw bytes while the classic doc-values contract expects a
 *       count/length frame. Values must round-trip through terms aggregations and
 *       {@code docvalue_fields} without bleeding into a neighbouring document's bytes.</li>
 *   <li><b>Exists</b> — the doc-values skipper is consulted for {@code BYTE_ARRAY} columns.
 *       A widened skipper gate must never drop or invent matches, including when only some
 *       documents carry the field.</li>
 *   <li><b>Derived source</b> — with {@code index.derived_source.enabled} the {@code _source} is
 *       reconstructed from doc values per hit. Values must be correct for every hit in a
 *       multi-hit, multi-field response (the reader-reuse cache is keyed per field and must not
 *       leak one document's value onto another).</li>
 * </ul>
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class CompositeBinaryFieldIT extends AbstractCompositeEngineIT {

    private static final int DOC_COUNT = 60;

    private void startCluster() {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
    }

    private Settings.Builder baseSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", "lucene");
    }

    /** Deterministic per-document payload; distinct length per doc so a misframed read is visible. */
    private static byte[] payload(int i) {
        byte[] bytes = new byte[1 + (i % 7)];
        for (int b = 0; b < bytes.length; b++) {
            bytes[b] = (byte) (i + b);
        }
        return bytes;
    }

    private static String encoded(int i) {
        return Base64.getEncoder().encodeToString(payload(i));
    }

    /**
     * Every value round-trips through a terms aggregation on the binary field. Bucket keys are
     * base64 of the stored bytes, so a framing error (reading a length prefix that was never
     * written, or overrunning into the next document's bytes in a shared page buffer) shows up
     * as a wrong or missing key rather than a silent pass.
     */
    public void testBinaryValuesRoundTripThroughTermsAggregation() {
        startCluster();
        client().admin()
            .indices()
            .prepareCreate("binary-aggs")
            .setSettings(baseSettings())
            .setMapping("payload", "type=binary,doc_values=true", "seq", "type=integer")
            .get();
        ensureGreen("binary-aggs");

        // Distinct payload per document: every bucket must have count 1 and a recoverable key.
        for (int i = 0; i < DOC_COUNT; i++) {
            IndexResponse response = client().prepareIndex().setIndex("binary-aggs").setSource("payload", encoded(i), "seq", i).get();
            assertEquals(RestStatus.CREATED, response.status());
        }
        refreshIndex("binary-aggs");

        SearchResponse response = client().prepareSearch("binary-aggs")
            .setSize(0)
            .addAggregation(AggregationBuilders.terms("by_payload").field("payload").size(DOC_COUNT * 2))
            .get();

        Terms terms = response.getAggregations().get("by_payload");
        Set<String> actualKeys = new HashSet<>();
        for (Terms.Bucket bucket : terms.getBuckets()) {
            actualKeys.add(bucket.getKeyAsString());
            assertEquals("each payload is unique so every bucket holds exactly one doc", 1L, bucket.getDocCount());
        }

        // DocValueFormat.BINARY emits unpadded base64 for bucket keys.
        Set<String> expectedKeys = new HashSet<>();
        for (int i = 0; i < DOC_COUNT; i++) {
            expectedKeys.add(Base64.getEncoder().withoutPadding().encodeToString(payload(i)));
        }
        assertEquals("terms agg must recover exactly the indexed payloads", expectedKeys, actualKeys);
    }

    /**
     * An {@code exists} query over a binary column must count exactly the documents that carry a
     * value. The skipper consults per-page statistics for {@code BYTE_ARRAY} columns; sentinel
     * min/max and an unknown null count must make it under-claim (decode) rather than wrongly
     * skip a page, so both the positive and negative sides are asserted.
     */
    public void testExistsQueryOnBinaryFieldCountsOnlyDocsWithValues() {
        startCluster();
        client().admin()
            .indices()
            .prepareCreate("binary-exists")
            .setSettings(baseSettings())
            .setMapping("payload", "type=binary,doc_values=true", "seq", "type=integer")
            .get();
        ensureGreen("binary-exists");

        // Every third document omits the binary field entirely.
        int withValue = 0;
        for (int i = 0; i < DOC_COUNT; i++) {
            boolean hasPayload = i % 3 != 0;
            IndexResponse response = hasPayload
                ? client().prepareIndex().setIndex("binary-exists").setSource("payload", encoded(i), "seq", i).get()
                : client().prepareIndex().setIndex("binary-exists").setSource("seq", i).get();
            assertEquals(RestStatus.CREATED, response.status());
            if (hasPayload) {
                withValue++;
            }
        }
        refreshIndex("binary-exists");

        SearchResponse existsResponse = client().prepareSearch("binary-exists")
            .setSize(0)
            .setQuery(QueryBuilders.existsQuery("payload"))
            .setTrackTotalHits(true)
            .get();
        assertEquals("exists must match exactly the docs carrying a binary value", withValue, existsResponse.getHits().getTotalHits().value());

        SearchResponse missingResponse = client().prepareSearch("binary-exists")
            .setSize(0)
            .setQuery(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery("payload")))
            .setTrackTotalHits(true)
            .get();
        assertEquals(
            "must_not exists must match exactly the docs without a binary value",
            DOC_COUNT - withValue,
            missingResponse.getHits().getTotalHits().value()
        );
    }

    /**
     * With derived source enabled, {@code _source} is rebuilt from doc values for every hit. This
     * asserts per-hit correctness across a multi-hit response with several fields, which is what
     * the per-field doc-values reader cache on the derive path must preserve: a cached reader may
     * be reused across hits but must still yield each document's own value.
     */
    public void testDerivedSourceReturnsCorrectBinaryValuePerHit() {
        startCluster();
        client().admin()
            .indices()
            .prepareCreate("binary-derived")
            .setSettings(baseSettings().put("index.derived_source.enabled", true))
            .setMapping("payload", "type=binary,doc_values=true", "seq", "type=integer", "label", "type=keyword")
            .get();
        ensureGreen("binary-derived");

        for (int i = 0; i < DOC_COUNT; i++) {
            IndexResponse response = client().prepareIndex()
                .setIndex("binary-derived")
                .setSource("payload", encoded(i), "seq", i, "label", "label_" + i)
                .get();
            assertEquals(RestStatus.CREATED, response.status());
        }
        refreshIndex("binary-derived");

        // Request every doc in one response so a single reader serves many hits.
        SearchResponse response = client().prepareSearch("binary-derived")
            .setQuery(QueryBuilders.matchAllQuery())
            .setSize(DOC_COUNT)
            .setTrackTotalHits(true)
            .get();

        assertEquals(DOC_COUNT, response.getHits().getTotalHits().value());
        assertEquals(DOC_COUNT, response.getHits().getHits().length);

        // Correlate by the doc's own seq value: payload, seq and label must all describe the same
        // document, which is exactly what a leaked cached reader would break.
        Set<Integer> seenSeq = new HashSet<>();
        for (SearchHit hit : response.getHits().getHits()) {
            Map<String, Object> source = hit.getSourceAsMap();
            assertNotNull("derived _source must be present for hit " + hit.getId(), source);
            Object seqValue = source.get("seq");
            assertNotNull("derived _source must carry the seq field for hit " + hit.getId(), seqValue);
            int seq = ((Number) seqValue).intValue();
            assertTrue("seq " + seq + " returned more than once", seenSeq.add(seq));

            Object payload = source.get("payload");
            assertNotNull("derived _source must carry the binary field for seq " + seq, payload);
            assertEquals("derived binary value must belong to its own document (seq " + seq + ")", encoded(seq), payload.toString());
            assertEquals(
                "sibling fields must stay aligned with the same document (seq " + seq + ")",
                "label_" + seq,
                String.valueOf(source.get("label"))
            );
        }
        assertEquals("every indexed document must appear exactly once", DOC_COUNT, seenSeq.size());
    }

    /**
     * Multi-valued binary stored as a Parquet LIST column: every value of every document must
     * round-trip through a terms aggregation (per-value bucket contributions, like the classic
     * path), and derived {@code _source} must echo each document's values sorted and
     * de-duplicated — the classic packed encoding's semantics.
     */
    public void testMultiValuedBinaryRoundTrip() {
        startCluster();
        client().admin()
            .indices()
            .prepareCreate("binary-multi")
            .setSettings(baseSettings().put("index.derived_source.enabled", true))
            .setMapping("payload", "type=binary,doc_values=true,multi_value=true", "seq", "type=integer")
            .get();
        ensureGreen("binary-multi");

        int docs = 30;
        for (int i = 0; i < docs; i++) {
            // Values deliberately unsorted with a duplicate: [v(i+1), v(i), v(i+1)] — the stored
            // and derived form must be sorted and de-duplicated: [v(i), v(i+1)].
            IndexResponse response = client().prepareIndex()
                .setIndex("binary-multi")
                .setSource("payload", new String[] { encoded(i + 1), encoded(i), encoded(i + 1) }, "seq", i)
                .get();
            assertEquals(RestStatus.CREATED, response.status());
        }
        refreshIndex("binary-multi");

        // Terms agg: value v(i) appears in doc i and doc i-1 → count 2 for interior values.
        SearchResponse aggResponse = client().prepareSearch("binary-multi")
            .setSize(0)
            .addAggregation(AggregationBuilders.terms("by_payload").field("payload").size(docs * 3))
            .get();
        Terms terms = aggResponse.getAggregations().get("by_payload");
        Map<String, Long> counts = new HashMap<>();
        for (Terms.Bucket bucket : terms.getBuckets()) {
            counts.put(bucket.getKeyAsString(), bucket.getDocCount());
        }
        assertEquals("distinct values are v(0)..v(docs)", docs + 1, counts.size());
        for (int v = 1; v < docs; v++) {
            String key = Base64.getEncoder().withoutPadding().encodeToString(payload(v));
            assertEquals("value v(" + v + ") lives in docs " + (v - 1) + " and " + v, Long.valueOf(2L), counts.get(key));
        }

        // Derived source: sorted, de-duplicated array per document.
        SearchResponse fetch = client().prepareSearch("binary-multi").setSize(docs).setTrackTotalHits(true).get();
        assertEquals(docs, fetch.getHits().getHits().length);
        for (SearchHit hit : fetch.getHits().getHits()) {
            Map<String, Object> source = hit.getSourceAsMap();
            int seq = ((Number) source.get("seq")).intValue();
            Object payload = source.get("payload");
            assertTrue("multi-valued binary must derive as a list (seq " + seq + ")", payload instanceof List);
            List<?> values = (List<?>) payload;
            assertEquals("duplicate must be dropped (seq " + seq + ")", 2, values.size());
            assertEquals("values must be sorted (seq " + seq + ")", encoded(seq), values.get(0).toString());
            assertEquals(encoded(seq + 1), values.get(1).toString());
        }
    }

    /**
     * Exists over a multi-valued binary column: presence is decided by the decoded list — docs
     * with values match, docs without the field do not, and an explicit empty array counts as
     * missing (classic parity: no values means no doc-values entry).
     */
    public void testExistsOnMultiValuedBinary() {
        startCluster();
        client().admin()
            .indices()
            .prepareCreate("binary-multi-exists")
            .setSettings(baseSettings())
            .setMapping("payload", "type=binary,doc_values=true,multi_value=true", "seq", "type=integer")
            .get();
        ensureGreen("binary-multi-exists");

        int withValues = 0;
        for (int i = 0; i < DOC_COUNT; i++) {
            IndexResponse response;
            if (i % 3 == 0) {
                response = client().prepareIndex().setIndex("binary-multi-exists").setSource("seq", i).get();
            } else if (i % 3 == 1) {
                response = client().prepareIndex()
                    .setIndex("binary-multi-exists")
                    .setSource("payload", new String[] {}, "seq", i)   // explicit empty array == missing
                    .get();
            } else {
                response = client().prepareIndex()
                    .setIndex("binary-multi-exists")
                    .setSource("payload", new String[] { encoded(i), encoded(i + 1) }, "seq", i)
                    .get();
                withValues++;
            }
            assertEquals(RestStatus.CREATED, response.status());
        }
        refreshIndex("binary-multi-exists");

        SearchResponse exists = client().prepareSearch("binary-multi-exists")
            .setSize(0)
            .setQuery(QueryBuilders.existsQuery("payload"))
            .setTrackTotalHits(true)
            .get();
        assertEquals("only docs with at least one value exist", withValues, exists.getHits().getTotalHits().value());

        SearchResponse missing = client().prepareSearch("binary-multi-exists")
            .setSize(0)
            .setQuery(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery("payload")))
            .setTrackTotalHits(true)
            .get();
        assertEquals(DOC_COUNT - withValues, missing.getHits().getTotalHits().value());
    }

    /**
     * AUTO promotion: a binary field with no {@code multi_value} setting accepts its first array
     * document by promoting to LIST storage, and both pre- and post-promotion documents stay
     * readable — pre-promotion segments hold scalar columns and are routed by physical shape.
     */
    public void testBinaryAutoPromotionOnFirstArray() {
        startCluster();
        client().admin()
            .indices()
            .prepareCreate("binary-promote")
            .setSettings(baseSettings().put("index.derived_source.enabled", true))
            .setMapping("payload", "type=binary,doc_values=true", "seq", "type=integer")
            .get();
        ensureGreen("binary-promote");

        // Scalar documents first, flushed so a scalar-column segment exists on disk.
        for (int i = 0; i < 10; i++) {
            assertEquals(
                RestStatus.CREATED,
                client().prepareIndex().setIndex("binary-promote").setSource("payload", encoded(i), "seq", i).get().status()
            );
        }
        refreshIndex("binary-promote");
        flushIndex("binary-promote");

        // First array document triggers the scalar-to-LIST promotion.
        assertEquals(
            RestStatus.CREATED,
            client().prepareIndex()
                .setIndex("binary-promote")
                .setSource("payload", new String[] { encoded(100), encoded(101) }, "seq", 100)
                .get()
                .status()
        );
        refreshIndex("binary-promote");

        SearchResponse exists = client().prepareSearch("binary-promote")
            .setSize(0)
            .setQuery(QueryBuilders.existsQuery("payload"))
            .setTrackTotalHits(true)
            .get();
        assertEquals("scalar and promoted docs must all be present", 11, exists.getHits().getTotalHits().value());

        SearchResponse fetch = client().prepareSearch("binary-promote")
            .setQuery(QueryBuilders.termQuery("seq", 100))
            .setSize(1)
            .get();
        assertEquals(1, fetch.getHits().getHits().length);
        Object payload = fetch.getHits().getAt(0).getSourceAsMap().get("payload");
        assertTrue("promoted doc must derive as a list", payload instanceof List);
        assertEquals(List.of(encoded(100), encoded(101)), payload);

        SearchResponse scalarFetch = client().prepareSearch("binary-promote")
            .setQuery(QueryBuilders.termQuery("seq", 3))
            .setSize(1)
            .get();
        assertEquals(1, scalarFetch.getHits().getHits().length);
        assertEquals("pre-promotion scalar doc must stay readable", encoded(3), scalarFetch.getHits().getAt(0).getSourceAsMap().get("payload"));
    }

    /**
     * Parity check 1 — GET by ID with derived source. The realtime path (pre-refresh) and the
     * index-reader path (post-refresh) both reconstruct {@code _source}; the risk under test is
     * the framing wrapper being skipped when the producer is constructed without a MapperService,
     * which would misparse the raw column bytes exactly like the pre-framing aggregation bug.
     */
    public void testGetByIdDerivesBinarySource() {
        startCluster();
        client().admin()
            .indices()
            .prepareCreate("binary-get")
            .setSettings(baseSettings().put("index.derived_source.enabled", true))
            .setMapping("payload", "type=binary,doc_values=true", "tags", "type=binary,doc_values=true,multi_value=true", "seq", "type=integer")
            .get();
        ensureGreen("binary-get");

        IndexResponse indexed = client().prepareIndex()
            .setIndex("binary-get")
            .setSource("payload", encoded(7), "tags", new String[] { encoded(9), encoded(8) }, "seq", 7)
            .get();
        assertEquals(RestStatus.CREATED, indexed.status());
        String id = indexed.getId();

        // Realtime GET before any refresh (translog-era document).
        GetResponse realtime = client().prepareGet("binary-get", id).setRealtime(true).get();
        assertTrue("realtime GET must find the doc", realtime.isExists());
        Map<String, Object> realtimeSource = realtime.getSourceAsMap();
        assertEquals("realtime GET must return the binary value intact", encoded(7), String.valueOf(realtimeSource.get("payload")));

        // GET served from the index reader (post refresh + flush), the derive path proper.
        refreshIndex("binary-get");
        flushIndex("binary-get");
        GetResponse fromIndex = client().prepareGet("binary-get", id).setRealtime(false).get();
        assertTrue(fromIndex.isExists());
        Map<String, Object> source = fromIndex.getSourceAsMap();
        assertEquals("derived GET must return the framed value, not raw column bytes", encoded(7), String.valueOf(source.get("payload")));
        Object tags = source.get("tags");
        assertTrue("multi-valued field must derive as a list in GET", tags instanceof List);
        assertEquals("values sorted per the stored normalization", List.of(encoded(8), encoded(9)), tags);
        assertEquals(7, ((Number) source.get("seq")).intValue());
    }

    /**
     * Parity check 2 — explicit {@code stored_fields} retrieval. {@code BinaryParquetField}
     * claims the STORED_FIELDS capability, so the stored value is served from the parquet
     * column; the bytes handed back must be the raw payload, byte for byte.
     */
    public void testStoredFieldsFetch() {
        startCluster();
        client().admin()
            .indices()
            .prepareCreate("binary-storedfields")
            .setSettings(baseSettings())
            .setMapping("payload", "type=binary,store=true,doc_values=true", "seq", "type=integer")
            .get();
        ensureGreen("binary-storedfields");

        int docs = 10;
        for (int i = 0; i < docs; i++) {
            assertEquals(
                RestStatus.CREATED,
                client().prepareIndex().setIndex("binary-storedfields").setSource("payload", encoded(i), "seq", i).get().status()
            );
        }
        refreshIndex("binary-storedfields");

        SearchResponse response = client().prepareSearch("binary-storedfields")
            .setQuery(QueryBuilders.matchAllQuery())
            .storedFields("payload")
            .addDocValueField("seq")
            .setSize(docs)
            .get();
        assertEquals(docs, response.getHits().getHits().length);
        for (SearchHit hit : response.getHits().getHits()) {
            int seq = ((Number) hit.getFields().get("seq").getValue()).intValue();
            DocumentField field = hit.getFields().get("payload");
            assertNotNull("stored_fields must return the binary field (seq " + seq + ")", field);
            assertArrayEquals("stored bytes must match the indexed payload (seq " + seq + ")", payload(seq), toBytes(field.getValue()));
        }
    }

    /**
     * Parity check 3 — the {@code store:true, doc_values:false} combination. With doc values
     * off, {@code MappedFieldType#existsQuery} routes through the {@code _field_names} metadata
     * field (served by the Lucene secondary) instead of {@code FieldExistsQuery}; presence
     * counts must stay exact on both polarities.
     */
    public void testExistsWithStoreOnlyBinary() {
        startCluster();
        client().admin()
            .indices()
            .prepareCreate("binary-storeonly")
            .setSettings(baseSettings())
            .setMapping("payload", "type=binary,store=true,doc_values=false", "seq", "type=integer")
            .get();
        ensureGreen("binary-storeonly");

        int withValue = 0;
        for (int i = 0; i < DOC_COUNT; i++) {
            IndexResponse response = i % 3 != 0
                ? client().prepareIndex().setIndex("binary-storeonly").setSource("payload", encoded(i), "seq", i).get()
                : client().prepareIndex().setIndex("binary-storeonly").setSource("seq", i).get();
            assertEquals(RestStatus.CREATED, response.status());
            if (i % 3 != 0) {
                withValue++;
            }
        }
        refreshIndex("binary-storeonly");

        SearchResponse exists = client().prepareSearch("binary-storeonly")
            .setSize(0)
            .setQuery(QueryBuilders.existsQuery("payload"))
            .setTrackTotalHits(true)
            .get();
        assertEquals("_field_names-routed exists must count exactly", withValue, exists.getHits().getTotalHits().value());

        SearchResponse missing = client().prepareSearch("binary-storeonly")
            .setSize(0)
            .setQuery(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery("payload")))
            .setTrackTotalHits(true)
            .get();
        assertEquals(DOC_COUNT - withValue, missing.getHits().getTotalHits().value());
    }

    /**
     * Parity check 4 — {@code docvalue_fields} retrieval, which reads through fielddata
     * ({@code AbstractBinaryDVLeafFieldData}): the same framed-decoding consumer scripts use.
     * Values come back as unpadded base64, one per value — including the multi-valued form,
     * whose values must appear sorted and de-duplicated per the stored normalization.
     */
    public void testDocValueFieldsFetch() {
        startCluster();
        client().admin()
            .indices()
            .prepareCreate("binary-dvfields")
            .setSettings(baseSettings())
            .setMapping("payload", "type=binary,doc_values=true", "tags", "type=binary,doc_values=true,multi_value=true", "seq", "type=integer")
            .get();
        ensureGreen("binary-dvfields");

        int docs = 10;
        for (int i = 0; i < docs; i++) {
            assertEquals(
                RestStatus.CREATED,
                client().prepareIndex()
                    .setIndex("binary-dvfields")
                    .setSource("payload", encoded(i), "tags", new String[] { encoded(i + 1), encoded(i), encoded(i + 1) }, "seq", i)
                    .get()
                    .status()
            );
        }
        refreshIndex("binary-dvfields");

        SearchResponse response = client().prepareSearch("binary-dvfields")
            .setQuery(QueryBuilders.matchAllQuery())
            .addDocValueField("payload")
            .addDocValueField("tags")
            .addDocValueField("seq")
            .setSize(docs)
            .get();
        assertEquals(docs, response.getHits().getHits().length);
        for (SearchHit hit : response.getHits().getHits()) {
            int seq = ((Number) hit.getFields().get("seq").getValue()).intValue();
            String expected = Base64.getEncoder().withoutPadding().encodeToString(payload(seq));
            assertEquals("docvalue_fields must decode the framed value (seq " + seq + ")", expected, hit.getFields().get("payload").getValue());

            List<Object> tagValues = hit.getFields().get("tags").getValues();
            assertEquals("duplicates dropped (seq " + seq + ")", 2, tagValues.size());
            assertEquals(Base64.getEncoder().withoutPadding().encodeToString(payload(seq)), tagValues.get(0));
            assertEquals(Base64.getEncoder().withoutPadding().encodeToString(payload(seq + 1)), tagValues.get(1));
        }
    }

    /** Normalizes the transport representation of a stored binary field to raw bytes. */
    private static byte[] toBytes(Object storedValue) {
        if (storedValue instanceof BytesReference bytesReference) {
            BytesRef ref = bytesReference.toBytesRef();
            return java.util.Arrays.copyOfRange(ref.bytes, ref.offset, ref.offset + ref.length);
        }
        if (storedValue instanceof BytesRef bytesRef) {
            return java.util.Arrays.copyOfRange(bytesRef.bytes, bytesRef.offset, bytesRef.offset + bytesRef.length);
        }
        if (storedValue instanceof byte[] bytes) {
            return bytes;
        }
        if (storedValue instanceof String base64) {
            return Base64.getDecoder().decode(base64);
        }
        throw new AssertionError("unexpected stored binary representation: " + storedValue.getClass());
    }

    /**
     * {@code store=true} with doc values must still derive from doc values on a parquet-primary
     * index: stored fields are not served from the parquet columns, so preferring them would
     * derive nothing. Asserts the value is recovered rather than dropped.
     */
    public void testDerivedSourceWithStoredBinaryField() {
        startCluster();
        client().admin()
            .indices()
            .prepareCreate("binary-stored")
            .setSettings(baseSettings().put("index.derived_source.enabled", true))
            .setMapping("payload", "type=binary,doc_values=true,store=true", "seq", "type=integer")
            .get();
        ensureGreen("binary-stored");

        int docs = 20;
        for (int i = 0; i < docs; i++) {
            IndexResponse response = client().prepareIndex().setIndex("binary-stored").setSource("payload", encoded(i), "seq", i).get();
            assertEquals(RestStatus.CREATED, response.status());
        }
        refreshIndex("binary-stored");

        SearchResponse response = client().prepareSearch("binary-stored")
            .setQuery(QueryBuilders.matchAllQuery())
            .setSize(docs)
            .setTrackTotalHits(true)
            .get();

        assertEquals(docs, response.getHits().getHits().length);
        for (SearchHit hit : response.getHits().getHits()) {
            Map<String, Object> source = hit.getSourceAsMap();
            assertNotNull("derived _source must be present for stored binary hit " + hit.getId(), source);
            int seq = ((Number) source.get("seq")).intValue();
            assertEquals(
                "stored+doc_values binary must derive from doc values on a parquet-primary index (seq " + seq + ")",
                encoded(seq),
                String.valueOf(source.get("payload"))
            );
        }
    }
}
