/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.IOContext;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.lucene.index.SequentialStoredFieldsLeafReader;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.parquet.bridge.BinaryPageReader;
import org.opensearch.parquet.codec.iter.ParquetDictionarySortedDocValues;
import org.opensearch.parquet.codec.iter.ParquetSortedDocValues;
import org.opensearch.parquet.codec.iter.ParquetUninvertedSortedDocValues;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link FilterLeafReader} that serves doc values for Parquet-resident fields from a
 * {@link ParquetDocValuesProducer}, while delegating everything else to the underlying Lucene
 * leaf reader.
 *
 * <p>This is the read-time integration of the Parquet DocValues codec for the case where a field
 * is <b>Parquet-only</b> — i.e. it has no {@link FieldInfo} in the Lucene segment at all (the
 * composite engine's Lucene secondary writes only text/keyword inverted indexes plus the row-id
 * doc values; numeric fields like {@code age} live solely in Parquet). Lucene's
 * {@code PerFieldDocValuesFormat} cannot route to such a field because there is no segment
 * {@code FieldInfo} to carry the format name. This reader closes that gap by:
 *
 * <ol>
 *   <li><b>Synthesizing {@link FieldInfo}s</b> — for every mapped field that the Parquet codec
 *       supports and that is absent (or DV-less) in the delegate's {@link FieldInfos}, a synthetic
 *       {@code FieldInfo} with the appropriate {@link DocValuesType} (from {@link FieldTypeMapping})
 *       is added so OpenSearch's value-source layer believes the doc values exist and asks for
 *       them.</li>
 *   <li><b>Overriding the five DV accessors</b> — for those synthetic fields the iterators come
 *       from a per-segment {@link ParquetDocValuesProducer}; all other fields delegate to the
 *       underlying reader unchanged.</li>
 * </ol>
 *
 * <p>One producer is built lazily per segment and closed when this reader closes. Not shared
 * across segments.
 *
 * <p>Extends {@link SequentialStoredFieldsLeafReader} (rather than plain {@link FilterLeafReader})
 * so the fetch phase can retrieve {@code _source}/stored fields on {@code size > 0} queries. On a
 * Parquet-primary index derived source is force-enabled, so the reader stack is
 * {@code DerivedSourceLeafReader -> ParquetDocValuesLeafReader -> SegmentReader}. When the fetch
 * phase asks the outer {@code DerivedSourceLeafReader} for a sequential stored-fields reader, it
 * unwraps to this reader; a plain {@code FilterLeafReader} is neither a {@code CodecReader} nor a
 * {@code SequentialStoredFieldsLeafReader}, so the unwrap threw and only {@code size:0} worked.
 * As a {@code SequentialStoredFieldsLeafReader} this reader is transparent to that unwrap: it
 * passes the underlying segment's stored-fields reader straight through, and the derived-source
 * layer still synthesizes {@code _source} from doc values on top of it.
 */
public final class ParquetDocValuesLeafReader extends SequentialStoredFieldsLeafReader {

    private final MapperService mapperService;

    /** Lazily constructed Parquet producer for this segment; null until first DV access. */
    private ParquetDocValuesProducer producer;
    private boolean producerInitialized;

    /** Synthetic + real merged field infos, computed once. */
    private final FieldInfos mergedFieldInfos;

    /** Field name -> synthetic FieldInfo for Parquet-resident DV fields served by this reader. */
    private final Map<String, FieldInfo> parquetFields;

    /** Fields whose stored values are served from the Parquet column; see selectParquetStoredFields. */
    private final Map<String, FieldInfo> parquetStoredFields;

    /** Request-scoped uninverted-ordinal leases keyed by field, released when this reader closes. */
    private final Map<String, UninvertedOrdinalsCache.Lease> uninvertedOrdinalsLeases = new HashMap<>();

    /** The segment read state used to build the producer (captured at construction). */
    private final SegmentReadState segmentReadState;

    /** Per-query stats accumulator shared across all leaves of one search; may be null in tests. */

    private ParquetDocValuesLeafReader(
        LeafReader in,
        MapperService mapperService,
        SegmentReadState segmentReadState,
        Map<String, FieldInfo> parquetFields,
        FieldInfos mergedFieldInfos
    ) {
        super(in);
        this.mapperService = mapperService;
        this.segmentReadState = segmentReadState;
        this.parquetFields = parquetFields;
        this.mergedFieldInfos = mergedFieldInfos;
        this.parquetStoredFields = selectParquetStoredFields(mapperService, parquetFields);
    }

    /**
     * The synthetic fields whose {@code stored_fields} requests this reader serves from the
     * Parquet column: {@code binary} fields mapped {@code store: true}. Their STORED_FIELDS
     * capability is claimed by the Parquet format, so the Lucene secondary holds no stored
     * value for them — the column is the store.
     */
    private static Map<String, FieldInfo> selectParquetStoredFields(MapperService mapperService, Map<String, FieldInfo> parquetFields) {
        if (mapperService == null) {
            return Map.of();
        }
        Map<String, FieldInfo> stored = new LinkedHashMap<>();
        for (Map.Entry<String, FieldInfo> entry : parquetFields.entrySet()) {
            MappedFieldType mft = mapperService.fieldType(entry.getKey());
            if (mft != null && "binary".equals(mft.typeName()) && mft.isStored()) {
                stored.put(entry.getKey(), entry.getValue());
            }
        }
        return stored;
    }

    /**
     * Builds a {@link ParquetDocValuesLeafReader} for {@code in} if a Parquet file resolves for the
     * segment and the mapping declares at least one Parquet-codec-supported field that is missing
     * doc values in the Lucene segment. Otherwise returns {@code in} unwrapped.
     */
    public static LeafReader wrapIfApplicable(LeafReader in, MapperService mapperService) throws IOException {
        SegmentReader segmentReader;
        try {
            segmentReader = Lucene.segmentReader(in);
        } catch (RuntimeException e) {
            // Not a segment-backed leaf (e.g. an in-memory test reader) — nothing to wrap.
            return in;
        }

        SegmentReadState state = new SegmentReadState(
            segmentReader.directory(),
            segmentReader.getSegmentInfo().info,
            segmentReader.getFieldInfos(),
            IOContext.DEFAULT
        );

        // Only proceed if a Parquet file exists for this segment.
        if (ParquetSegmentLayout.resolve(state) == null) {
            return in;
        }

        FieldInfos existing = in.getFieldInfos();
        Map<String, FieldInfo> parquetFields = new LinkedHashMap<>();
        List<FieldInfo> merged = new ArrayList<>();
        int maxNumber = -1;
        for (FieldInfo fi : existing) {
            merged.add(fi);
            maxNumber = Math.max(maxNumber, fi.number);
        }

        // Walk the mapping. For each field the Parquet codec supports whose doc values are NOT
        // already present in the Lucene segment, synthesize a FieldInfo with the mapped DV type.
        for (MappedFieldType mft : mapperService.fieldTypes()) {
            String name = mft.name();
            if (mapperService.isMetadataField(name)) {
                continue;
            }
            if (FieldTypeMapping.isSupported(mft.typeName()) == false) {
                continue;
            }
            FieldInfo realFi = existing.fieldInfo(name);
            if (realFi != null && realFi.getDocValuesType() != DocValuesType.NONE) {
                // Lucene already serves doc values for this field — leave it to the native reader.
                continue;
            }
            FieldTypeMapping.Mapping mapping = FieldTypeMapping.forType(mft.typeName());
            // Declare the doc-values type the field is CAPABLE of holding, not the single-valued one.
            // Lucene's FieldExistsQuery dispatches on this declaration, so a multi-valued column
            // declared NUMERIC sends `exists` to the single-valued getter, which finds no values and
            // silently reports that no document has the field. Aggregations are unaffected either way
            // because numeric and keyword value sources request SORTED_NUMERIC / SORTED_SET regardless
            // of what is declared here.
            //
            // Deriving this from the mapping is sound even though ROUTING must be physical and
            // per-segment: multi-value state is monotonic — AUTO or SCALAR promotes to LIST and never
            // back, enforced by the multi_value parameter's merge validator — so the declaration can
            // only ever be wider than the file on disk, never narrower. The getters then narrow it:
            // getSortedNumericDocValues inspects the column's physical shape and serves a still-scalar
            // segment through the singleton path. This relies on the promotion reaching the mapping
            // before the segment holding the first LIST column becomes searchable; were that ever
            // reordered, exists would route to the single-valued getter and fail loudly in the native
            // shape check rather than return a wrong answer.
            //
            // Fall back to the single-valued type when a mapping declares no multi-valued counterpart
            // (text and binary map it to NONE). Without this, wiring multi_value onto such a type would
            // synthesize a doc-values-less FieldInfo and silently drop the field from exists.
            boolean multiValued = mft.isMultiValued() && mapping.multiValued() != DocValuesType.NONE;
            DocValuesType dvType = multiValued ? mapping.multiValued() : mapping.singleValued();
            FieldInfo synthetic = newDocValuesFieldInfo(name, ++maxNumber, dvType, skipIndexTypeFor(mapping, multiValued));
            parquetFields.put(name, synthetic);
            // If a DV-less FieldInfo already exists for this field, replace it with the synthetic
            // one carrying the DV type; otherwise append.
            if (realFi != null) {
                merged.removeIf(fi -> fi.name.equals(name));
            }
            merged.add(synthetic);
        }

        if (parquetFields.isEmpty()) {
            // Nothing for us to serve — don't wrap.
            return in;
        }

        FieldInfos mergedInfos = new FieldInfos(merged.toArray(new FieldInfo[0]));
        return new ParquetDocValuesLeafReader(in, mapperService, state, parquetFields, mergedInfos);
    }

    /**
     * Skip-index declaration for a synthetic field: RANGE only when both halves agree.
     *
     * <p><b>Physical type</b> must be one the producer can serve stats for through a
     * {@link DocValuesSkipper} — integer-shaped columns (INT32/INT64/BOOL: raw-bits order ==
     * numeric order, full min/max) and BYTE_ARRAY columns (sentinel min/max that never wrongly
     * skip, plus exact null counts, enough for existence pruning). Float/double are excluded:
     * their IEEE-754 bit order diverges from numeric order for negatives.
     *
     * <p><b>DocValues type</b> must be one Lucene permits a RANGE skip index on. See
     * {@link DocValuesSkipIndexType#RANGE}: only NUMERIC, SORTED_NUMERIC, SORTED and
     * SORTED_SET are compatible, because a skip index records min/max and those are the
     * ordered doc-values kinds. {@link DocValuesType#BINARY} has no ordering, so Lucene's
     * FieldInfo constructor rejects the pair outright — which excludes {@code binary} and
     * {@code text} here even though their BYTE_ARRAY stats would be usable for presence.
     *
     * <p><b>Multi-valued declarations</b> forfeit the skipper regardless of both checks above:
     * once values repeat, OffsetIndex page rows no longer bound Lucene documents, so the
     * producer's getSkipper refuses repeated shapes and the declaration here must agree.
     *
     * <p>Must stay in sync with {@link ParquetDocValuesProducer#getSkipper}: declaring RANGE
     * for a field whose getSkipper returns null would break consumers that trust the
     * declaration.
     */
    // Package-private for tests: the (DV type, skip index type) pairs this produces must all be
    // constructible by Lucene's FieldInfo, which validates compatibility.
    static DocValuesSkipIndexType skipIndexTypeFor(FieldTypeMapping.Mapping mapping, boolean multiValued) {
        if (multiValued) {
            return DocValuesSkipIndexType.NONE;
        }
        ParquetPhysicalType phys = mapping.physical();
        boolean statsServable = phys == ParquetPhysicalType.INT32
            || phys == ParquetPhysicalType.INT64
            || phys == ParquetPhysicalType.BOOL
            || phys == ParquetPhysicalType.BYTE_ARRAY;
        return statsServable && rangeCompatible(mapping.singleValued())
            ? DocValuesSkipIndexType.RANGE
            : DocValuesSkipIndexType.NONE;
    }

    /**
     * Mirrors {@code DocValuesSkipIndexType.RANGE.isCompatibleWith}, which is package-private in
     * Lucene: a RANGE skip index records min/max, so it is only defined for the ordered
     * doc-values kinds. Keeping this explicit means an unsupported pair is filtered here rather
     * than throwing from Lucene's FieldInfo constructor when the synthetic infos are built.
     */
    private static boolean rangeCompatible(DocValuesType dvType) {
        return dvType == DocValuesType.NUMERIC
            || dvType == DocValuesType.SORTED_NUMERIC
            || dvType == DocValuesType.SORTED
            || dvType == DocValuesType.SORTED_SET;
    }

    /** Builds a synthetic doc-values {@link FieldInfo} carrying the given DV type. */
    private static FieldInfo newDocValuesFieldInfo(String name, int number, DocValuesType dvType, DocValuesSkipIndexType skipType) {
        return new FieldInfo(
            name,
            number,
            false,                       // storeTermVector
            true,                        // omitNorms
            false,                       // storePayloads
            IndexOptions.NONE,           // not indexed via this reader
            dvType,
            skipType,
            -1,                          // dvGen
            new HashMap<>(),             // attributes (mutable, per FieldInfo contract)
            0,                           // pointDimensionCount
            0,                           // pointIndexDimensionCount
            0,                           // pointNumBytes
            0,                           // vectorDimension
            VectorEncoding.FLOAT32,
            VectorSimilarityFunction.EUCLIDEAN,
            false,                       // softDeletes
            false                        // isParentField
        );
    }

    private synchronized ParquetDocValuesProducer producer() throws IOException {
        if (producerInitialized == false) {
            producer = new ParquetDocValuesProducer(segmentReadState, mapperService);
            producerInitialized = true;
        }
        if (producer != null && producer.isClosed()) {
            // This wrapper outlived its request: a cache (fielddata, global ordinals) retained it
            // and is calling back after the search closed the request producer. Serve through the
            // segment-lifetime shared producer so cached consumers stay valid until the segment
            // itself closes — the contract every reader-keyed cache in Lucene/OpenSearch assumes.
            ParquetDocValuesProducer shared = SharedProducerRegistry.get(in.getCoreCacheHelper(), segmentReadState, mapperService);
            if (shared == null) {
                throw new IllegalStateException("doc values requested after the search closed and the segment has no core cache identity");
            }
            return shared;
        }
        return producer;
    }

    /** Whether the mapper types this field (or subfield) as keyword — values indexed verbatim. */
    private boolean isKeywordField(String field) {
        org.opensearch.index.mapper.MappedFieldType fieldType = mapperService.fieldType(field);
        return fieldType != null && "keyword".equals(fieldType.typeName());
    }

    /** Returns the synthetic FieldInfo if the given field is served from Parquet, else null. */
    private FieldInfo parquetFieldInfo(String field) {
        return parquetFields.get(field);
    }

    /**
     * Builds a {@link RowIdResolver} that translates this segment's {@code docId}s to Parquet row
     * positions by reading the underlying leaf's {@code __row_id__} doc values. Each codec iterator
     * needs its own resolver (its own {@code __row_id__} iterator), so this is called per DV accessor.
     * Falls back to identity when the segment has no {@code __row_id__} field.
     */
    private RowIdResolver newRowIdResolver() throws IOException {
        // The write path GUARANTEES rowId == docId in every finished segment: row ids are rewritten to
        // sequential 0..maxDoc-1 after any sort/merge (SequentialRowIdProducer) and verified by
        // LuceneWriter.assertRowIdsSequential. So the per-doc __row_id__ lookup is pure waste here — it
        // re-reads a value that always equals the docId. Skip it: use the no-op IDENTITY resolver.
        //
        // Backed by an -ea assert that mirrors the writer's invariant; if a future write path ever
        // produced a non-identity segment, this trips in dev/test. (Costs nothing in prod.)
        assert assertRowIdsAreIdentity() : "non-identity __row_id__ segment reached read path; IDENTITY shortcut is unsafe here";
        return RowIdResolver.IDENTITY;
    }

    /** -ea-only check mirroring {@code LuceneWriter.assertRowIdsSequential}: every doc's __row_id__ == docId. */
    private boolean assertRowIdsAreIdentity() throws IOException {
        SortedNumericDocValues rowId = in.getSortedNumericDocValues(DocumentInput.ROW_ID_FIELD);
        if (rowId == null) {
            return true; // no row-id field => identity by definition
        }
        for (int docId = 0; docId < maxDoc(); docId++) {
            if (rowId.advanceExact(docId) == false) {
                return false;
            }
            if (rowId.nextValue() != docId) {
                return false;
            }
        }
        return true;
    }

    @Override
    public FieldInfos getFieldInfos() {
        return mergedFieldInfos;
    }

    @Override
    public NumericDocValues getNumericDocValues(String field) throws IOException {
        FieldInfo fi = parquetFieldInfo(field);
        if (fi != null && fi.getDocValuesType() == DocValuesType.NUMERIC) {
            // A multi-valued field is declared SORTED_NUMERIC (see the FieldInfos synthesis above), so
            // it does not enter this branch at all and falls through to the delegate, which correctly
            // reports no single-valued doc values for it. That is the honest answer: a repeated column
            // has no one value per document. Callers that can handle multiple values must request
            // SORTED_NUMERIC, which routes on the column's physical shape.
            RowIdResolver resolver = newRowIdResolver();
            NumericDocValues numeric = producer().getNumeric(fi);
            // IDENTITY (the guaranteed case — see newRowIdResolver) means docId == Parquet row, so the
            // remap wrapper is pure indirection: return the delegate, already in docId space, directly.
            return resolver == RowIdResolver.IDENTITY ? numeric : RowIdRemappingDocValues.numeric(numeric, resolver, maxDoc());
        }
        return in.getNumericDocValues(field);
    }

    @Override
    public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
        FieldInfo fi = parquetFieldInfo(field);
        if (fi != null) {
            // Genuinely multi-valued columns must go through the repeated reader. Two conditions,
            // deliberately in this order:
            //
            // 1. The DECLARATION must say SORTED_NUMERIC. Only a field the mapping declares
            // multi-valued can have a repeated column, because the state is monotonic (see the
            // FieldInfos synthesis above), so for a NUMERIC-declared field the probe could only
            // ever answer "scalar". Checking this first keeps the probe -- and the eager native
            // cursor open it forces -- entirely off the single-valued hot path, which is the
            // overwhelming majority of fields and would otherwise open a cursor per column per
            // segment even for columns a query never reads.
            // 2. The PHYSICAL shape must actually be repeated. The declaration is only ever wider
            // than the file: after a scalar-to-LIST promotion the mapping reports LIST for every
            // segment while files written earlier are still scalar, so those must fall through to
            // the singleton path below or they would trip check_column_shape in the native layer.
            if (fi.getDocValuesType() == DocValuesType.SORTED_NUMERIC && producer().isRepeated(fi)) {
                SortedNumericDocValues sortedNumeric = producer().getSortedNumeric(fi);
                RowIdResolver repeatedResolver = newRowIdResolver();
                return repeatedResolver == RowIdResolver.IDENTITY
                    ? sortedNumeric
                    : RowIdRemappingDocValues.sortedNumeric(sortedNumeric, repeatedResolver, maxDoc());
            }

            // OpenSearch numeric value sources request SORTED_NUMERIC even for single-valued fields,
            // then call DocValues.unwrapSingleton(...) to take a leaner single-valued collector when
            // possible. We therefore serve single-valued numerics through the CACHED single-valued
            // iterator (producer().getNumeric → ParquetNumericDocValues → PageCache hot path), apply
            // the docId→row remapping at the numeric level, and wrap the result with
            // DocValues.singleton(...) so the returned value is a real SingletonSortedNumericDocValues
            // that unwrapSingleton(...) can detect. This wins on two layers: the PageCache (no per-doc
            // FFM call) and the aggregator's single-valued fast path.
            FieldInfo asNumeric = fi.getDocValuesType() == DocValuesType.NUMERIC
                ? fi
                : newDocValuesFieldInfo(field, fi.number, DocValuesType.NUMERIC, fi.docValuesSkipIndexType());
            NumericDocValues numeric = producer().getNumeric(asNumeric);
            RowIdResolver resolver = newRowIdResolver();
            NumericDocValues remapped = resolver == RowIdResolver.IDENTITY
                ? numeric
                : RowIdRemappingDocValues.numeric(numeric, resolver, maxDoc());
            return DocValues.singleton(remapped);
        }
        return in.getSortedNumericDocValues(field);
    }

    @Override
    public BinaryDocValues getBinaryDocValues(String field) throws IOException {
        FieldInfo fi = parquetFieldInfo(field);
        if (fi != null && fi.getDocValuesType() == DocValuesType.BINARY) {
            RowIdResolver resolver = newRowIdResolver();
            BinaryDocValues binary = producer().getBinary(fi);
            return resolver == RowIdResolver.IDENTITY ? binary : RowIdRemappingDocValues.binary(binary, resolver, maxDoc());
        }
        return in.getBinaryDocValues(field);
    }

    @Override
    public SortedDocValues getSortedDocValues(String field) throws IOException {
        FieldInfo fi = parquetFieldInfo(field);
        if (fi != null && fi.getDocValuesType() == DocValuesType.SORTED) {
            RowIdResolver resolver = newRowIdResolver();
            SortedDocValues sorted = withDictionaryOrdinals(field, producer().getSorted(fi));
            return resolver == RowIdResolver.IDENTITY ? sorted : RowIdRemappingDocValues.sorted(sorted, resolver, maxDoc());
        }
        return in.getSortedDocValues(field);
    }

    /**
     * Upgrades a streaming sorted iterator to fully contract-compliant segment ordinals when the
     * field's cardinality fits the dictionary budget. The sorted term dictionary is read from
     * the composite index's Lucene sidecar (O(distinct), cached per segment) — never from a row
     * scan. Above-budget fields keep the streaming iterator, whose global-ordinal operations
     * fail fast rather than materialize.
     */
    private synchronized UninvertedOrdinalsCache.Lease acquireUninvertedOrdinalsLease(String field, long expectedNonNullDocs)
        throws IOException {
        UninvertedOrdinalsCache.Lease lease = uninvertedOrdinalsLeases.get(field);
        if (lease != null) {
            return lease;
        }
        lease = UninvertedOrdinalsCache.acquire(in, segmentReadState.segmentInfo, field, expectedNonNullDocs);
        if (lease != null) {
            uninvertedOrdinalsLeases.put(field, lease);
        }
        return lease;
    }

    private SortedDocValues withDictionaryOrdinals(String field, SortedDocValues sorted) throws IOException {
        // Ordinal tiers rank Parquet VALUES against the Lucene sidecar's TERMS, which only
        // coincide for untokenized (keyword) fields. A text field's terms are analyzer tokens:
        // ranking values against tokens would produce silently wrong ordinals. Text fields stay
        // on the streaming iterator, whose global operations fail fast toward execution_hint:map.
        if (isKeywordField(field) == false) {
            return sorted;
        }
        if (sorted instanceof ParquetSortedDocValues streaming) {
            TermDictionary dictionary = TermDictionaryCache.get(
                in,
                field,
                ParquetDocValuesProducer.dictionaryMaxTerms(),
                ParquetDocValuesProducer.dictionaryCacheBytes()
            );
            if (dictionary != null) {
                return new ParquetDictionarySortedDocValues(streaming, dictionary);
            }
            // Above the dictionary budget: disk-backed uninverted ordinals (built once per
            // segment from the sidecar's postings, memory-mapped, working-set resident).
            long expectedNonNull = producer().nonNullRowCount(parquetFieldInfo(field));
            UninvertedOrdinalsCache.Lease lease = acquireUninvertedOrdinalsLease(field, expectedNonNull);
            if (lease != null) {
                return new ParquetUninvertedSortedDocValues(lease.ordinals(), streaming, maxDoc());
            }
        }
        return sorted;
    }

    @Override
    public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
        FieldInfo fi = parquetFieldInfo(field);
        if (fi != null) {
            // Mirror getSortedNumericDocValues: keyword value sources request SORTED_SET even for
            // single-valued fields, then call DocValues.unwrapSingleton(...). Serve single-valued
            // keywords through the single-valued ordinal-table iterator (producer().getSorted →
            // ParquetSortedDocValues), remap docId→row, and wrap with DocValues.singleton(...) so the
            // returned value is a real SingletonSortedSetDocValues that unwrapSingleton(...) detects.
            //
            // Genuinely repeated columns go to the sorted-set iterator instead, gated the same way as
            // the numeric path: the declaration must say SORTED_SET (only a field the mapping declares
            // multi-valued can be repeated, so probing a SORTED-declared field could only answer
            // "scalar" while still forcing an eager native cursor open), and the physical shape must
            // then confirm it, because the mapping says LIST for every segment once a field is
            // promoted while segments written earlier are still scalar. Note that iterator's
            // global-ordinal operations (getValueCount, lookupTerm) throw, so ordinal-based terms
            // aggregations on a multi-valued keyword still fail fast toward execution_hint:map;
            // presence and per-document value iteration work.
            if (fi.getDocValuesType() == DocValuesType.SORTED_SET && producer().isRepeated(fi)) {
                SortedSetDocValues sortedSet = producer().getSortedSet(fi);
                RowIdResolver repeatedResolver = newRowIdResolver();
                return repeatedResolver == RowIdResolver.IDENTITY
                    ? sortedSet
                    : RowIdRemappingDocValues.sortedSet(sortedSet, repeatedResolver, maxDoc());
            }

            FieldInfo asSorted = fi.getDocValuesType() == DocValuesType.SORTED
                ? fi
                : newDocValuesFieldInfo(field, fi.number, DocValuesType.SORTED, fi.docValuesSkipIndexType());
            SortedDocValues sorted = withDictionaryOrdinals(field, producer().getSorted(asSorted));
            RowIdResolver resolver = newRowIdResolver();
            SortedDocValues remapped = resolver == RowIdResolver.IDENTITY
                ? sorted
                : RowIdRemappingDocValues.sorted(sorted, resolver, maxDoc());
            return DocValues.singleton(remapped);
        }
        return in.getSortedSetDocValues(field);
    }

    @Override
    public DocValuesSkipper getDocValuesSkipper(String field) throws IOException {
        FieldInfo fi = parquetFieldInfo(field);
        if (fi != null) {
            if (fi.docValuesSkipIndexType() == DocValuesSkipIndexType.NONE) {
                return null;
            }
            // Doc IDs and Parquet rows coincide (IDENTITY resolver — see newRowIdResolver), so
            // page row ranges are directly valid as skipper doc ID intervals.
            return producer().getSkipper(fi);
        }
        return in.getDocValuesSkipper(field);
    }

    @Override
    protected void doClose() throws IOException {
        closeParquetResources();
        super.doClose();
    }

    /**
     * Releases resources owned by this wrapper without closing the underlying Lucene leaf.
     *
     * {@link FilterDirectoryReader} closes its wrapped directory, not the synthetic leaf
     * wrappers returned by its {@code SubReaderWrapper}. The request-scoped directory reader
     * therefore calls this method explicitly before closing its non-closing delegate.
     */
    synchronized void closeParquetResources() throws IOException {
        for (UninvertedOrdinalsCache.Lease lease : uninvertedOrdinalsLeases.values()) {
            lease.close();
        }
        uninvertedOrdinalsLeases.clear();

        IOException first = null;
        try {
            if (producer != null) {
                producer.close();
            }
        } catch (IOException e) {
            first = e;
        }
        if (first != null) {
            throw first;
        }
    }

    /**
     * This reader serves no stored fields itself — it only overlays Parquet doc values. The
     * underlying segment reader holds the real stored fields, so return its sequential reader
     * unchanged. {@link SequentialStoredFieldsLeafReader#getSequentialStoredFieldsReader()} already
     * unwrapped {@code in} (a {@code CodecReader}/segment reader) down to {@code reader}; the
     * derived-source layer above wraps the result to synthesize {@code _source}.
     */
    @Override
    protected StoredFieldsReader doGetSequentialStoredFieldsReader(StoredFieldsReader reader) {
        if (parquetStoredFields.isEmpty()) {
            return reader;
        }
        return new ParquetBackedStoredFieldsReader(reader, new ParquetStoredLane());
    }

    @Override
    public StoredFields storedFields() throws IOException {
        StoredFields delegate = in.storedFields();
        if (parquetStoredFields.isEmpty()) {
            return delegate;
        }
        ParquetStoredLane lane = new ParquetStoredLane();
        return new StoredFields() {
            @Override
            public void document(int docID, StoredFieldVisitor visitor) throws IOException {
                delegate.document(docID, visitor);
                lane.visit(docID, visitor);
            }

            @Override
            public void prefetch(int docID) throws IOException {
                delegate.prefetch(docID);
            }
        };
    }

    /**
     * Per-consumer state for serving stored values out of Parquet columns: one dedicated column
     * reader per field (instance-scoped cursors — see the producer's dedicatedReaderFor note on
     * concurrent slices) and the segment's docId-to-rowId resolver, both created lazily. Each
     * wrapper instance (and each {@code clone()} of the sequential reader) owns its own lane so
     * cursors are never shared across threads.
     */
    private final class ParquetStoredLane {
        private final Map<String, BinaryPageReader> readers = new HashMap<>();
        private final Map<String, Boolean> repeatedByField = new HashMap<>();
        private RowIdResolver resolver;

        void visit(int docID, StoredFieldVisitor visitor) throws IOException {
            for (FieldInfo fi : parquetStoredFields.values()) {
                StoredFieldVisitor.Status status = visitor.needsField(fi);
                if (status == StoredFieldVisitor.Status.STOP) {
                    return;
                }
                if (status == StoredFieldVisitor.Status.NO) {
                    continue;
                }
                if (resolver == null) {
                    resolver = newRowIdResolver();
                }
                long rowId = resolver.toRowId(docID);
                boolean repeated = repeatedByField.computeIfAbsent(fi.name, name -> {
                    try {
                        return producer().isRepeated(fi);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
                BinaryPageReader reader = readers.computeIfAbsent(fi.name, name -> {
                    try {
                        return producer().storedBinaryReaderFor(fi, repeated);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
                if (repeated) {
                    // One stored entry per value, matching the classic path's one StoredField per value.
                    for (byte[] value : reader.readRepeatedBytesAtRow(rowId)) {
                        visitor.binaryField(fi, value);
                    }
                } else {
                    byte[] value = reader.readBytesAtRow(rowId);
                    if (value != null) {
                        visitor.binaryField(fi, value);
                    }
                }
            }
        }
    }

    /**
     * The sequential-access flavour: augments the delegate {@link StoredFieldsReader} with the
     * Parquet-served fields. {@code clone()} clones the delegate and takes a fresh lane, keeping
     * per-thread cursor isolation.
     */
    private final class ParquetBackedStoredFieldsReader extends StoredFieldsReader {
        private final StoredFieldsReader delegate;
        private final ParquetStoredLane lane;

        ParquetBackedStoredFieldsReader(StoredFieldsReader delegate, ParquetStoredLane lane) {
            this.delegate = delegate;
            this.lane = lane;
        }

        @Override
        public void document(int docID, StoredFieldVisitor visitor) throws IOException {
            delegate.document(docID, visitor);
            lane.visit(docID, visitor);
        }

        @Override
        public void prefetch(int docID) throws IOException {
            delegate.prefetch(docID);
        }

        @Override
        public StoredFieldsReader clone() {
            return new ParquetBackedStoredFieldsReader(delegate.clone(), new ParquetStoredLane());
        }

        @Override
        public void checkIntegrity() throws IOException {
            delegate.checkIntegrity();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    // Cache helpers must delegate to the underlying reader so query/segment caches stay coherent.
    @Override
    public CacheHelper getCoreCacheHelper() {
        // Full cache identity restored: filter cache, fielddata and global-ordinals caches all key
        // off this. Consumers cached beyond the request remain valid because producer() reroutes
        // post-close access to the segment-lifetime shared producer (SharedProducerRegistry).
        return in.getCoreCacheHelper();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return in.getReaderCacheHelper();
    }
}
