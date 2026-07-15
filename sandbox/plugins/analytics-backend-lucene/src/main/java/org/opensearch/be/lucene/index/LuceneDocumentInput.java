/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DocValuesType;
import org.opensearch.be.lucene.LuceneFieldFactory;
import org.opensearch.be.lucene.LuceneFieldFactoryRegistry;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.engine.dataformat.NestedScope;
import org.opensearch.index.engine.dataformat.NestedScopeTracker;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NestedPathFieldMapper;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Lucene-specific {@link DocumentInput} that builds a <em>block</em> of Lucene
 * {@link Document}s for one logical document: one document per nested child (in
 * block-join layout order) followed by the root document last.
 *
 * <p>For documents without nested children the block has exactly one document and
 * behaves identically to the previous single-document implementation.
 *
 * <h2>Block layout</h2>
 * Child documents are emitted in {@code endChild} order, which yields the same
 * deepest-first layout vanilla OpenSearch produces for nested mappings (an element's
 * descendants precede the element itself; the root is always last). Example for
 * {@code {comments: [{replies: [r0, r1]}, c1]}}: {@code [r0, r1, comments[0], comments[1], root]}.
 *
 * <h2>Row-ID stamping (Scheme C: sequential docId-space row-ids)</h2>
 * Two modes exist, chosen per index at construction time ({@code nestedBlocks}):
 * <ul>
 *   <li><b>Plain</b> (index has no nested mappings): the single document is stamped with
 *       the raw row id by this input — today's behavior, byte-for-byte. Child scopes are
 *       rejected.</li>
 *   <li><b>Nested blocks</b> (index has nested mappings): this input does <em>not</em>
 *       stamp row ids. Every physical document — children and root alike — receives a
 *       plain sequential docId-space row id ({@code __row_id__ == docId}, invariant I1),
 *       stamped by {@link LuceneWriter#addDoc} which owns the global doc counter. The
 *       block↔Parquet-row correspondence is positional (Kth parent == Kth Parquet row)
 *       and is re-derived at merge time from the block structure, per the design doc's
 *       §5.4 derivational scheme.</li>
 * </ul>
 *
 * <p>Child documents carry a {@link NestedPathFieldMapper#NAME} field with their nested
 * path, mirroring vanilla OpenSearch. This is what identifies child docs (and thereby
 * parents, by complement) for block-structure recovery at merge time and for per-path
 * bitsets on the read path.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneDocumentInput implements DocumentInput<List<Document>> {

    private final Document rootDocument;
    /** Finished child documents in emission (endChild) order — the block layout order. */
    private final List<Document> childDocuments = new ArrayList<>();
    /** In-flight documents for currently open child scopes. */
    private final Map<NestedScope, Document> openScopeDocuments = new IdentityHashMap<>();
    private final NestedScopeTracker scopeTracker = new NestedScopeTracker();
    private final LuceneFieldFactoryRegistry fieldFactoryRegistry;
    private final boolean nestedBlocks;
    private long rowId = -1L;

    /**
     * Creates a new LuceneDocumentInput with the default field factory registry and
     * plain row-id keys (no nested support).
     */
    public LuceneDocumentInput() {
        this(new LuceneFieldFactoryRegistry());
    }

    /**
     * Creates a new LuceneDocumentInput with a custom field factory registry and
     * plain row-id keys (no nested support).
     *
     * @param fieldFactoryRegistry the registry to use for field creation
     */
    public LuceneDocumentInput(LuceneFieldFactoryRegistry fieldFactoryRegistry) {
        this(fieldFactoryRegistry, false);
    }

    /**
     * Creates a new LuceneDocumentInput.
     *
     * @param fieldFactoryRegistry the registry to use for field creation
     * @param nestedBlocks whether this index supports nested blocks (indices with nested
     *                     mappings). In this mode row-id stamping is deferred to
     *                     {@link LuceneWriter#addDoc}, which assigns sequential
     *                     docId-space row ids to every physical document.
     */
    public LuceneDocumentInput(LuceneFieldFactoryRegistry fieldFactoryRegistry, boolean nestedBlocks) {
        this.rootDocument = new Document();
        this.fieldFactoryRegistry = fieldFactoryRegistry;
        this.nestedBlocks = nestedBlocks;
    }

    /**
     * Returns the block of Lucene documents for this logical document: child documents
     * in block-join layout order followed by the root document last. Single-element
     * for documents without nested children.
     *
     * @return the document block, root last
     */
    @Override
    public List<Document> getFinalInput() {
        if (openScopeDocuments.isEmpty() == false) {
            throw new IllegalStateException(
                "Cannot finalize block: " + openScopeDocuments.size() + " child scope(s) still open (missing endChild?)"
            );
        }
        List<Document> block = new ArrayList<>(childDocuments.size() + 1);
        block.addAll(childDocuments);
        block.add(rootDocument);
        return block;
    }

    @Override
    public void beginChild(String nestedPath) {
        if (nestedBlocks == false) {
            throw new IllegalStateException(
                "Nested child documents require nested-blocks mode, but this input was created in plain row-id mode "
                    + "(index has no nested mappings?) — nested path: ["
                    + nestedPath
                    + "]"
            );
        }
        NestedScope scope = scopeTracker.beginChild(nestedPath);
        openScopeDocuments.put(scope, new Document());
    }

    @Override
    public void endChild() {
        NestedScope finished = scopeTracker.current();
        if (finished == null) {
            throw new IllegalStateException("endChild() called without a matching beginChild()");
        }
        scopeTracker.endChild();
        Document childDoc = openScopeDocuments.remove(finished);
        // Mirror vanilla OpenSearch: child docs carry their nested path so the read
        // path can build per-path bitsets (block-join filters).
        childDoc.add(new StringField(NestedPathFieldMapper.NAME, finished.nestedPath(), Field.Store.NO));
        childDocuments.add(childDoc);
    }

    /**
     * Adds a field to the document of the currently active child scope, or to the root
     * document when no child scope is open. Field creation is delegated to the
     * {@link LuceneFieldFactory} registered for the field's type.
     *
     * <p>The field is accepted only if OWNING_FORMAT owns at least one capability
     * for this field according to {@link MappedFieldType#getCapabilityMap()}. Fields with
     * an empty capability map (no format declared support) and fields owned by other
     * formats are silently skipped, mirroring the per-format self-filtering used by
     * {@code ParquetDocumentInput}.
     *
     * @param fieldType the OpenSearch mapped field type
     * @param value     the field value
     */
    @Override
    public void addField(MappedFieldType fieldType, Object value) {
        Set<FieldTypeCapabilities.Capability> capabilities = fieldType.getCapabilityMap().getOrDefault(LucenePlugin.DATA_FORMAT, Set.of());
        if (capabilities.isEmpty()) {
            // nothing to support on this format for this field.
            return;
        }
        if (value == null) {
            throw new IllegalArgumentException(
                "Field value must not be null for: " + fieldType.name() + " of type: " + fieldType.typeName()
            );
        }
        LuceneFieldFactory factory = fieldFactory(fieldType);
        if (factory == null) {
            // capabilities need to be supported but actual implementation to support lucene field type does not exist.
            throw new IllegalArgumentException(
                "Field: " + fieldType.name() + " requests capability: " + capabilities + " but does not have any factory to support"
            );
        }
        FieldType luceneFieldType = getFieldType(fieldType, capabilities);
        factory.addField(targetDocument(), fieldType, value, luceneFieldType);
    }

    /** Returns the document field values are currently routed to: the open child scope's, or the root. */
    private Document targetDocument() {
        NestedScope current = scopeTracker.current();
        return current == null ? rootDocument : openScopeDocuments.get(current);
    }

    private static FieldType getFieldType(MappedFieldType fieldType, Set<FieldTypeCapabilities.Capability> capabilities) {
        FieldType luceneFieldType = null;
        if (fieldType.getTextSearchInfo() != null && fieldType.getTextSearchInfo().getLuceneFieldType() != null) {
            luceneFieldType = new FieldType(fieldType.getTextSearchInfo().getLuceneFieldType());
            if (!capabilities.contains(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE)) {
                // Disable doc values even if core mappers have set it on lucene fields
                // once we introduce more frontend params, we can remove this check.
                luceneFieldType.setDocValuesType(DocValuesType.NONE);
            }
            luceneFieldType.setStored(false);
            luceneFieldType.setOmitNorms(true);
        }
        return luceneFieldType;
    }

    private LuceneFieldFactory fieldFactory(MappedFieldType fieldType) {
        if (fieldType == null) {
            throw new IllegalArgumentException("Field type and value must not be null");
        }
        return fieldFactoryRegistry.get(fieldType.typeName());
    }

    /**
     * Records the logical row ID for this block.
     *
     * <p>Plain mode: the root document is stamped with the raw row id (previous behavior,
     * byte-identical — logical row == physical doc for flat indices).
     *
     * <p>Nested-blocks mode: no stamping happens here. The logical rowId from the
     * coordinator is recorded for validation only; physical sequential row ids
     * ({@code __row_id__ == docId}) are stamped on every document of the block by
     * {@link LuceneWriter#addDoc}, which owns the global physical doc counter that
     * this input cannot know at parse time.
     *
     * @param rowIdFieldName the name of the row ID field
     * @param rowId          the logical row ID (0-based sequential within the writer)
     */
    @Override
    public void setRowId(String rowIdFieldName, long rowId) {
        if (openScopeDocuments.isEmpty() == false) {
            throw new IllegalStateException("setRowId must not be called while a child scope is open");
        }
        this.rowId = rowId;
        if (nestedBlocks == false) {
            assert childDocuments.isEmpty() : "plain row-id mode must never see child documents";
            rootDocument.add(new SortedNumericDocValuesField(rowIdFieldName, rowId));
        }
    }

    /** Returns the row ID assigned via {@link #setRowId}, or {@code -1} if none. */
    public long getRowId() {
        return rowId;
    }

    /** Returns the number of documents this block will contain (children + root). */
    public int blockSize() {
        return childDocuments.size() + 1;
    }

    /** Returns {@code true} if this logical document has nested child documents. */
    public boolean hasChildren() {
        return childDocuments.isEmpty() == false;
    }

    @Override
    public long getFieldCount(String fieldName) {
        return rootDocument.getFields(fieldName).length;
    }

    /** No-op — this document input holds no closeable resources. */
    @Override
    public void close() {
        // No resources to release
    }
}
