/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.writer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.engine.dataformat.NestedScope;
import org.opensearch.index.engine.dataformat.NestedScopeTracker;
import org.opensearch.index.engine.exec.PrimaryTermFieldType;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.VersionFieldMapper;
import org.opensearch.parquet.ParquetDataFormatPlugin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Document input for the Parquet data format.
 *
 * <p>Implements {@link DocumentInput} to collect field-value pairs incrementally during
 * document indexing. Fields are stored as {@link FieldValuePair} objects and later transferred
 * to Arrow vectors by {@link org.opensearch.parquet.vsr.VSRManager#addDocument(ParquetDocumentInput)}.
 *
 * <p>Calling {@link #close()} clears all collected fields and resets the row ID,
 * allowing the instance to be discarded cleanly after use.
 */
public class ParquetDocumentInput implements DocumentInput<List<FieldValuePair>> {

    private static final Logger logger = LogManager.getLogger(ParquetDocumentInput.class);
    private final List<FieldValuePair> collectedFields = new ArrayList<>();
    /**
     * One-value-per-field is enforced per scope: the root document and each nested child
     * (e.g. comments[0], comments[1]) are independent scopes, so the same nested field
     * type may legitimately appear once per child. Keyed by the active {@link NestedScope}
     * instance (root uses a sentinel), with identity-based field-type sets as before.
     */
    private final Map<Object, Set<MappedFieldType>> dedupByScope = new IdentityHashMap<>();
    private static final Object ROOT_SCOPE_KEY = new Object();
    private final NestedScopeTracker scopeTracker = new NestedScopeTracker();
    private long rowId = -1;
    private boolean isClosed = false;

    @Override
    public void addField(MappedFieldType fieldType, Object value) {
        ensureOpen();
        Set<FieldTypeCapabilities.Capability> capabilities = fieldType.getCapabilityMap()
            .getOrDefault(ParquetDataFormatPlugin.PARQUET_DATA_FORMAT, Set.of());
        if (capabilities.isEmpty() && fieldType != PrimaryTermFieldType.INSTANCE) {
            // nothing to support on this format for this field.
            logger.trace("Ignored to add field: {} {}", fieldType.name(), fieldType.getCapabilityMap());
            return;
        }
        NestedScope scope = scopeTracker.current();
        Object scopeKey = scope == null ? ROOT_SCOPE_KEY : scope;
        Set<MappedFieldType> dedup = dedupByScope.computeIfAbsent(scopeKey, k -> Collections.newSetFromMap(new IdentityHashMap<>()));
        if (dedup.add(fieldType) == false) {
            throw new MapperParsingException(
                "Cannot accept multiple values for field: ["
                    + fieldType.name()
                    + "] of type: ["
                    + fieldType.typeName()
                    + "]"
                    + (scope == null ? "." : " within nested element [" + scope.positionalPath() + "].")
            );
        }
        collectedFields.add(new FieldValuePair(fieldType, value, scope));
    }

    @Override
    public void beginChild(String nestedPath) {
        ensureOpen();
        scopeTracker.beginChild(nestedPath);
    }

    @Override
    public void endChild() {
        ensureOpen();
        scopeTracker.endChild();
    }

    @Override
    public void setRowId(String rowIdFieldName, long rowId) {
        ensureOpen();
        this.rowId = rowId;
    }

    @Override
    public List<FieldValuePair> getFinalInput() {
        if (!isClosed) {
            assert rowId >= 0 : "Row ID must be set before calling getFinalInput";
            // assertions for parquet primary
            // TODO: once parquet is supported in secondary mode, this assertion would change
            assert getFieldCount(IdFieldMapper.NAME) == 1;
            assert getFieldCount(SeqNoFieldMapper.NAME) == 1;
            assert getFieldCount(VersionFieldMapper.NAME) == 1;
            assert getFieldCount(SeqNoFieldMapper.PRIMARY_TERM_NAME) == 1;
        }
        return collectedFields;
    }

    @Override
    public long getFieldCount(String fieldName) {
        return collectedFields.stream().filter(fvp -> fvp.getFieldType().name().equals(fieldName)).count();
    }

    @Override
    public void close() {
        isClosed = true;
        collectedFields.clear();
        dedupByScope.clear();
        scopeTracker.reset();
        rowId = -1;
    }

    private void ensureOpen() {
        if (isClosed) {
            throw new IllegalStateException("Cannot add more fields to a frozen document input");
        }
    }

    /**
     * Returns the row ID assigned to this document.
     *
     * @return the row ID, or -1 if not set
     */
    public long getRowId() {
        return rowId;
    }

    /**
     * Returns every nested child scope of this document in parse (depth-first) order —
     * including scopes whose fields were all filtered out by capability checks, so that
     * list element positions stay aligned with the Lucene block ordinals even for
     * children that contribute no Parquet columns.
     *
     * @return all nested scopes in parse order; empty for flat documents
     */
    public List<NestedScope> getNestedScopes() {
        return scopeTracker.scopesInParseOrder();
    }
}
