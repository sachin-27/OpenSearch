/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Objects;

/**
 * Immutable identity of one child document of a {@code nested} field within a single
 * logical document — the "compartment address" that qualifies buffered field values.
 *
 * <p>A scope is defined by its nested path (the full mapper path, e.g. {@code comments}
 * or {@code comments.replies}), its ordinal among siblings of the same path under the
 * same parent, and a reference to the enclosing parent scope ({@code null} when the
 * parent is the root document). For example, in
 * <pre>{@code { "comments": [ { "replies": [ {..}, {..} ] } ] }}</pre>
 * the second reply's scope renders as {@code comments[0].replies[1]}.
 *
 * <p>The ordinal assigned here at parse time is the single source of truth for child
 * identity across data formats: it must equal the child's position in the Parquet
 * {@code LIST<STRUCT>} column and the child's intra-block ordinal in the Lucene
 * block-join layout.
 *
 * <p>Instances are created only by {@link NestedScopeTracker}; all values buffered for
 * the same child share the same instance, so identity comparison is sufficient within
 * one document parse.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class NestedScope {

    private final NestedScope parent;
    private final String nestedPath;
    private final int ordinal;

    NestedScope(NestedScope parent, String nestedPath, int ordinal) {
        this.parent = parent;
        this.nestedPath = Objects.requireNonNull(nestedPath, "nestedPath must not be null");
        if (ordinal < 0) {
            throw new IllegalArgumentException("ordinal must be >= 0 but was " + ordinal);
        }
        this.ordinal = ordinal;
    }

    /** Returns the enclosing parent scope, or {@code null} if the parent is the root document. */
    public NestedScope parent() {
        return parent;
    }

    /** Returns the full mapper path of the nested field, e.g. {@code comments.replies}. */
    public String nestedPath() {
        return nestedPath;
    }

    /** Returns this child's position among siblings of the same nested path under the same parent. */
    public int ordinal() {
        return ordinal;
    }

    /** Returns the nesting depth: 1 for a top-level nested child, 2 for a child-of-child, etc. */
    public int depth() {
        int d = 1;
        for (NestedScope p = parent; p != null; p = p.parent) {
            d++;
        }
        return d;
    }

    /**
     * Renders the fully-qualified positional path, e.g. {@code comments[0].replies[1]}.
     * Each level shows only its own path segment (the suffix relative to the parent's path).
     */
    public String positionalPath() {
        String segment = nestedPath;
        if (parent != null && nestedPath.startsWith(parent.nestedPath() + ".")) {
            segment = nestedPath.substring(parent.nestedPath().length() + 1);
        }
        String rendered = segment + "[" + ordinal + "]";
        return parent == null ? rendered : parent.positionalPath() + "." + rendered;
    }

    @Override
    public String toString() {
        return "NestedScope[" + positionalPath() + "]";
    }
}
