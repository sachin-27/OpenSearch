/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Assigns {@link NestedScope} identities during a single document parse.
 *
 * <p>Maintains the current child-scope stack driven by
 * {@link DocumentInput#beginChild(String)} / {@link DocumentInput#endChild()} and
 * hands out sibling ordinals per (parent scope, nested path). Because every
 * {@link DocumentInput} implementation for a document must observe the same parse
 * traversal, the ordinals produced here are identical across data formats — this
 * class is the single source of truth for child identity.
 *
 * <p>Not thread-safe; a tracker belongs to exactly one document parse.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class NestedScopeTracker {

    private final Deque<NestedScope> stack = new ArrayDeque<>();
    /** Next sibling ordinal per "parentPositionalPath|nestedPath" key. Root parent uses "". */
    private final Map<String, Integer> nextOrdinal = new HashMap<>();
    private final List<NestedScope> allScopes = new ArrayList<>();

    /**
     * Enters a new child scope for the given nested path, assigning the next sibling
     * ordinal under the current scope (or the root if no scope is active).
     *
     * @param nestedPath the full mapper path of the nested field, e.g. {@code comments.replies}
     * @return the newly entered scope
     */
    public NestedScope beginChild(String nestedPath) {
        NestedScope parent = stack.peek();
        String key = (parent == null ? "" : parent.positionalPath()) + "|" + nestedPath;
        int ordinal = nextOrdinal.merge(key, 1, Integer::sum) - 1;
        NestedScope scope = new NestedScope(parent, nestedPath, ordinal);
        stack.push(scope);
        allScopes.add(scope);
        return scope;
    }

    /**
     * Leaves the current child scope, restoring the enclosing scope (or the root).
     *
     * @throws IllegalStateException if no child scope is active
     */
    public void endChild() {
        if (stack.isEmpty()) {
            throw new IllegalStateException("endChild() called without a matching beginChild()");
        }
        stack.pop();
    }

    /** Returns the currently active child scope, or {@code null} when at the root document. */
    public NestedScope current() {
        return stack.peek();
    }

    /** Returns {@code true} while a child scope is active. */
    public boolean inChild() {
        return stack.isEmpty() == false;
    }

    /** Returns all scopes entered so far, in parse (depth-first) order. */
    public List<NestedScope> scopesInParseOrder() {
        return List.copyOf(allScopes);
    }

    /** Resets all state so the tracker can be reused for a new document. */
    public void reset() {
        stack.clear();
        nextOrdinal.clear();
        allScopes.clear();
    }
}
