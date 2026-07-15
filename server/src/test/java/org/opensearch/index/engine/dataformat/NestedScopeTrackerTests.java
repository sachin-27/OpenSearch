/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for {@link NestedScopeTracker} and {@link NestedScope}.
 *
 * <p>The positional paths asserted here mirror the running example from the nested
 * document design: a document with {@code comments[].replies[]} plus a sibling
 * {@code reviews[]} array.
 */
public class NestedScopeTrackerTests extends OpenSearchTestCase {

    public void testSingleLevelSiblingsGetSequentialOrdinals() {
        NestedScopeTracker tracker = new NestedScopeTracker();

        NestedScope first = tracker.beginChild("comments");
        assertEquals(0, first.ordinal());
        assertEquals("comments[0]", first.positionalPath());
        assertNull(first.parent());
        tracker.endChild();

        NestedScope second = tracker.beginChild("comments");
        assertEquals(1, second.ordinal());
        assertEquals("comments[1]", second.positionalPath());
        tracker.endChild();

        assertFalse(tracker.inChild());
        assertNull(tracker.current());
    }

    public void testMultiLevelNestingResolvesParentAndPath() {
        NestedScopeTracker tracker = new NestedScopeTracker();

        NestedScope comment0 = tracker.beginChild("comments");
        NestedScope reply0 = tracker.beginChild("comments.replies");
        assertEquals("comments[0].replies[0]", reply0.positionalPath());
        assertSame(comment0, reply0.parent());
        assertEquals(2, reply0.depth());
        tracker.endChild();

        NestedScope reply1 = tracker.beginChild("comments.replies");
        assertEquals("comments[0].replies[1]", reply1.positionalPath());
        tracker.endChild();
        tracker.endChild(); // comments[0]

        // Second comment: reply ordinals restart because the parent scope changed.
        NestedScope comment1 = tracker.beginChild("comments");
        assertEquals("comments[1]", comment1.positionalPath());
        NestedScope reply0OfComment1 = tracker.beginChild("comments.replies");
        assertEquals("comments[1].replies[0]", reply0OfComment1.positionalPath());
        tracker.endChild();
        tracker.endChild();
    }

    public void testSiblingNestedArraysTrackIndependentOrdinals() {
        NestedScopeTracker tracker = new NestedScopeTracker();

        tracker.beginChild("comments");
        tracker.endChild();
        tracker.beginChild("comments");
        tracker.endChild();

        NestedScope review0 = tracker.beginChild("reviews");
        assertEquals("reviews[0]", review0.positionalPath());
        assertEquals(0, review0.ordinal());
        tracker.endChild();
    }

    public void testCurrentReflectsActiveScope() {
        NestedScopeTracker tracker = new NestedScopeTracker();
        assertNull(tracker.current());

        NestedScope comment = tracker.beginChild("comments");
        assertSame(comment, tracker.current());

        NestedScope reply = tracker.beginChild("comments.replies");
        assertSame(reply, tracker.current());

        tracker.endChild();
        assertSame(comment, tracker.current());

        tracker.endChild();
        assertNull(tracker.current());
    }

    public void testEndChildWithoutBeginThrows() {
        NestedScopeTracker tracker = new NestedScopeTracker();
        expectThrows(IllegalStateException.class, tracker::endChild);
    }

    public void testScopesInParseOrder() {
        NestedScopeTracker tracker = new NestedScopeTracker();
        tracker.beginChild("comments");
        tracker.beginChild("comments.replies");
        tracker.endChild();
        tracker.endChild();
        tracker.beginChild("reviews");
        tracker.endChild();

        var scopes = tracker.scopesInParseOrder();
        assertEquals(3, scopes.size());
        assertEquals("comments[0]", scopes.get(0).positionalPath());
        assertEquals("comments[0].replies[0]", scopes.get(1).positionalPath());
        assertEquals("reviews[0]", scopes.get(2).positionalPath());
    }

    public void testResetClearsAllState() {
        NestedScopeTracker tracker = new NestedScopeTracker();
        tracker.beginChild("comments");
        tracker.reset();

        assertFalse(tracker.inChild());
        assertTrue(tracker.scopesInParseOrder().isEmpty());
        // Ordinals restart after reset (fresh document).
        NestedScope scope = tracker.beginChild("comments");
        assertEquals(0, scope.ordinal());
    }
}
