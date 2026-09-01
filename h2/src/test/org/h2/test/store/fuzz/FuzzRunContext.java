/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store.fuzz;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import org.h2.mvstore.Cursor;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;

/**
 * Mutable state shared across all operations in a single fuzz run.
 */
public final class FuzzRunContext {

    /** A cursor kept open across other operations, paired with its expected snapshot. */
    public static final class OpenCursor {
        public final Cursor<Integer, String> cursor;
        public final Iterator<Map.Entry<Integer, String>> expected;
        public final int createdAtOp;

        public OpenCursor(Cursor<Integer, String> cursor,
                Iterator<Map.Entry<Integer, String>> expected, int createdAtOp) {
            this.cursor = cursor;
            this.expected = expected;
            this.createdAtOp = createdAtOp;
        }
    }

    public MVStore store;
    public MVMap<Integer, String> map;
    public TreeMap<Integer, String> shadow;
    public TreeMap<Integer, String> committedShadow;
    public Deque<OpenCursor> cursors;
    public String fileName;
    public FuzzConfig config;
    /** Current operation index, used in error messages. */
    public int opIndex;

    public FuzzRunContext(MVStore store, MVMap<Integer, String> map,
            String fileName, FuzzConfig config) {
        this.store = store;
        this.map = map;
        this.fileName = fileName;
        this.config = config;
        this.shadow = new TreeMap<>();
        this.committedShadow = new TreeMap<>();
        this.cursors = new ArrayDeque<>();
        this.opIndex = 0;
    }

    public void assertEquals(String message, Object expected, Object actual) {
        if (!Objects.equals(expected, actual)) {
            throw new AssertionError(message + " expected: " + expected + " actual: " + actual);
        }
    }

    public void assertTrue(String message, boolean condition) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    public void assertFalse(String message, boolean condition) {
        if (condition) {
            throw new AssertionError(message);
        }
    }

    /** Spot-check get(lastKey) and size after each operation. */
    public void spotCheck(int lastKey) {
        assertEquals("get(" + lastKey + ")", shadow.get(lastKey), map.get(lastKey));
        assertEquals("size", shadow.size(), map.size());
    }

    /** Full cursor scan of the entire map vs shadow. */
    public void fullVerify() {
        assertEquals("full size", shadow.size(), map.size());
        Iterator<Map.Entry<Integer, String>> expected = shadow.entrySet().iterator();
        Cursor<Integer, String> c = map.cursor(null);
        while (expected.hasNext()) {
            Map.Entry<Integer, String> e = expected.next();
            assertTrue("map exhausted early, expected key " + e.getKey(), c.hasNext());
            Integer key = c.next();
            assertEquals("full key", e.getKey(), key);
            assertEquals("full value for key " + key, e.getValue(), c.getValue());
        }
        assertFalse("map has extra entries", c.hasNext());
    }

    /** Advance a cursor one step and assert key/value match. Returns false when cursor exhausted. */
    public boolean advanceCursorStep(OpenCursor oc) {
        if (!oc.expected.hasNext()) {
            assertFalse("cursor from op " + oc.createdAtOp + " not exhausted",
                    oc.cursor.hasNext());
            cursors.remove(oc);
            return false;
        }
        Map.Entry<Integer, String> e = oc.expected.next();
        assertTrue("cursor from op " + oc.createdAtOp + " exhausted early, expected key " + e.getKey(),
                oc.cursor.hasNext());
        Integer key = oc.cursor.next();
        assertEquals("cursor from op " + oc.createdAtOp + " key", e.getKey(), key);
        assertEquals("cursor from op " + oc.createdAtOp + " value for key " + key,
                e.getValue(), oc.cursor.getValue());
        return true;
    }

    /** Drain a cursor fully. */
    public void drainCursor(OpenCursor oc) {
        while (oc.expected.hasNext()) {
            Map.Entry<Integer, String> e = oc.expected.next();
            assertTrue("cursor from op " + oc.createdAtOp + " exhausted early, expected key " + e.getKey(),
                    oc.cursor.hasNext());
            Integer key = oc.cursor.next();
            assertEquals("cursor from op " + oc.createdAtOp + " key", e.getKey(), key);
            assertEquals("cursor from op " + oc.createdAtOp + " value for key " + key,
                    e.getValue(), oc.cursor.getValue());
        }
        assertFalse("cursor from op " + oc.createdAtOp + " not exhausted",
                oc.cursor.hasNext());
    }
}
