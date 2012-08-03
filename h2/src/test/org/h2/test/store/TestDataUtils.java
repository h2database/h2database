/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License, Version
 * 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.test.store;

import org.h2.dev.store.btree.Page;
import org.h2.test.TestBase;

/**
 * Test utility classes.
 */
public class TestDataUtils extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws Exception {
        testPagePos();
    }

    private void testPagePos() {
        int lastCode = 0;
        assertEquals(0, Page.encodeLength(32));
        assertEquals(1, Page.encodeLength(33));
        assertEquals(1, Page.encodeLength(48));
        assertEquals(2, Page.encodeLength(49));
        assertEquals(30, Page.encodeLength(1024 * 1024));
        assertEquals(31, Page.encodeLength(1024 * 1024 + 1));
        for (int i = 1024 * 1024 + 1; i < 100 * 1024 * 1024; i += 1024) {
            int code = Page.encodeLength(i);
            assertEquals(31, code);

        }
        for (int i = 0; i < 1024 * 1024; i++) {
            int code = Page.encodeLength(i);
            assertTrue(code <= 31 && code >= 0);
            assertTrue(code >= lastCode);
            if (code > lastCode) {
                lastCode = code;
            }
            int max = Page.getMaxLength(code);
            assertTrue(max >= i && max >= 32);
        }
    }

}
