/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License, Version
 * 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.test.store;

import org.h2.dev.store.btree.DataUtils;
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
        testCheckValue();
        testPagePos();
        testEncodeLength();
    }

    private void testCheckValue() {
        // 0 xor 0 = 0
        assertEquals(0, DataUtils.getCheckValue(0));
        // 1111... xor 1111... = 0
        assertEquals(0, DataUtils.getCheckValue(-1));
        // 0 xor 1111... = 1111...
        assertEquals((short) -1, DataUtils.getCheckValue(-1 >>> 16));
        // 1111... xor 0 = 1111...
        assertEquals((short) -1, DataUtils.getCheckValue(-1 << 16));
        // 0 xor 1000... = 1000...
        assertEquals((short) (1 << 15), DataUtils.getCheckValue(1 << 15));
        // 1000... xor 0 = 1000...
        assertEquals((short) (1 << 15), DataUtils.getCheckValue(1 << 31));
    }

    private void testPagePos() {
        for (int chunkId = 0; chunkId < 67000000; chunkId += 670000) {
            for (long offset = 0; offset < Integer.MAX_VALUE; offset += Integer.MAX_VALUE / 100) {
                for (int length = 0; length < 2000000; length += 200000) {
                    long pos = DataUtils.getPos(chunkId, (int) offset, length);
                    assertEquals(chunkId, DataUtils.getChunkId(pos));
                    assertEquals(offset, DataUtils.getOffset(pos));
                    assertTrue(DataUtils.getMaxLength(pos) >= length);
                }
            }
        }
    }

    private void testEncodeLength() {
        int lastCode = 0;
        assertEquals(0, DataUtils.encodeLength(32));
        assertEquals(1, DataUtils.encodeLength(33));
        assertEquals(1, DataUtils.encodeLength(48));
        assertEquals(2, DataUtils.encodeLength(49));
        assertEquals(30, DataUtils.encodeLength(1024 * 1024));
        assertEquals(31, DataUtils.encodeLength(1024 * 1024 + 1));
        for (int i = 1024 * 1024 + 1; i < 100 * 1024 * 1024; i += 1024) {
            int code = DataUtils.encodeLength(i);
            assertEquals(31, code);
        }
        for (int i = 0; i < 2 * 1024 * 1024; i++) {
            int code = DataUtils.encodeLength(i);
            assertTrue(code <= 31 && code >= 0);
            assertTrue(code >= lastCode);
            if (code > lastCode) {
                lastCode = code;
            }
            int max = DataUtils.getMaxLength(code);
            assertTrue(max >= i && max >= 32);
        }
    }

}
