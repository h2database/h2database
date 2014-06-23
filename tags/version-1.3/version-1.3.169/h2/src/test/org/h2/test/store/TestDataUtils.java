/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License, Version
 * 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.test.store;

import java.nio.ByteBuffer;
import java.util.HashMap;
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
        testMap();
        testVarIntVarLong();
        testCheckValue();
        testPagePos();
        testEncodeLength();
    }

    private void testMap() {
        StringBuilder buff = new StringBuilder();
        DataUtils.appendMap(buff,  "", "");
        DataUtils.appendMap(buff,  "a", "1");
        DataUtils.appendMap(buff,  "b", ",");
        DataUtils.appendMap(buff,  "c", "1,2");
        DataUtils.appendMap(buff,  "d", "\"test\"");
        assertEquals(":,a:1,b:\",\",c:\"1,2\",d:\"\\\"test\\\"\"", buff.toString());

        HashMap<String, String> m = DataUtils.parseMap(buff.toString());
        assertEquals(5, m.size());
        assertEquals("", m.get(""));
        assertEquals("1", m.get("a"));
        assertEquals(",", m.get("b"));
        assertEquals("1,2", m.get("c"));
        assertEquals("\"test\"", m.get("d"));
    }

    private void testVarIntVarLong() {
        ByteBuffer buff = ByteBuffer.allocate(100);
        for (long x = 0; x < 1000; x++) {
            testVarIntVarLong(buff, x);
            testVarIntVarLong(buff, -x);
        }
        for (long x = Long.MIN_VALUE, i = 0; i < 1000; x++, i++) {
            testVarIntVarLong(buff, x);
        }
        for (long x = Long.MAX_VALUE, i = 0; i < 1000; x--, i++) {
            testVarIntVarLong(buff, x);
        }
        for (int shift = 0; shift < 64; shift++) {
            for (long x = 250; x < 260; x++) {
                testVarIntVarLong(buff, x << shift);
                testVarIntVarLong(buff, -(x << shift));
            }
        }
        // invalid varInt / varLong
        // should work, but not read far too much
        for (int i = 0; i < 50; i++) {
            buff.put((byte) 255);
        }
        buff.flip();
        assertEquals(-1, DataUtils.readVarInt(buff));
        assertEquals(5, buff.position());
        buff.rewind();
        assertEquals(-1, DataUtils.readVarLong(buff));
        assertEquals(10, buff.position());
    }

    private void testVarIntVarLong(ByteBuffer buff, long x) {
        int len;

        DataUtils.writeVarLong(buff, x);
        len = buff.position();
        buff.flip();
        long y = DataUtils.readVarLong(buff);
        assertEquals(y, x);
        assertEquals(len, buff.position());
        assertEquals(len, DataUtils.getVarLongLen(x));
        buff.clear();

        int intX = (int) x;
        DataUtils.writeVarInt(buff, intX);
        len = buff.position();
        buff.flip();
        int intY = DataUtils.readVarInt(buff);
        assertEquals(intY, intX);
        assertEquals(len, buff.position());
        assertEquals(len, DataUtils.getVarIntLen(intX));
        buff.clear();
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
        assertEquals(0, DataUtils.PAGE_TYPE_LEAF);
        assertEquals(1, DataUtils.PAGE_TYPE_NODE);
        for (int i = 0; i < 67000000; i++) {
            long pos = DataUtils.getPagePos(i, 3, 128, 1);
            assertEquals(i, DataUtils.getPageChunkId(pos));
            assertEquals(3, DataUtils.getPageOffset(pos));
            assertEquals(128, DataUtils.getPageMaxLength(pos));
            assertEquals(1, DataUtils.getPageType(pos));
        }
        for (int type = 0; type <= 1; type++) {
            for (int chunkId = 0; chunkId < 67000000; chunkId += 670000) {
                for (long offset = 0; offset < Integer.MAX_VALUE; offset += Integer.MAX_VALUE / 100) {
                    for (int length = 0; length < 2000000; length += 200000) {
                        long pos = DataUtils.getPagePos(chunkId, (int) offset, length, type);
                        assertEquals(chunkId, DataUtils.getPageChunkId(pos));
                        assertEquals(offset, DataUtils.getPageOffset(pos));
                        assertTrue(DataUtils.getPageMaxLength(pos) >= length);
                        assertTrue(DataUtils.getPageType(pos) == type);
                    }
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
            int max = DataUtils.getPageMaxLength(code << 1);
            assertTrue(max >= i && max >= 32);
        }
    }

}
