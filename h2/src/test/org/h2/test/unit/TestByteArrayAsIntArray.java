package org.h2.test.unit;

import org.h2.test.TestBase;

import static org.h2.util.Bits.INT_VH_BE;
import static org.h2.util.Bits.INT_VH_LE;

public class TestByteArrayAsIntArray extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().testFromMain();
    }

    @Override
    public void test() throws Exception {
        testBigEndianGet();
        testLittleEndianGet();
        testBigEndianSet();
        testLittleEndianSet();
    }

    public void testBigEndianGet() {
        byte[] buff = { 0x10, 0x11, 0x12, 0x13, (byte) 0x84, 0x15, 0x16, 0x17 };
        assertEquals(0x10111213, INT_VH_BE.get(buff, 0));
        assertEquals(0x84151617, INT_VH_BE.get(buff, 4));
    }

    public void testLittleEndianGet() {
        byte[] buff = { 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, (byte) 0x87 };
        assertEquals(0x13121110, INT_VH_LE.get(buff, 0));
        assertEquals(0x87161514, INT_VH_LE.get(buff, 4));
    }

    public void testBigEndianSet() {
        byte[] buff = new byte[8];
        INT_VH_BE.set(buff, 0, 0x10111213);
        INT_VH_BE.set(buff, 4, 0x84151617);
        assertEquals(new byte[] {0x10, 0x11, 0x12, 0x13, (byte) 0x84, 0x15, 0x16, 0x17}, buff);
    }

    public void testLittleEndianSet() {
        byte[] buff = new byte[8];
        INT_VH_LE.set(buff, 0, 0x10111213);
        INT_VH_LE.set(buff, 4, 0x84151617);
        assertEquals(new byte[] {0x13, 0x12, 0x11, 0x10, 0x17, 0x16, 0x15, (byte) 0x84}, buff);
    }

}
