package org.h2.test.unit;

import org.h2.test.TestBase;

import static org.h2.util.Bits.LONG_VH_BE;
import static org.h2.util.Bits.LONG_VH_LE;

public class TestByteArrayAsLongArray extends TestBase {

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
        byte[] buff = { 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, (byte) 0x88, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f };
        assertEquals(0x1011121314151617L, LONG_VH_BE.get(buff, 0));
        assertEquals(0x88191a1b1c1d1e1fL, LONG_VH_BE.get(buff, 8));
    }

    public void testLittleEndianGet() {
        byte[] buff = { 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, (byte) 0x8f };
        assertEquals(0x1716151413121110L, LONG_VH_LE.get(buff, 0));
        assertEquals(0x8f1e1d1c1b1a1918L, LONG_VH_LE.get(buff, 8));
    }

    public void testBigEndianSet() {
        byte[] buff = new byte[16];
        LONG_VH_BE.set(buff, 0, 0x1011121314151617L);
        LONG_VH_BE.set(buff, 8, 0x88191a1b1c1d1e1fL);
        assertEquals(new byte[] {0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, (byte) 0x88, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f }, buff);
    }

    public void testLittleEndianSet() {
        byte[] buff = new byte[16];
        LONG_VH_LE.set(buff, 0, 0x1011121314151617L);
        LONG_VH_LE.set(buff, 8, 0x88191a1b1c1d1e1fL);
        assertEquals(new byte[] { 0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11, 0x10, 0x1f, 0x1e, 0x1d, 0x1c, 0x1b, 0x1a, 0x19, (byte) 0x88 }, buff);
    }

}
