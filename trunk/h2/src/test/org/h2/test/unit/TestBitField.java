/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.util.BitSet;
import java.util.Random;

import org.h2.test.TestBase;
import org.h2.util.BitField;

/**
 * A unit test for bit fields.
 */
public class TestBitField extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String[] a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() {
        testNextClearBit();
        testNextSetBit();
        testByteOperations();
        testRandom();
        testGetSet();
        testRandomSetRange();
    }

    private void testNextSetBit() {
        Random random = new Random(1);
        BitField field = new BitField();
        for (int i = 0; i < 100000; i++) {
            int a = random.nextInt(120);
            int b = a + 1 + random.nextInt(200);
            field.set(a);
            field.set(b);
            assertEquals(b, field.nextSetBit(a + 1));
            field.clear(a);
            field.clear(b);
        }
    }

    private void testNextClearBit() {
        BitSet set = new BitSet();
        BitField field = new BitField();
        set.set(0, 640);
        field.setRange(0, 640, true);
        assertEquals(set.nextClearBit(0), field.nextClearBit(0));

        Random random = new Random(1);
        field = new BitField();
        field.setRange(0, 500, true);
        for (int i = 0; i < 100000; i++) {
            int a = random.nextInt(120);
            int b = a + 1 + random.nextInt(200);
            field.clear(a);
            field.clear(b);
            assertEquals(b, field.nextClearBit(a + 1));
            field.set(a);
            field.set(b);
        }
    }

    private void testByteOperations() {
        BitField used = new BitField();
        testSetFast(used, false);
        testSetFast(used, true);
    }

    private void testSetFast(BitField used, boolean init) {
        int len = 10000;
        Random random = new Random(1);
        for (int i = 0, x = 0; i < len / 8; i++) {
            int mask = random.nextInt() & 255;
            if (init) {
                assertEquals(mask, used.getByte(x));
                x += 8;
                // for (int j = 0; j < 8; j++, x++) {
                //    if (used.get(x) != ((mask & (1 << j)) != 0)) {
                //        throw Message.getInternalError(
                //          "Redo failure, block: " + x +
                //          " expected in-use bit: " + used.get(x));
                //    }
                // }
            } else {
                used.setByte(x, mask);
                x += 8;
                // for (int j = 0; j < 8; j++, x++) {
                //    if ((mask & (1 << j)) != 0) {
                //        used.set(x);
                //    }
                // }
            }
        }
    }

    private void testRandom() {
        BitField bits = new BitField();
        BitSet set = new BitSet();
        int max = 300;
        int count = 100000;
        Random random = new Random(1);
        for (int i = 0; i < count; i++) {
            int idx = random.nextInt(max);
            if (random.nextBoolean()) {
                if (random.nextBoolean()) {
                    bits.set(idx);
                    set.set(idx);
                } else {
                    bits.clear(idx);
                    set.clear(idx);
                }
            } else {
                assertEquals(bits.get(idx), set.get(idx));
                assertEquals(bits.nextClearBit(idx), set.nextClearBit(idx));
                assertEquals(bits.nextSetBit(idx), set.nextSetBit(idx));
            }
        }
    }

    private void testGetSet() {
        BitField bits = new BitField();
        for (int i = 0; i < 10000; i++) {
            bits.set(i);
            if (!bits.get(i)) {
                fail("not set: " + i);
            }
            if (bits.get(i + 1)) {
                fail("set: " + i);
            }
        }
        for (int i = 0; i < 10000; i++) {
            if (!bits.get(i)) {
                fail("not set: " + i);
            }
        }
        for (int i = 0; i < 1000; i++) {
            int k = bits.nextClearBit(0);
            if (k != 10000) {
                fail("" + k);
            }
        }
    }
    
    private void testRandomSetRange(){
    	BitField bits = new BitField();
    	BitSet set = new BitSet();
    	Random random = new Random(1);	
    	int maxoff = 500;
    	int maxlen = 500;
    	int total = maxoff+maxlen;
    	int count = 10000;
    	for (int i = 0; i < count; i++) {
    		int offset = random.nextInt(maxoff);
    		int len = random.nextInt(maxlen);
    		boolean val = random.nextBoolean();
    		set.set(offset,offset+len,val);
    		bits.setRange(offset,len,val);
    		for(int j=0; j<total; j++){
    			assertEquals(bits.get(j), set.get(j));
    		}
        }
    }
}
