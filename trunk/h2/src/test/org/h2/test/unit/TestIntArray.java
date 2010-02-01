/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.util.Arrays;
import java.util.Random;
import org.h2.test.TestBase;
import org.h2.util.IntArray;

/**
 * Tests the IntArray class.
 */
public class TestIntArray extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() {
        testInit();
        testRandom();
        testRemoveRange();
    }

    private void testRemoveRange() {
        IntArray array = new IntArray(new int[] {1, 2, 3, 4, 5});
        array.removeRange(1, 3);
        assertEquals(3, array.size());
        assertEquals(1, array.get(0));
        assertEquals(4, array.get(1));
        assertEquals(5, array.get(2));
    }

    private void testInit() {
        IntArray array = new IntArray(new int[0]);
        array.add(10);
    }

    private void testRandom() {
        IntArray array = new IntArray();
        int[] test = new int[0];
        Random random = new Random(1);
        for (int i = 0; i < 10000; i++) {
            int idx = test.length == 0 ? 0 : random.nextInt(test.length);
            int v = random.nextInt(100);
            int op = random.nextInt(5);
            switch (op) {
            case 0:
                array.add(v);
                test = add(test, v);
                break;
            case 1:
                if (test.length > idx) {
                    assertEquals(get(test, idx), array.get(idx));
                }
                break;
            case 2:
                array.remove(idx);
                test = remove(test, idx);
                break;
            case 3:
                if (test.length > idx) {
                    v = test[idx];
                    array.removeValue(v);
                    test = removeValue(test, v);
                }
                break;
            case 4:
                assertEquals(test.length, array.size());
                break;
            default:
            }
            assertEquals(test.length, array.size());
            for (int j = 0; j < test.length; j++) {
                assertEquals(test[j], array.get(j));
            }

        }
    }

    private int[] add(int[] array, int i, int value) {
        int[] a2 = new int[array.length + 1];
        System.arraycopy(array, 0, a2, 0, array.length);
        if (i < array.length) {
            System.arraycopy(a2, i, a2, i + 1, a2.length - i - 1);
        }
        array = a2;
        array[i] = value;
        return array;
    }

    private int[] add(int[] array, int value) {
        return add(array, array.length, value);
    }

    private int[] addValueSorted(int[] array, int value) {
        for (int i = 0; i < array.length; i++) {
            if (array[i] < value) {
                continue;
            }
            if (array[i] == value) {
                return array;
            }
            return add(array, i, value);
        }
        return add(array, value);
    }

    private int findNextValueIndex(int[] array, int value) {
        for (int i = 0; i < array.length; i++) {
            if (array[i] >= value) {
                return i;
            }
        }
        return array.length;
    }

    private int get(int[] array, int i) {
        return array[i];
    }

    private int[] remove(int[] array, int i) {
        int[] a2 = new int[array.length - 1];
        System.arraycopy(array, 0, a2, 0, i);
        if (i < a2.length) {
            System.arraycopy(array, i + 1, a2, i, array.length - i - 1);
        }
        return a2;
    }

    private int[] removeValue(int[] array, int value) {
        for (int i = 0; i < array.length; i++) {
            if (array[i] == value) {
                return remove(array, i);
            }
        }
        return array;
    }

    private int[] set(int[] array, int i, int value) {
        array[i] = value;
        return array;
    }

    private int[] sort(int[] array) {
        Arrays.sort(array);
        return array;
    }

}
