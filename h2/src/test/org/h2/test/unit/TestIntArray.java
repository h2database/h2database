/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
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

    public void test() throws Exception {
        testRandom();
    }

    private void testRandom() throws Exception {
        IntArray array = new IntArray();
        int[] test = new int[0];
        Random random = new Random(1);
        for (int i = 0; i < 10000; i++) {
            int idx = test.length == 0 ? 0 : random.nextInt(test.length);
            int v = random.nextInt(100);
            int op = random.nextInt(9);
            switch (op) {
            case 0:
                array.add(idx, v);
                test = add(test, idx, v);
                break;
            case 1:
                array.add(v);
                test = add(test, v);
                break;
            case 2:
                array.sort();
                test = sort(test);
                array.addValueSorted(v);
                test = addValueSorted(test, v);
                break;
            case 3:
                array.sort();
                test = sort(test);
                int a = array.findNextValueIndex(v);
                int b = findNextValueIndex(test, v);
                assertEquals(a, b);
                break;
            case 4:
                if (test.length > idx) {
                    assertEquals(array.get(idx), get(test, idx));
                }
                break;
            case 5:
                array.remove(idx);
                test = remove(test, idx);
                break;
            case 6:
                if (test.length > idx) {
                    v = test[idx];
                    array.removeValue(v);
                    test = removeValue(test, v);
                }
                break;
            case 7:
                array.set(idx, v);
                test = set(test, idx, v);
                break;
            case 8:
                assertEquals(array.size(), test.length);
                break;
            default:
            }
            assertEquals(array.size(), test.length);
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
