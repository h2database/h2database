/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import org.h2.constant.SysProperties;
import org.h2.message.Message;

/**
 * An array with integer element.
 */
public class IntArray {

    private int[] data;
    private int size;
    private int hash;

    /**
     * Create an int array with the default initial capacity.
     */
    public IntArray() {
        data = new int[10];
    }

    /**
     * Create an int array with the given values and size.
     */
    public IntArray(int[] data) {
        this.data = data;
        size = data.length;
    }

    /**
     * Append a value.
     *
     * @param value the value to append
     */
    public void add(int value) {
        checkCapacity();
        data[size++] = value;
    }

    /**
     * Get the value at the given index.
     *
     * @param index the index
     * @return the value
     */
    public int get(int index) {
        if (SysProperties.CHECK && index >= size) {
            throw new ArrayIndexOutOfBoundsException("i=" + index + " size=" + size);
        }
        return data[index];
    }

    /**
     * Remove the value at the given index.
     *
     * @param index the index
     */
    public void remove(int index) {
        if (SysProperties.CHECK && index >= size) {
            throw new ArrayIndexOutOfBoundsException("i=" + index + " size=" + size);
        }
        System.arraycopy(data, index + 1, data, index, size - index - 1);
        size--;
    }

    private void checkCapacity() {
        if (size >= data.length) {
            int[] d = new int[Math.max(4, data.length * 2)];
            System.arraycopy(data, 0, d, 0, data.length);
            data = d;
        }
    }

    /**
     * Insert an element at the given position. The element at this position and
     * all elements with a higher index move one element.
     *
     * @param index the index where to insert the value
     * @param value the value to insert
     */
    public void add(int index, int value) {
        if (SysProperties.CHECK && index > size) {
            throw new ArrayIndexOutOfBoundsException("i=" + index + " size=" + size);
        }
        checkCapacity();
        if (index == size) {
            add(value);
        } else {
            System.arraycopy(data, index, data, index + 1, size - index);
            data[index] = value;
            size++;
        }
    }

    /**
     * Update the value at the given index.
     *
     * @param index the index
     * @param value the new value
     */
    public void set(int index, int value) {
        if (SysProperties.CHECK && index >= size) {
            throw new ArrayIndexOutOfBoundsException("i=" + index + " size=" + size);
        }
        data[index] = value;
    }

    public boolean equals(Object obj) {
        if (!(obj instanceof IntArray)) {
            return false;
        }
        IntArray other = (IntArray) obj;
        if (hashCode() != other.hashCode() || size != other.size) {
            return false;
        }
        for (int i = 0; i < size; i++) {
            if (data[i] != other.data[i]) {
                return false;
            }
        }
        return true;
    }

    public int hashCode() {
        if (hash != 0) {
            return hash;
        }
        int h = size + 1;
        for (int i = 0; i < size; i++) {
            h = h * 31 + data[i];
        }
        hash = h;
        return h;
    }

    /**
     * Get the size of the list.
     *
     * @return the size
     */
    public int size() {
        return size;
    }

    /**
     * Insert an element at the correct position in a sorted list.
     * If the list is not sorted, the result of this operation is undefined.
     *
     * @param value the value to insert
     */
    public void addValueSorted(int value) {
        int l = 0, r = size;
        while (l < r) {
            int i = (l + r) >>> 1;
            int d = data[i];
            if (d == value) {
                return;
            } else if (d > value) {
                r = i;
            } else {
                l = i + 1;
            }
        }
        add(l, value);
    }

//    public void addValueSorted(int value) {
//        int l = 0, r = size - 1;
//        while(l <= r) {
//            int i = (l + r) >>> 1;
//            int d = data[i];
//            if(d == value) {
//                return;
//            } else if(d > value) {
//                r = i - 1;
//            } else {
//                l =  i + 1;
//            }
//        }
//        add(l, value);
//    }

    /**
     * Remove the first element of this list that matches this value.
     *
     * @param value the value to be remove
     */
    public void removeValue(int value) {
        for (int i = 0; i < size; i++) {
            if (data[i] == value) {
                remove(i);
                return;
            }
        }
        Message.throwInternalError();
    }

    /**
     * Remove the last element of this list that matches this value.
     *
     * @param value the value to be remove
     */
    public void removeLastValue(int value) {
        for (int i = size - 1; i >= 0; i--) {
            if (data[i] == value) {
                remove(i);
                return;
            }
        }
        Message.throwInternalError();
    }

    /**
     * Return the index with a this value.
     * If the list is not sorted, the result of this operation is undefined.
     *
     * @param value the value to find
     * @return the index or -1 if not found
     */
    public int findIndexSorted(int value) {
        int l = 0, r = size;
        while (l < r) {
            int i = (l + r) >>> 1;
            int d = data[i];
            if (d == value) {
                return i;
            } else if (d > value) {
                r = i;
            } else {
                l = i + 1;
            }
        }
        return -1;
    }

    /**
     * Return the next index with a value larger than this one.
     * If the list is not sorted, the result of this operation is undefined.
     *
     * @param value the value to find
     * @return the index
     */
    public int findNextIndexSorted(int value) {
        int l = 0, r = size;
        while (l < r) {
            int i = (l + r) >>> 1;
            int d = data[i];
            if (d >= value) {
                r = i;
            } else {
                l = i + 1;
            }
        }
        return l;
    }

    /**
     * Sort the array by value.
     */
    public void sort() {
        // insertion sort
        for (int i = 1, j; i < size(); i++) {
            int t = get(i);
            for (j = i - 1; j >= 0 && (get(j) > t); j--) {
                set(j + 1, get(j));
            }
            set(j + 1, t);
        }
    }

    /**
     * Convert this list to an array. The target array must be big enough.
     *
     * @param array the target array
     */
    public void toArray(int[] array) {
        System.arraycopy(data, 0, array, 0, size);
    }

    /**
     * Remove all values from the given sorted list from this sorted list.
     *
     * @param removeSorted the value to remove
     */
    public void removeAllSorted(IntArray removeSorted) {
        int[] d = new int[data.length];
        int newSize = 0;
        for (int i = 0; i < size; i++) {
            int old = data[i];
            if (removeSorted.findIndexSorted(old) == -1) {
                d[newSize++] = old;
            }
        }
        data = d;
        size = newSize;
    }

    public String toString() {
        StatementBuilder buff = new StatementBuilder("{");
        for (int i = 0; i < size; i++) {
            buff.appendExceptFirst(", ");
            buff.append(data[i]);
        }
        return buff.append('}').toString();
    }

    /**
     * Remove a number of elements.
     *
     * @param fromIndex the index of the first item to remove
     * @param toIndex upper bound (exclusive)
     */
    public void removeRange(int fromIndex, int toIndex) {
        if (SysProperties.CHECK) {
            if (fromIndex > toIndex || toIndex > size) {
                throw new ArrayIndexOutOfBoundsException("from=" + fromIndex + " to=" + toIndex + " size=" + size);
            }
        }
        System.arraycopy(data, toIndex, data, fromIndex, size - toIndex);
        size -= toIndex - fromIndex;
    }

}
