/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import org.h2.constant.SysProperties;
import org.h2.message.Message;

/**
 * @author Thomas
 */
public class IntArray {

    private int[] data;
    private int size;
    private int hash;

    public IntArray() {
        data = new int[10];
    }

    public IntArray(int[] data) {
        this.data = data;
        size = data.length;
    }
    
    public static int[] clone(int[] array) {
        if(array == null) {
            return null;
        }
        int[] copy = new int[array.length];
        System.arraycopy(array, 0, copy, 0, array.length);
        return copy;
    }
    
    public static boolean equals(int[] a, int[] b) {
        if(a == null || b == null) {
            return a == b;
        }
        if(a.length != b.length) {
            return false;
        }
        for(int i=0; i<a.length; i++) {
            if(a[i] != b[i]) {
                return false;
            }
        }
        return true;
    }

    public void add(int value) {
        checkCapacity();
        data[size++] = value;
    }

    public int get(int i) {
        if (SysProperties.CHECK && i >= size) {
            throw new ArrayIndexOutOfBoundsException("i=" + i + " size=" + size);
        }
        return data[i];
    }

    public int remove(int i) {
        if (SysProperties.CHECK &&  i >= size) {
            throw new ArrayIndexOutOfBoundsException("i=" + i + " size=" + size);
        }
        int value = data[i];
        System.arraycopy(data, i + 1, data, i, size - i - 1);
        size--;
        return value;
    }

    private void checkCapacity() {
        if (size >= data.length) {
            int[] d = new int[data.length * 2];
            System.arraycopy(data, 0, d, 0, data.length);
            data = d;
        }
    }

    public void add(int i, int value) {
        if (SysProperties.CHECK && i > size) {
            throw new ArrayIndexOutOfBoundsException("i=" + i + " size=" + size);
        }
        checkCapacity();
        if (i == size) {
            add(value);
        } else {
            System.arraycopy(data, i, data, i + 1, size - i);
            data[i] = value;
            size++;
        }
    }

    public void set(int i, int value) {
        if (SysProperties.CHECK && i >= size) {
            throw new ArrayIndexOutOfBoundsException("i=" + i + " size=" + size);
        }
        data[i] = value;
    }
    
    public boolean equals(Object obj) {
        if(!(obj instanceof IntArray)) {
            return false;
        }
        IntArray other = (IntArray) obj;
        if(hashCode() != other.hashCode() || size != other.size) {
            return false;
        }
        for(int i=0; i<size; i++) {
            if(data[i] != other.data[i]) {
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
        for(int i=0; i<size; i++) {
            h = h * 31 + data[i];
        }
        hash = h;
        return h;
    }

    public int size() {
        return size;
    }

    public void addValueSorted(int value) {
        int l = 0, r = size;
        while(l < r) {
            int i = (l + r) >>> 1;
            int d = data[i];
            if(d == value) {
                return;
            } else if(d > value) {
                r = i;
            } else {
                l =  i + 1;
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

    public void removeValue(int value) {
        for(int i=0; i<size; i++) {
            if(data[i] == value) {
                remove(i);
                return;
            }
        }
        throw Message.getInternalError();
    }

    public int findNextValueIndex(int value) {
        int l = 0, r = size;
        while(l < r) {
            int i = (l + r) >>> 1;
            int d = data[i];
            if(d >= value) {
                r = i;
            } else {
                l =  i + 1;
            }
        }
        return l;

//        for(int i=0; i<size; i++) {
//            if(data[i] >= value) {
//                return i;
//            }
//        }
//        return size;
    }

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

    public void toArray(int[] array) {
        System.arraycopy(data, 0, array, 0, size);
    }

//    ArrayList data = new ArrayList();
//
//    public IntArray() {
//    }
//
//    public IntArray(int[] data) {
//        for (int i = 0; i < data.length; i++) {
//            this.data.add(new Integer(data[i]));
//        }
//    }
//
//    public void add(int value) {
//        this.data.add(new Integer(value));
//    }
//
//    public int get(int i) {
//        return ((Integer) data.get(i)).intValue();
//    }
//
//    public void remove(int i) {
//        data.remove(i);
//    }
//
//    public void add(int i, int value) {
//        data.add(i, new Integer(value));
//    }
//
//    public void set(int i, int value) {
//        data.set(i, new Integer(value));
//    }
//
//    public int size() {
//        return data.size();
//    }

}
