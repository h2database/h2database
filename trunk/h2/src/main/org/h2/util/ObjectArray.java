/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;

import org.h2.constant.SysProperties;

/**
 * @author Thomas
 */

public class ObjectArray {
    private static final int SIZE_INIT = 4, SIZE_SHRINK = 256;

    private Object[] data;
    private int size;

    public ObjectArray() {
        this(SIZE_INIT);
    }
    
    public ObjectArray(int size) {
        data = new Object[size > 1 ? size : 1];
    }

    public ObjectArray(Object[] data) {
        this.data = data;
        size = data.length;
    }

    public ObjectArray(Collection collection) {
        // TODO lib: Collection should not be required
        size = collection.size();
        data = new Object[size];
        Iterator it = collection.iterator();
        for(int i=0; i<size; i++) {
            data[i] = it.next();
        }
    }
    
    private void throwException(int index) {
        throw new ArrayIndexOutOfBoundsException("i=" + index + " size=" + size);
    }

    public void add(Object value) {
        if(size >= data.length) {
            ensureCapacity(size);
        }
        data[size++] = value;
    }

    public Object get(int i) {
        if (SysProperties.CHECK && i >= size) {
            throwException(i);
        }
        return data[i];
    }

    public Object remove(int i) {
        // TODO performance: the app should (where possible) remove from end to start, to avoid O(n^2)
        if (SysProperties.CHECK && i >= size) {
            throwException(i);
        }
        Object value = data[i];
        System.arraycopy(data, i + 1, data, i, size - i - 1);
        size--;
        data[size] = null;
        // TODO optimization / lib: could shrink ObjectArray on element remove
        return value;
    }
    
    public void removeRange(int from, int to) {
        if (SysProperties.CHECK && (to > size || from > to)) {
            throw new ArrayIndexOutOfBoundsException("to=" + to + " from="+from+" size=" + size);
        }
        System.arraycopy(data, to, data, from, size - to);
        size -= to - from;
        for(int i=size + (to-from) - 1; i>=size; i--) {
            data[i] = null;
        }
    }
    
    public void setSize(int i) {
        ensureCapacity(i);
        this.size = i;
    }

    private void ensureCapacity(int i) {
        while (i >= data.length) {
            Object[] d = new Object[data.length * 2];
            System.arraycopy(data, 0, d, 0, data.length);
            data = d;
        }
    }

    public void add(int i, Object value) {
        if (SysProperties.CHECK && i > size) {
            throwException(i);
        }
        ensureCapacity(size);
        if (i == size) {
            add(value);
        } else {
            System.arraycopy(data, i, data, i + 1, size - i);
            data[i] = value;
            size++;
        }
    }

    public void set(int i, Object value) {
        if (SysProperties.CHECK && i >= size) {
            throwException(i);
        }
        data[i] = value;
    }

    public int size() {
        return size;
    }

    public void toArray(Object[] array) {
        for(int i=0; i<size; i++) {
            array[i] = data[i];
        }
    }

    public void clear() {
        if(data.length > SIZE_SHRINK) {
            data = new Object[SIZE_INIT];
        } else {
            for(int i=0; i<size; i++) {
                data[i] = null;
            }
        }
        size = 0;
    }

    public int indexOf(Object o) {
        for(int i=0; i<size; i++) {
            if(data[i] == o) {
                return i;
            }
        }
        return -1;
    }

    public void addAll(ObjectArray list) {
        for(int i=0; i<list.size; i++) {
            add(list.get(i));
        }
    }
    
    private void swap(int l, int r) {
        Object t = data[r];
        data[r] = data[l];
        data[l] = t;
    }
    
    public void sort(Comparator comp) {
        sort(comp, 0, size-1);
    }
    
    private void sort(Comparator comp, int l, int r) {
        int i, j;
        while (r - l > 10) {
            i = (r + l) >> 1;
            if (comp.compare(get(l), get(r)) > 0) {
                swap(l, r);
            }
            if (comp.compare(get(i), get(l)) < 0) {
                swap(l, i);
            } else if (comp.compare(get(i), get(r)) > 0) {
                swap(i, r);
            }
            j = r - 1;
            swap(i, j);
            Object p = get(j);
            i = l;
            while (true) {
                do {
                    ++i;
                } while (comp.compare(get(i), p) < 0);
                do {
                    --j;
                } while (comp.compare(get(j), p) > 0);
                if (i >= j) {
                    break;
                }
                swap(i, j);
            }
            swap(i, r - 1);
            sort(comp, l, i - 1);
            l = i + 1;
        }
        for (i = l + 1; i <= r; i++) {
            Object t = get(i);
            for (j = i - 1; j >= l && (comp.compare(get(j), t) > 0); j--) {
                set(j + 1, get(j));
            }
            set(j + 1, t);
        }
    }

//    public void sortInsertion(Comparator comp) {
//        for (int i = 1, j; i < size(); i++) {
//            Object t = get(i);
//            for (j = i - 1; j >= 0 && (comp.compare(get(j), t) < 0); j--) {
//                set(j + 1, get(j));
//            }
//            set(j + 1, t);
//        }
//    }

}
