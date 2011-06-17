/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import org.h2.constant.SysProperties;

/**
 * The object array is basically the same as ArrayList.
 * It is a bit faster than ArrayList in some versions of Java.
 *
 * @param <T> the element type
 */
public class ObjectArray<T> implements Iterable<T> {
    private static final int CAPACITY_INIT = 4, CAPACITY_SHRINK = 256;

    int size;
    private T[] data;

    private ObjectArray(int capacity) {
        data = createArray(capacity);
    }

    private ObjectArray(Collection<T> collection) {
        size = collection.size();
        data = createArray(size);
        Iterator<T> it = collection.iterator();
        for (int i = 0; i < size; i++) {
            data[i] = it.next();
        }
    }

    /**
     * Create a new object with the given initial capacity.
     *
     * @param capacity the initial capacity
     * @return the object
     */
    public static <T> ObjectArray<T> newInstance(int capacity) {
        return new ObjectArray<T>(CAPACITY_INIT);
    }

    /**
     * Create a new object with the given values.
     *
     * @param list the initial elements
     * @return the object
     */
    public static <T> ObjectArray<T> newInstance(T... list) {
        ObjectArray<T> t = new ObjectArray<T>(CAPACITY_INIT);
        for (T x : list) {
            t.add(x);
        }
        return t;
    }

    /**
     * Create a new object with the default initial capacity.
     *
     * @return the object
     */
    public static <T> ObjectArray<T> newInstance() {
        return new ObjectArray<T>(CAPACITY_INIT);
    }

    /**
     * Create a new object with all elements of the given collection.
     *
     * @param collection the collection with all elements
     * @return the object
     */
    public static <T> ObjectArray<T> newInstance(Collection<T> collection) {
        return new ObjectArray<T>(collection);
    }

    @SuppressWarnings("unchecked")
    private T[] createArray(int capacity) {
        return (T[]) new Object[capacity > 1 ? capacity : 1];
    }

    private void throwException(int index) {
        throw new ArrayIndexOutOfBoundsException("i=" + index + " size=" + size);
    }

    /**
     * Append an object at the end of the list.
     *
     * @param value the value
     */
    public void add(T value) {
        if (size >= data.length) {
            ensureCapacity(size);
        }
        data[size++] = value;
    }

    /**
     * Get the object at the given index.
     *
     * @param index the index
     * @return the value
     */
    public T get(int index) {
        if (SysProperties.CHECK2 && index >= size) {
            throwException(index);
        }
        return data[index];
    }

    /**
     * Remove the object at the given index.
     *
     * @param index the index
     * @return the removed object
     */
    public Object remove(int index) {
        // TODO performance: the app should (where possible)
        // remove from end to start, to avoid O(n^2)
        if (SysProperties.CHECK2 && index >= size) {
            throwException(index);
        }
        Object value = data[index];
        System.arraycopy(data, index + 1, data, index, size - index - 1);
        size--;
        data[size] = null;
        // TODO optimization / lib: could shrink ObjectArray on element remove
        return value;
    }

    /**
     * Remove a number of elements from the given start and end index.
     *
     * @param from the start index
     * @param to the end index
     */
    public void removeRange(int from, int to) {
        if (SysProperties.CHECK2 && (to > size || from > to)) {
            throw new ArrayIndexOutOfBoundsException("to=" + to + " from="+from+" size=" + size);
        }
        System.arraycopy(data, to, data, from, size - to);
        size -= to - from;
        for (int i = size + (to - from) - 1; i >= size; i--) {
            data[i] = null;
        }
    }

    /**
     * Fill the list with empty elements until it reaches the given size.
     *
     * @param size the new size
     */
    public void setSize(int size) {
        ensureCapacity(size);
        this.size = size;
    }

    private void ensureCapacity(int i) {
        while (i >= data.length) {
            T[] d = createArray(Math.max(CAPACITY_INIT, data.length * 2));
            System.arraycopy(data, 0, d, 0, size);
            data = d;
        }
    }

    /**
     * Shrink the array to the required size.
     */
    public void trimToSize() {
        T[] d = createArray(size);
        System.arraycopy(data, 0, d, 0, size);
        data = d;
    }

    /**
     * Insert an element at the given position. The element at this position and
     * all elements with a higher index move one element.
     *
     * @param index the index where to insert the object
     * @param value the object to insert
     */
    public void add(int index, T value) {
        if (SysProperties.CHECK2 && index > size) {
            throwException(index);
        }
        ensureCapacity(size);
        if (index == size) {
            add(value);
        } else {
            System.arraycopy(data, index, data, index + 1, size - index);
            data[index] = value;
            size++;
        }
    }

    /**
     * Update the object at the given index.
     *
     * @param index the index
     * @param value the new value
     */
    public void set(int index, T value) {
        if (SysProperties.CHECK2 && index >= size) {
            throwException(index);
        }
        data[index] = value;
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
     * Convert this list to an array. The target array must be big enough.
     *
     * @param array the target array
     */
    public void toArray(Object[] array) {
        ObjectUtils.arrayCopy(data, array, size);
    }

    /**
     * Remove all elements from the list.
     */
    public void clear() {
        if (data.length > CAPACITY_SHRINK) {
            data = createArray(CAPACITY_INIT);
        } else {
            for (int i = 0; i < size; i++) {
                data[i] = null;
            }
        }
        size = 0;
    }

    /**
     * Get the index of the given object, or -1 if not found.
     *
     * @param o the object to search
     * @return the index
     */
    public int indexOf(Object o) {
        for (int i = 0; i < size; i++) {
            if (data[i] == o) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Add all objects from the given list.
     *
     * @param list the list
     */
    public void addAll(ObjectArray< ? extends T> list) {
        for (int i = 0; i < list.size; i++) {
            add(list.data[i]);
        }
    }

    private void swap(int l, int r) {
        T t = data[r];
        data[r] = data[l];
        data[l] = t;
    }

    /**
     * Sort the elements using the given comparator.
     *
     * @param comp the comparator
     */
    public void sort(Comparator<T> comp) {
        sort(comp, 0, size - 1);
    }

    /**
     * Sort using the quicksort algorithm.
     *
     * @param comp the comparator
     * @param l the first element (left)
     * @param r the last element (right)
     */
    private void sort(Comparator<T> comp, int l, int r) {
        int i, j;
        while (r - l > 10) {
            // randomized pivot to avoid worst case
            i = RandomUtils.nextInt(r - l - 4) + l + 2;
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
            T p = get(j);
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
            T t = get(i);
            for (j = i - 1; j >= l && (comp.compare(get(j), t) > 0); j--) {
                set(j + 1, get(j));
            }
            set(j + 1, t);
        }
    }

    /**
     * The iterator for this list.
     */
    class ObjectArrayIterator implements Iterator<T> {
        private int index;

        public boolean hasNext() {
            return index < size;
        }

        public T next() {
            return get(index++);
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    public Iterator<T> iterator() {
        return new ObjectArrayIterator();
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

    public String toString() {
        StatementBuilder buff = new StatementBuilder("{");
        for (int i = 0; i < size; i++) {
            buff.appendExceptFirst(", ");
            T t = get(i);
            buff.append(t == null ? "" : t.toString());
        }
        return buff.append('}').toString();
    }

}
