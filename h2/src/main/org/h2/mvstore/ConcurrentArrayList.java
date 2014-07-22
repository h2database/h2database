/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.util.Arrays;
import java.util.Iterator;


/**
 * A very simple array list that supports concurrent access.
 * Internally, it uses immutable objects.
 *
 * @param <K> the key type
 */
public class ConcurrentArrayList<K> {

    /**
     * The list.
     */
    volatile List<K> list = new List<K>(null, 0, 0);

    /**
     * Get the first element, or null if none.
     *
     * @return the first element
     */
    public K peekFirst() {
        return list.peekFirst();
    }

    /**
     * Get the last element, or null if none.
     *
     * @return the last element
     */
    public K peekLast() {
        return list.peekLast();
    }

    /**
     * Add an element at the end.
     *
     * @param obj the element
     */
    public synchronized void add(K obj) {
        K[] array = new K[list.length + 1];
        if (list.length > 0) {
            System.arraycopy(list.list, list.offset, dest, destPos, length)
        Arrays.copyOf(, newLength)
        list = new List<K>(list.add(obj);
    }

    /**
     * Remove the first element, if it matches.
     *
     * @param obj the element to remove
     * @return true if the element matched and was removed
     */
    public synchronized boolean removeFirst(K obj) {
        if (head.obj != obj) {
            return false;
        }
        head = head.next;
        return true;
    }

    /**
     * Remove the last element, if it matches.
     *
     * @param obj the element to remove
     * @return true if the element matched and was removed
     */
    public synchronized boolean removeLast(K obj) {
        if (peekLast() != obj) {
            return false;
        }
        head = Entry.removeLast(head);
        return true;
    }

    /**
     * Get an iterator over all entries.
     *
     * @return the iterator
     */
    public Iterator<K> iterator() {
        return new Iterator<K>() {

            List<K> list = head;

            @Override
            public boolean hasNext() {
                return current != NULL;
            }

            @Override
            public K next() {
                K x = current.obj;
                current = current.next;
                return x;
            }

            @Override
            public void remove() {
                throw DataUtils.newUnsupportedOperationException("remove");
            }

        };
    }

    /**
     * An entry in the linked list.
     */
    private static class List<K> {
        final K[] list;
        final int offset;
        final int length;

        List(K[] list, int offset, int length) {
            this.list = list;
            this.offset = offset;
            this.length = length;
        }

        public K peekFirst() {
            return length == 0 ? null : list[offset];
        }

        public K peekLast() {
            return length == 0 ? null : list[offset + length - 1];
        }

    }

}
