/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.util.Iterator;

/**
 * A very simple linked list that supports concurrent access.
 * 
 * @param <K> the key type
 */
public class ConcurrentLinkedList<K> {

    volatile Entry<K> head;
    private volatile Entry<K> tail;
    
    public K peekFirst() {
        Entry<K> x = head;
        return x == null ? null : x.obj;
    }

    public K peekLast() {
        Entry<K> x = tail;
        return x == null ? null : x.obj;
    }

    public void add(K obj) {
        Entry<K> x = new Entry<K>(obj);
        Entry<K> t = tail;
        if (t != null) {
            t.next = x;
        }
        tail = x;
        if (head == null) {
            head = x;
        }
    }
    
    public void removeFirst(K obj) {
        Entry<K> x = head;
        if (x == null) {
            return;
        }
        if (x.obj.equals(obj)) {
            if (head == tail) {
                tail = x.next;
            }
            head = x.next;
        }
    }

    public void removeLast(K obj) {
        Entry<K> x = head;
        if (x == null) {
            return;
        }
        Entry<K> prev = null;
        while (x.next != null) {
            prev = x;
            x = x.next;
        }
        if (x.obj.equals(obj)) {
            if (prev != null) {
                prev.next = null;
            }
            if (head == tail) {
                head = prev;
            }
            tail = prev;
        }
    }

    public Iterator<K> iterator() {
        return new Iterator<K>() {

            Entry<K> current = head;

            @Override
            public boolean hasNext() {
                return current != null;
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
    private static class Entry<K> {
        final K obj;
        Entry<K> next;
        
        Entry(K obj) {
            this.obj = obj;
        }
    }

}
