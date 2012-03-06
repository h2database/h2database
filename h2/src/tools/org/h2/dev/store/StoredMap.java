/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.store;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * A stored map.
 *
 * @param <K> the key class
 * @param <V> the value class
 */
public class StoredMap<K, V> {

    private final TreeMapStore store;
    private final String name;
    private final KeyType keyType;
    private final ValueType valueType;
    private Node root;

    private StoredMap(TreeMapStore store, String name, Class<K> keyClass, Class<V> valueClass) {
        this.store = store;
        this.name = name;
        if (keyClass == Integer.class) {
            keyType = new IntegerType();
        } else if (keyClass == String.class) {
            keyType = new StringType();
        } else {
            throw new RuntimeException("Unsupported key class " + keyClass.toString());
        }
        if (valueClass == Integer.class) {
            valueType = new IntegerType();
        } else if (valueClass == String.class) {
            valueType = new StringType();
        } else {
            throw new RuntimeException("Unsupported value class " + keyClass.toString());
        }
    }

    static <K, V> StoredMap<K, V> open(TreeMapStore store, String name, Class<K> keyClass, Class<V> valueClass) {
        return new StoredMap<K, V>(store, name, keyClass, valueClass);
    }

    public void put(K key, V data) {
        if (!isChanged()) {
            store.changed(name, this);
        }
        root = Node.put(this, root, key, data);
    }

    @SuppressWarnings("unchecked")
    public V get(K key) {
        Node n = Node.getNode(root, key);
        return (V) (n == null ? null : n.getData());
    }

    public void remove(K key) {
        if (!isChanged()) {
            store.changed(name, this);
        }
        root = Node.remove(root, key);
    }

    boolean isChanged() {
        return root != null && root.getId() < 0;
    }

    /**
     * A value type.
     */
    static interface ValueType {
        int length(Object obj);
        void write(ByteBuffer buff, Object x);
        Object read(ByteBuffer buff);
    }

    /**
     * A key type.
     */
    static interface KeyType extends ValueType {
        int compare(Object a, Object b);
    }

    int compare(Object a, Object b) {
        return keyType.compare(a, b);
    }

    /**
     * An integer type.
     */
    static class IntegerType implements KeyType {

        public int compare(Object a, Object b) {
            return ((Integer) a).compareTo((Integer) b);
        }

        public int length(Object obj) {
            return TreeMapStore.getVarIntLen((Integer) obj);
        }

        public Integer read(ByteBuffer buff) {
            return TreeMapStore.readVarInt(buff);
        }

        public void write(ByteBuffer buff, Object x) {
            TreeMapStore.writeVarInt(buff, (Integer) x);
        }

    }

    /**
     * A string type.
     */
    static class StringType implements KeyType {

        public int compare(Object a, Object b) {
            return a.toString().compareTo(b.toString());
        }

        public int length(Object obj) {
            try {
                byte[] bytes = obj.toString().getBytes("UTF-8");
                return TreeMapStore.getVarIntLen(bytes.length) + bytes.length;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public String read(ByteBuffer buff) {
            int len = TreeMapStore.readVarInt(buff);
            byte[] bytes = new byte[len];
            buff.get(bytes);
            try {
                return new String(bytes, "UTF-8");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public void write(ByteBuffer buff, Object x) {
            try {
                byte[] bytes = x.toString().getBytes("UTF-8");
                TreeMapStore.writeVarInt(buff, bytes.length);
                buff.put(bytes);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

    }

    KeyType getKeyType() {
        return keyType;
    }

    ValueType getValueType() {
        return valueType;
    }

    long getTransaction() {
        return store.getTransaction();
    }

    long nextTempNodeId() {
        return store.nextTempNodeId();
    }

    public Node readNode(long id) {
        return store.readNode(this, id);
    }

    void removeNode(long id) {
        store.removeNode(id);
    }

    void setRoot(long rootPos) {
        root = readNode(rootPos);
    }


    public Iterator<K> keyIterator(K from) {
        return new Cursor(root, from);
    }

    /**
     * A cursor to iterate over elements in ascending order.
     */
    class Cursor implements Iterator<K> {
        Node current;
        ArrayList<Node> parents = new ArrayList<Node>();

        Cursor(Node root, K from) {
            min(root, from);
        }

        private void min(Node n, K key) {
            while (n != null) {
                int compare = key == null ? -1 : n.compare(key);
                if (compare == 0) {
                    current = n;
                    return;
                } else if (compare > 0) {
                    n = n.getRight();
                } else {
                    parents.add(n);
                    n = n.getLeft();
                }
            }
            if (parents.size() == 0) {
                current = null;
                return;
            }
            current = parents.remove(parents.size() - 1);
        }

        @SuppressWarnings("unchecked")
        public K next() {
            Node c = current;
            if (c != null) {
                fetchNext();
            }
            return c == null ? null : (K) c.getKey();
        }

        private void fetchNext() {
            Node r = current.getRight();
            if (r != null) {
                min(r, null);
                return;
            }
            if (parents.size() == 0) {
                current = null;
                return;
            }
            current = parents.remove(parents.size() - 1);
        }

        public boolean hasNext() {
            return current != null;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    Node getRoot() {
        return root;
    }

    String getName() {
        return name;
    }

}
