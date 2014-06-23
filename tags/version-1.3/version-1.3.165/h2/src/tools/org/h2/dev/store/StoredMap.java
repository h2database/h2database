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

    /**
     * Get the class with the given tag name.
     *
     * @param name the tag name
     * @return the class
     */
    static Class<?> getClass(String name) {
        if (name.equals("i")) {
            return Integer.class;
        } else if (name.equals("s")) {
            return String.class;
        }
        throw new RuntimeException("Unknown class name " + name);
    }

    /**
     * Open a map.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param store the tree store
     * @param name the name of the map
     * @param keyClass the key class
     * @param valueClass the value class
     * @return the map
     */
    static <K, V> StoredMap<K, V> open(TreeMapStore store, String name, Class<K> keyClass, Class<V> valueClass) {
        return new StoredMap<K, V>(store, name, keyClass, valueClass);
    }

    /**
     * Store a key-value pair.
     *
     * @param key the key
     * @param data the value
     */
    public void put(K key, V data) {
        if (!isChanged()) {
            store.markChanged(name, this);
        }
        root = Node.put(this, root, key, data);
    }

    /**
     * Get a value.
     *
     * @param key the key
     * @return the value
     */
    @SuppressWarnings("unchecked")
    public V get(K key) {
        Node n = Node.getNode(root, key);
        return (V) (n == null ? null : n.getData());
    }

    /**
     * Get the node with the given key.
     *
     * @param key the key
     * @return the node
     */
    Node getNode(Object key) {
        return Node.getNode(root, key);
    }

    /**
     * Remove a key-value pair.
     *
     * @param key the key
     */
    public void remove(K key) {
        if (!isChanged()) {
            store.markChanged(name, this);
        }
        root = Node.remove(root, key);
    }

    /**
     * Was this map changed.
     *
     * @return true if yes
     */
    boolean isChanged() {
        return root != null && root.getId() < 0;
    }

    /**
     * A value type.
     */
    static interface ValueType {

        /**
         * Get the length in bytes.
         *
         * @param obj the object
         * @return the length
         */
        int length(Object obj);

        /**
         * Write the object.
         *
         * @param buff the target buffer
         * @param x the value
         */
        void write(ByteBuffer buff, Object x);

        /**
         * Read an object.
         *
         * @param buff the source buffer
         * @return the object
         */
        Object read(ByteBuffer buff);

        /**
         * Get the tag name of the class.
         *
         * @return the tag name
         */
        String getName();

    }

    /**
     * A key type.
     */
    static interface KeyType extends ValueType {

        /**
         * Compare two keys.
         *
         * @param a the first key
         * @param b the second key
         * @return -1 if the first key is smaller, 1 if larger, and 0 if equal
         */
        int compare(Object a, Object b);
    }

    /**
     * Compare two keys.
     *
     * @param a the first key
     * @param b the second key
     * @return -1 if the first key is smaller, 1 if bigger, 0 if equal
     */
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
            return getVarIntLen((Integer) obj);
        }

        public Integer read(ByteBuffer buff) {
            return readVarInt(buff);
        }

        public void write(ByteBuffer buff, Object x) {
            writeVarInt(buff, (Integer) x);
        }

        public String getName() {
            return "i";
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
                return getVarIntLen(bytes.length) + bytes.length;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public String read(ByteBuffer buff) {
            int len = readVarInt(buff);
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
                writeVarInt(buff, bytes.length);
                buff.put(bytes);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public String getName() {
            return "s";
        }

    }

    /**
     * Get the key type.
     *
     * @return the key type
     */
    KeyType getKeyType() {
        return keyType;
    }

    /**
     * Get the value type.
     *
     * @return the value type
     */
    ValueType getValueType() {
        return valueType;
    }

    long getTransaction() {
        return store.getTransaction();
    }

    /**
     * Get the next temporary node id.
     *
     * @return the node id
     */
    long nextTempNodeId() {
        return store.nextTempNodeId();
    }

    /**
     * Read a node.
     *
     * @param id the node id
     * @return the node
     */
    Node readNode(long id) {
        return store.readNode(this, id);
    }

    /**
     * Remove a node.
     *
     * @param id the node id
     */
    void removeNode(long id) {
        store.removeNode(id);
    }

    /**
     * Set the position of the root node.
     *
     * @param rootPos the position
     */
    void setRoot(long rootPos) {
        root = readNode(rootPos);
    }

    /**
     * Iterate over all keys.
     *
     * @param from the first key to return
     * @return the iterator
     */
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

    /**
     * Get the root node.
     *
     * @return the root node
     */
    Node getRoot() {
        return root;
    }

    /**
     * Get the map name.
     *
     * @return the name
     */
    String getName() {
        return name;
    }


    /**
     * Read a variable size int.
     *
     * @param buff the source buffer
     * @return the value
     */
    static int readVarInt(ByteBuffer buff) {
        int b = buff.get();
        if (b >= 0) {
            return b;
        }
        // a separate function so that this one can be inlined
        return readVarIntRest(buff, b);
    }

    private static int readVarIntRest(ByteBuffer buff, int b) {
        int x = b & 0x7f;
        b = buff.get();
        if (b >= 0) {
            return x | (b << 7);
        }
        x |= (b & 0x7f) << 7;
        b = buff.get();
        if (b >= 0) {
            return x | (b << 14);
        }
        x |= (b & 0x7f) << 14;
        b = buff.get();
        if (b >= 0) {
            return x | b << 21;
        }
        x |= ((b & 0x7f) << 21) | (buff.get() << 28);
        return x;
    }

    /**
     * Get the length of the variable size int.
     *
     * @param x the value
     * @return the length in bytes
     */
    static int getVarIntLen(int x) {
        if ((x & (-1 << 7)) == 0) {
            return 1;
        } else if ((x & (-1 << 14)) == 0) {
            return 2;
        } else if ((x & (-1 << 21)) == 0) {
            return 3;
        } else if ((x & (-1 << 28)) == 0) {
            return 4;
        }
        return 5;
    }

    /**
     * Write a variable size int.
     *
     * @param buff the target buffer
     * @param x the value
     */
    static void writeVarInt(ByteBuffer buff, int x) {
        while ((x & ~0x7f) != 0) {
            buff.put((byte) (0x80 | (x & 0x7f)));
            x >>>= 7;
        }
        buff.put((byte) x);
    }

}
