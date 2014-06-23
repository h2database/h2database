/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import org.h2.mvstore.type.DataType;
import org.h2.mvstore.type.ObjectDataType;

/**
 * A stored map. Read operations can happen concurrently with all other
 * operations, without risk of corruption.
 * <p>
 * Write operations first read the relevant area from disk to memory
 * concurrently, and only then modify the data. The in-memory part of write
 * operations is synchronized. For scalable concurrent in-memory write
 * operations, the map should be split into multiple smaller sub-maps that are
 * then synchronized independently.
 *
 * @param <K> the key class
 * @param <V> the value class
 */
public class MVMapConcurrent<K, V> extends MVMap<K, V> {

    public MVMapConcurrent(DataType keyType, DataType valueType) {
        super(keyType, valueType);
    }

    @Override
    protected Page copyOnWrite(Page p, long writeVersion) {
        return p.copy(writeVersion);
    }

    @Override
    protected void checkConcurrentWrite() {
        // ignore (writes are synchronized)
    }

    @Override
    @SuppressWarnings("unchecked")
    public V put(K key, V value) {
        beforeWrite();
        try {
            // even if the result is the same, we still update the value
            // (otherwise compact doesn't work)
            get(key);
            long v = writeVersion;
            synchronized (this) {
                Page p = copyOnWrite(root, v);
                p = splitRootIfNeeded(p, v);
                V result = (V) put(p, v, key, value);
                newRoot(p);
                return result;
            }
        } finally {
            afterWrite();
        }
    }

    @Override
    protected void waitUntilWritten(long version) {
        // no need to wait
    }

    @Override
    @SuppressWarnings("unchecked")
    public V remove(Object key) {
        beforeWrite();
        try {
            V result = get(key);
            if (result == null) {
                return null;
            }
            long v = writeVersion;
            synchronized (this) {
                Page p = copyOnWrite(root, v);
                result = (V) remove(p, v, key);
                if (!p.isLeaf() && p.getTotalCount() == 0) {
                    p.removePage();
                    p = Page.createEmpty(this,  p.getVersion());
                }
                newRoot(p);
            }
            return result;
        } finally {
            afterWrite();
        }
    }

    /**
     * A builder for this class.
     *
     * @param <K> the key type
     * @param <V> the value type
     */
    public static class Builder<K, V> implements
            MapBuilder<MVMapConcurrent<K, V>, K, V> {

        protected DataType keyType;
        protected DataType valueType;

        /**
         * Create a new builder with the default key and value data types.
         */
        public Builder() {
            // ignore
        }

        /**
         * Set the key data type.
         *
         * @param keyType the key type
         * @return this
         */
        public Builder<K, V> keyType(DataType keyType) {
            this.keyType = keyType;
            return this;
        }

        /**
         * Set the key data type.
         *
         * @param valueType the key type
         * @return this
         */
        public Builder<K, V> valueType(DataType valueType) {
            this.valueType = valueType;
            return this;
        }

        @Override
        public MVMapConcurrent<K, V> create() {
            if (keyType == null) {
                keyType = new ObjectDataType();
            }
            if (valueType == null) {
                valueType = new ObjectDataType();
            }
            return new MVMapConcurrent<K, V>(keyType, valueType);
        }

    }

}
