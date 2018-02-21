/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import org.h2.mvstore.type.DataType;
import org.h2.mvstore.type.ObjectDataType;

import java.util.Map;

/**
 * A class used for backward compatibility.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class MVMapConcurrent<K, V> extends MVMap<K, V> {

    public MVMapConcurrent(Map<String, Object> config) {
        super(config);
    }

    /**
     * A builder for this class.
     *
     * @param <K> the key type
     * @param <V> the value type
     */
    public static class Builder<K, V> extends MVMap.Builder<K,V> {
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
            setKeyType(keyType);
            return this;
        }

        /**
         * Set the key data type.
         *
         * @param valueType the key type
         * @return this
         */
        public Builder<K, V> valueType(DataType valueType) {
            setValueType(valueType);
            return this;
        }

        @Override
        protected MVMapConcurrent<K, V> create(Map<String, Object> config) {
            return new MVMapConcurrent<>(config);
        }
    }
}
