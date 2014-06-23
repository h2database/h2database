/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.util.HashMap;
import org.h2.mvstore.type.DataTypeFactory;
import org.h2.util.New;

/**
 * A builder for an MVStore.
 */
public class MVStoreBuilder {

    private final HashMap<String, Object> config = New.hashMap();

    /**
     * Use the following file name. If the file does not exist, it is
     * automatically created.
     *
     * @param fileName the file name
     * @return this
     */
    public static MVStoreBuilder fileBased(String fileName) {
        return new MVStoreBuilder().fileName(fileName);
    }

    /**
     * Open the store in-memory. In this case, no data may be saved.
     *
     * @return the store
     */
    public static MVStoreBuilder inMemory() {
        return new MVStoreBuilder();
    }

    private MVStoreBuilder set(String key, Object value) {
        if (config.containsKey(key)) {
            throw new IllegalArgumentException("Parameter " + config.get(key) + " is already set");
        }
        config.put(key, value);
        return this;
    }

    private MVStoreBuilder fileName(String fileName) {
        return set("fileName", fileName);
    }

    /**
     * Open the file in read-only mode. In this case, a shared lock will be
     * acquired to ensure the file is not concurrently opened in write mode.
     * <p>
     * If this option is not used, the file is locked exclusively.
     * <p>
     * Please note a store may only be opened once in every JVM (no matter
     * whether it is opened in read-only or read-write mode), because each file
     * may be locked only once in a process.
     *
     * @return this
     */
    public MVStoreBuilder readOnly() {
        return set("openMode", "r");
    }

    /**
     * Set the read cache size in MB. The default is 16 MB.
     *
     * @param mb the cache size
     * @return this
     */
    public MVStoreBuilder cacheSizeMB(int mb) {
        return set("cacheSize", Integer.toString(mb));
    }

    /**
     * Use the given data type factory.
     *
     * @param factory the data type factory
     * @return this
     */
    public MVStoreBuilder with(DataTypeFactory factory) {
        return set("dataTypeFactory", factory);
    }

    /**
     * Open the store.
     *
     * @return the opened store
     */
    public MVStore open() {
        MVStore s = new MVStore(config);
        s.open();
        return s;
    }

    public String toString() {
        return DataUtils.appendMap(new StringBuilder(), config).toString();
    }

    /**
     * Read the configuration from a string.
     *
     * @param s the string representation
     * @return the builder
     */
    public static MVStoreBuilder fromString(String s) {
        HashMap<String, String> config = DataUtils.parseMap(s);
        MVStoreBuilder builder = new MVStoreBuilder();
        builder.config.putAll(config);
        return builder;
    }

}
