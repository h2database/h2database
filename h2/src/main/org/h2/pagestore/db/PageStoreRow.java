/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.pagestore.db;

import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.store.Data;
import org.h2.value.Value;

/**
 * Page Store implementation of a row.
 */
public class PageStoreRow extends Row {

    /**
     * An empty array of Row objects.
     */
    static final Row[] EMPTY_ARRAY = new Row[0];

    /**
     * An empty array of SearchRow objects.
     */
    static final SearchRow[] EMPTY_SEARCH_ARRAY = new SearchRow[0];

    /**
     * Creates a new row.
     *
     * @param data values of columns, or null
     * @param memory used memory
     * @return the allocated row
     */
    public static PageStoreRow get(Value[] data, int memory) {
        return new PageStoreRow(data, memory);
    }

    /**
     * Creates a new row with the specified key.
     *
     * @param data values of columns, or null
     * @param memory used memory
     * @param key the key
     * @return the allocated row
     */
    public static PageStoreRow get(Value[] data, int memory, long key) {
        PageStoreRow r = new PageStoreRow(data, memory);
        r.setKey(key);
        return r;
    }

    private PageStoreRow(Value[] data, int memory) {
        super(data, memory);
    }

    /**
     * Get the number of bytes required for the data.
     *
     * @param dummy the template buffer
     * @return the number of bytes
     */
    public int getByteCount(Data dummy) {
        int size = 0;
        for (Value v : data) {
            size += dummy.getValueLen(v);
        }
        return size;
    }

}
