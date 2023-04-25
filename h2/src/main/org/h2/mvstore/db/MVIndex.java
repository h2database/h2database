/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import java.util.List;

import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.mvstore.MVMap;
import org.h2.result.Row;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.value.VersionedValue;

/**
 * An index that stores the data in an MVStore.
 */
public abstract class MVIndex<K,V> extends Index {

    protected MVIndex(Table newTable, int id, String name, IndexColumn[] newIndexColumns, int uniqueColumnCount,
            IndexType newIndexType) {
        super(newTable, id, name, newIndexColumns, uniqueColumnCount, newIndexType);
    }

    /**
     * Add the rows to a temporary storage (not to the index yet). The rows are
     * sorted by the index columns. This is to more quickly build the index.
     *
     * @param rows the rows
     * @param bufferName the name of the temporary storage
     */
    public abstract void addRowsToBuffer(List<Row> rows, String bufferName);

    /**
     * Add all the index data from the buffers to the index. The index will
     * typically use merge sort to add the data more quickly in sorted order.
     *
     * @param bufferNames the names of the temporary storage
     */
    public abstract void addBufferedRows(List<String> bufferNames);

    public abstract MVMap<K,VersionedValue<V>> getMVMap();

}
