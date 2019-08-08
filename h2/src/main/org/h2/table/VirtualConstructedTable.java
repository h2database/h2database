/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import org.h2.engine.Session;
import org.h2.index.Index;
import org.h2.index.VirtualConstructedTableIndex;
import org.h2.result.ResultInterface;
import org.h2.schema.Schema;

/**
 * A base class for virtual tables that construct all their content at once.
 */
public abstract class VirtualConstructedTable extends VirtualTable {

    protected VirtualConstructedTable(Schema schema, int id, String name) {
        super(schema, id, name);
    }

    /**
     * Read the rows from the table.
     *
     * @param session
     *            the session
     * @return the result
     */
    public abstract ResultInterface getResult(Session session);

    @Override
    public Index getScanIndex(Session session) {
        return new VirtualConstructedTableIndex(this, IndexColumn.wrap(columns));
    }

    @Override
    public long getMaxDataModificationId() {
        // TODO optimization: virtual table currently doesn't know the
        // last modified date
        return Long.MAX_VALUE;
    }

}
