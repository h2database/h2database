/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.table.IndexColumn;
import org.h2.table.VirtualTable;

/**
 * An base class for indexes of virtual tables.
 */
public abstract class VirtualTableIndex extends BaseIndex {

    protected VirtualTableIndex(VirtualTable table, String name, IndexColumn[] columns) {
        super(table, 0, name, columns, IndexType.createNonUnique(true));
    }

    @Override
    public void close(Session session) {
        // nothing to do
    }

    @Override
    public void add(Session session, Row row) {
        throw DbException.getUnsupportedException("Virtual table");
    }

    @Override
    public void remove(Session session, Row row) {
        throw DbException.getUnsupportedException("Virtual table");
    }

    @Override
    public void remove(Session session) {
        throw DbException.getUnsupportedException("Virtual table");
    }

    @Override
    public void truncate(Session session) {
        throw DbException.getUnsupportedException("Virtual table");
    }

    @Override
    public boolean needRebuild() {
        return false;
    }

    @Override
    public void checkRename() {
        throw DbException.getUnsupportedException("Virtual table");
    }

    @Override
    public Cursor findFirstOrLast(Session session, boolean first) {
        throw DbException.getUnsupportedException("Virtual table");
    }

    @Override
    public long getRowCount(Session session) {
        return table.getRowCount(session);
    }

    @Override
    public long getRowCountApproximation() {
        return table.getRowCountApproximation();
    }

    @Override
    public long getDiskSpaceUsed() {
        return 0;
    }

}
