/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import org.h2.engine.SessionLocal;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.table.IndexColumn;
import org.h2.table.VirtualTable;

/**
 * An base class for indexes of virtual tables.
 */
public abstract class VirtualTableIndex extends Index {

    protected VirtualTableIndex(VirtualTable table, String name, IndexColumn[] columns) {
        super(table, 0, name, columns, 0, IndexType.createNonUnique(true));
    }

    @Override
    public void close(SessionLocal session) {
        // nothing to do
    }

    @Override
    public void add(SessionLocal session, Row row) {
        throw DbException.getUnsupportedException("Virtual table");
    }

    @Override
    public void remove(SessionLocal session, Row row) {
        throw DbException.getUnsupportedException("Virtual table");
    }

    @Override
    public void remove(SessionLocal session) {
        throw DbException.getUnsupportedException("Virtual table");
    }

    @Override
    public void truncate(SessionLocal session) {
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
    public long getRowCount(SessionLocal session) {
        return table.getRowCount(session);
    }

    @Override
    public long getRowCountApproximation(SessionLocal session) {
        return table.getRowCountApproximation(session);
    }

}
