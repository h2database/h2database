/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.util.ArrayList;

import org.h2.engine.SessionLocal;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.schema.Schema;

/**
 * A base class for virtual tables.
 */
public abstract class VirtualTable extends Table {

    protected VirtualTable(Schema schema, int id, String name) {
        super(schema, id, name, false, true);
    }

    @Override
    public void close(SessionLocal session) {
        // Nothing to do
    }

    @Override
    public Index addIndex(SessionLocal session, String indexName, int indexId, IndexColumn[] cols,
            int uniqueColumnCount, IndexType indexType, boolean create, String indexComment) {
        throw DbException.getUnsupportedException("Virtual table");
    }

    @Override
    public boolean isInsertable() {
        return false;
    }

    @Override
    public void removeRow(SessionLocal session, Row row) {
        throw DbException.getUnsupportedException("Virtual table");

    }

    @Override
    public long truncate(SessionLocal session) {
        throw DbException.getUnsupportedException("Virtual table");
    }

    @Override
    public void addRow(SessionLocal session, Row row) {
        throw DbException.getUnsupportedException("Virtual table");
    }

    @Override
    public void checkSupportAlter() {
        throw DbException.getUnsupportedException("Virtual table");
    }

    @Override
    public TableType getTableType() {
        return null;
    }

    @Override
    public ArrayList<Index> getIndexes() {
        return null;
    }

    @Override
    public boolean canReference() {
        return false;
    }

    @Override
    public boolean canDrop() {
        throw DbException.getInternalError(toString());
    }

    @Override
    public String getCreateSQL() {
        return null;
    }

    @Override
    public void checkRename() {
        throw DbException.getUnsupportedException("Virtual table");
    }

}
