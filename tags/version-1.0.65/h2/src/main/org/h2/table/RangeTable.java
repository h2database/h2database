/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.sql.SQLException;

import org.h2.engine.Session;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.index.RangeIndex;
import org.h2.message.Message;
import org.h2.result.Row;
import org.h2.schema.Schema;
import org.h2.util.ObjectArray;
import org.h2.value.Value;

/**
 * The table SYSTEM_RANGE is a virtual table that generates incrementing numbers
 * with a given start end end point.
 */
public class RangeTable extends Table {

    public static final String NAME = "SYSTEM_RANGE";
    private final long min, max;

    public RangeTable(Schema schema, long min, long max) throws SQLException {
        super(schema, 0, NAME, true);
        Column[] cols = new Column[]{
                new Column("X", Value.LONG)
        };
        this.min = min;
        this.max = max;
        setColumns(cols);
    }

    public String getDropSQL() {
        return null;
    }

    public String getCreateSQL() {
        return null;
    }

    public String getSQL() {
        return NAME + "(" + min + ", " + max + ")";
    }

    public void lock(Session session, boolean exclusive, boolean force) throws SQLException {
    }

    public void close(Session session) throws SQLException {
    }

    public void unlock(Session s) {
    }

    public boolean isLockedExclusively() {
        return false;
    }

    public Index addIndex(Session session, String indexName, int indexId, IndexColumn[] cols, IndexType indexType, int headPos, String comment) throws SQLException {
        throw Message.getUnsupportedException();
    }

    public void removeRow(Session session, Row row) throws SQLException {
        throw Message.getUnsupportedException();
    }

    public void addRow(Session session, Row row) throws SQLException {
        throw Message.getUnsupportedException();
    }

    public void checkSupportAlter() throws SQLException {
        throw Message.getUnsupportedException();
    }

    public void checkRename() throws SQLException {
        throw Message.getUnsupportedException();
    }

    public boolean canGetRowCount() {
        return true;
    }

    public boolean canDrop() {
        return false;
    }

    public long getRowCount(Session session) throws SQLException {
        return max - min;
    }

    public String getTableType() {
        throw Message.getInternalError();
    }

    public Index getScanIndex(Session session) throws SQLException {
        return new RangeIndex(this, IndexColumn.wrap(columns), min, max);
    }

    public ObjectArray getIndexes() {
        return null;
    }

    public void truncate(Session session) throws SQLException {
        throw Message.getUnsupportedException();
    }

    public long getMaxDataModificationId() {
        return 0;
    }

    public Index getUniqueIndex() {
        return null;
    }

}
