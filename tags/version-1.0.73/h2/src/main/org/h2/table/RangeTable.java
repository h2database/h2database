/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.sql.SQLException;

import org.h2.engine.Session;
import org.h2.expression.Expression;
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
    private Expression min, max;
    private boolean optimized;
    
    public RangeTable(Schema schema, Expression min, Expression max) throws SQLException {
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
        return NAME + "(" + min.getSQL() + ", " + max.getSQL() + ")";
    }

    public void lock(Session session, boolean exclusive, boolean force) {
        // nothing to do
    }

    public void close(Session session) {
        // nothing to do
    }

    public void unlock(Session s) {
        // nothing to do
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
        return getMax(session) - getMin(session);
    }

    public String getTableType() {
        throw Message.getInternalError();
    }

    public Index getScanIndex(Session session) {
        return new RangeIndex(this, IndexColumn.wrap(columns));
    }
    
    public long getMin(Session s) throws SQLException {
        optimize(s);
        return min.getValue(s).getLong();
    }

    public long getMax(Session s) throws SQLException {
        optimize(s);
        return max.getValue(s).getLong();
    }
    
    private void optimize(Session s) throws SQLException {
        if (!optimized) {
            min = min.optimize(s);
            max = max.optimize(s);
            optimized = true;
        }
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
