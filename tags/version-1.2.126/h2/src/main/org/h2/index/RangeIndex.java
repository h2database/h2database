/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;

import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.IndexColumn;
import org.h2.table.RangeTable;

/**
 * An index for the SYSTEM_RANGE table.
 * This index can only scan through all rows, search is not supported.
 */
public class RangeIndex extends BaseIndex {

    private RangeTable rangeTable;

    public RangeIndex(RangeTable table, IndexColumn[] columns) {
        initBaseIndex(table, 0, "RANGE_INDEX", columns, IndexType.createNonUnique(true));
        this.rangeTable = table;
    }

    public void close(Session session) {
        // nothing to do
    }

    public void add(Session session, Row row) throws SQLException {
        throw Message.getUnsupportedException("SYSTEM_RANGE");
    }

    public void remove(Session session, Row row) throws SQLException {
        throw Message.getUnsupportedException("SYSTEM_RANGE");
    }

    public Cursor find(Session session, SearchRow first, SearchRow last) throws SQLException {
        long min = rangeTable.getMin(session);
        long max = rangeTable.getMax(session);
        long start = Math.max(min, first == null ? min : first.getValue(0).getLong());
        long end = Math.min(max, last == null ? max : last.getValue(0).getLong());
        return new RangeCursor(start, end);
    }

    public double getCost(Session session, int[] masks) {
        return 1;
    }

    public String getCreateSQL() {
        return null;
    }

    public void remove(Session session) throws SQLException {
        throw Message.getUnsupportedException("SYSTEM_RANGE");
    }

    public void truncate(Session session) throws SQLException {
        throw Message.getUnsupportedException("SYSTEM_RANGE");
    }

    public boolean needRebuild() {
        return false;
    }

    public void checkRename() throws SQLException {
        throw Message.getUnsupportedException("SYSTEM_RANGE");
    }

    public boolean canGetFirstOrLast() {
        return true;
    }

    public Cursor findFirstOrLast(Session session, boolean first) throws SQLException {
        long pos = first ? rangeTable.getMin(session) : rangeTable.getMax(session);
        return new RangeCursor(pos, pos);
    }

    public long getRowCount(Session session) {
        return rangeTable.getRowCountApproximation();
    }

    public long getRowCountApproximation() {
        return rangeTable.getRowCountApproximation();
    }

}
