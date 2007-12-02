/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
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
import org.h2.value.Value;
import org.h2.value.ValueLong;

public class RangeIndex extends BaseIndex {
    
    private long min, max; 
    
    public RangeIndex(RangeTable table, IndexColumn[] columns, long min, long max) {
        super(table, 0, "RANGE_INDEX", columns, IndexType.createNonUnique(true));
        this.min = min;
        this.max = max;
    }

    public void close(Session session) throws SQLException {
    }

    public void add(Session session, Row row) throws SQLException {
        throw Message.getUnsupportedException();
    }

    public void remove(Session session, Row row) throws SQLException {
        throw Message.getUnsupportedException();
    }

    public Cursor find(Session session, SearchRow first, SearchRow last) throws SQLException {
        long start = Math.max(min, first == null ? min : first.getValue(0).getLong());
        long end = Math.min(max, last == null ? max : last.getValue(0).getLong());
        return new RangeCursor(start, end);
    }

    public double getCost(Session session, int[] masks) throws SQLException {
        return 1;
    }

    public String getCreateSQL() {
        return null;
    }

    public void remove(Session session) throws SQLException {
        throw Message.getUnsupportedException();
    }

    public void truncate(Session session) throws SQLException {
        throw Message.getUnsupportedException();
    }

    public boolean needRebuild() {
        return false;
    }

    public void checkRename() throws SQLException {
        throw Message.getUnsupportedException();
    }

    public boolean canGetFirstOrLast() {
        return true;
    }

    public SearchRow findFirstOrLast(Session session, boolean first) throws SQLException {
        return new Row(new Value[] { ValueLong.get(first ? min : max) }, 0);
    }

}
