/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.schema.SchemaObject;
import org.h2.table.Column;
import org.h2.table.Table;

public interface Index extends SchemaObject {

    public static final int EMPTY_HEAD = -1;

    public abstract SQLException getDuplicateKeyException();

    public abstract String getPlanSQL();

    public abstract void close(Session session) throws SQLException;

    public abstract void add(Session session, Row row) throws SQLException;

    public abstract void remove(Session session, Row row) throws SQLException;

    public abstract Cursor find(Session session, SearchRow first, SearchRow last)
            throws SQLException;

    public abstract double getCost(Session session, int[] masks)
            throws SQLException;

    public abstract void remove(Session session) throws SQLException;

    public abstract void truncate(Session session) throws SQLException;

    public abstract boolean canGetFirstOrLast(boolean first);

    public abstract SearchRow findFirstOrLast(Session session, boolean first)
            throws SQLException;

    public abstract boolean needRebuild();

    public abstract long getRowCount(Session session);

    public abstract int getLookupCost(long rowCount);

    public abstract long getCostRangeIndex(int[] masks, long rowCount)
            throws SQLException;

    public abstract int compareRows(SearchRow rowData, SearchRow compare)
            throws SQLException;

    public abstract boolean isNull(Row newRow);

    public abstract int compareKeys(SearchRow rowData, SearchRow compare);

    public abstract int getColumnIndex(Column col);

    public abstract String getColumnListSQL();

    public abstract Column[] getColumns();

    public abstract IndexType getIndexType();

    public abstract Table getTable();

    public abstract void commit(Row row) throws SQLException;

}