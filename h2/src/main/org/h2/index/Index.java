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
import org.h2.table.IndexColumn;
import org.h2.table.Table;

public interface Index extends SchemaObject {

    int EMPTY_HEAD = -1;

    SQLException getDuplicateKeyException();

    String getPlanSQL();

    void close(Session session) throws SQLException;

    void add(Session session, Row row) throws SQLException;

    void remove(Session session, Row row) throws SQLException;

    Cursor find(Session session, SearchRow first, SearchRow last) throws SQLException;

    double getCost(Session session, int[] masks) throws SQLException;

    void remove(Session session) throws SQLException;

    void truncate(Session session) throws SQLException;

    boolean canGetFirstOrLast(boolean first);

    SearchRow findFirstOrLast(Session session, boolean first) throws SQLException;

    boolean needRebuild();

    long getRowCount(Session session);

    int getLookupCost(long rowCount);

    long getCostRangeIndex(int[] masks, long rowCount) throws SQLException;

    int compareRows(SearchRow rowData, SearchRow compare) throws SQLException;

    boolean isNull(Row newRow);

    int compareKeys(SearchRow rowData, SearchRow compare);

    int getColumnIndex(Column col);

    String getColumnListSQL();

    IndexColumn[] getIndexColumns();

    Column[] getColumns();

    IndexType getIndexType();

    Table getTable();

    void commit(int operation, Row row) throws SQLException;

}
