/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableLink;
import org.h2.util.StatementBuilder;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * A linked index is a index for a linked (remote) table.
 * It is backed by an index on the remote table which is accessed over JDBC.
 */
public class LinkedIndex extends BaseIndex {

    private TableLink link;
    private String targetTableName;
    private long rowCount;

    public LinkedIndex(TableLink table, int id, IndexColumn[] columns, IndexType indexType) {
        initBaseIndex(table, id, null, columns, indexType);
        link = table;
        targetTableName = link.getQualifiedTable();
    }

    public String getCreateSQL() {
        return null;
    }

    public void close(Session session) {
        // nothing to do
    }

    private boolean isNull(Value v) {
        return v == null || v == ValueNull.INSTANCE;
    }

    public void add(Session session, Row row) throws SQLException {
        StatementBuilder buff = new StatementBuilder("INSERT INTO ");
        buff.append(targetTableName).append(" VALUES(");
        for (int i = 0; i < row.getColumnCount(); i++) {
            Value v = row.getValue(i);
            buff.appendExceptFirst(",");
            if (isNull(v)) {
                buff.append("NULL");
            } else {
                buff.append('?');
            }
        }
        buff.append(')');
        String sql = buff.toString();
        synchronized (link.getConnection()) {
            try {
                PreparedStatement prep = link.getPreparedStatement(sql, false);
                for (int i = 0, j = 0; i < row.getColumnCount(); i++) {
                    Value v = row.getValue(i);
                    if (v != null && v != ValueNull.INSTANCE) {
                        v.set(prep, j + 1);
                        j++;
                    }
                }
                prep.executeUpdate();
                rowCount++;
            } catch (SQLException e) {
                throw link.wrapException(sql, e);
            }
        }
    }

    public Cursor find(Session session, SearchRow first, SearchRow last) throws SQLException {
        StatementBuilder buff = new StatementBuilder("SELECT * FROM ");
        buff.append(targetTableName).append(" T");
        for (int i = 0; first != null && i < first.getColumnCount(); i++) {
            Value v = first.getValue(i);
            if (v != null) {
                buff.appendOnlyFirst(" WHERE ");
                buff.appendExceptFirst(" AND ");
                Column col = table.getColumn(i);
                buff.append(col.getSQL()).append(">=");
                addParameter(buff, col);
            }
        }
        for (int i = 0; last != null && i < last.getColumnCount(); i++) {
            Value v = last.getValue(i);
            if (v != null) {
                buff.appendOnlyFirst(" WHERE ");
                buff.appendExceptFirst(" AND ");
                Column col = table.getColumn(i);
                buff.append(col.getSQL()).append("<=");
                addParameter(buff, col);
            }
        }
        String sql = buff.toString();
        synchronized (link.getConnection()) {
            try {
                PreparedStatement prep = link.getPreparedStatement(sql, true);
                int j = 0;
                for (int i = 0; first != null && i < first.getColumnCount(); i++) {
                    Value v = first.getValue(i);
                    if (v != null) {
                        v.set(prep, j + 1);
                        j++;
                    }
                }
                for (int i = 0; last != null && i < last.getColumnCount(); i++) {
                    Value v = last.getValue(i);
                    if (v != null) {
                        v.set(prep, j + 1);
                        j++;
                    }
                }
                ResultSet rs = prep.executeQuery();
                return new LinkedCursor(link, rs, session, sql, prep);
            } catch (SQLException e) {
                throw link.wrapException(sql, e);
            }
        }
    }

    private void addParameter(StatementBuilder buff, Column col) {
        if (col.getType() == Value.STRING_FIXED && link.isOracle()) {
            // workaround for Oracle
            // create table test(id int primary key, name char(15));
            // insert into test values(1, 'Hello')
            // select * from test where name = ? -- where ? = "Hello" > no rows
            buff.append("CAST(? AS CHAR(").append(col.getPrecision()).append("))");
        } else {
            buff.append('?');
        }
    }

    public double getCost(Session session, int[] masks) {
        return 100 + getCostRangeIndex(masks, rowCount + Constants.COST_ROW_OFFSET);
    }

    public void remove(Session session) {
        // nothing to do
    }

    public void truncate(Session session) {
        // nothing to do
    }

    public void checkRename() throws SQLException {
        throw Message.getUnsupportedException("LINKED");
    }

    public boolean needRebuild() {
        return false;
    }

    public boolean canGetFirstOrLast() {
        return false;
    }

    public Cursor findFirstOrLast(Session session, boolean first) throws SQLException {
        // TODO optimization: could get the first or last value (in any case;
        // maybe not optimized)
        throw Message.getUnsupportedException("LINKED");
    }

    public void remove(Session session, Row row) throws SQLException {
        StatementBuilder buff = new StatementBuilder("DELETE FROM ");
        buff.append(targetTableName).append(" WHERE ");
        for (int i = 0; i < row.getColumnCount(); i++) {
            buff.appendExceptFirst("AND ");
            Column col = table.getColumn(i);
            buff.append(col.getSQL());
            Value v = row.getValue(i);
            if (isNull(v)) {
                buff.append(" IS NULL ");
            } else {
                buff.append('=');
                addParameter(buff, col);
                buff.append(' ');
            }
        }
        String sql = buff.toString();
        synchronized (link.getConnection()) {
            try {
                PreparedStatement prep = link.getPreparedStatement(sql, false);
                for (int i = 0, j = 0; i < row.getColumnCount(); i++) {
                    Value v = row.getValue(i);
                    if (!isNull(v)) {
                        v.set(prep, j + 1);
                        j++;
                    }
                }
                int count = prep.executeUpdate();
                rowCount -= count;
            } catch (SQLException e) {
                throw link.wrapException(sql, e);
            }
        }
    }

    /**
     * Update a row using a UPDATE statement. This method is to be called if the
     * emit updates option is enabled.
     *
     * @param oldRow the old data
     * @param newRow the new data
     */
    public void update(Row oldRow, Row newRow) throws SQLException {
        StatementBuilder buff = new StatementBuilder("UPDATE ");
        buff.append(targetTableName).append(" SET ");
        for (int i = 0; i < newRow.getColumnCount(); i++) {
            buff.appendExceptFirst(", ");
            buff.append(table.getColumn(i).getSQL()).append("=?");
        }
        buff.append(" WHERE ");
        buff.resetCount();
        for (int i = 0; i < oldRow.getColumnCount(); i++) {
            Column col = table.getColumn(i);
            buff.appendExceptFirst(" AND ");
            buff.append(col.getSQL());
            Value v = oldRow.getValue(i);
            if (isNull(v)) {
                buff.append(" IS NULL");
            } else {
                buff.append('=');
                addParameter(buff, col);
            }
        }
        String sql = buff.toString();
        synchronized (link.getConnection()) {
            try {
                int j = 1;
                PreparedStatement prep = link.getPreparedStatement(sql, false);
                for (int i = 0; i < newRow.getColumnCount(); i++) {
                    newRow.getValue(i).set(prep, j);
                    j++;
                }
                for (int i = 0; i < oldRow.getColumnCount(); i++) {
                    Value v = oldRow.getValue(i);
                    if (!isNull(v)) {
                        v.set(prep, j);
                        j++;
                    }
                }
                int count = prep.executeUpdate();
                // this has no effect but at least it allows to debug the update count
                rowCount = rowCount + count - count;
            } catch (SQLException e) {
                throw link.wrapException(sql, e);
            }
        }
    }

    public long getRowCount(Session session) {
        return rowCount;
    }

    public long getRowCountApproximation() {
        return rowCount;
    }

}
