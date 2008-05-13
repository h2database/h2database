/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.IndexColumn;
import org.h2.table.TableLink;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * A linked index is a index for a linked (remote) table.
 * It is backed by an index on the remote table which is accessed over JDBC.
 */
public class LinkedIndex extends BaseIndex {

    private TableLink link;
    private String targetTableName;

    public LinkedIndex(TableLink table, int id, IndexColumn[] columns, IndexType indexType) {
        initBaseIndex(table, id, null, columns, indexType);
        link = table;
        targetTableName = link.getQualifiedTable();
    }

    public String getCreateSQL() {
        return null;
    }

    public void close(Session session) throws SQLException {
    }

    private boolean isNull(Value v) {
        return v == null || v == ValueNull.INSTANCE;
      }

    public void add(Session session, Row row) throws SQLException {
        StringBuffer buff = new StringBuffer("INSERT INTO ");
        buff.append(targetTableName);
        buff.append(" VALUES(");
        for (int i = 0, j = 0; i < row.getColumnCount(); i++) {
            Value v = row.getValue(i);
            if (j > 0) {
                buff.append(',');
            }
            j++;
            if (isNull(v)) {
                buff.append("NULL");
            } else {
                buff.append('?');
            }
        }
        buff.append(')');
        String sql = buff.toString();
        try {
            PreparedStatement prep = link.getPreparedStatement(sql);
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
            throw wrapException(sql, e);
        }
    }

    public Cursor find(Session session, SearchRow first, SearchRow last) throws SQLException {
        StringBuffer buff = new StringBuffer();
        for (int i = 0; first != null && i < first.getColumnCount(); i++) {
            Value v = first.getValue(i);
            if (v != null) {
                if (buff.length() != 0) {
                    buff.append(" AND ");
                }
                buff.append(table.getColumn(i).getSQL());
                buff.append(">=?");
            }
        }
        for (int i = 0; last != null && i < last.getColumnCount(); i++) {
            Value v = last.getValue(i);
            if (v != null) {
                if (buff.length() != 0) {
                    buff.append(" AND ");
                }
                buff.append(table.getColumn(i).getSQL());
                buff.append("<=?");
            }
        }
        if (buff.length() > 0) {
            buff.insert(0, " WHERE ");
        }
        buff.insert(0, "SELECT * FROM " + targetTableName + " T");
        String sql = buff.toString();
        try {
            PreparedStatement prep = link.getPreparedStatement(sql);
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
            return new LinkedCursor(table, rs, session);
        } catch (SQLException e) {
            throw wrapException(sql, e);
        }
    }

    public double getCost(Session session, int[] masks) throws SQLException {
        return 100 + getCostRangeIndex(masks, rowCount + Constants.COST_ROW_OFFSET);
    }

    public void remove(Session session) throws SQLException {
    }

    public void truncate(Session session) throws SQLException {
    }

    public void checkRename() throws SQLException {
        throw Message.getUnsupportedException();
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
        throw Message.getUnsupportedException();
    }

    public void remove(Session session, Row row) throws SQLException {
        StringBuffer buff = new StringBuffer("DELETE FROM ");
        buff.append(targetTableName);
        buff.append(" WHERE ");
        for (int i = 0; i < row.getColumnCount(); i++) {
            if (i > 0) {
                buff.append("AND ");
            }
            buff.append(table.getColumn(i).getSQL());
            Value v = row.getValue(i);
            if (isNull(v)) {
                buff.append(" IS NULL ");
            } else {
                buff.append("=? ");
            }
        }
        String sql = buff.toString();
        try {
            PreparedStatement prep = link.getPreparedStatement(sql);
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
            throw wrapException(sql, e);
        }
    }

    public void update(Session session, Row oldRow, Row newRow) throws SQLException {
        StringBuffer buff = new StringBuffer("UPDATE ");
        buff.append(targetTableName).append(" SET ");
        for (int i = 0; i < newRow.getColumnCount(); i++) {
            if (i > 0) {
                buff.append(", ");
            }
            buff.append(table.getColumn(i).getSQL()).append("=?");
        }
        buff.append(" WHERE ");
        for (int i = 0; i < oldRow.getColumnCount(); i++) {
            if (i > 0) {
                buff.append("AND ");
            }
            buff.append(table.getColumn(i).getSQL());
            Value v = oldRow.getValue(i);
            if (isNull(v)) {
                buff.append(" IS NULL ");
            } else {
                buff.append("=? ");
            }
        }
        String sql = buff.toString();
        try {
            int j = 1;
            PreparedStatement prep = link.getPreparedStatement(sql);
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
            throw wrapException(sql, e);
        }
    }

    private SQLException wrapException(String sql, SQLException e) {
        return Message.getSQLException(ErrorCode.ERROR_ACCESSING_LINKED_TABLE_2, new String[] { sql, e.toString() }, e);
    }

}
