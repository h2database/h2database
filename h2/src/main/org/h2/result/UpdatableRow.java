/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.engine.SessionInterface;
import org.h2.message.Message;
import org.h2.util.ObjectArray;
import org.h2.util.StringUtils;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueNull;

public class UpdatableRow {

    private SessionInterface session;
    private Connection conn;
    private DatabaseMetaData meta;
    private ResultInterface result;
    private int columnCount;
    private String schemaName;
    private String tableName;
    private ObjectArray key;
    private boolean isUpdatable;

    public UpdatableRow(Connection conn, ResultInterface result, SessionInterface session) throws SQLException {
        this.conn = conn;
        this.meta = conn.getMetaData();
        this.result = result;
        this.session = session;
        columnCount = result.getVisibleColumnCount();
        for (int i = 0; i < columnCount; i++) {
            String t = result.getTableName(i);
            String s = result.getSchemaName(i);
            if (t == null || s == null) {
                return;
            }
            if (tableName == null) {
                tableName = t;
            } else if (!tableName.equals(t)) {
                return;
            }
            if (schemaName == null) {
                schemaName = s;
            } else if (!schemaName.equals(s)) {
                return;
            }
        }
        ResultSet rs = meta.getTables(null, schemaName, tableName, new String[] { "TABLE" });
        if (!rs.next()) {
            return;
        }
        if (rs.getString("SQL") == null) {
            // system table
            return;
        }
        key = new ObjectArray();
        rs = meta.getPrimaryKeys(null, schemaName, tableName);
        while (rs.next()) {
            key.add(rs.getString("COLUMN_NAME"));
        }
        if (key.size() == 0) {
            return;
        }
        isUpdatable = true;
    }

    public boolean isUpdatable() {
        return isUpdatable;
    }

    void initKey() throws SQLException {
    }

    private int getColumnIndex(String columnName) throws SQLException {
        for (int i = 0; i < columnCount; i++) {
            String col = result.getColumnName(i);
            if (col.equals(columnName)) {
                return i;
            }
        }
        throw Message.getSQLException(ErrorCode.COLUMN_NOT_FOUND_1, columnName);
    }

    private void appendColumnList(StringBuffer buff, boolean set) {
        for (int i = 0; i < columnCount; i++) {
            if (i > 0) {
                buff.append(", ");
            }
            String col = result.getColumnName(i);
            buff.append(StringUtils.quoteIdentifier(col));
            if (set) {
                buff.append("=? ");
            }
        }
    }

    private void appendKeyCondition(StringBuffer buff) {
        buff.append(" WHERE ");
        for (int i = 0; i < key.size(); i++) {
            if (i > 0) {
                buff.append(" AND ");
            }
            buff.append(StringUtils.quoteIdentifier((String) key.get(i)));
            buff.append("=?");
        }
    }

    private void setKey(PreparedStatement prep, int start, Value[] current) throws SQLException {
        for (int i = 0; i < key.size(); i++) {
            String col = (String) key.get(i);
            int idx = getColumnIndex(col);
            Value v = current[idx];
            v.set(prep, start + i);
        }
    }

//    public boolean isRowDeleted(Value[] row) throws SQLException {
//        StringBuffer buff = new StringBuffer();
//        buff.append("SELECT COUNT(*) FROM ");
//        buff.append(StringUtils.quoteIdentifier(tableName));
//        appendKeyCondition(buff);
//        PreparedStatement prep = conn.prepareStatement(buff.toString());
//        setKey(prep, 1, row);
//        ResultSet rs = prep.executeQuery();
//        rs.next();
//        return rs.getInt(1) == 0;
//    }

    public void refreshRow(Value[] row) throws SQLException {
        Value[] newRow = readRow(row);
        for (int i = 0; i < columnCount; i++) {
            row[i] = newRow[i];
        }
    }

    private void appendTableName(StringBuffer buff) {
        if (schemaName != null && schemaName.length() > 0) {
            buff.append(StringUtils.quoteIdentifier(schemaName));
            buff.append('.');
        }
        buff.append(StringUtils.quoteIdentifier(tableName));
    }

    private Value[] readRow(Value[] row) throws SQLException {
        StringBuffer buff = new StringBuffer();
        buff.append("SELECT ");
        appendColumnList(buff, false);
        buff.append(" FROM ");
        appendTableName(buff);
        appendKeyCondition(buff);
        PreparedStatement prep = conn.prepareStatement(buff.toString());
        setKey(prep, 1, row);
        ResultSet rs = prep.executeQuery();
        if (!rs.next()) {
            throw Message.getSQLException(ErrorCode.NO_DATA_AVAILABLE);
        }
        Value[] newRow = new Value[columnCount];
        for (int i = 0; i < columnCount; i++) {
            int type = result.getColumnType(i);
            // TODO lob: support updatable rows
            newRow[i] = DataType.readValue(session, rs, i + 1, type);
        }
        return newRow;
    }

    public void deleteRow(Value[] current) throws SQLException {
        StringBuffer buff = new StringBuffer();
        buff.append("DELETE FROM ");
        appendTableName(buff);
        appendKeyCondition(buff);
        PreparedStatement prep = conn.prepareStatement(buff.toString());
        setKey(prep, 1, current);
        int count = prep.executeUpdate();
        if (count != 1) {
            throw Message.getSQLException(ErrorCode.NO_DATA_AVAILABLE);
        }
    }

    public void updateRow(Value[] current, Value[] updateRow) throws SQLException {
        StringBuffer buff = new StringBuffer();
        buff.append("UPDATE ");
        appendTableName(buff);
        buff.append(" SET ");
        appendColumnList(buff, true);
        // TODO updatable result set: we could add all current values to the
        // where clause
        // - like this optimistic ('no') locking is possible
        appendKeyCondition(buff);
        PreparedStatement prep = conn.prepareStatement(buff.toString());
        int j = 1;
        for (int i = 0; i < columnCount; i++) {
            Value v = updateRow[i];
            if (v == null) {
                v = current[i];
            }
            v.set(prep, j++);
        }
        setKey(prep, j, current);
        prep.execute();
    }

    public void insertRow(Value[] row) throws SQLException {
        StringBuffer buff = new StringBuffer();
        buff.append("INSERT INTO ");
        appendTableName(buff);
        buff.append("(");
        appendColumnList(buff, false);
        buff.append(")VALUES(");
        for (int i = 0; i < columnCount; i++) {
            if (i > 0) {
                buff.append(",");
            }
            buff.append("?");
        }
        buff.append(")");
        PreparedStatement prep = conn.prepareStatement(buff.toString());
        for (int i = 0; i < columnCount; i++) {
            Value v = row[i];
            if (v == null) {
                v = ValueNull.INSTANCE;
            }
            v.set(prep, i + 1);
        }
        prep.executeUpdate();
    }

}
