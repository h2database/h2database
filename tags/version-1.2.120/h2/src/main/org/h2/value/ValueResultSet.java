/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.h2.message.Message;
import org.h2.tools.SimpleResultSet;
import org.h2.util.StatementBuilder;

/**
 * Implementation of the RESULT_SET data type.
 */
public class ValueResultSet extends Value {

    private final ResultSet result;

    private ValueResultSet(ResultSet rs) {
        this.result = rs;
    }

    /**
     * Create a result set value for the given result set.
     * The result set will be wrapped.
     *
     * @param rs the result set
     * @return the value
     */
    public static ValueResultSet get(ResultSet rs) {
        ValueResultSet val = new ValueResultSet(rs);
        return val;
    }

    /**
     * Create a result set value for the given result set. The result set will
     * be fully read in memory.
     *
     * @param rs the result set
     * @param maxrows the maximum number of rows to read (0 to just read the
     *            meta data)
     * @return the value
     */
    public static ValueResultSet getCopy(ResultSet rs, int maxrows) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();
        SimpleResultSet simple = new SimpleResultSet();
        ValueResultSet val = new ValueResultSet(simple);
        for (int i = 0; i < columnCount; i++) {
            String name = meta.getColumnLabel(i + 1);
            int sqlType = meta.getColumnType(i + 1);
            int precision = meta.getPrecision(i + 1);
            int scale = meta.getScale(i + 1);
            simple.addColumn(name, sqlType, precision, scale);
        }
        for (int i = 0; i < maxrows && rs.next(); i++) {
            Object[] list = new Object[columnCount];
            for (int j = 0; j < columnCount; j++) {
                list[j] = rs.getObject(j + 1);
            }
            simple.addRow(list);
        }
        return val;
    }

    public int getType() {
        return Value.RESULT_SET;
    }

    public long getPrecision() {
        return 0;
    }

    public int getDisplaySize() {
        // it doesn't make sense to calculate it
        return Integer.MAX_VALUE;
    }

    public String getString() {
        try {
            StatementBuilder buff = new StatementBuilder("(");
            result.beforeFirst();
            ResultSetMetaData meta = result.getMetaData();
            int columnCount = meta.getColumnCount();
            for (int i = 0; result.next(); i++) {
                if (i > 0) {
                    buff.append(", ");
                }
                buff.append('(');
                buff.resetCount();
                for (int j = 0; j < columnCount; j++) {
                    buff.appendExceptFirst(", ");
                    int t = DataType.convertSQLTypeToValueType(meta.getColumnType(j + 1));
                    Value v = DataType.readValue(null, result, j+1, t);
                    buff.append(v.getString());
                }
                buff.append(')');
            }
            result.beforeFirst();
            return buff.append(')').toString();
        } catch (SQLException e) {
            throw Message.convertToInternal(e);
        }
    }

    protected int compareSecure(Value v, CompareMode mode) throws SQLException {
        throw throwUnsupportedExceptionForType();
    }

    public boolean equals(Object other) {
        return other == this;
    }

    public int hashCode() {
        return 0;
    }

    public Object getObject() {
        return result;
    }

    public ResultSet getResultSet() {
        return result;
    }

    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        throw throwUnsupportedExceptionForType();
    }

    public String getSQL() {
        return "";
    }

}
