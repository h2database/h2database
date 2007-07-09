/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.h2.message.Message;
import org.h2.tools.SimpleResultSet;

public class ValueResultSet extends Value {

    private final ResultSet result;
    
    private ValueResultSet(ResultSet rs) {
        this.result = rs;
    }
    
    public static ValueResultSet get(ResultSet rs) throws SQLException {
        ValueResultSet val = new ValueResultSet(rs);
        return val;
    }
    
    public static ValueResultSet getCopy(ResultSet rs, int maxrows) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();
        SimpleResultSet simple = new SimpleResultSet();
        ValueResultSet val = new ValueResultSet(simple);
        for(int i=0; i<columnCount; i++) {
            String name = meta.getColumnLabel(i+1);
            int sqlType = meta.getColumnType(i+1);
            int precision = meta.getPrecision(i+1);
            int scale = meta.getScale(i+1);
            simple.addColumn(name, sqlType, precision, scale);
        }
        for(int i=0; i<maxrows && rs.next(); i++) {
            Object[] list = new Object[columnCount];
            for(int j=0; j<columnCount; j++) {
                list[j] = rs.getObject(j+1);
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
        // it doesn't make sense to calculate this
        return 100;
    }

    public String getString() throws SQLException {
        StringBuffer buff = new StringBuffer();
        buff.append("(");
        result.beforeFirst();
        ResultSetMetaData meta = result.getMetaData();
        int columnCount = meta.getColumnCount();
        for(int i=0; result.next(); i++) {
            if(i>0) {
                buff.append(", ");
            }
            buff.append('(');
            for(int j=0; j<columnCount; j++) {
                if (j > 0) {
                    buff.append(", ");
                }
                int t = DataType.convertSQLTypeToValueType(meta.getColumnType(j + 1));
                Value v = DataType.readValue(null, result, j+1, t);
                buff.append(v.getString());
            }
            buff.append(')');
        }
        buff.append(")");
        result.beforeFirst();
        return buff.toString();
    }

    protected int compareSecure(Value v, CompareMode mode) throws SQLException {
        throw Message.getUnsupportedException();
    }

    protected boolean isEqual(Value v) {
        return false;
    }

    public Object getObject() throws SQLException {
        return result;
    }
    
    public ResultSet getResultSet() {
        return result;
    }

    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        throw Message.getUnsupportedException();
    }

    public String getSQL() {
        return "";
    }       

}
