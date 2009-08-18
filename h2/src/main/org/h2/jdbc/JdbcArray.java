/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbc;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;
import org.h2.constant.ErrorCode;
import org.h2.message.Message;
import org.h2.message.TraceObject;
import org.h2.tools.SimpleResultSet;
import org.h2.value.Value;

/**
 * Represents an ARRAY value.
 */
public class JdbcArray extends TraceObject implements Array {

    private Value value;
    private JdbcConnection conn;

    /**
     * INTERNAL
     */
    JdbcArray(JdbcConnection conn, Value value, int id) {
        setTrace(conn.getSession().getTrace(), TraceObject.ARRAY, id);
        this.conn = conn;
        this.value = value;
    }

    /**
     * Returns the value as a Java array.
     * This method always returns an Object[].
     *
     * @return the Object array
     * @throws SQLException
     */
    public Object getArray() throws SQLException {
        try {
            debugCodeCall("getArray");
            checkClosed();
            return get();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value as a Java array.
     * This method always returns an Object[].
     *
     * @param map is ignored. Only empty or null maps are supported
     * @return the Object array
     * @throws SQLException
     */
    public Object getArray(Map<String, Class< ? >> map) throws SQLException {
        try {
            debugCode("getArray("+quoteMap(map)+");");
            checkMap(map);
            checkClosed();
            return get();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value as a Java array. A subset of the array is returned,
     * starting from the index (1 meaning the first element) and up to the given
     * object count. This method always returns an Object[].
     *
     * @param index the start index of the subset (starting with 1)
     * @param count the maximum number of values
     * @return the Object array
     * @throws SQLException
     */
    public Object getArray(long index, int count) throws SQLException {
        try {
            debugCode("getArray(" + index + ", " + count + ");");
            checkClosed();
            return get(index, count);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value as a Java array. A subset of the array is returned,
     * starting from the index (1 meaning the first element) and up to the given
     * object count. This method always returns an Object[].
     *
     * @param index the start index of the subset (starting with 1)
     * @param count the maximum number of values
     * @param map is ignored. Only empty or null maps are supported
     * @return the Object array
     * @throws SQLException
     */
    public Object getArray(long index, int count, Map<String, Class< ? >> map) throws SQLException {
        try {
            debugCode("getArray(" + index + ", " + count + ", " + quoteMap(map)+");");
            checkClosed();
            checkMap(map);
            return get(index, count);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the base type of the array. This database does support mixed type
     * arrays and therefore there is no base type.
     *
     * @return Types.NULL
     * @throws SQLException
     */
    public int getBaseType() throws SQLException {
        try {
            debugCodeCall("getBaseType");
            checkClosed();
            return Types.NULL;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the base type name of the array. This database does support mixed
     * type arrays and therefore there is no base type.
     *
     * @return "NULL"
     * @throws SQLException
     */
    public String getBaseTypeName() throws SQLException {
        try {
            debugCodeCall("getBaseTypeName");
            checkClosed();
            return "NULL";
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value as a result set.
     * The first column contains the index
     * (starting with 1) and the second column the value.
     *
     * @return the result set
     * @throws SQLException
     */
    public ResultSet getResultSet() throws SQLException {
        try {
            debugCodeCall("getResultSet");
            checkClosed();
            return getResultSet(get(), 0);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value as a result set. The first column contains the index
     * (starting with 1) and the second column the value.
     *
     * @param map is ignored. Only empty or null maps are supported
     * @return the result set
     * @throws SQLException
     */
    public ResultSet getResultSet(Map<String, Class< ? >> map) throws SQLException {
        try {
            debugCode("getResultSet("+quoteMap(map)+");");
            checkClosed();
            checkMap(map);
            return getResultSet(get(), 0);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value as a result set. The first column contains the index
     * (starting with 1) and the second column the value. A subset of the array
     * is returned, starting from the index (1 meaning the first element) and
     * up to the given object count.
     *
     * @param index the start index of the subset (starting with 1)
     * @param count the maximum number of values
     * @return the result set
     * @throws SQLException
     */
    public ResultSet getResultSet(long index, int count) throws SQLException {
        try {
            debugCode("getResultSet("+index+", " + count+");");
            checkClosed();
            return getResultSet(get(index, count), index);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value as a result set.
     * The first column contains the index
     * (starting with 1) and the second column the value.
     * A subset of the array is returned, starting from the index
     * (1 meaning the first element) and up to the given object count.
     *
     * @param index the start index of the subset (starting with 1)
     * @param count the maximum number of values
     * @param map is ignored. Only empty or null maps are supported
     * @return the result set
     * @throws SQLException
     */
    public ResultSet getResultSet(long index, int count, Map<String, Class< ? >> map) throws SQLException {
        try {
            debugCode("getResultSet("+index+", " + count+", " + quoteMap(map)+");");
            checkClosed();
            checkMap(map);
            return getResultSet(get(index, count), index);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Release all resources of this object.
     */
    public void free() {
        debugCodeCall("free");
        value = null;
    }

    private ResultSet getResultSet(Object[] array, long offset) throws SQLException {
        SimpleResultSet rs = new SimpleResultSet();
        rs.addColumn("INDEX", Types.BIGINT, 0, 0);
        // TODO array result set: there are multiple data types possible
        rs.addColumn("VALUE", Types.NULL, 0, 0);
        for (int i = 0; i < array.length; i++) {
            rs.addRow(Long.valueOf(offset + i + 1), array[i]);
        }
        return rs;
    }

    private void checkClosed() throws SQLException {
        conn.checkClosed();
        if (value == null) {
            throw Message.getSQLException(ErrorCode.OBJECT_CLOSED);
        }
    }

    private Object[] get() throws SQLException {
        return (Object[]) value.convertTo(Value.ARRAY).getObject();
    }

    private Object[] get(long index, int count) throws SQLException {
        Object[] array = get();
        if (count < 0 || count > array.length) {
            throw Message.getInvalidValueException("" + count, "count (1.."
                    + array.length + ")");
        }
        if (index < 1 || index > array.length) {
            throw Message.getInvalidValueException("" + index, "index (1.."
                    + array.length + ")");
        }
        Object[] subset = new Object[count];
        System.arraycopy(array, (int) (index - 1), subset, 0, count);
        return subset;
    }

    private void checkMap(Map<String, Class< ? >> map) throws SQLException {
        if (map != null && map.size() > 0) {
            throw Message.getUnsupportedException("map.size > 0");
        }
    }

    /**
     * INTERNAL
     */
    public String toString() {
        return getTraceObjectName() + ": " + value.getTraceSQL();
    }
}
