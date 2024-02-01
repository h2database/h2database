/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbc;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import org.h2.api.ErrorCode;
import org.h2.message.DbException;
import org.h2.message.TraceObject;
import org.h2.result.SimpleResult;
import org.h2.value.DataType;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueBigint;
import org.h2.value.ValueToObjectConverter;

/**
 * Represents an ARRAY value.
 */
public final class JdbcArray extends TraceObject implements Array {

    private ValueArray value;
    private final JdbcConnection conn;

    /**
     * INTERNAL
     * @param conn it belongs to
     * @param value of
     * @param id of the trace object
     */
    public JdbcArray(JdbcConnection conn, Value value, int id) {
        setTrace(conn.getSession().getTrace(), TraceObject.ARRAY, id);
        this.conn = conn;
        this.value = value.convertToAnyArray(conn);
    }

    /**
     * Returns the value as a Java array.
     * This method always returns an Object[].
     *
     * @return the Object array
     */
    @Override
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
     */
    @Override
    public Object getArray(Map<String, Class<?>> map) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getArray(" + quoteMap(map) + ')');
            }
            JdbcConnection.checkMap(map);
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
     */
    @Override
    public Object getArray(long index, int count) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getArray(" + index + ", " + count + ')');
            }
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
     */
    @Override
    public Object getArray(long index, int count, Map<String, Class<?>> map)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getArray(" + index + ", " + count + ", " + quoteMap(map) + ')');
            }
            checkClosed();
            JdbcConnection.checkMap(map);
            return get(index, count);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the base type of the array.
     *
     * @return the base type or Types.NULL
     */
    @Override
    public int getBaseType() throws SQLException {
        try {
            debugCodeCall("getBaseType");
            checkClosed();
            return DataType.convertTypeToSQLType(value.getComponentType());
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the base type name of the array. This database does support mixed
     * type arrays and therefore there is no base type.
     *
     * @return the base type name or "NULL"
     */
    @Override
    public String getBaseTypeName() throws SQLException {
        try {
            debugCodeCall("getBaseTypeName");
            checkClosed();
            return value.getComponentType().getDeclaredTypeName();
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
     */
    @Override
    public ResultSet getResultSet() throws SQLException {
        try {
            debugCodeCall("getResultSet");
            checkClosed();
            return getResultSetImpl(1L, Integer.MAX_VALUE);
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
     */
    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getResultSet(" + quoteMap(map) + ')');
            }
            checkClosed();
            JdbcConnection.checkMap(map);
            return getResultSetImpl(1L, Integer.MAX_VALUE);
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
     */
    @Override
    public ResultSet getResultSet(long index, int count) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getResultSet(" + index + ", " + count + ')');
            }
            checkClosed();
            return getResultSetImpl(index, count);
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
     */
    @Override
    public ResultSet getResultSet(long index, int count,
            Map<String, Class<?>> map) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getResultSet(" + index + ", " + count + ", " + quoteMap(map) + ')');
            }
            checkClosed();
            JdbcConnection.checkMap(map);
            return getResultSetImpl(index, count);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Release all resources of this object.
     */
    @Override
    public void free() {
        debugCodeCall("free");
        value = null;
    }

    private ResultSet getResultSetImpl(long index, int count) {
        int id = getNextId(TraceObject.RESULT_SET);
        SimpleResult rs = new SimpleResult();
        rs.addColumn("INDEX", TypeInfo.TYPE_BIGINT);
        rs.addColumn("VALUE", value.getComponentType());
        Value[] values = value.getList();
        count = checkRange(index, count, values.length);
        for (int i = (int) index; i < index + count; i++) {
            rs.addRow(ValueBigint.get(i), values[i - 1]);
        }
        return new JdbcResultSet(conn, null, null, rs, id, true, false, false);
    }

    private void checkClosed() {
        conn.checkClosed();
        if (value == null) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED);
        }
    }

    private Object get() {
        return ValueToObjectConverter.valueToDefaultArray(value, conn, true);
    }

    private Object get(long index, int count) {
        Value[] values = value.getList();
        count = checkRange(index, count, values.length);
        Object[] a = new Object[count];
        for (int i = 0, j = (int) index - 1; i < count; i++, j++) {
            a[i] = ValueToObjectConverter.valueToDefaultObject(values[j], conn, true);
        }
        return a;
    }

    private static int checkRange(long index, int count, int len) {
        if (index < 1 || (index != 1 && index > len)) {
            throw DbException.getInvalidValueException("index (1.." + len + ')', index);
        }
        int rem = len - (int) index + 1;
        if (count < 0) {
            throw DbException.getInvalidValueException("count (0.." + rem + ')', count);
        }
        return Math.min(rem, count);
    }

    /**
     * INTERNAL
     */
    @Override
    public String toString() {
        return value == null ? "null" :
            (getTraceObjectName() + ": " + value.getTraceSQL());
    }
}
