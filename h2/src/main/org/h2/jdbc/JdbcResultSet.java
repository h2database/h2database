/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.engine.Session;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.message.TraceObject;
import org.h2.result.ResultInterface;
import org.h2.result.UpdatableRow;
import org.h2.util.IOUtils;
import org.h2.util.LegacyDateTimeUtils;
import org.h2.util.StringUtils;
import org.h2.value.CompareMode;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueBigint;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueDouble;
import org.h2.value.ValueInteger;
import org.h2.value.ValueNull;
import org.h2.value.ValueNumeric;
import org.h2.value.ValueReal;
import org.h2.value.ValueSmallint;
import org.h2.value.ValueTinyint;
import org.h2.value.ValueToObjectConverter;
import org.h2.value.ValueVarbinary;
import org.h2.value.ValueVarchar;

/**
 * Represents a result set.
 * <p>
 * Column labels are case-insensitive, quotes are not supported. The first
 * column has the column index 1.
 * </p>
 * <p>
 * Thread safety: the result set is not thread-safe and must not be used by
 * multiple threads concurrently.
 * </p>
 * <p>
 * Updatable result sets: Result sets are updatable when the result only
 * contains columns from one table, and if it contains all columns of a unique
 * index (primary key or other) of this table. Key columns may not contain NULL
 * (because multiple rows with NULL could exist). In updatable result sets, own
 * changes are visible, but not own inserts and deletes.
 * </p>
 */
public final class JdbcResultSet extends TraceObject implements ResultSet {

    private final boolean scrollable;
    private final boolean updatable;
    private final boolean triggerUpdatable;
    ResultInterface result;
    private JdbcConnection conn;
    private JdbcStatement stat;
    private int columnCount;
    private boolean wasNull;
    private Value[] insertRow;
    private Value[] updateRow;
    private HashMap<String, Integer> columnLabelMap;
    private HashMap<Long, Value[]> patchedRows;
    private JdbcPreparedStatement preparedStatement;
    private final CommandInterface command;

    public JdbcResultSet(JdbcConnection conn, JdbcStatement stat, CommandInterface command, ResultInterface result,
            int id, boolean scrollable, boolean updatable, boolean triggerUpdatable) {
        setTrace(conn.getSession().getTrace(), TraceObject.RESULT_SET, id);
        this.conn = conn;
        this.stat = stat;
        this.command = command;
        this.result = result;
        this.columnCount = result.getVisibleColumnCount();
        this.scrollable = scrollable;
        this.updatable = updatable;
        this.triggerUpdatable = triggerUpdatable;
    }

    JdbcResultSet(JdbcConnection conn, JdbcPreparedStatement preparedStatement, CommandInterface command,
            ResultInterface result, int id, boolean scrollable, boolean updatable,
            HashMap<String, Integer> columnLabelMap) {
        this(conn, preparedStatement, command, result, id, scrollable, updatable, false);
        this.columnLabelMap = columnLabelMap;
        this.preparedStatement = preparedStatement;
    }

    /**
     * Moves the cursor to the next row of the result set.
     *
     * @return true if successful, false if there are no more rows
     */
    @Override
    public boolean next() throws SQLException {
        try {
            debugCodeCall("next");
            checkClosed();
            return nextRow();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the meta data of this result set.
     *
     * @return the meta data
     */
    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        try {
            int id = getNextId(TraceObject.RESULT_SET_META_DATA);
            debugCodeAssign("ResultSetMetaData", TraceObject.RESULT_SET_META_DATA, id, "getMetaData()");
            checkClosed();
            String catalog = conn.getCatalog();
            return new JdbcResultSetMetaData(this, null, result, catalog, conn.getSession().getTrace(), id);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns whether the last column accessed was null.
     *
     * @return true if the last column accessed was null
     */
    @Override
    public boolean wasNull() throws SQLException {
        try {
            debugCodeCall("wasNull");
            checkClosed();
            return wasNull;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Searches for a specific column in the result set. A case-insensitive
     * search is made.
     *
     * @param columnLabel the column label
     * @return the column index (1,2,...)
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public int findColumn(String columnLabel) throws SQLException {
        try {
            debugCodeCall("findColumn", columnLabel);
            return getColumnIndex(columnLabel);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Closes the result set.
     */
    @Override
    public void close() throws SQLException {
        try {
            debugCodeCall("close");
            closeInternal(false);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Close the result set. This method also closes the statement if required.
     * @param fromStatement if true - close statement in the end
     */
    void closeInternal(boolean fromStatement) {
        if (result != null) {
            try {
                if (result.isLazy()) {
                    stat.onLazyResultSetClose(command, preparedStatement == null);
                }
                result.close();
            } finally {
                JdbcStatement s = stat;
                columnCount = 0;
                result = null;
                stat = null;
                conn = null;
                insertRow = null;
                updateRow = null;
                if (!fromStatement && s != null) {
                    s.closeIfCloseOnCompletion();
                }
            }
        }
    }

    /**
     * Returns the statement that created this object.
     *
     * @return the statement or prepared statement, or null if created by a
     *         DatabaseMetaData call.
     */
    @Override
    public Statement getStatement() throws SQLException {
        try {
            debugCodeCall("getStatement");
            checkClosed();
            return stat;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the first warning reported by calls on this object.
     *
     * @return null
     */
    @Override
    public SQLWarning getWarnings() throws SQLException {
        try {
            debugCodeCall("getWarnings");
            checkClosed();
            return null;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Clears all warnings.
     */
    @Override
    public void clearWarnings() throws SQLException {
        try {
            debugCodeCall("clearWarnings");
            checkClosed();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    // =============================================================

    /**
     * Returns the value of the specified column as a String.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public String getString(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getString", columnIndex);
            return get(checkColumnIndex(columnIndex)).getString();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a String.
     *
     * @param columnLabel the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public String getString(String columnLabel) throws SQLException {
        try {
            debugCodeCall("getString", columnLabel);
            return get(getColumnIndex(columnLabel)).getString();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as an int.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public int getInt(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getInt", columnIndex);
            return getIntInternal(checkColumnIndex(columnIndex));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as an int.
     *
     * @param columnLabel the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public int getInt(String columnLabel) throws SQLException {
        try {
            debugCodeCall("getInt", columnLabel);
            return getIntInternal(getColumnIndex(columnLabel));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    private int getIntInternal(int columnIndex) {
        Value v = getInternal(columnIndex);
        int result;
        if (v != ValueNull.INSTANCE) {
            wasNull = false;
            result = v.getInt();
        } else {
            wasNull = true;
            result = 0;
        }
        return result;
    }

    /**
     * Returns the value of the specified column as a BigDecimal.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getBigDecimal", columnIndex);
            return get(checkColumnIndex(columnIndex)).getBigDecimal();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Date.
     * <p>
     * Usage of this method is discouraged. Use
     * {@code getObject(columnIndex, LocalDate.class)} instead.
     * </p>
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     * @see #getObject(int, Class)
     */
    @Override
    public Date getDate(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getDate", columnIndex);
            return LegacyDateTimeUtils.toDate(conn, null, get(checkColumnIndex(columnIndex)));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Time.
     * <p>
     * Usage of this method is discouraged. Use
     * {@code getObject(columnIndex, LocalTime.class)} instead.
     * </p>
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     * @see #getObject(int, Class)
     */
    @Override
    public Time getTime(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getTime", columnIndex);
            return LegacyDateTimeUtils.toTime(conn, null, get(checkColumnIndex(columnIndex)));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Timestamp.
     * <p>
     * Usage of this method is discouraged. Use
     * {@code getObject(columnIndex, LocalDateTime.class)} instead.
     * </p>
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     * @see #getObject(int, Class)
     */
    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getTimestamp", columnIndex);
            return LegacyDateTimeUtils.toTimestamp(conn, null, get(checkColumnIndex(columnIndex)));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a BigDecimal.
     *
     * @param columnLabel the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        try {
            debugCodeCall("getBigDecimal", columnLabel);
            return get(getColumnIndex(columnLabel)).getBigDecimal();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Date.
     * <p>
     * Usage of this method is discouraged. Use
     * {@code getObject(columnLabel, LocalDate.class)} instead.
     * </p>
     *
     * @param columnLabel the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     * @see #getObject(String, Class)
     */
    @Override
    public Date getDate(String columnLabel) throws SQLException {
        try {
            debugCodeCall("getDate", columnLabel);
            return LegacyDateTimeUtils.toDate(conn, null, get(getColumnIndex(columnLabel)));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Time.
     * <p>
     * Usage of this method is discouraged. Use
     * {@code getObject(columnLabel, LocalTime.class)} instead.
     * </p>
     *
     * @param columnLabel the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     * @see #getObject(String, Class)
     */
    @Override
    public Time getTime(String columnLabel) throws SQLException {
        try {
            debugCodeCall("getTime", columnLabel);
            return LegacyDateTimeUtils.toTime(conn, null, get(getColumnIndex(columnLabel)));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Timestamp.
     * <p>
     * Usage of this method is discouraged. Use
     * {@code getObject(columnLabel, LocalDateTime.class)} instead.
     * </p>
     *
     * @param columnLabel the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     * @see #getObject(String, Class)
     */
    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        try {
            debugCodeCall("getTimestamp", columnLabel);
            return LegacyDateTimeUtils.toTimestamp(conn, null, get(getColumnIndex(columnLabel)));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns a column value as a Java object. The data is
     * de-serialized into a Java object (on the client side).
     *
     * @param columnIndex (1,2,...)
     * @return the value or null
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public Object getObject(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getObject", columnIndex);
            return ValueToObjectConverter.valueToDefaultObject(get(checkColumnIndex(columnIndex)), conn, true);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns a column value as a Java object. The data is
     * de-serialized into a Java object (on the client side).
     *
     * @param columnLabel the column label
     * @return the value or null
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public Object getObject(String columnLabel) throws SQLException {
        try {
            debugCodeCall("getObject", columnLabel);
            return ValueToObjectConverter.valueToDefaultObject(get(getColumnIndex(columnLabel)), conn, true);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a boolean.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getBoolean", columnIndex);
            return getBooleanInternal(checkColumnIndex(columnIndex));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a boolean.
     *
     * @param columnLabel the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        try {
            debugCodeCall("getBoolean", columnLabel);
            return getBooleanInternal(getColumnIndex(columnLabel));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    private boolean getBooleanInternal(int columnIndex) {
        Value v = getInternal(columnIndex);
        boolean result;
        if (v != ValueNull.INSTANCE) {
            wasNull = false;
            result = v.getBoolean();
        } else {
            wasNull = true;
            result = false;
        }
        return result;
    }

    /**
     * Returns the value of the specified column as a byte.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public byte getByte(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getByte", columnIndex);
            return getByteInternal(checkColumnIndex(columnIndex));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a byte.
     *
     * @param columnLabel the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public byte getByte(String columnLabel) throws SQLException {
        try {
            debugCodeCall("getByte", columnLabel);
            return getByteInternal(getColumnIndex(columnLabel));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    private byte getByteInternal(int columnIndex) {
        Value v = getInternal(columnIndex);
        byte result;
        if (v != ValueNull.INSTANCE) {
            wasNull = false;
            result = v.getByte();
        } else {
            wasNull = true;
            result = 0;
        }
        return result;
    }

    /**
     * Returns the value of the specified column as a short.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public short getShort(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getShort", columnIndex);
            return getShortInternal(checkColumnIndex(columnIndex));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a short.
     *
     * @param columnLabel the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public short getShort(String columnLabel) throws SQLException {
        try {
            debugCodeCall("getShort", columnLabel);
            return getShortInternal(getColumnIndex(columnLabel));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    private short getShortInternal(int columnIndex) {
        Value v = getInternal(columnIndex);
        short result;
        if (v != ValueNull.INSTANCE) {
            wasNull = false;
            result = v.getShort();
        } else {
            wasNull = true;
            result = 0;
        }
        return result;
    }

    /**
     * Returns the value of the specified column as a long.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public long getLong(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getLong", columnIndex);
            return getLongInternal(checkColumnIndex(columnIndex));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a long.
     *
     * @param columnLabel the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public long getLong(String columnLabel) throws SQLException {
        try {
            debugCodeCall("getLong", columnLabel);
            return getLongInternal(getColumnIndex(columnLabel));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    private long getLongInternal(int columnIndex) {
        Value v = getInternal(columnIndex);
        long result;
        if (v != ValueNull.INSTANCE) {
            wasNull = false;
            result = v.getLong();
        } else {
            wasNull = true;
            result = 0L;
        }
        return result;
    }

    /**
     * Returns the value of the specified column as a float.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public float getFloat(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getFloat", columnIndex);
            return getFloatInternal(checkColumnIndex(columnIndex));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a float.
     *
     * @param columnLabel the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public float getFloat(String columnLabel) throws SQLException {
        try {
            debugCodeCall("getFloat", columnLabel);
            return getFloatInternal(getColumnIndex(columnLabel));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    private float getFloatInternal(int columnIndex) {
        Value v = getInternal(columnIndex);
        float result;
        if (v != ValueNull.INSTANCE) {
            wasNull = false;
            result = v.getFloat();
        } else {
            wasNull = true;
            result = 0f;
        }
        return result;
    }

    /**
     * Returns the value of the specified column as a double.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public double getDouble(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getDouble", columnIndex);
            return getDoubleInternal(checkColumnIndex(columnIndex));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a double.
     *
     * @param columnLabel the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public double getDouble(String columnLabel) throws SQLException {
        try {
            debugCodeCall("getDouble", columnLabel);
            return getDoubleInternal(getColumnIndex(columnLabel));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    private double getDoubleInternal(int columnIndex) {
        Value v = getInternal(columnIndex);
        double result;
        if (v != ValueNull.INSTANCE) {
            wasNull = false;
            result = v.getDouble();
        } else {
            wasNull = true;
            result = 0d;
        }
        return result;
    }

    /**
     * Returns the value of the specified column as a BigDecimal.
     *
     * @deprecated use {@link #getBigDecimal(String)}
     *
     * @param columnLabel the column label
     * @param scale the scale of the returned value
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Deprecated
    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getBigDecimal(" + quote(columnLabel) + ", " + scale + ')');
            }
            if (scale < 0) {
                throw DbException.getInvalidValueException("scale", scale);
            }
            BigDecimal bd = get(getColumnIndex(columnLabel)).getBigDecimal();
            return bd == null ? null : ValueNumeric.setScale(bd, scale);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a BigDecimal.
     *
     * @deprecated use {@link #getBigDecimal(int)}
     *
     * @param columnIndex (1,2,...)
     * @param scale the scale of the returned value
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Deprecated
    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getBigDecimal(" + columnIndex + ", " + scale + ')');
            }
            if (scale < 0) {
                throw DbException.getInvalidValueException("scale", scale);
            }
            BigDecimal bd = get(checkColumnIndex(columnIndex)).getBigDecimal();
            return bd == null ? null : ValueNumeric.setScale(bd, scale);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported]
     * @deprecated since JDBC 2.0, use getCharacterStream
     */
    @Deprecated
    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw unsupported("unicodeStream");
    }

    /**
     * [Not supported]
     * @deprecated since JDBC 2.0, use setCharacterStream
     */
    @Deprecated
    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw unsupported("unicodeStream");
    }

    /**
     * [Not supported] Gets a column as a object using the specified type
     * mapping.
     */
    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map)
            throws SQLException {
        throw unsupported("map");
    }

    /**
     * [Not supported] Gets a column as a object using the specified type
     * mapping.
     */
    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map)
            throws SQLException {
        throw unsupported("map");
    }

    /**
     * [Not supported] Gets a column as a reference.
     */
    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw unsupported("ref");
    }

    /**
     * [Not supported] Gets a column as a reference.
     */
    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        throw unsupported("ref");
    }

    /**
     * Returns the value of the specified column as a java.sql.Date using a
     * specified time zone.
     * <p>
     * Usage of this method is discouraged. Use
     * {@code getObject(columnIndex, LocalDate.class)} instead.
     * </p>
     *
     * @param columnIndex (1,2,...)
     * @param calendar the calendar
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     * @see #getObject(int, Class)
     */
    @Override
    public Date getDate(int columnIndex, Calendar calendar) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getDate(" + columnIndex + ", calendar)");
            }
            return LegacyDateTimeUtils.toDate(conn, calendar != null ? calendar.getTimeZone() : null,
                    get(checkColumnIndex(columnIndex)));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Date using a
     * specified time zone.
     * <p>
     * Usage of this method is discouraged. Use
     * {@code getObject(columnLabel, LocalDate.class)} instead.
     * </p>
     *
     * @param columnLabel the column label
     * @param calendar the calendar
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     * @see #getObject(String, Class)
     */
    @Override
    public Date getDate(String columnLabel, Calendar calendar) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getDate(" + quote(columnLabel) + ", calendar)");
            }
            return LegacyDateTimeUtils.toDate(conn, calendar != null ? calendar.getTimeZone() : null,
                    get(getColumnIndex(columnLabel)));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Time using a
     * specified time zone.
     * <p>
     * Usage of this method is discouraged. Use
     * {@code getObject(columnIndex, LocalTime.class)} instead.
     * </p>
     *
     * @param columnIndex (1,2,...)
     * @param calendar the calendar
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     * @see #getObject(int, Class)
     */
    @Override
    public Time getTime(int columnIndex, Calendar calendar) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getTime(" + columnIndex + ", calendar)");
            }
            return LegacyDateTimeUtils.toTime(conn, calendar != null ? calendar.getTimeZone() : null,
                    get(checkColumnIndex(columnIndex)));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Time using a
     * specified time zone.
     * <p>
     * Usage of this method is discouraged. Use
     * {@code getObject(columnLabel, LocalTime.class)} instead.
     * </p>
     *
     * @param columnLabel the column label
     * @param calendar the calendar
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     * @see #getObject(String, Class)
     */
    @Override
    public Time getTime(String columnLabel, Calendar calendar) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getTime(" + quote(columnLabel) + ", calendar)");
            }
            return LegacyDateTimeUtils.toTime(conn, calendar != null ? calendar.getTimeZone() : null,
                    get(getColumnIndex(columnLabel)));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Timestamp using a
     * specified time zone.
     * <p>
     * Usage of this method is discouraged. Use
     * {@code getObject(columnIndex, LocalDateTime.class)} instead.
     * </p>
     *
     * @param columnIndex (1,2,...)
     * @param calendar the calendar
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     * @see #getObject(int, Class)
     */
    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar calendar) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getTimestamp(" + columnIndex + ", calendar)");
            }
            return LegacyDateTimeUtils.toTimestamp(conn, calendar != null ? calendar.getTimeZone() : null,
                    get(checkColumnIndex(columnIndex)));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Timestamp.
     * <p>
     * Usage of this method is discouraged. Use
     * {@code getObject(columnLabel, LocalDateTime.class)} instead.
     * </p>
     *
     * @param columnLabel the column label
     * @param calendar the calendar
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     * @see #getObject(String, Class)
     */
    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar calendar) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getTimestamp(" + quote(columnLabel) + ", calendar)");
            }
            return LegacyDateTimeUtils.toTimestamp(conn, calendar != null ? calendar.getTimeZone() : null,
                    get(getColumnIndex(columnLabel)));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a Blob.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        try {
            int id = getNextId(TraceObject.BLOB);
            if (isDebugEnabled()) {
                debugCodeAssign("Blob", TraceObject.BLOB, id, "getBlob(" + columnIndex + ')');
            }
            return getBlob(id, checkColumnIndex(columnIndex));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a Blob.
     *
     * @param columnLabel the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        try {
            int id = getNextId(TraceObject.BLOB);
            if (isDebugEnabled()) {
                debugCodeAssign("Blob", TraceObject.BLOB, id, "getBlob(" + quote(columnLabel) + ')');
            }
            return getBlob(id, getColumnIndex(columnLabel));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    private JdbcBlob getBlob(int id, int columnIndex) {
        Value v = getInternal(columnIndex);
        JdbcBlob result;
        if (v != ValueNull.INSTANCE) {
            wasNull = false;
            result = new JdbcBlob(conn, v, JdbcLob.State.WITH_VALUE, id);
        } else {
            wasNull = true;
            result = null;
        }
        return result;
    }

    /**
     * Returns the value of the specified column as a byte array.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getBytes", columnIndex);
            return get(checkColumnIndex(columnIndex)).getBytes();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a byte array.
     *
     * @param columnLabel the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        try {
            debugCodeCall("getBytes", columnLabel);
            return get(getColumnIndex(columnLabel)).getBytes();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as an input stream.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getBinaryStream", columnIndex);
            return get(checkColumnIndex(columnIndex)).getInputStream();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as an input stream.
     *
     * @param columnLabel the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        try {
            debugCodeCall("getBinaryStream", columnLabel);
            return get(getColumnIndex(columnLabel)).getInputStream();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }


    /**
     * Returns the value of the specified column as a Clob.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        try {
            int id = getNextId(TraceObject.CLOB);
            if (isDebugEnabled()) {
                debugCodeAssign("Clob", TraceObject.CLOB, id, "getClob(" + columnIndex + ')');
            }
            return getClob(id, checkColumnIndex(columnIndex));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a Clob.
     *
     * @param columnLabel the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        try {
            int id = getNextId(TraceObject.CLOB);
            if (isDebugEnabled()) {
                debugCodeAssign("Clob", TraceObject.CLOB, id, "getClob(" + quote(columnLabel) + ')');
            }
            return getClob(id, getColumnIndex(columnLabel));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as an Array.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public Array getArray(int columnIndex) throws SQLException {
        try {
            int id = getNextId(TraceObject.ARRAY);
            if (isDebugEnabled()) {
                debugCodeAssign("Array", TraceObject.ARRAY, id, "getArray(" + columnIndex + ')');
            }
            return getArray(id, checkColumnIndex(columnIndex));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as an Array.
     *
     * @param columnLabel the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public Array getArray(String columnLabel) throws SQLException {
        try {
            int id = getNextId(TraceObject.ARRAY);
            if (isDebugEnabled()) {
                debugCodeAssign("Array", TraceObject.ARRAY, id, "getArray(" + quote(columnLabel) + ')');
            }
            return getArray(id, getColumnIndex(columnLabel));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    private Array getArray(int id, int columnIndex) {
        Value v = getInternal(columnIndex);
        JdbcArray result;
        if (v != ValueNull.INSTANCE) {
            wasNull = false;
            result = new JdbcArray(conn, v, id);
        } else {
            wasNull = true;
            result = null;
        }
        return result;
    }

    /**
     * Returns the value of the specified column as an input stream.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getAsciiStream", columnIndex);
            String s = get(checkColumnIndex(columnIndex)).getString();
            return s == null ? null : IOUtils.getInputStreamFromString(s);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as an input stream.
     *
     * @param columnLabel the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        try {
            debugCodeCall("getAsciiStream", columnLabel);
            String s = get(getColumnIndex(columnLabel)).getString();
            return IOUtils.getInputStreamFromString(s);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a reader.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getCharacterStream", columnIndex);
            return get(checkColumnIndex(columnIndex)).getReader();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a reader.
     *
     * @param columnLabel the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        try {
            debugCodeCall("getCharacterStream", columnLabel);
            return get(getColumnIndex(columnLabel)).getReader();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported]
     */
    @Override
    public URL getURL(int columnIndex) throws SQLException {
        throw unsupported("url");
    }

    /**
     * [Not supported]
     */
    @Override
    public URL getURL(String columnLabel) throws SQLException {
        throw unsupported("url");
    }

    // =============================================================

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateNull(int columnIndex) throws SQLException {
        try {
            debugCodeCall("updateNull", columnIndex);
            update(checkColumnIndex(columnIndex), ValueNull.INSTANCE);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateNull(String columnLabel) throws SQLException {
        try {
            debugCodeCall("updateNull", columnLabel);
            update(getColumnIndex(columnLabel), ValueNull.INSTANCE);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateBoolean(" + columnIndex + ", " + x + ')');
            }
            update(checkColumnIndex(columnIndex), ValueBoolean.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if result set is closed or not updatable
     */
    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateBoolean(" + quote(columnLabel) + ", " + x + ')');
            }
            update(getColumnIndex(columnLabel), ValueBoolean.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateByte(" + columnIndex + ", " + x + ')');
            }
            update(checkColumnIndex(columnIndex), ValueTinyint.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateByte(" + quote(columnLabel) + ", " + x + ')');
            }
            update(getColumnIndex(columnLabel), ValueTinyint.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateBytes(" + columnIndex + ", x)");
            }
            update(checkColumnIndex(columnIndex), x == null ? ValueNull.INSTANCE : ValueVarbinary.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateBytes(" + quote(columnLabel) + ", x)");
            }
            update(getColumnIndex(columnLabel), x == null ? ValueNull.INSTANCE : ValueVarbinary.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateShort(" + columnIndex + ", (short) " + x + ')');
            }
            update(checkColumnIndex(columnIndex), ValueSmallint.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateShort(" + quote(columnLabel) + ", (short) " + x + ')');
            }
            update(getColumnIndex(columnLabel), ValueSmallint.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateInt(" + columnIndex + ", " + x + ')');
            }
            update(checkColumnIndex(columnIndex), ValueInteger.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateInt(" + quote(columnLabel) + ", " + x + ')');
            }
            update(getColumnIndex(columnLabel), ValueInteger.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateLong(" + columnIndex + ", " + x + "L)");
            }
            update(checkColumnIndex(columnIndex), ValueBigint.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateLong(" + quote(columnLabel) + ", " + x + "L)");
            }
            update(getColumnIndex(columnLabel), ValueBigint.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateFloat(" + columnIndex + ", " + x + "f)");
            }
            update(checkColumnIndex(columnIndex), ValueReal.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateFloat(" + quote(columnLabel) + ", " + x + "f)");
            }
            update(getColumnIndex(columnLabel), ValueReal.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateDouble(" + columnIndex + ", " + x + "d)");
            }
            update(checkColumnIndex(columnIndex), ValueDouble.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateDouble(" + quote(columnLabel) + ", " + x + "d)");
            }
            update(getColumnIndex(columnLabel), ValueDouble.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateBigDecimal(" + columnIndex + ", " + quoteBigDecimal(x) + ')');
            }
            update(checkColumnIndex(columnIndex), x == null ? ValueNull.INSTANCE : ValueNumeric.getAnyScale(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateBigDecimal(" + quote(columnLabel) + ", " + quoteBigDecimal(x) + ')');
            }
            update(getColumnIndex(columnLabel), x == null ? ValueNull.INSTANCE : ValueNumeric.getAnyScale(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateString(" + columnIndex + ", " + quote(x) + ')');
            }
            update(checkColumnIndex(columnIndex), x == null ? ValueNull.INSTANCE : ValueVarchar.get(x, conn));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateString(" + quote(columnLabel) + ", " + quote(x) + ')');
            }
            update(getColumnIndex(columnLabel), x == null ? ValueNull.INSTANCE : ValueVarchar.get(x, conn));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * <p>
     * Usage of this method is discouraged. Use
     * {@code updateObject(columnIndex, value)} with {@link java.time.LocalDate}
     * parameter instead.
     * </p>
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     * @see #updateObject(int, Object)
     */
    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateDate(" + columnIndex + ", " + quoteDate(x) + ')');
            }
            update(checkColumnIndex(columnIndex),
                    x == null ? ValueNull.INSTANCE : LegacyDateTimeUtils.fromDate(conn, null, x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * <p>
     * Usage of this method is discouraged. Use
     * {@code updateObject(columnLabel, value)} with {@link java.time.LocalDate}
     * parameter instead.
     * </p>
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     * @see #updateObject(String, Object)
     */
    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateDate(" + quote(columnLabel) + ", " + quoteDate(x) + ')');
            }
            update(getColumnIndex(columnLabel),
                    x == null ? ValueNull.INSTANCE : LegacyDateTimeUtils.fromDate(conn, null, x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * <p>
     * Usage of this method is discouraged. Use
     * {@code updateObject(columnIndex, value)} with {@link java.time.LocalTime}
     * parameter instead.
     * </p>
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     * @see #updateObject(int, Object)
     */
    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateTime(" + columnIndex + ", " + quoteTime(x) + ')');
            }
            update(checkColumnIndex(columnIndex),
                    x == null ? ValueNull.INSTANCE : LegacyDateTimeUtils.fromTime(conn, null, x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * <p>
     * Usage of this method is discouraged. Use
     * {@code updateObject(columnLabel, value)} with {@link java.time.LocalTime}
     * parameter instead.
     * </p>
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     * @see #updateObject(String, Object)
     */
    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateTime(" + quote(columnLabel) + ", " + quoteTime(x) + ')');
            }
            update(getColumnIndex(columnLabel),
                    x == null ? ValueNull.INSTANCE : LegacyDateTimeUtils.fromTime(conn, null, x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * <p>
     * Usage of this method is discouraged. Use
     * {@code updateObject(columnIndex, value)} with
     * {@link java.time.LocalDateTime} parameter instead.
     * </p>
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     * @see #updateObject(int, Object)
     */
    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateTimestamp(" + columnIndex + ", " + quoteTimestamp(x) + ')');
            }
            update(checkColumnIndex(columnIndex),
                    x == null ? ValueNull.INSTANCE : LegacyDateTimeUtils.fromTimestamp(conn, null, x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * <p>
     * Usage of this method is discouraged. Use
     * {@code updateObject(columnLabel, value)} with
     * {@link java.time.LocalDateTime} parameter instead.
     * </p>
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     * @see #updateObject(String, Object)
     */
    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateTimestamp(" + quote(columnLabel) + ", " + quoteTimestamp(x) + ')');
            }
            update(getColumnIndex(columnLabel),
                    x == null ? ValueNull.INSTANCE : LegacyDateTimeUtils.fromTimestamp(conn, null, x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @param length the number of characters
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateAsciiStream(" + columnIndex + ", x, " + length + ')');
            }
            updateAscii(checkColumnIndex(columnIndex), x, length);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateAsciiStream(" + columnIndex + ", x)");
            }
            updateAscii(checkColumnIndex(columnIndex), x, -1L);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @param length the number of characters
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateAsciiStream(" + columnIndex + ", x, " + length + "L)");
            }
            updateAscii(checkColumnIndex(columnIndex), x, length);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @param length the number of characters
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateAsciiStream(" + quote(columnLabel) + ", x, " + length + ')');
            }
            updateAscii(getColumnIndex(columnLabel), x, length);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if the result set is closed
     */
    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateAsciiStream(" + quote(columnLabel) + ", x)");
            }
            updateAscii(getColumnIndex(columnLabel), x, -1L);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @param length the number of characters
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateAsciiStream(" + quote(columnLabel) + ", x, " + length + "L)");
            }
            updateAscii(getColumnIndex(columnLabel), x, length);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    private void updateAscii(int columnIndex, InputStream x, long length) {
        update(columnIndex, conn.createClob(IOUtils.getAsciiReader(x), length));
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @param length the number of characters
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateBinaryStream(" + columnIndex + ", x, " + length + ')');
            }
            updateBlobImpl(checkColumnIndex(columnIndex), x, length);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateBinaryStream(" + columnIndex + ", x)");
            }
            updateBlobImpl(checkColumnIndex(columnIndex), x, -1L);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @param length the number of characters
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateBinaryStream(" + columnIndex + ", x, " + length + "L)");
            }
            updateBlobImpl(checkColumnIndex(columnIndex), x, length);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateBinaryStream(" + quote(columnLabel) + ", x)");
            }
            updateBlobImpl(getColumnIndex(columnLabel), x, -1L);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @param length the number of characters
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateBinaryStream(" + quote(columnLabel) + ", x, " + length + ')');
            }
            updateBlobImpl(getColumnIndex(columnLabel), x, length);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @param length the number of characters
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateBinaryStream(" + quote(columnLabel) + ", x, " + length + "L)");
            }
            updateBlobImpl(getColumnIndex(columnLabel), x, length);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @param length the number of characters
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateCharacterStream(" + columnIndex + ", x, " + length + "L)");
            }
            updateClobImpl(checkColumnIndex(columnIndex), x, length);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @param length the number of characters
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateCharacterStream(" + columnIndex + ", x, " + length + ')');
            }
            updateClobImpl(checkColumnIndex(columnIndex), x, length);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateCharacterStream(" + columnIndex + ", x)");
            }
            updateClobImpl(checkColumnIndex(columnIndex), x, -1L);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @param length the number of characters
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateCharacterStream(String columnLabel, Reader x, int length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateCharacterStream(" + quote(columnLabel) + ", x, " + length + ')');
            }
            updateClobImpl(getColumnIndex(columnLabel), x, length);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateCharacterStream(String columnLabel, Reader x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateCharacterStream(" + quote(columnLabel) + ", x)");
            }
            updateClobImpl(getColumnIndex(columnLabel), x, -1L);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @param length the number of characters
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateCharacterStream(String columnLabel, Reader x, long length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateCharacterStream(" + quote(columnLabel) + ", x, " + length + "L)");
            }
            updateClobImpl(getColumnIndex(columnLabel), x, length);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @param scale is ignored
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateObject(int columnIndex, Object x, int scale) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateObject(" + columnIndex + ", x, " + scale + ')');
            }
            update(checkColumnIndex(columnIndex), convertToUnknownValue(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @param scale is ignored
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateObject(String columnLabel, Object x, int scale) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateObject(" + quote(columnLabel) + ", x, " + scale + ')');
            }
            update(getColumnIndex(columnLabel), convertToUnknownValue(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateObject(" + columnIndex + ", x)");
            }
            update(checkColumnIndex(columnIndex), convertToUnknownValue(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateObject(" + quote(columnLabel) + ", x)");
            }
            update(getColumnIndex(columnLabel), convertToUnknownValue(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @param targetSqlType the SQL type
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateObject(int columnIndex, Object x, SQLType targetSqlType) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateObject(" + columnIndex + ", x, " + DataType.sqlTypeToString(targetSqlType) + ')');
            }
            update(checkColumnIndex(columnIndex), convertToValue(x, targetSqlType));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @param targetSqlType the SQL type
     * @param scaleOrLength is ignored
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateObject(int columnIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateObject(" + columnIndex + ", x, " + DataType.sqlTypeToString(targetSqlType) + ", "
                        + scaleOrLength + ')');
            }
            update(checkColumnIndex(columnIndex), convertToValue(x, targetSqlType));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @param targetSqlType the SQL type
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateObject(" + quote(columnLabel) + ", x, " + DataType.sqlTypeToString(targetSqlType)
                        + ')');
            }
            update(getColumnIndex(columnLabel), convertToValue(x, targetSqlType));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @param targetSqlType the SQL type
     * @param scaleOrLength is ignored
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType, int scaleOrLength)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateObject(" + quote(columnLabel) + ", x, " + DataType.sqlTypeToString(targetSqlType)
                        + ", " + scaleOrLength + ')');
            }
            update(getColumnIndex(columnLabel), convertToValue(x, targetSqlType));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported]
     */
    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw unsupported("ref");
    }

    /**
     * [Not supported]
     */
    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw unsupported("ref");
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateBlob(int columnIndex, InputStream x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateBlob(" + columnIndex + ", (InputStream) x)");
            }
            updateBlobImpl(checkColumnIndex(columnIndex), x, -1L);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @param length the length
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateBlob(int columnIndex, InputStream x, long length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateBlob(" + columnIndex + ", (InputStream) x, " + length + "L)");
            }
            updateBlobImpl(checkColumnIndex(columnIndex), x, length);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateBlob(" + columnIndex + ", (Blob) x)");
            }
            updateBlobImpl(checkColumnIndex(columnIndex), x, -1L);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateBlob(" + quote(columnLabel) + ", (Blob) x)");
            }
            updateBlobImpl(getColumnIndex(columnLabel), x, -1L);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    private void updateBlobImpl(int columnIndex, Blob x, long length) throws SQLException {
        update(columnIndex, x == null ? ValueNull.INSTANCE : conn.createBlob(x.getBinaryStream(), length));
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateBlob(String columnLabel, InputStream x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateBlob(" + quote(columnLabel) + ", (InputStream) x)");
            }
            updateBlobImpl(getColumnIndex(columnLabel), x, -1L);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @param length the length
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateBlob(String columnLabel, InputStream x, long length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateBlob(" + quote(columnLabel) + ", (InputStream) x, " + length + "L)");
            }
            updateBlobImpl(getColumnIndex(columnLabel), x, length);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    private void updateBlobImpl(int columnIndex, InputStream x, long length) {
        update(columnIndex, conn.createBlob(x, length));
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateClob(" + columnIndex + ", (Clob) x)");
            }
            updateClobImpl(checkColumnIndex(columnIndex), x);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateClob(int columnIndex, Reader x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateClob(" + columnIndex + ", (Reader) x)");
            }
            updateClobImpl(checkColumnIndex(columnIndex), x, -1L);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @param length the length
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateClob(int columnIndex, Reader x, long length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateClob(" + columnIndex + ", (Reader) x, " + length + "L)");
            }
            updateClobImpl(checkColumnIndex(columnIndex), x, length);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateClob(" + quote(columnLabel) + ", (Clob) x)");
            }
            updateClobImpl(getColumnIndex(columnLabel), x);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateClob(String columnLabel, Reader x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateClob(" + quote(columnLabel) + ", (Reader) x)");
            }
            updateClobImpl(getColumnIndex(columnLabel), x, -1L);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @param length the length
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateClob(String columnLabel, Reader x, long length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateClob(" + quote(columnLabel) + ", (Reader) x, " + length + "L)");
            }
            updateClobImpl(getColumnIndex(columnLabel), x, length);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateArray(" + columnIndex + ", x)");
            }
            updateArrayImpl(checkColumnIndex(columnIndex), x);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateArray(" + quote(columnLabel) + ", x)");
            }
            updateArrayImpl(getColumnIndex(columnLabel), x);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    private void updateArrayImpl(int columnIndex, Array x) throws SQLException {
        update(columnIndex, x == null ? ValueNull.INSTANCE
                : ValueToObjectConverter.objectToValue(stat.session, x.getArray(), Value.ARRAY));
    }

    /**
     * [Not supported] Gets the cursor name if it was defined. This feature is
     * superseded by updateX methods. This method throws a SQLException because
     * cursor names are not supported.
     */
    @Override
    public String getCursorName() throws SQLException {
        throw unsupported("cursorName");
    }

    /**
     * Gets the current row number. The first row is row 1, the second 2 and so
     * on. This method returns 0 before the first and after the last row.
     *
     * @return the row number
     */
    @Override
    public int getRow() throws SQLException {
        try {
            debugCodeCall("getRow");
            checkClosed();
            if (result.isAfterLast()) {
                return 0;
            }
            long rowNumber = result.getRowId() + 1;
            return rowNumber <= Integer.MAX_VALUE ? (int) rowNumber : Statement.SUCCESS_NO_INFO;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the result set concurrency. Result sets are only updatable if the
     * statement was created with updatable concurrency, and if the result set
     * contains all columns of the primary key or of a unique index of a table.
     *
     * @return ResultSet.CONCUR_UPDATABLE if the result set is updatable, or
     *         ResultSet.CONCUR_READ_ONLY otherwise
     */
    @Override
    public int getConcurrency() throws SQLException {
        try {
            debugCodeCall("getConcurrency");
            checkClosed();
            if (!updatable) {
                return ResultSet.CONCUR_READ_ONLY;
            }
            UpdatableRow row = new UpdatableRow(conn, result);
            return row.isUpdatable() ? ResultSet.CONCUR_UPDATABLE
                    : ResultSet.CONCUR_READ_ONLY;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the fetch direction.
     *
     * @return the direction: FETCH_FORWARD
     */
    @Override
    public int getFetchDirection() throws SQLException {
        try {
            debugCodeCall("getFetchDirection");
            checkClosed();
            return ResultSet.FETCH_FORWARD;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the number of rows suggested to read in one step.
     *
     * @return the current fetch size
     */
    @Override
    public int getFetchSize() throws SQLException {
        try {
            debugCodeCall("getFetchSize");
            checkClosed();
            return result.getFetchSize();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the number of rows suggested to read in one step. This value cannot
     * be higher than the maximum rows (setMaxRows) set by the statement or
     * prepared statement, otherwise an exception is throws. Setting the value
     * to 0 will set the default value. The default value can be changed using
     * the system property h2.serverResultSetFetchSize.
     *
     * @param rows the number of rows
     */
    @Override
    public void setFetchSize(int rows) throws SQLException {
        try {
            debugCodeCall("setFetchSize", rows);
            checkClosed();
            if (rows < 0) {
                throw DbException.getInvalidValueException("rows", rows);
            } else if (rows > 0) {
                if (stat != null) {
                    int maxRows = stat.getMaxRows();
                    if (maxRows > 0 && rows > maxRows) {
                        throw DbException.getInvalidValueException("rows", rows);
                    }
                }
            } else {
                rows = SysProperties.SERVER_RESULT_SET_FETCH_SIZE;
            }
            result.setFetchSize(rows);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported]
     * Sets (changes) the fetch direction for this result set. This method
     * should only be called for scrollable result sets, otherwise it will throw
     * an exception (no matter what direction is used).
     *
     * @param direction the new fetch direction
     * @throws SQLException Unsupported Feature if the method is called for a
     *             forward-only result set
     */
    @Override
    public void setFetchDirection(int direction) throws SQLException {
        debugCodeCall("setFetchDirection", direction);
        // ignore FETCH_FORWARD, that's the default value, which we do support
        if (direction != ResultSet.FETCH_FORWARD) {
            throw unsupported("setFetchDirection");
        }
    }

    /**
     * Get the result set type.
     *
     * @return the result set type (TYPE_FORWARD_ONLY, TYPE_SCROLL_INSENSITIVE
     *         or TYPE_SCROLL_SENSITIVE)
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public int getType() throws SQLException {
        try {
            debugCodeCall("getType");
            checkClosed();
            return stat == null ? ResultSet.TYPE_FORWARD_ONLY : stat.resultSetType;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if the current position is before the first row, that means next()
     * was not called yet, and there is at least one row.
     *
     * @return if there are results and the current position is before the first
     *         row
     * @throws SQLException if the result set is closed
     */
    @Override
    public boolean isBeforeFirst() throws SQLException {
        try {
            debugCodeCall("isBeforeFirst");
            checkClosed();
            return result.getRowId() < 0 && result.hasNext();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if the current position is after the last row, that means next()
     * was called and returned false, and there was at least one row.
     *
     * @return if there are results and the current position is after the last
     *         row
     * @throws SQLException if the result set is closed
     */
    @Override
    public boolean isAfterLast() throws SQLException {
        try {
            debugCodeCall("isAfterLast");
            checkClosed();
            return result.getRowId() > 0 && result.isAfterLast();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if the current position is row 1, that means next() was called
     * once and returned true.
     *
     * @return if the current position is the first row
     * @throws SQLException if the result set is closed
     */
    @Override
    public boolean isFirst() throws SQLException {
        try {
            debugCodeCall("isFirst");
            checkClosed();
            return result.getRowId() == 0 && !result.isAfterLast();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if the current position is the last row, that means next() was
     * called and did not yet returned false, but will in the next call.
     *
     * @return if the current position is the last row
     * @throws SQLException if the result set is closed
     */
    @Override
    public boolean isLast() throws SQLException {
        try {
            debugCodeCall("isLast");
            checkClosed();
            long rowId = result.getRowId();
            return rowId >= 0 && !result.isAfterLast() && !result.hasNext();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Moves the current position to before the first row, that means resets the
     * result set.
     *
     * @throws SQLException if the result set is closed
     */
    @Override
    public void beforeFirst() throws SQLException {
        try {
            debugCodeCall("beforeFirst");
            checkClosed();
            if (result.getRowId() >= 0) {
                resetResult();
            }
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Moves the current position to after the last row, that means after the
     * end.
     *
     * @throws SQLException if the result set is closed
     */
    @Override
    public void afterLast() throws SQLException {
        try {
            debugCodeCall("afterLast");
            checkClosed();
            while (nextRow()) {
                // nothing
            }
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Moves the current position to the first row. This is the same as calling
     * beforeFirst() followed by next().
     *
     * @return true if there is a row available, false if not
     * @throws SQLException if the result set is closed
     */
    @Override
    public boolean first() throws SQLException {
        try {
            debugCodeCall("first");
            checkClosed();
            if (result.getRowId() >= 0) {
                resetResult();
            }
            return nextRow();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Moves the current position to the last row.
     *
     * @return true if there is a row available, false if not
     * @throws SQLException if the result set is closed
     */
    @Override
    public boolean last() throws SQLException {
        try {
            debugCodeCall("last");
            checkClosed();
            if (result.isAfterLast()) {
                resetResult();
            }
            while (result.hasNext()) {
                nextRow();
            }
            return isOnValidRow();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Moves the current position to a specific row.
     *
     * @param rowNumber the row number. 0 is not allowed, 1 means the first row,
     *            2 the second. -1 means the last row, -2 the row before the
     *            last row. If the value is too large, the position is moved
     *            after the last row, if the value is too small it is moved
     *            before the first row.
     * @return true if there is a row available, false if not
     * @throws SQLException if the result set is closed
     */
    @Override
    public boolean absolute(int rowNumber) throws SQLException {
        try {
            debugCodeCall("absolute", rowNumber);
            checkClosed();
            long longRowNumber = rowNumber >= 0 ? rowNumber : result.getRowCount() + rowNumber + 1;
            if (--longRowNumber < result.getRowId()) {
                resetResult();
            }
            while (result.getRowId() < longRowNumber) {
                if (!nextRow()) {
                    return false;
                }
            }
            return isOnValidRow();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Moves the current position to a specific row relative to the current row.
     *
     * @param rowCount 0 means don't do anything, 1 is the next row, -1 the
     *            previous. If the value is too large, the position is moved
     *            after the last row, if the value is too small it is moved
     *            before the first row.
     * @return true if there is a row available, false if not
     * @throws SQLException if the result set is closed
     */
    @Override
    public boolean relative(int rowCount) throws SQLException {
        try {
            debugCodeCall("relative", rowCount);
            checkClosed();
            long longRowCount;
            if (rowCount < 0) {
                longRowCount = result.getRowId() + rowCount + 1;
                resetResult();
            } else {
                longRowCount = rowCount;
            }
            while (longRowCount-- > 0) {
                if (!nextRow()) {
                    return false;
                }
            }
            return isOnValidRow();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Moves the cursor to the last row, or row before first row if the current
     * position is the first row.
     *
     * @return true if there is a row available, false if not
     * @throws SQLException if the result set is closed
     */
    @Override
    public boolean previous() throws SQLException {
        try {
            debugCodeCall("previous");
            checkClosed();
            return relative(-1);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Moves the current position to the insert row. The current row is
     * remembered.
     *
     * @throws SQLException if the result set is closed or is not updatable
     */
    @Override
    public void moveToInsertRow() throws SQLException {
        try {
            debugCodeCall("moveToInsertRow");
            checkUpdatable();
            insertRow = new Value[columnCount];
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Moves the current position to the current row.
     *
     * @throws SQLException if the result set is closed or is not updatable
     */
    @Override
    public void moveToCurrentRow() throws SQLException {
        try {
            debugCodeCall("moveToCurrentRow");
            checkUpdatable();
            insertRow = null;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Detects if the row was updated (by somebody else or the caller).
     *
     * @return false because this driver does not detect this
     */
    @Override
    public boolean rowUpdated() throws SQLException {
        try {
            debugCodeCall("rowUpdated");
            return false;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Detects if the row was inserted.
     *
     * @return false because this driver does not detect this
     */
    @Override
    public boolean rowInserted() throws SQLException {
        try {
            debugCodeCall("rowInserted");
            return false;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Detects if the row was deleted (by somebody else or the caller).
     *
     * @return false because this driver does not detect this
     */
    @Override
    public boolean rowDeleted() throws SQLException {
        try {
            debugCodeCall("rowDeleted");
            return false;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Inserts the current row. The current position must be the insert row.
     *
     * @throws SQLException if the result set is closed or if not on the insert
     *             row, or if the result set it not updatable
     */
    @Override
    public void insertRow() throws SQLException {
        try {
            debugCodeCall("insertRow");
            checkUpdatable();
            if (insertRow == null) {
                throw DbException.get(ErrorCode.NOT_ON_UPDATABLE_ROW);
            }
            getUpdatableRow().insertRow(insertRow);
            insertRow = null;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates the current row.
     *
     * @throws SQLException if the result set is closed, if the current row is
     *             the insert row or if not on a valid row, or if the result set
     *             it not updatable
     */
    @Override
    public void updateRow() throws SQLException {
        try {
            debugCodeCall("updateRow");
            checkUpdatable();
            if (insertRow != null) {
                throw DbException.get(ErrorCode.NOT_ON_UPDATABLE_ROW);
            }
            checkOnValidRow();
            if (updateRow != null) {
                UpdatableRow row = getUpdatableRow();
                Value[] current = new Value[columnCount];
                for (int i = 0; i < updateRow.length; i++) {
                    current[i] = getInternal(checkColumnIndex(i + 1));
                }
                row.updateRow(current, updateRow);
                for (int i = 0; i < updateRow.length; i++) {
                    if (updateRow[i] == null) {
                        updateRow[i] = current[i];
                    }
                }
                Value[] patch = row.readRow(updateRow);
                patchCurrentRow(patch);
                updateRow = null;
            }
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Deletes the current row.
     *
     * @throws SQLException if the result set is closed, if the current row is
     *             the insert row or if not on a valid row, or if the result set
     *             it not updatable
     */
    @Override
    public void deleteRow() throws SQLException {
        try {
            debugCodeCall("deleteRow");
            checkUpdatable();
            if (insertRow != null) {
                throw DbException.get(ErrorCode.NOT_ON_UPDATABLE_ROW);
            }
            checkOnValidRow();
            getUpdatableRow().deleteRow(result.currentRow());
            updateRow = null;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Re-reads the current row from the database.
     *
     * @throws SQLException if the result set is closed or if the current row is
     *             the insert row or if the row has been deleted or if not on a
     *             valid row
     */
    @Override
    public void refreshRow() throws SQLException {
        try {
            debugCodeCall("refreshRow");
            checkClosed();
            if (insertRow != null) {
                throw DbException.get(ErrorCode.NO_DATA_AVAILABLE);
            }
            checkOnValidRow();
            patchCurrentRow(getUpdatableRow().readRow(result.currentRow()));
            updateRow = null;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Cancels updating a row.
     *
     * @throws SQLException if the result set is closed or if the current row is
     *             the insert row
     */
    @Override
    public void cancelRowUpdates() throws SQLException {
        try {
            debugCodeCall("cancelRowUpdates");
            checkClosed();
            if (insertRow != null) {
                throw DbException.get(ErrorCode.NO_DATA_AVAILABLE);
            }
            updateRow = null;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    // =============================================================

    private UpdatableRow getUpdatableRow() throws SQLException {
        UpdatableRow row = new UpdatableRow(conn, result);
        if (!row.isUpdatable()) {
            throw DbException.get(ErrorCode.RESULT_SET_NOT_UPDATABLE);
        }
        return row;
    }

    private int getColumnIndex(String columnLabel) {
        checkClosed();
        if (columnLabel == null) {
            throw DbException.getInvalidValueException("columnLabel", null);
        }
        if (columnCount >= 3) {
            // use a hash table if more than 2 columns
            if (columnLabelMap == null) {
                HashMap<String, Integer> map = new HashMap<>();
                // column labels have higher priority
                for (int i = 0; i < columnCount; i++) {
                    String c = StringUtils.toUpperEnglish(result.getAlias(i));
                    // Don't override previous mapping
                    map.putIfAbsent(c, i);
                }
                for (int i = 0; i < columnCount; i++) {
                    String colName = result.getColumnName(i);
                    if (colName != null) {
                        colName = StringUtils.toUpperEnglish(colName);
                        // Don't override previous mapping
                        map.putIfAbsent(colName, i);
                        String tabName = result.getTableName(i);
                        if (tabName != null) {
                            colName = StringUtils.toUpperEnglish(tabName) + '.' + colName;
                            // Don't override previous mapping
                            map.putIfAbsent(colName, i);
                        }
                    }
                }
                // assign at the end so concurrent access is supported
                columnLabelMap = map;
                if (preparedStatement != null) {
                    preparedStatement.setCachedColumnLabelMap(columnLabelMap);
                }
            }
            Integer index = columnLabelMap.get(StringUtils.toUpperEnglish(columnLabel));
            if (index == null) {
                throw DbException.get(ErrorCode.COLUMN_NOT_FOUND_1, columnLabel);
            }
            return index + 1;
        }
        for (int i = 0; i < columnCount; i++) {
            if (columnLabel.equalsIgnoreCase(result.getAlias(i))) {
                return i + 1;
            }
        }
        int idx = columnLabel.indexOf('.');
        if (idx > 0) {
            String table = columnLabel.substring(0, idx);
            String col = columnLabel.substring(idx+1);
            for (int i = 0; i < columnCount; i++) {
                if (table.equalsIgnoreCase(result.getTableName(i)) &&
                        col.equalsIgnoreCase(result.getColumnName(i))) {
                    return i + 1;
                }
            }
        } else {
            for (int i = 0; i < columnCount; i++) {
                if (columnLabel.equalsIgnoreCase(result.getColumnName(i))) {
                    return i + 1;
                }
            }
        }
        throw DbException.get(ErrorCode.COLUMN_NOT_FOUND_1, columnLabel);
    }

    private int checkColumnIndex(int columnIndex) {
        checkClosed();
        if (columnIndex < 1 || columnIndex > columnCount) {
            throw DbException.getInvalidValueException("columnIndex", columnIndex);
        }
        return columnIndex;
    }

    /**
     * Check if this result set is closed.
     *
     * @throws DbException if it is closed
     */
    void checkClosed() {
        if (result == null) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED);
        }
        if (stat != null) {
            stat.checkClosed();
        }
        if (conn != null) {
            conn.checkClosed();
        }
    }

    private boolean isOnValidRow() {
        return result.getRowId() >= 0 && !result.isAfterLast();
    }

    private void checkOnValidRow() {
        if (!isOnValidRow()) {
            throw DbException.get(ErrorCode.NO_DATA_AVAILABLE);
        }
    }

    private Value get(int columnIndex) {
        Value value = getInternal(columnIndex);
        wasNull = value == ValueNull.INSTANCE;
        return value;
    }

    /**
     * INTERNAL
     *
     * @param columnIndex
     *            index of a column
     * @return internal representation of the value in the specified column
     */
    public Value getInternal(int columnIndex) {
        checkOnValidRow();
        Value[] list;
        if (patchedRows == null || (list = patchedRows.get(result.getRowId())) == null) {
            list = result.currentRow();
        }
        return list[columnIndex - 1];
    }

    private void update(int columnIndex, Value v) {
        if (!triggerUpdatable) {
            checkUpdatable();
        }
        if (insertRow != null) {
            insertRow[columnIndex - 1] = v;
        } else {
            if (updateRow == null) {
                updateRow = new Value[columnCount];
            }
            updateRow[columnIndex - 1] = v;
        }
    }

    private boolean nextRow() {
        boolean next = result.isLazy() ? nextLazyRow() : result.next();
        if (!next && !scrollable) {
            result.close();
        }
        return next;
    }

    private boolean nextLazyRow() {
        Session session;
        if (stat.isCancelled() || conn == null || (session = conn.getSession()) == null) {
            throw DbException.get(ErrorCode.STATEMENT_WAS_CANCELED);
        }
        Session oldSession = session.setThreadLocalSession();
        boolean next;
        try {
            next = result.next();
        } finally {
            session.resetThreadLocalSession(oldSession);
        }
        return next;
    }

    private void resetResult() {
        if (!scrollable) {
            throw DbException.get(ErrorCode.RESULT_SET_NOT_SCROLLABLE);
        }
        result.reset();
    }

    /**
     * [Not supported] Returns the value of the specified column as a row id.
     *
     * @param columnIndex (1,2,...)
     */
    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw unsupported("rowId");
    }

    /**
     * [Not supported] Returns the value of the specified column as a row id.
     *
     * @param columnLabel the column label
     */
    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw unsupported("rowId");
    }

    /**
     * [Not supported] Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     */
    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw unsupported("rowId");
    }

    /**
     * [Not supported] Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     */
    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw unsupported("rowId");
    }

    /**
     * Returns the current result set holdability.
     *
     * @return the holdability
     * @throws SQLException if the connection is closed
     */
    @Override
    public int getHoldability() throws SQLException {
        try {
            debugCodeCall("getHoldability");
            checkClosed();
            return conn.getHoldability();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns whether this result set is closed.
     *
     * @return true if the result set is closed
     */
    @Override
    public boolean isClosed() throws SQLException {
        try {
            debugCodeCall("isClosed");
            return result == null;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateNString(int columnIndex, String x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateNString(" + columnIndex + ", " + quote(x) + ')');
            }
            update(checkColumnIndex(columnIndex), x == null ? ValueNull.INSTANCE : ValueVarchar.get(x, conn));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateNString(String columnLabel, String x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateNString(" + quote(columnLabel) + ", " + quote(x) + ')');
            }
            update(getColumnIndex(columnLabel), x == null ? ValueNull.INSTANCE : ValueVarchar.get(x, conn));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateNClob(int columnIndex, NClob x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateNClob(" + columnIndex + ", (NClob) x)");
            }
            updateClobImpl(checkColumnIndex(columnIndex), x);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateNClob(int columnIndex, Reader x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateNClob(" + columnIndex + ", (Reader) x)");
            }
            updateClobImpl(checkColumnIndex(columnIndex), x, -1L);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @param length the length
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateNClob(int columnIndex, Reader x, long length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateNClob(" + columnIndex + ", (Reader) x, " + length + "L)");
            }
            updateClobImpl(checkColumnIndex(columnIndex), x, length);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateNClob(String columnLabel, Reader x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateNClob(" + quote(columnLabel) + ", (Reader) x)");
            }
            updateClobImpl(getColumnIndex(columnLabel), x, -1L);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @param length the length
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateNClob(String columnLabel, Reader x, long length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateNClob(" + quote(columnLabel) + ", (Reader) x, " + length + "L)");
            }
            updateClobImpl(getColumnIndex(columnLabel), x, length);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateNClob(String columnLabel, NClob x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateNClob(" + quote(columnLabel) + ", (NClob) x)");
            }
            updateClobImpl(getColumnIndex(columnLabel), x);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    private void updateClobImpl(int columnIndex, Clob x) throws SQLException {
        update(columnIndex, x == null ? ValueNull.INSTANCE : conn.createClob(x.getCharacterStream(), -1));
    }

    /**
     * Returns the value of the specified column as a Clob.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        try {
            int id = getNextId(TraceObject.CLOB);
            if (isDebugEnabled()) {
                debugCodeAssign("NClob", TraceObject.CLOB, id, "getNClob(" + columnIndex + ')');
            }
            return getClob(id, checkColumnIndex(columnIndex));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a Clob.
     *
     * @param columnLabel the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        try {
            int id = getNextId(TraceObject.CLOB);
            if (isDebugEnabled()) {
                debugCodeAssign("NClob", TraceObject.CLOB, id, "getNClob(" + quote(columnLabel) + ')');
            }
            return getClob(id, getColumnIndex(columnLabel));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    private JdbcClob getClob(int id, int columnIndex) {
        Value v = getInternal(columnIndex);
        JdbcClob result;
        if (v != ValueNull.INSTANCE) {
            wasNull = false;
            result = new JdbcClob(conn, v, JdbcLob.State.WITH_VALUE, id);
        } else {
            wasNull = true;
            result = null;
        }
        return result;
    }

    /**
     * Returns the value of the specified column as a SQLXML.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        try {
            int id = getNextId(TraceObject.SQLXML);
            if (isDebugEnabled()) {
                debugCodeAssign("SQLXML", TraceObject.SQLXML, id, "getSQLXML(" + columnIndex + ')');
            }
            Value v = get(checkColumnIndex(columnIndex));
            return v == ValueNull.INSTANCE ? null : new JdbcSQLXML(conn, v, JdbcLob.State.WITH_VALUE, id);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a SQLXML.
     *
     * @param columnLabel the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        try {
            int id = getNextId(TraceObject.SQLXML);
            if (isDebugEnabled()) {
                debugCodeAssign("SQLXML", TraceObject.SQLXML, id, "getSQLXML(" + quote(columnLabel) + ')');
            }
            Value v = get(getColumnIndex(columnLabel));
            return v == ValueNull.INSTANCE ? null : new JdbcSQLXML(conn, v, JdbcLob.State.WITH_VALUE, id);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param xmlObject the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateSQLXML(" + columnIndex + ", x)");
            }
            updateSQLXMLImpl(checkColumnIndex(columnIndex), xmlObject);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param xmlObject the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateSQLXML(" + quote(columnLabel) + ", x)");
            }
            updateSQLXMLImpl(getColumnIndex(columnLabel), xmlObject);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    private void updateSQLXMLImpl(int columnIndex, SQLXML xmlObject) throws SQLException {
        update(columnIndex,
                xmlObject == null ? ValueNull.INSTANCE : conn.createClob(xmlObject.getCharacterStream(), -1));
    }

    /**
     * Returns the value of the specified column as a String.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public String getNString(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getNString", columnIndex);
            return get(checkColumnIndex(columnIndex)).getString();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a String.
     *
     * @param columnLabel the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public String getNString(String columnLabel) throws SQLException {
        try {
            debugCodeCall("getNString", columnLabel);
            return get(getColumnIndex(columnLabel)).getString();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a reader.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getNCharacterStream", columnIndex);
            return get(checkColumnIndex(columnIndex)).getReader();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a reader.
     *
     * @param columnLabel the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        try {
            debugCodeCall("getNCharacterStream", columnLabel);
            return get(getColumnIndex(columnLabel)).getReader();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateNCharacterStream(" + columnIndex + ", x)");
            }
            updateClobImpl(checkColumnIndex(columnIndex), x, -1L);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @param length the number of characters
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateNCharacterStream(" + columnIndex + ", x, " + length + "L)");
            }
            updateClobImpl(checkColumnIndex(columnIndex), x, length);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateNCharacterStream(String columnLabel, Reader x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateNCharacterStream(" + quote(columnLabel) + ", x)");
            }
            updateClobImpl(getColumnIndex(columnLabel), x, -1L);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @param length the number of characters
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateNCharacterStream(String columnLabel, Reader x, long length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateNCharacterStream(" + quote(columnLabel) + ", x, " + length + "L)");
            }
            updateClobImpl(getColumnIndex(columnLabel), x, length);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    private void updateClobImpl(int columnIndex, Reader x, long length) {
        update(columnIndex, conn.createClob(x, length));
    }

    /**
     * Return an object of this class if possible.
     *
     * @param iface the class
     * @return this
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            if (isWrapperFor(iface)) {
                return (T) this;
            }
            throw DbException.getInvalidValueException("iface", iface);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if unwrap can return an object of this class.
     *
     * @param iface the class
     * @return whether or not the interface is assignable from this class
     */
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface != null && iface.isAssignableFrom(getClass());
    }

    /**
     * Returns a column value as a Java object of the specified type.
     *
     * @param columnIndex the column index (1, 2, ...)
     * @param type the class of the returned value
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        try {
            if (type == null) {
                throw DbException.getInvalidValueException("type", type);
            }
            debugCodeCall("getObject", columnIndex);
            return ValueToObjectConverter.valueToObject(type, get(checkColumnIndex(columnIndex)), conn);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns a column value as a Java object of the specified type.
     *
     * @param columnName the column name
     * @param type the class of the returned value
     * @return the value
     */
    @Override
    public <T> T getObject(String columnName, Class<T> type) throws SQLException {
        try {
            if (type == null) {
                throw DbException.getInvalidValueException("type", type);
            }
            debugCodeCall("getObject", columnName);
            return ValueToObjectConverter.valueToObject(type, get(getColumnIndex(columnName)), conn);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * INTERNAL
     */
    @Override
    public String toString() {
        return getTraceObjectName() + ": " + result;
    }

    private void patchCurrentRow(Value[] row) {
        boolean changed = false;
        Value[] current = result.currentRow();
        CompareMode compareMode = conn.getCompareMode();
        for (int i = 0; i < row.length; i++) {
            if (row[i].compareTo(current[i], conn, compareMode) != 0) {
                changed = true;
                break;
            }
        }
        if (patchedRows == null) {
            patchedRows = new HashMap<>();
        }
        Long rowId = result.getRowId();
        if (!changed) {
            patchedRows.remove(rowId);
        } else {
            patchedRows.put(rowId, row);
        }
    }

    private Value convertToValue(Object x, SQLType targetSqlType) {
        if (x == null) {
            return ValueNull.INSTANCE;
        } else {
            int type = DataType.convertSQLTypeToValueType(targetSqlType);
            Value v = ValueToObjectConverter.objectToValue(conn.getSession(), x, type);
            return v.convertTo(type, conn);
        }
    }

    private Value convertToUnknownValue(Object x) {
        return ValueToObjectConverter.objectToValue(conn.getSession(), x, Value.UNKNOWN);
    }

    private void checkUpdatable() {
        checkClosed();
        if (!updatable) {
            throw DbException.get(ErrorCode.RESULT_SET_READONLY);
        }
    }

    /**
     * INTERNAL
     *
     * @return array of column values for the current row
     */
    public Value[] getUpdateRow() {
        return updateRow;
    }

    /**
     * INTERNAL
     *
     * @return result
     */
    public ResultInterface getResult() {
        return result;
    }

}
