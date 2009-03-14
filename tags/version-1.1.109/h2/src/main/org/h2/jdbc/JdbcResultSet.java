/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
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
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

/*## Java 1.6 begin ##
import java.sql.NClob;
import java.sql.RowId;
import java.sql.SQLXML;
## Java 1.6 end ##*/

import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.message.Message;
import org.h2.message.TraceObject;
import org.h2.result.ResultInterface;
import org.h2.result.UpdatableRow;
import org.h2.util.DateTimeUtils;
import org.h2.util.IOUtils;
import org.h2.util.MathUtils;
import org.h2.util.ObjectUtils;
import org.h2.util.StringUtils;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueByte;
import org.h2.value.ValueBytes;
import org.h2.value.ValueDate;
import org.h2.value.ValueDecimal;
import org.h2.value.ValueDouble;
import org.h2.value.ValueFloat;
import org.h2.value.ValueInt;
import org.h2.value.ValueLong;
import org.h2.value.ValueNull;
import org.h2.value.ValueShort;
import org.h2.value.ValueString;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;

/**
 * <p>
 * Represents a result set.
 * </p>
 * <p>
 * Column names are case-insensitive, quotes are not supported. The first column
 * has the column index 1.
 * </p>
 * <p>
 * Updatable result sets:
 * Result sets are updatable when the result only contains columns from one
 * table, and if it contains all columns of a unique index (primary key or
 * other) of this table. Key columns may not contain NULL (because multiple rows
 * with NULL could exist). In updatable result sets, own changes are visible,
 * but not own inserts and deletes.
 * </p>
 */
public class JdbcResultSet extends TraceObject implements ResultSet {
    private final boolean closeStatement;
    private final boolean scrollable;
    private ResultInterface result;
    private JdbcConnection conn;
    private JdbcStatement stat;
    private int columnCount;
    private boolean wasNull;
    private Value[] insertRow;
    private Value[] updateRow;
    private HashMap columnNameMap;
    private HashMap patchedRows;

    JdbcResultSet(JdbcConnection conn, JdbcStatement stat, ResultInterface result, int id,
                boolean closeStatement, boolean scrollable) {
        setTrace(conn.getSession().getTrace(), TraceObject.RESULT_SET, id);
        this.conn = conn;
        this.stat = stat;
        this.result = result;
        columnCount = result.getVisibleColumnCount();
        this.closeStatement = closeStatement;
        this.scrollable = scrollable;
    }

    /**
     * Moves the cursor to the next row of the result set.
     *
     * @return true if successful, false if there are no more rows
     */
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
    public ResultSetMetaData getMetaData() throws SQLException {
        try {
            int id = getNextId(TraceObject.RESULT_SET_META_DATA);
            if (isDebugEnabled()) {
                debugCodeAssign("ResultSetMetaData", TraceObject.RESULT_SET_META_DATA, id, "getMetaData()");
            }
            checkClosed();
            String catalog = conn.getCatalog();
            JdbcResultSetMetaData meta = new JdbcResultSetMetaData(this, null, result, catalog, conn.getSession().getTrace(), id);
            return meta;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns whether the last column accessed was a null value.
     *
     * @return true if the last column accessed was a null value
     */
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
     * @param columnName the name of the column label
     * @return the column index (1,2,...)
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public int findColumn(String columnName) throws SQLException {
        try {
            debugCodeCall("findColumn", columnName);
            return getColumnIndex(columnName);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Closes the result set.
     */
    public void close() throws SQLException {
        try {
            debugCodeCall("close");
            closeInternal();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Close the result set. This method also closes the statement if required.
     */
    void closeInternal() throws SQLException {
        if (result != null) {
            try {
                result.close();
                if (closeStatement && stat != null) {
                    stat.close();
                }
            } finally {
                columnCount = 0;
                result = null;
                stat = null;
                conn = null;
                insertRow = null;
                updateRow = null;
            }
        }
    }

    /**
     * Returns the statement that created this object.
     *
     * @return the statement or prepared statement, or null if created by a
     *         DatabaseMetaData call.
     */
    public Statement getStatement() throws SQLException {
        try {
            debugCodeCall("getStatement");
            checkClosed();
            if (closeStatement) {
                // if the result set was opened by a DatabaseMetaData call
                return null;
            }
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
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public String getString(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getString", columnIndex);
            return get(columnIndex).getString();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a String.
     *
     * @param columnName the name of the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public String getString(String columnName) throws SQLException {
        try {
            debugCodeCall("getString", columnName);
            return get(columnName).getString();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as an int.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public int getInt(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getInt", columnIndex);
            return get(columnIndex).getInt();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as an int.
     *
     * @param columnName the name of the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public int getInt(String columnName) throws SQLException {
        try {
            debugCodeCall("getInt", columnName);
            return get(columnName).getInt();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a String.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getBigDecimal", columnIndex);
            return get(columnIndex).getBigDecimal();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Date.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public Date getDate(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getDate", columnIndex);
            return get(columnIndex).getDate();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Time.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public Time getTime(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getTime", columnIndex);
            return get(columnIndex).getTime();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Timestamp.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getTimestamp", columnIndex);
            return get(columnIndex).getTimestamp();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a String.
     *
     * @param columnName the name of the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public BigDecimal getBigDecimal(String columnName) throws SQLException {
        try {
            debugCodeCall("getBigDecimal", columnName);
            return get(columnName).getBigDecimal();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Date.
     *
     * @param columnName the name of the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public Date getDate(String columnName) throws SQLException {
        try {
            debugCodeCall("getDate", columnName);
            return get(columnName).getDate();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Time.
     *
     * @param columnName the name of the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public Time getTime(String columnName) throws SQLException {
        try {
            debugCodeCall("getTime", columnName);
            return get(columnName).getTime();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Timestamp.
     *
     * @param columnName the name of the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public Timestamp getTimestamp(String columnName) throws SQLException {
        try {
            debugCodeCall("getTimestamp", columnName);
            return get(columnName).getTimestamp();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns a column value as a Java object. For BINARY data, the data is
     * de-serialized into a Java Object.
     *
     * @param columnIndex (1,2,...)
     * @return the value or null
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public Object getObject(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getObject", columnIndex);
            Value v = get(columnIndex);
            return conn.convertToDefaultObject(v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns a column value as a Java object. For BINARY data, the data is
     * de-serialized into a Java Object.
     *
     * @param columnName the name of the column label
     * @return the value or null
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public Object getObject(String columnName) throws SQLException {
        try {
            debugCodeCall("getObject", columnName);
            Value v = get(columnName);
            return conn.convertToDefaultObject(v);
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
    public boolean getBoolean(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getBoolean", columnIndex);
            Boolean v = get(columnIndex).getBoolean();
            return v == null ? false : v.booleanValue();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a boolean.
     *
     * @param columnName the name of the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public boolean getBoolean(String columnName) throws SQLException {
        try {
            debugCodeCall("getBoolean", columnName);
            Boolean v = get(columnName).getBoolean();
            return v == null ? false : v.booleanValue();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a byte.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public byte getByte(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getByte", columnIndex);
            return get(columnIndex).getByte();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a byte.
     *
     * @param columnName the name of the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public byte getByte(String columnName) throws SQLException {
        try {
            debugCodeCall("getByte", columnName);
            return get(columnName).getByte();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a short.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public short getShort(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getShort", columnIndex);
            return get(columnIndex).getShort();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a short.
     *
     * @param columnName the name of the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public short getShort(String columnName) throws SQLException {
        try {
            debugCodeCall("getShort", columnName);
            return get(columnName).getShort();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a long.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public long getLong(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getLong", columnIndex);
            return get(columnIndex).getLong();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a long.
     *
     * @param columnName the name of the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public long getLong(String columnName) throws SQLException {
        try {
            debugCodeCall("getLong", columnName);
            return get(columnName).getLong();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a float.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public float getFloat(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getFloat", columnIndex);
            return get(columnIndex).getFloat();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a float.
     *
     * @param columnName the name of the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public float getFloat(String columnName) throws SQLException {
        try {
            debugCodeCall("getFloat", columnName);
            return get(columnName).getFloat();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a double.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public double getDouble(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getDouble", columnIndex);
            return get(columnIndex).getDouble();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a double.
     *
     * @param columnName the name of the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public double getDouble(String columnName) throws SQLException {
        try {
            debugCodeCall("getDouble", columnName);
            return get(columnName).getDouble();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a String.
     *
     * @deprecated
     *
     * @param columnName the column name
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public BigDecimal getBigDecimal(String columnName, int scale) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getBigDecimal(" + StringUtils.quoteJavaString(columnName)+", "+scale+");");
            }
            if (scale < 0) {
                throw Message.getInvalidValueException(""+scale, "scale");
            }
            BigDecimal bd = get(columnName).getBigDecimal();
            return bd == null ? null : MathUtils.setScale(bd, scale);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a String.
     *
     * @deprecated
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getBigDecimal(" + columnIndex+", "+scale+");");
            }
            if (scale < 0) {
                throw Message.getInvalidValueException(""+scale, "scale");
            }
            BigDecimal bd = get(columnIndex).getBigDecimal();
            return bd == null ? null : MathUtils.setScale(bd, scale);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported]
     * @deprecated
     */
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getUnicodeStream", columnIndex);
            throw Message.getUnsupportedException();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported]
     * @deprecated
     */
    public InputStream getUnicodeStream(String columnName) throws SQLException {
        try {
            debugCodeCall("getUnicodeStream", columnName);
            throw Message.getUnsupportedException();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported] Gets a column as a object using the specified type
     * mapping.
     */
    public Object getObject(int columnIndex, Map map) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getObject(" + columnIndex + ", map);");
            }
            throw Message.getUnsupportedException();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported] Gets a column as a object using the specified type
     * mapping.
     */
    public Object getObject(String columnName, Map map) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getObject(" + quote(columnName) + ", map);");
            }
            throw Message.getUnsupportedException();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported] Gets a column as a reference.
     */
    public Ref getRef(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getRef", columnIndex);
            throw Message.getUnsupportedException();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported] Gets a column as a reference.
     */
    public Ref getRef(String columnName) throws SQLException {
        try {
            debugCodeCall("getRef", columnName);
            throw Message.getUnsupportedException();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Date using a
     * specified time zone.
     *
     * @param columnIndex (1,2,...)
     * @param calendar the calendar
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public Date getDate(int columnIndex, Calendar calendar) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getDate(" + columnIndex + ", calendar)");
            }
            Date x = get(columnIndex).getDate();
            return DateTimeUtils.convertDateToCalendar(x, calendar);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Date using a
     * specified time zone.
     *
     * @param columnName the name of the column label
     * @param calendar the calendar
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public Date getDate(String columnName, Calendar calendar) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getDate(" + StringUtils.quoteJavaString(columnName) + ", calendar)");
            }
            Date x = get(columnName).getDate();
            return DateTimeUtils.convertDateToCalendar(x, calendar);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Time using a
     * specified time zone.
     *
     * @param columnIndex (1,2,...)
     * @param calendar the calendar
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public Time getTime(int columnIndex, Calendar calendar) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getTime(" + columnIndex + ", calendar)");
            }
            Time x = get(columnIndex).getTime();
            return DateTimeUtils.convertTimeToCalendar(x, calendar);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Time using a
     * specified time zone.
     *
     * @param columnName the name of the column label
     * @param calendar the calendar
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public Time getTime(String columnName, Calendar calendar) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getTime(" + StringUtils.quoteJavaString(columnName) + ", calendar)");
            }
            Time x = get(columnName).getTime();
            return DateTimeUtils.convertTimeToCalendar(x, calendar);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Timestamp using a
     * specified time zone.
     *
     * @param columnIndex (1,2,...)
     * @param calendar the calendar
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public Timestamp getTimestamp(int columnIndex, Calendar calendar) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getTimestamp(" + columnIndex + ", calendar)");
            }
            Timestamp x = get(columnIndex).getTimestamp();
            return DateTimeUtils.convertTimestampToCalendar(x, calendar);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Timestamp.
     *
     * @param columnName the name of the column label
     * @param calendar the calendar
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public Timestamp getTimestamp(String columnName, Calendar calendar) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getTimestamp(" + StringUtils.quoteJavaString(columnName) + ", calendar)");
            }
            Timestamp x = get(columnName).getTimestamp();
            return DateTimeUtils.convertTimestampToCalendar(x, calendar);
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
    public Blob getBlob(int columnIndex) throws SQLException {
        try {
            int id = getNextId(TraceObject.BLOB);
            debugCodeAssign("Blob", TraceObject.BLOB, id, "getBlob(" + columnIndex + ")");
            Value v = get(columnIndex);
            return v == ValueNull.INSTANCE ? null : new JdbcBlob(conn, v, id);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a Blob.
     *
     * @param columnName the name of the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public Blob getBlob(String columnName) throws SQLException {
        try {
            int id = getNextId(TraceObject.BLOB);
            debugCodeAssign("Blob", TraceObject.BLOB, id, "getBlob(" + quote(columnName) + ")");
            Value v = get(columnName);
            return v == ValueNull.INSTANCE ? null : new JdbcBlob(conn, v, id);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a byte array.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public byte[] getBytes(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getBytes", columnIndex);
            return get(columnIndex).getBytes();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a byte array.
     *
     * @param columnName the name of the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public byte[] getBytes(String columnName) throws SQLException {
        try {
            debugCodeCall("getBytes", columnName);
            return get(columnName).getBytes();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as input stream.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getBinaryStream", columnIndex);
            return get(columnIndex).getInputStream();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as input stream.
     *
     * @param columnName the name of the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public InputStream getBinaryStream(String columnName) throws SQLException {
        try {
            debugCodeCall("getBinaryStream", columnName);
            return get(columnName).getInputStream();
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
    public Clob getClob(int columnIndex) throws SQLException {
        try {
            int id = getNextId(TraceObject.CLOB);
            debugCodeAssign("Clob", TraceObject.CLOB, id, "getClob(" + columnIndex + ")");
            Value v = get(columnIndex);
            return v == ValueNull.INSTANCE ? null : new JdbcClob(conn, v, id);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a Clob.
     *
     * @param columnName the name of the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public Clob getClob(String columnName) throws SQLException {
        try {
            int id = getNextId(TraceObject.CLOB);
            debugCodeAssign("Clob", TraceObject.CLOB, id, "getClob(" + quote(columnName) + ")");
            Value v = get(columnName);
            return v == ValueNull.INSTANCE ? null : new JdbcClob(conn, v, id);
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
    public Array getArray(int columnIndex) throws SQLException {
        try {
            int id = getNextId(TraceObject.ARRAY);
            debugCodeAssign("Clob", TraceObject.ARRAY, id, "getArray(" + columnIndex + ")");
            Value v = get(columnIndex);
            return v == ValueNull.INSTANCE ? null : new JdbcArray(conn, v, id);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as an Array.
     *
     * @param columnName the name of the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public Array getArray(String columnName) throws SQLException {
        try {
            int id = getNextId(TraceObject.ARRAY);
            debugCodeAssign("Clob", TraceObject.ARRAY, id, "getArray(" + quote(columnName) + ")");
            Value v = get(columnName);
            return v == ValueNull.INSTANCE ? null : new JdbcArray(conn, v, id);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as input stream.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getAsciiStream", columnIndex);
            String s = get(columnIndex).getString();
            return s == null ? null : IOUtils.getInputStream(s);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as input stream.
     *
     * @param columnName the name of the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public InputStream getAsciiStream(String columnName) throws SQLException {
        try {
            debugCodeCall("getAsciiStream", columnName);
            String s = get(columnName).getString();
            return IOUtils.getInputStream(s);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as input stream.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getCharacterStream", columnIndex);
            return get(columnIndex).getReader();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as input stream.
     *
     * @param columnName the name of the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public Reader getCharacterStream(String columnName) throws SQLException {
        try {
            debugCodeCall("getCharacterStream", columnName);
            return get(columnName).getReader();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported]
     */
    public URL getURL(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getURL", columnIndex);
            throw Message.getUnsupportedException();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported]
     */
    public URL getURL(String columnName) throws SQLException {
        try {
            debugCodeCall("getURL", columnName);
            throw Message.getUnsupportedException();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    // =============================================================

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @throws SQLException if the result set is closed
     */
    public void updateNull(int columnIndex) throws SQLException {
        try {
            debugCodeCall("updateNull", columnIndex);
            update(columnIndex, ValueNull.INSTANCE);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnName the name of the column label
     * @throws SQLException if the result set is closed
     */
    public void updateNull(String columnName) throws SQLException {
        try {
            debugCodeCall("updateNull", columnName);
            update(columnName, ValueNull.INSTANCE);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed
     */
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateBoolean("+columnIndex+", "+x+");");
            }
            update(columnIndex, ValueBoolean.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnName the name of the column label
     * @param x the value
     * @throws SQLException if result set is closed
     */
    public void updateBoolean(String columnName, boolean x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateBoolean("+quote(columnName)+", "+x+");");
            }
            update(columnName, ValueBoolean.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed
     */
    public void updateByte(int columnIndex, byte x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateByte("+columnIndex+", "+x+");");
            }
            update(columnIndex, ValueByte.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnName the name of the column label
     * @param x the value
     * @throws SQLException if the result set is closed
     */
    public void updateByte(String columnName, byte x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateByte("+columnName+", "+x+");");
            }
            update(columnName, ValueByte.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed
     */
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateBytes("+columnIndex+", x);");
            }
            update(columnIndex, x == null ? (Value) ValueNull.INSTANCE : ValueBytes.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnName the name of the column label
     * @param x the value
     * @throws SQLException if the result set is closed
     */
    public void updateBytes(String columnName, byte[] x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateBytes("+quote(columnName)+", x);");
            }
            update(columnName, x == null ? (Value) ValueNull.INSTANCE : ValueBytes.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed
     */
    public void updateShort(int columnIndex, short x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateShort("+columnIndex+", (short) "+x+");");
            }
            update(columnIndex, ValueShort.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnName the name of the column label
     * @param x the value
     * @throws SQLException if the result set is closed
     */
    public void updateShort(String columnName, short x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateShort("+quote(columnName)+", (short) "+x+");");
            }
            update(columnName, ValueShort.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed
     */
    public void updateInt(int columnIndex, int x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateInt("+columnIndex+", "+x+");");
            }
            update(columnIndex, ValueInt.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnName the name of the column label
     * @param x the value
     * @throws SQLException if the result set is closed
     */
    public void updateInt(String columnName, int x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateInt("+quote(columnName)+", "+x+");");
            }
            update(columnName, ValueInt.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed
     */
    public void updateLong(int columnIndex, long x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateLong("+columnIndex+", "+x+"L);");
            }
            update(columnIndex, ValueLong.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnName the name of the column label
     * @param x the value
     * @throws SQLException if the result set is closed
     */
    public void updateLong(String columnName, long x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateLong("+quote(columnName)+", "+x+"L);");
            }
            update(columnName, ValueLong.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed
     */
    public void updateFloat(int columnIndex, float x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateFloat("+columnIndex+", "+x+"f);");
            }
            update(columnIndex, ValueFloat.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnName the name of the column label
     * @param x the value
     * @throws SQLException if the result set is closed
     */
    public void updateFloat(String columnName, float x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateFloat("+quote(columnName)+", "+x+"f);");
            }
            update(columnName, ValueFloat.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed
     */
    public void updateDouble(int columnIndex, double x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateDouble("+columnIndex+", "+x+"d);");
            }
            update(columnIndex, ValueDouble.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnName the name of the column label
     * @param x the value
     * @throws SQLException if the result set is closed
     */
    public void updateDouble(String columnName, double x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateDouble("+quote(columnName)+", "+x+"d);");
            }
            update(columnName, ValueDouble.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed
     */
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateBigDecimal("+columnIndex+", " + quoteBigDecimal(x) + ");");
            }
            update(columnIndex, x == null ? (Value) ValueNull.INSTANCE : ValueDecimal.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnName the name of the column label
     * @param x the value
     * @throws SQLException if the result set is closed
     */
    public void updateBigDecimal(String columnName, BigDecimal x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateBigDecimal("+quote(columnName)+", " + quoteBigDecimal(x) + ");");
            }
            update(columnName, x == null ? (Value) ValueNull.INSTANCE : ValueDecimal.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed
     */
    public void updateString(int columnIndex, String x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateString("+columnIndex+", "+quote(x)+");");
            }
            update(columnIndex, x == null ? (Value) ValueNull.INSTANCE : ValueString.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnName the name of the column label
     * @param x the value
     * @throws SQLException if the result set is closed
     */
    public void updateString(String columnName, String x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateString("+quote(columnName)+", "+quote(x)+");");
            }
            update(columnName, x == null ? (Value) ValueNull.INSTANCE : ValueString.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed
     */
    public void updateDate(int columnIndex, Date x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateDate("+columnIndex+", x);");
            }
            update(columnIndex, x == null ? (Value) ValueNull.INSTANCE : ValueDate.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnName the name of the column label
     * @param x the value
     * @throws SQLException if the result set is closed
     */
    public void updateDate(String columnName, Date x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateDate("+quote(columnName)+", x);");
            }
            update(columnName, x == null ? (Value) ValueNull.INSTANCE : ValueDate.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed
     */
    public void updateTime(int columnIndex, Time x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateTime("+columnIndex+", x);");
            }
            update(columnIndex, x == null ? (Value) ValueNull.INSTANCE : ValueTime.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnName the name of the column label
     * @param x the value
     * @throws SQLException if the result set is closed
     */
    public void updateTime(String columnName, Time x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateTime("+quote(columnName)+", x);");
            }
            update(columnName, x == null ? (Value) ValueNull.INSTANCE : ValueTime.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed
     */
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateTimestamp("+columnIndex+", x);");
            }
            update(columnIndex, x == null ? (Value) ValueNull.INSTANCE : ValueTimestamp.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnName the name of the column label
     * @param x the value
     * @throws SQLException if the result set is closed
     */
    public void updateTimestamp(String columnName, Timestamp x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateTimestamp("+quote(columnName)+", x);");
            }
            update(columnName, x == null ? (Value) ValueNull.INSTANCE : ValueTimestamp.get(x));
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
     * @throws SQLException if the result set is closed
     */
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        updateAsciiStream(columnIndex, x, (long) length);
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed
     */
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        updateAsciiStream(columnIndex, x, -1);
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @param length the number of characters
     * @throws SQLException if the result set is closed
     */
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateAsciiStream("+columnIndex+", x, "+length+"L);");
            }
            checkClosed();
            Value v = conn.createClob(IOUtils.getAsciiReader(x), length);
            update(columnIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnName the name of the column label
     * @param x the value
     * @param length the number of characters
     * @throws SQLException if the result set is closed
     */
    public void updateAsciiStream(String columnName, InputStream x, int length) throws SQLException {
        updateAsciiStream(columnName, x, (long) length);
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnName the name of the column label
     * @param x the value
     * @throws SQLException if the result set is closed
     */
    public void updateAsciiStream(String columnName, InputStream x) throws SQLException {
        updateAsciiStream(columnName, x, -1);
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnName the name of the column label
     * @param x the value
     * @param length the number of characters
     * @throws SQLException if the result set is closed
     */
    public void updateAsciiStream(String columnName, InputStream x, long length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateAsciiStream("+quote(columnName)+", x, "+length+"L);");
            }
            checkClosed();
            Value v = conn.createClob(IOUtils.getAsciiReader(x), length);
            update(columnName, v);
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
     * @throws SQLException if the result set is closed
     */
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        updateBinaryStream(columnIndex, x, (long) length);
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed
     */
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        updateBinaryStream(columnIndex, x, -1);
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @param length the number of characters
     * @throws SQLException if the result set is closed
     */
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateBinaryStream("+columnIndex+", x, "+length+"L);");
            }
            checkClosed();
            Value v = conn.createBlob(x, length);
            update(columnIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnName the name of the column label
     * @param x the value
     * @throws SQLException if the result set is closed
     */
    public void updateBinaryStream(String columnName, InputStream x) throws SQLException {
        updateBinaryStream(columnName, x, -1);
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnName the name of the column label
     * @param x the value
     * @param length the number of characters
     * @throws SQLException if the result set is closed
     */
    public void updateBinaryStream(String columnName, InputStream x, int length) throws SQLException {
        updateBinaryStream(columnName, x, (long) length);
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnName the name of the column label
     * @param x the value
     * @param length the number of characters
     * @throws SQLException if the result set is closed
     */
    public void updateBinaryStream(String columnName, InputStream x, long length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateBinaryStream("+quote(columnName)+", x, "+length+"L);");
            }
            checkClosed();
            Value v = conn.createBlob(x, length);
            update(columnName, v);
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
     * @throws SQLException if the result set is closed
     */
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateCharacterStream("+columnIndex+", x, "+length+"L);");
            }
            checkClosed();
            Value v = conn.createClob(x, length);
            update(columnIndex, v);
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
     * @throws SQLException if the result set is closed
     */
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        updateCharacterStream(columnIndex, x, (long) length);
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed
     */
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        updateCharacterStream(columnIndex, x, -1);
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnName the name of the column label
     * @param x the value
     * @param length the number of characters
     * @throws SQLException if the result set is closed
     */
    public void updateCharacterStream(String columnName, Reader x, int length) throws SQLException {
        updateCharacterStream(columnName, x, (long) length);
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnName the name of the column label
     * @param x the value
     * @throws SQLException if the result set is closed
     */
    public void updateCharacterStream(String columnName, Reader x) throws SQLException {
        updateCharacterStream(columnName, x, -1);
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnName the name of the column label
     * @param x the value
     * @param length the number of characters
     * @throws SQLException if the result set is closed
     */
    public void updateCharacterStream(String columnName, Reader x, long length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateCharacterStream("+quote(columnName)+", x, "+length+"L);");
            }
            checkClosed();
            Value v = conn.createClob(x, length);
            update(columnName, v);
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
     * @throws SQLException if the result set is closed
     */
    public void updateObject(int columnIndex, Object x, int scale) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateObject("+columnIndex+", x, "+scale+");");
            }
            update(columnIndex, convertToUnknownValue(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }



    /**
     * Updates a column in the current or insert row.
     *
     * @param columnName the name of the column label
     * @param x the value
     * @param scale is ignored
     * @throws SQLException if the result set is closed
     */
    public void updateObject(String columnName, Object x, int scale) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateObject("+quote(columnName)+", x, "+scale+");");
            }
            update(columnName, convertToUnknownValue(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed
     */
    public void updateObject(int columnIndex, Object x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateObject("+columnIndex+", x);");
            }
            update(columnIndex, convertToUnknownValue(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnName the name of the column label
     * @param x the value
     * @throws SQLException if the result set is closed
     */
    public void updateObject(String columnName, Object x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateObject("+quote(columnName)+", x);");
            }
            update(columnName, convertToUnknownValue(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported]
     */
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateRef("+columnIndex+", x);");
            }
            throw Message.getUnsupportedException();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported]
     */
    public void updateRef(String columnName, Ref x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateRef("+quote(columnName)+", x);");
            }
            throw Message.getUnsupportedException();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed
     */
    public void updateBlob(int columnIndex, InputStream x) throws SQLException {
        updateBlob(columnIndex, x, -1);
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @param length the length
     * @throws SQLException if the result set is closed
     */
    public void updateBlob(int columnIndex, InputStream x, long length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateBlob("+columnIndex+", x, " + length + "L);");
            }
            checkClosed();
            Value v = conn.createBlob(x, length);
            update(columnIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed
     */
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateBlob("+columnIndex+", x);");
            }
            checkClosed();
            Value v;
            if (x == null) {
                v = ValueNull.INSTANCE;
            } else {
                v = conn.createBlob(x.getBinaryStream(), -1);
            }
            update(columnIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnName the name of the column label
     * @param x the value
     * @throws SQLException if the result set is closed
     */
    public void updateBlob(String columnName, Blob x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateBlob("+quote(columnName)+", x);");
            }
            checkClosed();
            Value v;
            if (x == null) {
                v = ValueNull.INSTANCE;
            } else {
                v = conn.createBlob(x.getBinaryStream(), -1);
            }
            update(columnName, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnName the name of the column label
     * @param x the value
     * @throws SQLException if the result set is closed
     */
    public void updateBlob(String columnName, InputStream x) throws SQLException {
        updateBlob(columnName, x, -1);
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnName the name of the column label
     * @param x the value
     * @param length the length
     * @throws SQLException if the result set is closed
     */
    public void updateBlob(String columnName, InputStream x, long length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateBlob("+quote(columnName)+", x, " + length + "L);");
            }
            checkClosed();
            Value v = conn.createBlob(x, -1);
            update(columnName, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed
     */
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateClob("+columnIndex+", x);");
            }
            checkClosed();
            Value v;
            if (x == null) {
                v = ValueNull.INSTANCE;
            } else {
                v = conn.createClob(x.getCharacterStream(), -1);
            }
            update(columnIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed
     */
    public void updateClob(int columnIndex, Reader x) throws SQLException {
        updateClob(columnIndex, x, -1);
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @param length the length
     * @throws SQLException if the result set is closed
     */
    public void updateClob(int columnIndex, Reader x, long length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateClob("+columnIndex+", x, " + length + "L);");
            }
            checkClosed();
            Value v = conn.createClob(x, length);
            update(columnIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnName the name of the column label
     * @param x the value
     * @throws SQLException if the result set is closed
     */
    public void updateClob(String columnName, Clob x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateClob("+quote(columnName)+", x);");
            }
            checkClosed();
            Value v;
            if (x == null) {
                v = ValueNull.INSTANCE;
            } else {
                v = conn.createClob(x.getCharacterStream(), -1);
            }
            update(columnName, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnName the name of the column label
     * @param x the value
     * @throws SQLException if the result set is closed
     */
    public void updateClob(String columnName, Reader x) throws SQLException {
        updateClob(columnName, x, -1);
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnName the name of the column label
     * @param x the value
     * @param length the length
     * @throws SQLException if the result set is closed
     */
    public void updateClob(String columnName, Reader x, long length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateClob("+quote(columnName)+", x, " + length + "L);");
            }
            checkClosed();
            Value v = conn.createClob(x, length);
            update(columnName, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported]
     */
    public void updateArray(int columnIndex, Array x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateArray("+columnIndex+", x);");
            }
            throw Message.getUnsupportedException();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported]
     */
    public void updateArray(String columnName, Array x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateArray("+quote(columnName)+", x);");
            }
            throw Message.getUnsupportedException();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported] Gets the cursor name if it was defined. This feature is
     * superseded by updateX methods. This method throws a SQLException because
     * cursor names are not supported.
     */
    public String getCursorName() throws SQLException {
        try {
            debugCodeCall("getCursorName");
            throw Message.getUnsupportedException();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the current row number. The first row is row 1, the second 2 and so
     * on. This method returns 0 before the first and after the last row.
     *
     * @return the row number
     */
    public int getRow() throws SQLException {
        try {
            debugCodeCall("getRow");
            checkClosed();
            int rowId = result.getRowId();
            if (rowId >= result.getRowCount()) {
                return 0;
            }
            return rowId + 1;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the result set concurrency.
     *
     * @return ResultSet.CONCUR_UPDATABLE
     */
    public int getConcurrency() throws SQLException {
        try {
            debugCodeCall("getConcurrency");
            checkClosed();
            UpdatableRow row = new UpdatableRow(conn, result);
            return row.isUpdatable() ? ResultSet.CONCUR_UPDATABLE : ResultSet.CONCUR_READ_ONLY;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the fetch direction.
     *
     * @return the direction: FETCH_FORWARD
     */
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
    public void setFetchSize(int rows) throws SQLException {
        try {
            debugCodeCall("setFetchSize", rows);
            checkClosed();

            if (rows < 0) {
                throw Message.getInvalidValueException("" + rows, "rows");
            } else if (rows > 0) {
                if (stat != null) {
                    int maxRows = stat.getMaxRows();
                    if (maxRows > 0 && rows > maxRows) {
                        throw Message.getInvalidValueException("" + rows, "rows");
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
     * Sets (changes) the fetch direction for this result set. This method
     * should only be called for scrollable result sets, otherwise it will throw
     * an exception (no matter what direction is used).
     *
     * @param direction the new fetch direction
     * @throws SQLException Unsupported Feature if the method is called for a
     *             forward-only result set
     */
    public void setFetchDirection(int direction) throws SQLException {
        try {
            debugCodeCall("setFetchDirection", direction);
            throw Message.getUnsupportedException();
        } catch (Exception e) {
            throw logAndConvert(e);
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
     * was not called yet.
     *
     * @return if the current position is before the first row
     * @throws SQLException if the result set is closed
     */
    public boolean isBeforeFirst() throws SQLException {
        try {
            debugCodeCall("isBeforeFirst");
            checkClosed();
            return result.getRowId() < 0;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if the current position is after the last row, that means next()
     * was called and returned false.
     *
     * @return if the current position is after the last row
     * @throws SQLException if the result set is closed
     */
    public boolean isAfterLast() throws SQLException {
        try {
            debugCodeCall("isAfterLast");
            checkClosed();
            int row = result.getRowId();
            int count = result.getRowCount();
            return row >= count || count == 0;
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
    public boolean isFirst() throws SQLException {
        try {
            debugCodeCall("isFirst");
            checkClosed();
            int row = result.getRowId();
            return row == 0 && row < result.getRowCount();
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
    public boolean isLast() throws SQLException {
        try {
            debugCodeCall("isLast");
            checkClosed();
            int row = result.getRowId();
            return row >= 0 && row == result.getRowCount() - 1;
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
     * Moves the current position to after the last row, that means after the end.
     *
     * @throws SQLException if the result set is closed
     */
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
    public boolean first() throws SQLException {
        try {
            debugCodeCall("first");
            checkClosed();
            if (result.getRowId() < 0) {
                return nextRow();
            }
            resetResult();
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
    public boolean last() throws SQLException {
        try {
            debugCodeCall("last");
            checkClosed();
            return absolute(-1);
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
     *            after the last row, if if the value is too small it is moved
     *            before the first row.
     * @return true if there is a row available, false if not
     * @throws SQLException if the result set is closed
     */
    public boolean absolute(int rowNumber) throws SQLException {
        try {
            debugCodeCall("absolute", rowNumber);
            checkClosed();
            if (rowNumber < 0) {
                rowNumber = result.getRowCount() + rowNumber + 1;
            } else if (rowNumber > result.getRowCount() + 1) {
                rowNumber = result.getRowCount() + 1;
            }
            if (rowNumber <= result.getRowId()) {
                resetResult();
            }
            while (result.getRowId() + 1 < rowNumber) {
                nextRow();
            }
            int row = result.getRowId();
            return row >= 0 && row < result.getRowCount();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Moves the current position to a specific row relative to the current row.
     *
     * @param rowCount 0 means don't do anything, 1 is the next row, -1 the
     *            previous. If the value is too large, the position is moved
     *            after the last row, if if the value is too small it is moved
     *            before the first row.
     * @return true if there is a row available, false if not
     * @throws SQLException if the result set is closed
     */
    public boolean relative(int rowCount) throws SQLException {
        try {
            debugCodeCall("relative", rowCount);
            checkClosed();
            int row = result.getRowId() + 1 + rowCount;
            if (row < 0) {
                row = 0;
            } else if (row > result.getRowCount()) {
                row = result.getRowCount() + 1;
            }
            return absolute(row);
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
     * Moves the current position to the insert row. The current row is remembered.
     *
     * @throws SQLException if the result set is closed
     */
    public void moveToInsertRow() throws SQLException {
        try {
            debugCodeCall("moveToInsertRow");
            checkClosed();
            insertRow = new Value[columnCount];
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Moves the current position to the current row.
     *
     * @throws SQLException if the result set is closed
     */
    public void moveToCurrentRow() throws SQLException {
        try {
            debugCodeCall("moveToCurrentRow");
            checkClosed();
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
     * @throws SQLException if the result set is closed or if not on the insert row
     */
    public void insertRow() throws SQLException {
        try {
            debugCodeCall("insertRow");
            checkClosed();
            if (insertRow == null) {
                throw Message.getSQLException(ErrorCode.NOT_ON_UPDATABLE_ROW);
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
     * @throws SQLException if the result set is closed or if the current row is
     *             the insert row or if not on a valid row
     */
    public void updateRow() throws SQLException {
        try {
            debugCodeCall("updateRow");
            checkClosed();
            if (insertRow != null) {
                throw Message.getSQLException(ErrorCode.NOT_ON_UPDATABLE_ROW);
            }
            checkOnValidRow();
            if (updateRow != null) {
                UpdatableRow row = getUpdatableRow();
                Value[] current = new Value[columnCount];
                for (int i = 0; i < updateRow.length; i++) {
                    current[i] = get(i + 1);
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
     * @throws SQLException if the result set is closed or if the current row is
     *             the insert row or if not on a valid row
     */
    public void deleteRow() throws SQLException {
        try {
            debugCodeCall("deleteRow");
            checkClosed();
            if (insertRow != null) {
                throw Message.getSQLException(ErrorCode.NOT_ON_UPDATABLE_ROW);
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
    public void refreshRow() throws SQLException {
        try {
            debugCodeCall("refreshRow");
            checkClosed();
            if (insertRow != null) {
                throw Message.getSQLException(ErrorCode.NO_DATA_AVAILABLE);
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
    public void cancelRowUpdates() throws SQLException {
        try {
            debugCodeCall("cancelRowUpdates");
            checkClosed();
            if (insertRow != null) {
                throw Message.getSQLException(ErrorCode.NO_DATA_AVAILABLE);
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
            throw Message.getSQLException(ErrorCode.RESULT_SET_NOT_UPDATABLE);
        }
        return row;
    }

    private int getColumnIndex(String columnName) throws SQLException {
        checkClosed();
        if (columnName == null) {
            throw Message.getInvalidValueException("columnName", null);
        }
        if (columnCount >= SysProperties.MIN_COLUMN_NAME_MAP) {
            if (columnNameMap == null) {
                HashMap map = new HashMap(columnCount);
                for (int i = 0; i < columnCount; i++) {
                    String c = result.getAlias(i).toUpperCase();
                    map.put(c, ObjectUtils.getInteger(i));
                    String tabName = result.getTableName(i);
                    if (tabName != null) {
                        String colName = result.getColumnName(i);
                        if (colName != null) {
                            c =  tabName + "." + colName;
                            if (!map.containsKey(c)) {
                                map.put(c, ObjectUtils.getInteger(i));
                            }
                        }
                    }
                }
                columnNameMap = map;
            }
            Integer index = (Integer) columnNameMap.get(columnName.toUpperCase());
            if (index == null) {
                throw Message.getSQLException(ErrorCode.COLUMN_NOT_FOUND_1, columnName);
            }
            return index.intValue() + 1;
        }
        for (int i = 0; i < columnCount; i++) {
            if (columnName.equalsIgnoreCase(result.getAlias(i))) {
                return i + 1;
            }
        }
        int idx = columnName.indexOf('.');
        if (idx > 0) {
            String table = columnName.substring(0, idx);
            String col = columnName.substring(idx+1);
            for (int i = 0; i < columnCount; i++) {
                if (table.equalsIgnoreCase(result.getTableName(i)) && col.equalsIgnoreCase(result.getColumnName(i))) {
                    return i + 1;
                }
            }
        }
        throw Message.getSQLException(ErrorCode.COLUMN_NOT_FOUND_1, columnName);
    }

    private void checkColumnIndex(int columnIndex) throws SQLException {
        checkClosed();
        if (columnIndex < 1 || columnIndex > columnCount) {
            throw Message.getInvalidValueException("" + columnIndex, "columnIndex");
        }
    }

    /**
     * Check if this result set is closed.
     *
     * @throws SQLException if it is closed
     */
    void checkClosed() throws SQLException {
        if (result == null) {
            throw Message.getSQLException(ErrorCode.OBJECT_CLOSED);
        }
        if (stat != null) {
            stat.checkClosed();
        }
        if (conn != null) {
            conn.checkClosed();
        }
    }

    private void checkOnValidRow() throws SQLException {
        if (result.getRowId() < 0 || result.getRowId() >= result.getRowCount()) {
            throw Message.getSQLException(ErrorCode.NO_DATA_AVAILABLE);
        }
    }

    private Value get(int columnIndex) throws SQLException {
        checkColumnIndex(columnIndex);
        checkOnValidRow();
        Value[] list;
        if (patchedRows == null) {
            list = result.currentRow();
        } else {
            list = (Value[]) patchedRows.get(ObjectUtils.getInteger(result.getRowId()));
            if (list == null) {
                list = result.currentRow();
            }
        }
        Value value = list[columnIndex - 1];
        wasNull = value == ValueNull.INSTANCE;
        return value;
    }

    private Value get(String columnName) throws SQLException {
        int columnIndex = getColumnIndex(columnName);
        return get(columnIndex);
    }

    private void update(String columnName, Value v) throws SQLException {
        int columnIndex = getColumnIndex(columnName);
        update(columnIndex, v);
    }

    private void update(int columnIndex, Value v) throws SQLException {
        checkColumnIndex(columnIndex);
        if (insertRow != null) {
            insertRow[columnIndex - 1] = v;
        } else {
            if (updateRow == null) {
                updateRow = new Value[columnCount];
            }
            updateRow[columnIndex - 1] = v;
        }
    }

    private boolean nextRow() throws SQLException {
        boolean next = result.next();
        if (!next && !scrollable) {
            result.close();
        }
        return next;
    }

    private void resetResult() throws SQLException {
        if (!scrollable) {
            throw Message.getSQLException(ErrorCode.RESULT_SET_NOT_SCROLLABLE);
        }
        result.reset();
    }

    /**
     * [Not supported] Returns the value of the specified column as a row id.
     *
     * @param columnIndex (1,2,...)
     */
/*## Java 1.6 begin ##
    public RowId getRowId(int columnIndex) throws SQLException {
        throw Message.getUnsupportedException();
    }
## Java 1.6 end ##*/

    /**
     * [Not supported] Returns the value of the specified column as a row id.
     *
     * @param columnName the name of the column label
     */
/*## Java 1.6 begin ##
    public RowId getRowId(String columnName) throws SQLException {
        throw Message.getUnsupportedException();
    }
## Java 1.6 end ##*/

    /**
     * [Not supported] Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     */
/*## Java 1.6 begin ##
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw Message.getUnsupportedException();
    }
## Java 1.6 end ##*/

    /**
     * [Not supported] Updates a column in the current or insert row.
     *
     * @param columnName the name of the column label
     * @param x the value
     */
/*## Java 1.6 begin ##
    public void updateRowId(String columnName, RowId x) throws SQLException {
        throw Message.getUnsupportedException();
    }
## Java 1.6 end ##*/

    /**
     * Returns the current result set holdability.
     *
     * @return the holdability
     * @throws SQLException if the connection is closed
     */
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
     * @throws SQLException if the result set is closed
     */
    public void updateNString(int columnIndex, String x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateNString("+columnIndex+", "+quote(x)+");");
            }
            update(columnIndex, x == null ? (Value) ValueNull.INSTANCE : ValueString.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnName the name of the column label
     * @param x the value
     * @throws SQLException if the result set is closed
     */
    public void updateNString(String columnName, String x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateNString("+quote(columnName)+", "+quote(x)+");");
            }
            update(columnName, x == null ? (Value) ValueNull.INSTANCE : ValueString.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported]
     */
/*## Java 1.6 begin ##
    public void updateNClob(int columnIndex, NClob x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateNClob("+columnIndex+", x);");
            }
            throw Message.getUnsupportedException();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }
## Java 1.6 end ##*/

    /**
     * [Not supported]
     */
/*## Java 1.6 begin ##
    public void updateNClob(int columnIndex, Reader x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateNClob("+columnIndex+", x);");
            }
            throw Message.getUnsupportedException();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }
## Java 1.6 end ##*/

    /**
     * [Not supported]
     */
/*## Java 1.6 begin ##
    public void updateNClob(int columnIndex, Reader x, long length)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateNClob("+columnIndex+", x, " + length + "L);");
            }
            throw Message.getUnsupportedException();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }
## Java 1.6 end ##*/

    /**
     * [Not supported]
     */
/*## Java 1.6 begin ##
    public void updateNClob(String columnName, Reader x)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateNClob("+quote(columnName)+", x);");
            }
            throw Message.getUnsupportedException();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }
## Java 1.6 end ##*/

    /**
     * [Not supported]
     */
/*## Java 1.6 begin ##
    public void updateNClob(String columnName, Reader x, long length)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateNClob("+quote(columnName)+", x, " + length+"L);");
            }
            throw Message.getUnsupportedException();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }
## Java 1.6 end ##*/

    /**
     * [Not supported]
     */
/*## Java 1.6 begin ##
    public void updateNClob(String columnName, NClob x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateNClob("+quote(columnName)+", x);");
            }
            throw Message.getUnsupportedException();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }
## Java 1.6 end ##*/


    /**
     * Returns the value of the specified column as a Clob.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
/*## Java 1.6 begin ##
    public NClob getNClob(int columnIndex) throws SQLException {
        try {
            int id = getNextId(TraceObject.CLOB);
            debugCodeAssign("NClob", TraceObject.CLOB, id, "getNClob(" + columnIndex + ")");
            Value v = get(columnIndex);
            return v == ValueNull.INSTANCE ? null : new JdbcClob(conn, v, id);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }
## Java 1.6 end ##*/

    /**
     * Returns the value of the specified column as a Clob.
     *
     * @param columnName the name of the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
/*## Java 1.6 begin ##
    public NClob getNClob(String columnName) throws SQLException {
        try {
            int id = getNextId(TraceObject.CLOB);
            debugCodeAssign("NClob", TraceObject.CLOB, id, "getNClob(" + columnName + ")");
            Value v = get(columnName);
            return v == ValueNull.INSTANCE ? null : new JdbcClob(conn, v, id);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }
## Java 1.6 end ##*/

    /**
     * [Not supported] Returns the value of the specified column as a SQLXML object.
     */
/*## Java 1.6 begin ##
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw Message.getUnsupportedException();
    }
## Java 1.6 end ##*/

    /**
     * [Not supported] Returns the value of the specified column as a SQLXML object.
     */
/*## Java 1.6 begin ##
    public SQLXML getSQLXML(String columnName) throws SQLException {
        throw Message.getUnsupportedException();
    }
## Java 1.6 end ##*/

    /**
     * [Not supported] Updates a column in the current or insert row.
     */
/*## Java 1.6 begin ##
    public void updateSQLXML(int columnIndex, SQLXML xmlObject)
            throws SQLException {
        throw Message.getUnsupportedException();
    }
## Java 1.6 end ##*/

    /**
     * [Not supported] Updates a column in the current or insert row.
     */
/*## Java 1.6 begin ##
    public void updateSQLXML(String columnName, SQLXML xmlObject)
            throws SQLException {
        throw Message.getUnsupportedException();
    }
## Java 1.6 end ##*/

    /**
     * Returns the value of the specified column as a String.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public String getNString(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getNString", columnIndex);
            return get(columnIndex).getString();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a String.
     *
     * @param columnName the column name
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public String getNString(String columnName) throws SQLException {
        try {
            debugCodeCall("getNString", columnName);
            return get(columnName).getString();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as input stream.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getNCharacterStream", columnIndex);
            return get(columnIndex).getReader();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as input stream.
     *
     * @param columnName the name of the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    public Reader getNCharacterStream(String columnName) throws SQLException {
        try {
            debugCodeCall("getNCharacterStream", columnName);
            return get(columnName).getReader();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed
     */
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        updateNCharacterStream(columnIndex, x, -1);
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @param length the number of characters
     * @throws SQLException if the result set is closed
     */
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateNCharacterStream("+columnIndex+", x, "+length+"L);");
            }
            checkClosed();
            Value v = conn.createClob(x, length);
            update(columnIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnName the name of the column label
     * @param x the value
     * @throws SQLException if the result set is closed
     */
    public void updateNCharacterStream(String columnName, Reader x) throws SQLException {
        updateNCharacterStream(columnName, x, -1);
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnName the name of the column label
     * @param x the value
     * @param length the number of characters
     * @throws SQLException if the result set is closed
     */
    public void updateNCharacterStream(String columnName, Reader x, long length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateNCharacterStream("+quote(columnName)+", x, "+length+"L);");
            }
            checkClosed();
            Value v = conn.createClob(x, length);
            update(columnName, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported] Return an object of this class if possible.
     */
/*## Java 1.6 begin ##
    public <T> T unwrap(Class<T> iface) throws SQLException {
        debugCode("unwrap");
        throw Message.getUnsupportedException();
    }
## Java 1.6 end ##*/

    /**
     * [Not supported] Checks if unwrap can return an object of this class.
     */
/*## Java 1.6 begin ##
    public boolean isWrapperFor(Class< ? > iface) throws SQLException {
        debugCode("isWrapperFor");
        throw Message.getUnsupportedException();
    }
## Java 1.6 end ##*/

    /**
     * INTERNAL
     */
    public String toString() {
        return getTraceObjectName() + ": " + result;
    }

    private void patchCurrentRow(Value[] row) throws SQLException {
        boolean changed = false;
        Value[] current = result.currentRow();
        for (int i = 0; i < row.length; i++) {
            if (!row[i].compareEqual(current[i])) {
                changed = true;
                break;
            }
        }
        if (patchedRows == null) {
            patchedRows = new HashMap();
        }
        Integer rowId = ObjectUtils.getInteger(result.getRowId());
        if (!changed) {
            patchedRows.remove(rowId);
        } else {
            patchedRows.put(rowId, row);
        }
    }

    private Value convertToUnknownValue(Object x) throws SQLException {
        checkClosed();
        return DataType.convertToValue(conn.getSession(), x, Value.UNKNOWN);
    }

}
