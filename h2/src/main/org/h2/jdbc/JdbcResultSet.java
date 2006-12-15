/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;

import org.h2.engine.Constants;
import org.h2.engine.SessionInterface;
import org.h2.message.*;
import org.h2.result.ResultInterface;
import org.h2.util.MathUtils;
import org.h2.util.StringUtils;
import org.h2.util.TypeConverter;
import org.h2.util.UpdatableRow;
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
 * Represents a result set. Column names are case-insensitive, quotes are not supported. The first column has the column
 * index 1.
 */
public class JdbcResultSet extends TraceObject implements ResultSet {
    private SessionInterface session;
    private ResultInterface result;
    private JdbcConnection conn;
    private JdbcStatement stat;
    private int columnCount;
    private boolean wasNull;
    private Value[] insertRow;
    private Value[] updateRow;
    private boolean closeStatement;

    /**
     * Moves the cursor to the next row of the result set.
     *
     * @return true if successfull, false if there are no more rows
     */
    public boolean next() throws SQLException {
        try {
            debugCodeCall("next");
            checkClosed();
            return result.next();
        } catch(Throwable e) {
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
            if(debug()) {
                debugCodeAssign("ResultSetMetaData", TraceObject.RESULT_SET_META_DATA, id);
                debugCodeCall("getMetaData");
            }
            checkClosed();
            JdbcResultSetMetaData meta = new JdbcResultSetMetaData(this, result, session.getTrace(), id);
            return meta;
        } catch(Throwable e) {
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
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Searches for a specific column in the result set. A case-insensitive search is made.
     *
     * @param columnName the name of the column label
     * @return the column index (1,2,...)
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public int findColumn(String columnName) throws SQLException {
        try {
            debugCodeCall("findColumn", columnName);
            return getColumnIndex(columnName);
        } catch(Throwable e) {
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
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    void closeInternal() throws SQLException {
        if (result != null) {
            try {
                result.close();
                if(closeStatement && stat != null) {
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
     * @return the statement or prepared statement, or null if created by a DatabaseMetaData call.
     */
    public Statement getStatement() throws SQLException {
        try {
            debugCodeCall("getStatement");
            checkClosed();
            if(closeStatement) {
                // if the result set was opened by a DatabaseMetaData call
                return null;
            }
            return stat;
        } catch(Throwable e) {
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
        } catch(Throwable e) {
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
        } catch(Throwable e) {
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
        } catch(Throwable e) {
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
        } catch(Throwable e) {
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
        } catch(Throwable e) {
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
        } catch(Throwable e) {
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
        } catch(Throwable e) {
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
        } catch(Throwable e) {
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
        } catch(Throwable e) {
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
        } catch(Throwable e) {
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
        } catch(Throwable e) {
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
        } catch(Throwable e) {
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
        } catch(Throwable e) {
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
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns a column value as a Java object. For BINARY data, the data is deserialized into a Java Object.
     *
     * @param columnIndex (1,2,...)
     * @return the value or null
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public Object getObject(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getObject", columnIndex);
            Value v = get(columnIndex);
            if(Constants.SERIALIZE_JAVA_OBJECTS) {
                if (v.getType() == Value.JAVA_OBJECT) {
                    return TypeConverter.deserialize(v.getBytes());
                }
            }
            return v.getObject();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns a column value as a Java object. For BINARY data, the data is deserialized into a Java Object.
     *
     * @param columnName the name of the column label
     * @return the value or null
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public Object getObject(String columnName) throws SQLException {
        try {
            debugCodeCall("getObject", columnName);
            Value v = get(columnName);
            if(Constants.SERIALIZE_JAVA_OBJECTS) {
                if (v.getType() == Value.JAVA_OBJECT) {
                    return TypeConverter.deserialize(v.getBytes());
                }
            }
            return v.getObject();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a boolean.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public boolean getBoolean(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getBoolean", columnIndex);
            Boolean v = get(columnIndex).getBoolean();
            return v == null ? false : v.booleanValue();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a boolean.
     *
     * @param columnName the name of the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public boolean getBoolean(String columnName) throws SQLException {
        try {
            debugCodeCall("getBoolean", columnName);
            Boolean v = get(columnName).getBoolean();
            return v == null ? false : v.booleanValue();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a byte.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public byte getByte(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getByte", columnIndex);
            return get(columnIndex).getByte();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a byte.
     *
     * @param columnName the name of the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public byte getByte(String columnName) throws SQLException {
        try {
            debugCodeCall("getByte", columnName);
            return get(columnName).getByte();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a short.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public short getShort(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getShort", columnIndex);
            return get(columnIndex).getShort();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a short.
     *
     * @param columnName the name of the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public short getShort(String columnName) throws SQLException {
        try {
            debugCodeCall("getShort", columnName);
            return get(columnName).getShort();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a long.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public long getLong(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getLong", columnIndex);
            return get(columnIndex).getLong();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a long.
     *
     * @param columnName the name of the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public long getLong(String columnName) throws SQLException {
        try {
            debugCodeCall("getLong", columnName);
            return get(columnName).getLong();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a float.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public float getFloat(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getFloat", columnIndex);
            return get(columnIndex).getFloat();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a float.
     *
     * @param columnName the name of the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public float getFloat(String columnName) throws SQLException {
        try {
            debugCodeCall("getFloat", columnName);
            return get(columnName).getFloat();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a double.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public double getDouble(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getDouble", columnIndex);
            return get(columnIndex).getDouble();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a double.
     *
     * @param columnName the name of the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public double getDouble(String columnName) throws SQLException {
        try {
            debugCodeCall("getDouble", columnName);
            return get(columnName).getDouble();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a String.
     * @deprecated
     *
     * @param columnName
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public BigDecimal getBigDecimal(String columnName, int scale) throws SQLException {
        try {
            if(debug()) {
                debugCode("getBigDecimal(" + StringUtils.quoteJavaString(columnName)+", "+scale+");");
            }
            if(scale < 0) {
                throw Message.getInvalidValueException(""+scale, "scale");
            }
            BigDecimal bd = get(columnName).getBigDecimal();
            return bd == null ? null : MathUtils.setScale(bd, scale);
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a String.
     * @deprecated
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        try {
            if(debug()) {
                debugCode("getBigDecimal(" + columnIndex+", "+scale+");");
            }
            if(scale < 0) {
                throw Message.getInvalidValueException(""+scale, "scale");
            }
            BigDecimal bd = get(columnIndex).getBigDecimal();
            return bd == null ? null : MathUtils.setScale(bd, scale);
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * This feature is deprecated.
     * @deprecated
     *
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getUnicodeStream", columnIndex);
            throw Message.getUnsupportedException();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * This feature is deprecated.
     * @deprecated
     *
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    public InputStream getUnicodeStream(String columnName) throws SQLException {
        try {
            debugCodeCall("getUnicodeStream", columnName);
            throw Message.getUnsupportedException();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets a column as a object using the specified type mapping.
     *
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    public Object getObject(int columnIndex, Map map) throws SQLException {
        try {
            if(debug()) {
                debugCode("getObject(" + columnIndex + ", map);");
            }
            throw Message.getUnsupportedException();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets a column as a object using the specified type mapping.
     *
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    public Object getObject(String columnName, Map map) throws SQLException {
        try {
            if(debug()) {
                debugCode("getObject(" + quote(columnName) + ", map);");
            }
            throw Message.getUnsupportedException();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets a column as a reference.
     *
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    public Ref getRef(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getRef", columnIndex);
            throw Message.getUnsupportedException();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets a column as a reference.
     *
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    public Ref getRef(String columnName) throws SQLException {
        try {
            debugCodeCall("getRef", columnName);
            throw Message.getUnsupportedException();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Date using a specified timezone.
     *
     * @param columnIndex (1,2,...)
     * @param calendar the calendar
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public Date getDate(int columnIndex, Calendar calendar) throws SQLException {
        try {
            if(debug()) {
                debugCode("getDate(" + columnIndex + ", calendar)");
            }
            Date x = get(columnIndex).getDate();
            return TypeConverter.convertDateToCalendar(x, calendar);
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Date using a specified timezone.
     *
     * @param columnName the name of the column label
     * @param calendar the calendar
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public Date getDate(String columnName, Calendar calendar) throws SQLException {
        try {
            if(debug()) {
                debugCode("getDate(" + StringUtils.quoteJavaString(columnName) + ", calendar)");
            }
            Date x = get(columnName).getDate();
            return TypeConverter.convertDateToCalendar(x, calendar);
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Time using a specified timezone.
     *
     * @param columnIndex (1,2,...)
     * @param calendar the calendar
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public Time getTime(int columnIndex, Calendar calendar) throws SQLException {
        try {
            if(debug()) {
                debugCode("getTime(" + columnIndex + ", calendar)");
            }
            Time x = get(columnIndex).getTime();
            return TypeConverter.convertTimeToCalendar(x, calendar);
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Time using a specified timezone.
     *
     * @param columnName the name of the column label
     * @param calendar the calendar
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public Time getTime(String columnName, Calendar calendar) throws SQLException {
        try {
            if(debug()) {
                debugCode("getTime(" + StringUtils.quoteJavaString(columnName) + ", calendar)");
            }
            Time x = get(columnName).getTime();
            return TypeConverter.convertTimeToCalendar(x, calendar);
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Timestamp using a specified timezone.
     *
     * @param columnIndex (1,2,...)
     * @param calendar the calendar
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public Timestamp getTimestamp(int columnIndex, Calendar calendar) throws SQLException {
        try {
            if(debug()) {
                debugCode("getTimestamp(" + columnIndex + ", calendar)");
            }
            Timestamp x = get(columnIndex).getTimestamp();
            return TypeConverter.convertTimestampToCalendar(x, calendar);
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Timestamp.
     *
     * @param columnName the name of the column label
     * @param calendar the calendar
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public Timestamp getTimestamp(String columnName, Calendar calendar) throws SQLException {
        try {
            if(debug()) {
                debugCode("getTimestamp(" + StringUtils.quoteJavaString(columnName) + ", calendar)");
            }
            Timestamp x = get(columnName).getTimestamp();
            return TypeConverter.convertTimestampToCalendar(x, calendar);
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a Blob.
     *
     * @param columnName the name of the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public Blob getBlob(int columnIndex) throws SQLException {
        try {
            int id = getNextId(TraceObject.BLOB);
            debugCodeAssign("Blob", TraceObject.BLOB, id);
            debugCodeCall("getBlob", columnIndex);
            Value v = get(columnIndex);
            return v == ValueNull.INSTANCE ? null : new JdbcBlob(session, conn, v, id);
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a Blob.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public Blob getBlob(String columnName) throws SQLException {
        try {
            int id = getNextId(TraceObject.BLOB);
            debugCodeAssign("Blob", TraceObject.BLOB, id);
            debugCodeCall("getBlob", columnName);
            Value v = get(columnName);
            return v == ValueNull.INSTANCE ? null : new JdbcBlob(session, conn, v, id);
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a byte array.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public byte[] getBytes(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getBytes", columnIndex);
            return get(columnIndex).getBytes();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a byte array.
     *
     * @param columnName the name of the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public byte[] getBytes(String columnName) throws SQLException {
        try {
            debugCodeCall("getBytes", columnName);
            return get(columnName).getBytes();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as input stream.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getBinaryStream", columnIndex);
            return get(columnIndex).getInputStream();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as input stream.
     *
     * @param columnName the name of the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public InputStream getBinaryStream(String columnName) throws SQLException {
        try {
            debugCodeCall("getBinaryStream", columnName);
            return get(columnName).getInputStream();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }


    /**
     * Returns the value of the specified column as a Clob.
     *
     * @param columnName the name of the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public Clob getClob(int columnIndex) throws SQLException {
        try {
            int id = getNextId(TraceObject.CLOB);
            debugCodeAssign("Clob", TraceObject.CLOB, id);
            debugCodeCall("getClob", columnIndex);
            Value v = get(columnIndex);
            return v == ValueNull.INSTANCE ? null : new JdbcClob(session, conn, v, id);
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a Clob.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public Clob getClob(String columnName) throws SQLException {
        try {
            int id = getNextId(TraceObject.CLOB);
            debugCodeAssign("Clob", TraceObject.CLOB, id);
            debugCodeCall("getClob", columnName);
            Value v = get(columnName);
            return v == ValueNull.INSTANCE ? null : new JdbcClob(session, conn, v, id);
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a Array.
     *
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    public Array getArray(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getArray", columnIndex);
            throw Message.getUnsupportedException();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a Array.
     *
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    public Array getArray(String columnName) throws SQLException {
        try {
            debugCodeCall("getArray", columnName);
            throw Message.getUnsupportedException();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as input stream.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getAsciiStream", columnIndex);
            String s = get(columnIndex).getString();
            // TODO ascii stream: convert the reader to a ascii stream
            return s == null ? null : TypeConverter.getInputStream(s);
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as input stream.
     *
     * @param columnName the name of the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public InputStream getAsciiStream(String columnName) throws SQLException {
        try {
            debugCodeCall("getAsciiStream", columnName);
            String s = get(columnName).getString();
            // TODO ascii stream: convert the reader to a ascii stream
            return s == null ? null : TypeConverter.getInputStream(s);
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as input stream.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getCharacterStream", columnIndex);
            return get(columnIndex).getReader();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as input stream.
     *
     * @param columnName the name of the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public Reader getCharacterStream(String columnName) throws SQLException {
        try {
            debugCodeCall("getCharacterStream", columnName);
            return get(columnName).getReader();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * THIS FEATURE IS NOT SUPPORTED.
     *
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    public URL getURL(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getURL", columnIndex);
            throw Message.getUnsupportedException();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * THIS FEATURE IS NOT SUPPORTED.
     *
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    public URL getURL(String columnName) throws SQLException {
        try {
            debugCodeCall("getURL", columnName);
            throw Message.getUnsupportedException();
        } catch(Throwable e) {
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
        } catch(Throwable e) {
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
        } catch(Throwable e) {
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
            if(debug()) {
                debugCode("updateBoolean("+columnIndex+", "+x+");");
            }
            update(columnIndex, ValueBoolean.get(x));
        } catch(Throwable e) {
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
            if(debug()) {
                debugCode("updateBoolean("+quote(columnName)+", "+x+");");
            }
            update(columnName, ValueBoolean.get(x));
        } catch(Throwable e) {
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
            if(debug()) {
                debugCode("updateByte("+columnIndex+", "+x+");");
            }
            update(columnIndex, ValueByte.get(x));
        } catch(Throwable e) {
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
            if(debug()) {
                debugCode("updateByte("+columnName+", "+x+");");
            }
            update(columnName, ValueByte.get(x));
        } catch(Throwable e) {
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
            if(debug()) {
                debugCode("updateBytes("+columnIndex+", x);");
            }
            update(columnIndex, x == null ? (Value) ValueNull.INSTANCE : ValueBytes.get(x));
        } catch(Throwable e) {
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
            if(debug()) {
                debugCode("updateBytes("+quote(columnName)+", x);");
            }
            update(columnName, x == null ? (Value) ValueNull.INSTANCE : ValueBytes.get(x));
        } catch(Throwable e) {
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
            if(debug()) {
                debugCode("updateShort("+columnIndex+", "+x+");");
            }
            update(columnIndex, ValueShort.get(x));
        } catch(Throwable e) {
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
            if(debug()) {
                debugCode("updateShort("+quote(columnName)+", "+x+");");
            }
            update(columnName, ValueShort.get(x));
        } catch(Throwable e) {
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
            if(debug()) {
                debugCode("updateInt("+columnIndex+", "+x+");");
            }
            update(columnIndex, ValueInt.get(x));
        } catch(Throwable e) {
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
            if(debug()) {
                debugCode("updateInt("+quote(columnName)+", "+x+");");
            }
            update(columnName, ValueInt.get(x));
        } catch(Throwable e) {
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
            if(debug()) {
                debugCode("updateLong("+columnIndex+", "+x+");");
            }
            update(columnIndex, ValueLong.get(x));
        } catch(Throwable e) {
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
            if(debug()) {
                debugCode("updateLong("+quote(columnName)+", "+x+");");
            }
            update(columnName, ValueLong.get(x));
        } catch(Throwable e) {
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
            if(debug()) {
                debugCode("updateFloat("+columnIndex+", "+x+"f);");
            }
            update(columnIndex, ValueFloat.get(x));
        } catch(Throwable e) {
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
            if(debug()) {
                debugCode("updateFloat("+quote(columnName)+", "+x+"f);");
            }
            update(columnName, ValueFloat.get(x));
        } catch(Throwable e) {
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
            if(debug()) {
                debugCode("updateDouble("+columnIndex+", "+x+");");
            }
            update(columnIndex, ValueDouble.get(x));
        } catch(Throwable e) {
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
            if(debug()) {
                debugCode("updateDouble("+quote(columnName)+", "+x+");");
            }
            update(columnName, ValueDouble.get(x));
        } catch(Throwable e) {
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
            if(debug()) {
                debugCode("updateBigDecimal("+columnIndex+", x);");
            }
            update(columnIndex, x == null ? (Value) ValueNull.INSTANCE : ValueDecimal.get(x));
        } catch(Throwable e) {
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
            if(debug()) {
                debugCode("updateBigDecimal("+quote(columnName)+", x);");
            }
            update(columnName, x == null ? (Value) ValueNull.INSTANCE : ValueDecimal.get(x));
        } catch(Throwable e) {
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
            if(debug()) {
                debugCode("updateString("+columnIndex+", "+quote(x)+");");
            }
            update(columnIndex, x == null ? (Value) ValueNull.INSTANCE : ValueString.get(x));
        } catch(Throwable e) {
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
            if(debug()) {
                debugCode("updateString("+quote(columnName)+", "+quote(x)+");");
            }
            update(columnName, x == null ? (Value) ValueNull.INSTANCE : ValueString.get(x));
        } catch(Throwable e) {
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
            if(debug()) {
                debugCode("updateDate("+columnIndex+", x);");
            }
            update(columnIndex, x == null ? (Value) ValueNull.INSTANCE : ValueDate.get(x));
        } catch(Throwable e) {
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
            if(debug()) {
                debugCode("updateDate("+quote(columnName)+", x);");
            }
            update(columnName, x == null ? (Value) ValueNull.INSTANCE : ValueDate.get(x));
        } catch(Throwable e) {
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
            if(debug()) {
                debugCode("updateTime("+columnIndex+", x);");
            }
            update(columnIndex, x == null ? (Value) ValueNull.INSTANCE : ValueTime.get(x));
        } catch(Throwable e) {
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
            if(debug()) {
                debugCode("updateTime("+quote(columnName)+", x);");
            }
            update(columnName, x == null ? (Value) ValueNull.INSTANCE : ValueTime.get(x));
        } catch(Throwable e) {
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
            if(debug()) {
                debugCode("updateTimestamp("+columnIndex+", x);");
            }
            update(columnIndex, x == null ? (Value) ValueNull.INSTANCE : ValueTimestamp.get(x));
        } catch(Throwable e) {
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
            if(debug()) {
                debugCode("updateTimestamp("+quote(columnName)+", x);");
            }
            update(columnName, x == null ? (Value) ValueNull.INSTANCE : ValueTimestamp.get(x));
        } catch(Throwable e) {
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
        try {
            if(debug()) {
                debugCode("updateAsciiStream("+columnIndex+", x, "+length+");");
            }
            checkClosed();            
            Value v = conn.createClob(TypeConverter.getAsciiReader(x), length);
            update(columnIndex, v);
        } catch(Throwable e) {
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
        try {
            if(debug()) {
                debugCode("updateAsciiStream("+quote(columnName)+", x, "+length+");");
            }
            checkClosed();            
            Value v = conn.createClob(TypeConverter.getAsciiReader(x), length);
            update(columnName, v);
        } catch(Throwable e) {
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
        try {
            if(debug()) {
                debugCode("updateBinaryStream("+columnIndex+", x, "+length+");");
            }
            checkClosed();            
            Value v = conn.createBlob(x, length);
            update(columnIndex, v);
        } catch(Throwable e) {
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
    public void updateBinaryStream(String columnName, InputStream x, int length) throws SQLException {
        try {
            if(debug()) {
                debugCode("updateBinaryStream("+quote(columnName)+", x, "+length+");");
            }
            checkClosed();            
            Value v = conn.createBlob(x, length);
            update(columnName, v);
        } catch(Throwable e) {
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
        try {
            if(debug()) {
                debugCode("updateCharacterStream("+columnIndex+", x, "+length+");");
            }
            checkClosed();            
            Value v = conn.createClob(x, length);
            update(columnIndex, v);
        } catch(Throwable e) {
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
    public void updateCharacterStream(String columnName, Reader x, int length) throws SQLException {
        try {
            if(debug()) {
                debugCode("updateCharacterStream("+quote(columnName)+", x, "+length+");");
            }
            checkClosed();            
            Value v = conn.createClob(x, length);
            update(columnName, v);
        } catch(Throwable e) {
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
            if(debug()) {
                debugCode("updateObject("+columnIndex+", x, "+scale+");");
            }
            update(columnIndex, DataType.convertToValue(session, x, Value.UNKNOWN));
        } catch(Throwable e) {
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
            if(debug()) {
                debugCode("updateObject("+quote(columnName)+", x, "+scale+");");
            }
            update(columnName, DataType.convertToValue(session, x, Value.UNKNOWN));
        } catch(Throwable e) {
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
            if(debug()) {
                debugCode("updateObject("+columnIndex+", x);");
            }
            update(columnIndex, DataType.convertToValue(session, x, Value.UNKNOWN));
        } catch(Throwable e) {
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
            if(debug()) {
                debugCode("updateObject("+quote(columnName)+", x);");
            }
            update(columnName, DataType.convertToValue(session, x, Value.UNKNOWN));
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * THIS FEATURE IS NOT SUPPORTED.
     *
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        try {
            if(debug()) {
                debugCode("updateRef("+columnIndex+", x);");
            }
            throw Message.getUnsupportedException();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * THIS FEATURE IS NOT SUPPORTED.
     *
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    public void updateRef(String columnName, Ref x) throws SQLException {
        try {
            if(debug()) {
                debugCode("updateRef("+quote(columnName)+", x);");
            }
            throw Message.getUnsupportedException();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * THIS FEATURE IS NOT SUPPORTED.
     *
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        try {
            if(debug()) {
                debugCode("updateBlob("+columnIndex+", x);");
            }
            throw Message.getUnsupportedException();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * THIS FEATURE IS NOT SUPPORTED.
     *
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    public void updateBlob(String columnName, Blob x) throws SQLException {
        try {
            if(debug()) {
                debugCode("updateBlob("+quote(columnName)+", x);");
            }
            throw Message.getUnsupportedException();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * THIS FEATURE IS NOT SUPPORTED.
     *
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        try {
            if(debug()) {
                debugCode("updateClob("+columnIndex+", x);");
            }
            throw Message.getUnsupportedException();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * THIS FEATURE IS NOT SUPPORTED.
     *
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    public void updateClob(String columnName, Clob x) throws SQLException {
        try {
            if(debug()) {
                debugCode("updateClob("+quote(columnName)+", x);");
            }
            throw Message.getUnsupportedException();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * THIS FEATURE IS NOT SUPPORTED.
     *
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    public void updateArray(int columnIndex, Array x) throws SQLException {
        try {
            if(debug()) {
                debugCode("updateArray("+columnIndex+", x);");
            }
            throw Message.getUnsupportedException();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * THIS FEATURE IS NOT SUPPORTED.
     *
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    public void updateArray(String columnName, Array x) throws SQLException {
        try {
            if(debug()) {
                debugCode("updateArray("+quote(columnName)+", x);");
            }
            throw Message.getUnsupportedException();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the cursor name if it was defined. Not all databases support cursor names, and this feature is superseded by
     * updateXXX methods. This method throws a SQLException because cursor names are not supported. This is as defined
     * in the in the JDBC specs.
     *
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    public String getCursorName() throws SQLException {
        try {
            debugCodeCall("getCursorName");
            throw Message.getUnsupportedException();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the current row number. The first row is row 1, the second 2 and so on. This method returns 0 before the
     * first and after the last row.
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
            } else {
                return rowId + 1;
            }
        } catch(Throwable e) {
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
            UpdatableRow upd = new UpdatableRow(conn, result, session);
            return upd.isUpdatable() ? ResultSet.CONCUR_UPDATABLE : ResultSet.CONCUR_READ_ONLY;
        } catch(Throwable e) {
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
        } catch(Throwable e) {
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
            return 0;
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the number of rows suggested to read in one step. This value cannot be higher than the maximum rows
     * (setMaxRows) set by the statement or prepared statement, otherwise an exception is throws.
     *
     * @param rowCount the number of rows
     */
    public void setFetchSize(int rowCount) throws SQLException {
        try {
            debugCodeCall("setFetchSize", rowCount);
            checkClosed();
            if (rowCount < 0) {
                throw Message.getInvalidValueException("" + rowCount, "rowCount");
            }
            if (rowCount > 0) {
                if (stat != null) {
                    int maxRows = stat.getMaxRows();
                    if (maxRows > 0 && rowCount > maxRows) {
                        throw Message.getInvalidValueException("" + rowCount, "rowCount");
                    }
                }
            }
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets (changes) the fetch direction for this result set. This method should only be called for scrollable
     * result sets, otherwise it will throw an exception (no matter what direction is used).
     *
     * @param direction the new fetch direction
     * @throws SQLException Unsupported Feature (SQL State 0A000) if the method is called for a forward-only result set
     */
        public void setFetchDirection(int direction) throws SQLException {
            try {
                debugCodeCall("setFetchDirection", direction);
                throw Message.getUnsupportedException();
            } catch(Throwable e) {
                throw logAndConvert(e);
            }
        }

    /**
     * Get the result set type.
     *
     * @return the result set type (TYPE_FORWARD_ONLY, TYPE_SCROLL_INSENSITIVE or TYPE_SCROLL_SENSITIVE)
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public int getType() throws SQLException {
        try {
            debugCodeCall("getType");
            checkClosed();
            return stat == null ? ResultSet.TYPE_FORWARD_ONLY : stat.resultSetType;
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if the current position is before the first row, that means next() was not called yet.
     *
     * @return if the current position is before the first row
     * @throws SQLException if the result set is closed
     */
    public boolean isBeforeFirst() throws SQLException {
        try {
            debugCodeCall("isBeforeFirst");
            checkClosed();
            return result.getRowId() < 0;
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if the current position is after the last row, that means next() was called and returned false.
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
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if the current position is row 1, that means next() was called once and returned true.
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
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if the current position is the last row, that means next() was called and did not yet returned false, but
     * will in the next call.
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
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Moves the current position to before the first row, that means resets the result set.
     *
     * @throws SQLException if the result set is closed
     */
    public void beforeFirst() throws SQLException {
        try {
            debugCodeCall("beforeFirst");
            checkClosed();
            result.reset();
        } catch(Throwable e) {
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
            while (result.next()) {
                // nothing
            }
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
}

    /**
     * Moves the current position to the first row. This is the same as calling beforeFirst() followed by next().
     *
     * @return true if there is a row available, false if not
     * @throws SQLException if the result set is closed
     */
    public boolean first() throws SQLException {
        try {
            debugCodeCall("first");
            checkClosed();
            result.reset();
            return result.next();
        } catch(Throwable e) {
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
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Moves the current position to a specific row.
     *
     * @param rowNumber the row number. 0 is not allowed, 1 means the first row, 2 the second. -1 means the last row, -2
     *            the row before the last row. If the value is too large, the position is moved after the last row, if
     *            if the value is too small it is moved before the first row.
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
//            if (rowNumber == 0) {
//                throw Message.getInvalidValueException("" + rowNumber, "rowNumber");
//            } else
            if (rowNumber <= result.getRowId()) {
                result.reset();
            }
            while (result.getRowId() + 1 < rowNumber) {
                result.next();
            }
            int row = result.getRowId();
            return row >= 0 && row < result.getRowCount();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    // TODO javadoc: check that there are no - - in the javadoc
    /**
     * Moves the current position to a specific row relative to the current row.
     *
     * @param rowCount 0 means don't do anything, 1 is the next row, -1 the previous. If the value is too large, the
     *            position is moved after the last row, if if the value is too small it is moved before the first row.
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
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Moves the cursor to the last row, or row before first row if the current position is the first row.
     *
     * @return true if there is a row available, false if not
     * @throws SQLException if the result set is closed
     */
    public boolean previous() throws SQLException {
        try {
            debugCodeCall("previous");
            checkClosed();
            return relative(-1);
        } catch(Throwable e) {
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
        } catch(Throwable e) {
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
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Detects if the row was updated (by somebody else or the caller).
     *
     * @return false because this driver does detect this
     */
    public boolean rowUpdated() throws SQLException {
        try {
            debugCodeCall("rowUpdated");
            return false;
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Detects if the row was inserted.
     *
     * @return false because this driver does detect this
     */
    public boolean rowInserted() throws SQLException {
        try {
            debugCodeCall("rowInserted");
            return false;
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Detects if the row was deleted (by somebody else or the caller).
     *
     * @return false because this driver does detect this
     */
    public boolean rowDeleted() throws SQLException {
        try {
            debugCodeCall("rowDeleted");
            return false;
        } catch(Throwable e) {
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
                throw Message.getSQLException(Message.NOT_ON_UPDATABLE_ROW);
            }
            getUpdatableRow().insertRow(insertRow);
            insertRow = null;
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates the current row.
     *
     * @throws SQLException if the result set is closed or if the current row is the insert row or if not on a valid row
     */
    public void updateRow() throws SQLException {
        try {
            debugCodeCall("updateRow");
            checkClosed();
            if (insertRow != null) {
                throw Message.getSQLException(Message.NOT_ON_UPDATABLE_ROW);
            }
            checkOnValidRow();
            if (updateRow != null) {
                getUpdatableRow().updateRow(result.currentRow(), updateRow);
                updateRow = null;
            }
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Deletes the current row.
     *
     * @throws SQLException if the result set is closed or if the current row is the insert row or if not on a valid row
     */
    public void deleteRow() throws SQLException {
        try {
            debugCodeCall("deleteRow");
            checkClosed();
            if (insertRow != null) {
                throw Message.getSQLException(Message.NOT_ON_UPDATABLE_ROW);
            }
            checkOnValidRow();
            getUpdatableRow().deleteRow(result.currentRow());
            updateRow = null;
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Re-reads the current row from the database.
     *
     * @throws SQLException if the result set is closed or if the current row is the insert row or if the row has been
     *             deleted or if not on a valid row
     */
    public void refreshRow() throws SQLException {
        try {
            debugCodeCall("refreshRow");
            checkClosed();
            if (insertRow != null) {
                throw Message.getSQLException(Message.NO_DATA_AVAILABLE);
            }
            checkOnValidRow();
            getUpdatableRow().refreshRow(result.currentRow());
            updateRow = null;
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Cancels updating a row.
     *
     * @throws SQLException if the result set is closed or if the current row is the insert row
     */
    public void cancelRowUpdates() throws SQLException {
        try {
            debugCodeCall("cancelRowUpdates");
            checkClosed();
            if (insertRow != null) {
                throw Message.getSQLException(Message.NO_DATA_AVAILABLE);
            }
            updateRow = null;
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    // =============================================================

    JdbcResultSet(SessionInterface session, JdbcConnection conn, JdbcStatement stat, ResultInterface result, int id, boolean closeStatement) {
        setTrace(session.getTrace(), TraceObject.RESULT_SET, id);
        this.session = session;
        this.conn = conn;
        this.stat = stat;
        this.result = result;
        columnCount = result.getVisibleColumnCount();
        this.closeStatement = closeStatement;
    }

    private UpdatableRow getUpdatableRow() throws SQLException {
        UpdatableRow upd = new UpdatableRow(conn, result, session);
        if(!upd.isUpdatable()) {
            throw Message.getSQLException(Message.NOT_ON_UPDATABLE_ROW);
        }
        return upd;
    }

    int getColumnIndex(String columnName) throws SQLException {
        checkClosed();
        if (columnName == null) {
            throw Message.getInvalidValueException("columnName", null);
        }
        for (int i = 0; i < columnCount; i++) {
            if (columnName.equalsIgnoreCase(result.getAlias(i))) {
                return i + 1;
            }
        }
        int idx = columnName.indexOf('.');
        if(idx > 0) {
            String table = columnName.substring(0, idx);
            String col = columnName.substring(idx+1);
            for (int i = 0; i < columnCount; i++) {
                if (table.equals(result.getTableName(i)) && col.equalsIgnoreCase(result.getColumnName(i))) {
                    return i + 1;
                }
            }
        }
        throw Message.getSQLException(Message.COLUMN_NOT_FOUND_1, columnName);
    }

    void checkColumnIndex(int columnIndex) throws SQLException {
        checkClosed();
        if (columnIndex < 1 || columnIndex > columnCount) {
            throw Message.getInvalidValueException("" + columnIndex, "columnIndex");
        }
    }

    void checkClosed() throws SQLException {
        if (result == null) {
            throw Message.getSQLException(Message.OBJECT_CLOSED);
        }
        if (stat != null) {
            stat.checkClosed();
        }
        if(conn != null) {
            conn.checkClosed();
        }
    }

    private void checkOnValidRow() throws SQLException {
        if (result.getRowId() < 0 || result.getRowId() >= result.getRowCount()) {
            throw Message.getSQLException(Message.NO_DATA_AVAILABLE);
        }
    }

    private Value get(int columnIndex) throws SQLException {
        checkColumnIndex(columnIndex);
        checkOnValidRow();
        Value[] list = result.currentRow();
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

    /**
     * INTERNAL
     */
    public int getTraceId() {
        return super.getTraceId();
    }

    JdbcConnection getConnection() {
        return conn;
    }

    /**
     * Returns the value of the specified column as a row id.
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    //#ifdef JDK16
/*
    public RowId getRowId(int columnIndex) throws SQLException {
        throw Message.getUnsupportedException();
    }
*/
    //#endif

    /**
     * Returns the value of the specified column as a row id.
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    //#ifdef JDK16
/*
    public RowId getRowId(String columnName) throws SQLException {
        throw Message.getUnsupportedException();
    }
*/
    //#endif

    /**
     * Updates a column in the current or insert row.
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    //#ifdef JDK16
/*
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw Message.getUnsupportedException();
    }
*/
    //#endif

    /**
     * Updates a column in the current or insert row.
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    //#ifdef JDK16
/*
    public void updateRowId(String columnName, RowId x) throws SQLException {
        throw Message.getUnsupportedException();
    }
*/
    //#endif

    /**
     * Returns the current result set holdability.
     *
     * @return the holdability
     * @throws SQLException if the connection is closed
     */
    public int getHoldability() throws SQLException {
        try {
            debugCodeCall("getHoldability");
            return conn.getHoldability();
        } catch(Throwable e) {
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
        } catch(Throwable e) {
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
            if(debug()) {
                debugCode("updateNString("+columnIndex+", "+quote(x)+");");
            }
            update(columnIndex, x == null ? (Value) ValueNull.INSTANCE : ValueString.get(x));
        } catch(Throwable e) {
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
    public void updateNString(String columnName, String x) throws SQLException {
        try {
            if(debug()) {
                debugCode("updateNString("+quote(columnName)+", "+quote(x)+");");
            }
            update(columnName, x == null ? (Value) ValueNull.INSTANCE : ValueString.get(x));
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * THIS FEATURE IS NOT SUPPORTED.
     *
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    //#ifdef JDK16
/*
    public void updateNClob(int columnIndex, NClob x) throws SQLException {
        try {
            if(debug()) {
                debugCode("updateNClob("+columnIndex+", x);");
            }
            throw Message.getUnsupportedException();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }
*/
    //#endif

    /**
     * THIS FEATURE IS NOT SUPPORTED.
     *
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    //#ifdef JDK16
/*
    public void updateNClob(String columnName, NClob nClob) throws SQLException {
        try {
            if(debug()) {
                debugCode("updateNClob("+quote(columnName)+", x);");
            }
            throw Message.getUnsupportedException();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }
*/
    //#endif


    /**
     * Returns the value of the specified column as a Clob.
     *
     * @param columnName the name of the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    //#ifdef JDK16
/*
    public NClob getNClob(int columnIndex) throws SQLException {
        try {
            int id = getNextId(TraceObject.CLOB);
            debugCodeAssign("NClob", TraceObject.CLOB, id);
            debugCodeCall("getNClob", columnIndex);
            Value v = get(columnIndex);
            return v == ValueNull.INSTANCE ? null : new JdbcClob(session, conn, v, id);
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }
*/
    //#endif

    /**
     * Returns the value of the specified column as a Clob.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    //#ifdef JDK16
/*
    public NClob getNClob(String columnName) throws SQLException {
        try {
            int id = getNextId(TraceObject.CLOB);
            debugCodeAssign("NClob", TraceObject.CLOB, id);
            debugCodeCall("getNClob", columnName);
            Value v = get(columnName);
            return v == ValueNull.INSTANCE ? null : new JdbcClob(session, conn, v, id);
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }
*/
    //#endif

    /**
     * Returns the value of the specified column as a SQLXML object.
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    //#ifdef JDK16
/*
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw Message.getUnsupportedException();
    }
*/
    //#endif

    /**
     * Returns the value of the specified column as a SQLXML object.
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    //#ifdef JDK16
/*
    public SQLXML getSQLXML(String columnName) throws SQLException {
        throw Message.getUnsupportedException();
    }
*/
    //#endif

    /**
     * Updates a column in the current or insert row.
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    //#ifdef JDK16
/*
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw Message.getUnsupportedException();
    }
*/
    //#endif

    /**
     * Updates a column in the current or insert row.
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    //#ifdef JDK16
/*
    public void updateSQLXML(String columnName, SQLXML xmlObject) throws SQLException {
        throw Message.getUnsupportedException();
    }
*/
    //#endif

    /**
     * Returns the value of the specified column as a String.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public String getNString(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getNString", columnIndex);
            return get(columnIndex).getString();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a String.
     *
     * @param columnName
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public String getNString(String columnName) throws SQLException {
        try {
            debugCodeCall("getNString", columnName);
            return get(columnName).getString();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as input stream.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getNCharacterStream", columnIndex);
            return get(columnIndex).getReader();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as input stream.
     *
     * @param columnName the name of the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is closed
     */
    public Reader getNCharacterStream(String columnName) throws SQLException {
        try {
            debugCodeCall("getNCharacterStream", columnName);
            return get(columnName).getReader();
        } catch(Throwable e) {
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
    public void updateNCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        try {
            if(debug()) {
                debugCode("updateNCharacterStream("+columnIndex+", x, "+length+");");
            }
            Value v = conn.createClob(x, length);
            update(columnIndex, v);
        } catch(Throwable e) {
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
    public void updateNCharacterStream(String columnName, Reader x, int length) throws SQLException {
        try {
            if(debug()) {
                debugCode("updateNCharacterStream("+quote(columnName)+", x, "+length+");");
            }
            Value v = conn.createClob(x, length);
            update(columnName, v);
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Return an object of this class if possible.
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    //#ifdef JDK16
/*
    public Object unwrap(Class<?> iface) throws SQLException {
        throw Message.getUnsupportedException();
    }
*/
    //#endif

    /**
     * Checks if unwrap can return an object of this class.
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    //#ifdef JDK16
/*
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw Message.getUnsupportedException();
    }
*/
    //#endif

}
