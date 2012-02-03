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
//## Java 1.4 begin ##
import java.sql.ParameterMetaData;
import java.sql.Statement;
//## Java 1.4 end ##
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Calendar;

import org.h2.command.CommandInterface;
import org.h2.constant.ErrorCode;
import org.h2.expression.ParameterInterface;
import org.h2.message.Message;
import org.h2.message.TraceObject;
import org.h2.result.ResultInterface;
import org.h2.util.DateTimeUtils;
import org.h2.util.IOUtils;
import org.h2.util.ObjectArray;
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

/*## Java 1.6 begin ##
import java.sql.RowId;
import java.sql.NClob;
import java.sql.SQLXML;
## Java 1.6 end ##*/

/**
 * Represents a prepared statement.
 */
public class JdbcPreparedStatement extends JdbcStatement implements PreparedStatement {

    private final String sql;
    private CommandInterface command;
    private ObjectArray<Value[]> batchParameters;

    JdbcPreparedStatement(JdbcConnection conn, String sql, int id, int resultSetType,
                int resultSetConcurrency, boolean closeWithResultSet) throws SQLException {
        super(conn, id, resultSetType, resultSetConcurrency, closeWithResultSet);
        setTrace(session.getTrace(), TraceObject.PREPARED_STATEMENT, id);
        this.sql = sql;
        command = conn.prepareCommand(sql, fetchSize);
    }

    /**
     * Executes a query (select statement) and returns the result set. If
     * another result set exists for this statement, this will be closed (even
     * if this statement fails).
     *
     * @return the result set
     * @throws SQLException if this object is closed or invalid
     */
    public ResultSet executeQuery() throws SQLException {
        try {
            int id = getNextId(TraceObject.RESULT_SET);
            if (isDebugEnabled()) {
                debugCodeAssign("ResultSet", TraceObject.RESULT_SET, id, "executeQuery()");
            }
            checkClosed();
            closeOldResultSet();
            ResultInterface result;
            boolean scrollable = resultSetType != ResultSet.TYPE_FORWARD_ONLY;
            boolean updatable = resultSetConcurrency == ResultSet.CONCUR_UPDATABLE;
            synchronized (session) {
                try {
                    setExecutingStatement(command);
                    result = command.executeQuery(maxRows, scrollable);
                } finally {
                    setExecutingStatement(null);
                }
            }
            resultSet = new JdbcResultSet(conn, this, result, id, closedByResultSet, scrollable, updatable);
            return resultSet;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Executes a statement (insert, update, delete, create, drop, commit,
     * rollback) and returns the update count. If another result set exists for
     * this statement, this will be closed (even if this statement fails).
     *
     * If the statement is a create or drop and does not throw an exception, the
     * current transaction (if any) is committed after executing the statement.
     * If auto commit is on, this statement will be committed.
     *
     * @return the update count (number of row affected by an insert, update or
     *         delete, or 0 if no rows or the statement was a create, drop,
     *         commit or rollback)
     * @throws SQLException if this object is closed or invalid
     */
    public int executeUpdate() throws SQLException {
        try {
            debugCodeCall("executeUpdate");
            checkClosedForWrite();
            return executeUpdateInternal();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    private int executeUpdateInternal() throws SQLException {
        closeOldResultSet();
        synchronized (session) {
            try {
                setExecutingStatement(command);
                updateCount = command.executeUpdate();
            } finally {
                setExecutingStatement(null);
            }
        }
        return updateCount;
    }

    /**
     * Executes an arbitrary statement. If another result set exists for this
     * statement, this will be closed (even if this statement fails). If auto
     * commit is on, and the statement is not a select, this statement will be
     * committed.
     *
     * @return true if a result set is available, false if not
     * @throws SQLException if this object is closed or invalid
     */
    public boolean execute() throws SQLException {
        try {
            int id = getNextId(TraceObject.RESULT_SET);
            if (isDebugEnabled()) {
                debugCodeCall("execute");
            }
            checkClosedForWrite();
            closeOldResultSet();
            boolean returnsResultSet;
            synchronized (conn.getSession()) {
                try {
                    setExecutingStatement(command);
                    if (command.isQuery()) {
                        returnsResultSet = true;
                        boolean scrollable = resultSetType != ResultSet.TYPE_FORWARD_ONLY;
                        boolean updatable = resultSetConcurrency == ResultSet.CONCUR_UPDATABLE;
                        ResultInterface result = command.executeQuery(maxRows, scrollable);
                        resultSet = new JdbcResultSet(conn, this, result, id, closedByResultSet, scrollable, updatable);
                    } else {
                        returnsResultSet = false;
                        updateCount = command.executeUpdate();
                    }
                } finally {
                    setExecutingStatement(null);
                }
            }
            return returnsResultSet;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Clears all parameters.
     *
     * @throws SQLException if this object is closed or invalid
     */
    public void clearParameters() throws SQLException {
        try {
            debugCodeCall("clearParameters");
            checkClosed();
            ObjectArray< ? extends ParameterInterface> parameters = command.getParameters();
            for (ParameterInterface param : parameters) {
                // can only delete old temp files if they are not in the batch
                param.setValue(null, batchParameters == null);
            }
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Calling this method is not legal on a PreparedStatement.
     *
     * @throws SQLException Unsupported Feature
     */
    public ResultSet executeQuery(String sql) throws SQLException {
        try {
            debugCodeCall("executeQuery", sql);
            throw Message.getSQLException(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Calling this method is not legal on a PreparedStatement.
     *
     * @throws SQLException Unsupported Feature
     */
    public void addBatch(String sql) throws SQLException {
        try {
            debugCodeCall("addBatch", sql);
            throw Message.getSQLException(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Calling this method is not legal on a PreparedStatement.
     *
     * @throws SQLException Unsupported Feature
     */
    public int executeUpdate(String sql) throws SQLException {
        try {
            debugCodeCall("executeUpdate", sql);
            throw Message.getSQLException(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Calling this method is not legal on a PreparedStatement.
     *
     * @throws SQLException Unsupported Feature
     */
    public boolean execute(String sql) throws SQLException {
        try {
            debugCodeCall("execute", sql);
            throw Message.getSQLException(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    // =============================================================

    /**
     * Sets a parameter to null.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param sqlType the data type (Types.x)
     * @throws SQLException if this object is closed
     */
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setNull("+parameterIndex+", "+sqlType+");");
            }
            setParameter(parameterIndex, ValueNull.INSTANCE);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    public void setInt(int parameterIndex, int x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setInt("+parameterIndex+", "+x+");");
            }
            setParameter(parameterIndex, ValueInt.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    public void setString(int parameterIndex, String x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setString("+parameterIndex+", "+quote(x)+");");
            }
            Value v = x == null ? (Value) ValueNull.INSTANCE : ValueString.get(x);
            setParameter(parameterIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setBigDecimal("+parameterIndex+", " + quoteBigDecimal(x) + ");");
            }
            Value v = x == null ? (Value) ValueNull.INSTANCE : ValueDecimal.get(x);
            setParameter(parameterIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    public void setDate(int parameterIndex, java.sql.Date x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setDate("+parameterIndex+", " + quoteDate(x) + ");");
            }
            Value v = x == null ? (Value) ValueNull.INSTANCE : ValueDate.get(x);
            setParameter(parameterIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    public void setTime(int parameterIndex, java.sql.Time x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setTime("+parameterIndex+", " + quoteTime(x) + ");");
            }
            Value v = x == null ? (Value) ValueNull.INSTANCE : ValueTime.get(x);
            setParameter(parameterIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    public void setTimestamp(int parameterIndex, java.sql.Timestamp x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setTimestamp("+parameterIndex+", " + quoteTimestamp(x) + ");");
            }
            Value v = x == null ? (Value) ValueNull.INSTANCE : ValueTimestamp.get(x);
            setParameter(parameterIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    public void setObject(int parameterIndex, Object x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setObject("+parameterIndex+", x);");
            }
            if (x == null) {
                // throw Errors.getInvalidValueException("null", "x");
                setParameter(parameterIndex, ValueNull.INSTANCE);
            } else {
                setParameter(parameterIndex, DataType.convertToValue(session, x, Value.UNKNOWN));
            }
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter. The object is converted, if required, to
     * the specified data type before sending to the database.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value, null is allowed
     * @param targetSqlType the type as defined in java.sql.Types
     * @throws SQLException if this object is closed
     */
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setObject("+parameterIndex+", x, "+targetSqlType+");");
            }
            int type = DataType.convertSQLTypeToValueType(targetSqlType);
            if (x == null) {
                setParameter(parameterIndex, ValueNull.INSTANCE);
            } else {
                Value v = DataType.convertToValue(conn.getSession(), x, type);
                setParameter(parameterIndex, v.convertTo(type));
            }
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter. The object is converted, if required, to
     * the specified data type before sending to the database.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value, null is allowed
     * @param targetSqlType the type as defined in java.sql.Types
     * @param scale is ignored
     * @throws SQLException if this object is closed
     */
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scale) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setObject("+parameterIndex+", x, "+targetSqlType+", "+scale+");");
            }
            setObject(parameterIndex, x, targetSqlType);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setBoolean("+parameterIndex+", "+x+");");
            }
            setParameter(parameterIndex, ValueBoolean.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    public void setByte(int parameterIndex, byte x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setByte("+parameterIndex+", "+x+");");
            }
            setParameter(parameterIndex, ValueByte.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    public void setShort(int parameterIndex, short x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setShort("+parameterIndex+", (short) "+x+");");
            }
            setParameter(parameterIndex, ValueShort.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    public void setLong(int parameterIndex, long x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setLong("+parameterIndex+", "+x+"L);");
            }
            setParameter(parameterIndex, ValueLong.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    public void setFloat(int parameterIndex, float x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setFloat("+parameterIndex+", "+x+"f);");
            }
            setParameter(parameterIndex, ValueFloat.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    public void setDouble(int parameterIndex, double x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setDouble("+parameterIndex+", "+x+"d);");
            }
            setParameter(parameterIndex, ValueDouble.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported] Sets the value of a column as a reference.
     */
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setRef("+parameterIndex+", x);");
            }
            throw Message.getUnsupportedException("ref");
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the date using a specified time zone. The value will be converted to
     * the local time zone.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @param calendar the calendar
     * @throws SQLException if this object is closed
     */
    public void setDate(int parameterIndex, java.sql.Date x, Calendar calendar) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setDate("+parameterIndex+", " + quoteDate(x) + ", calendar);");
            }
            if (x == null) {
                setParameter(parameterIndex, ValueNull.INSTANCE);
            } else {
                setParameter(parameterIndex, DateTimeUtils.convertDateToUniversal(x, calendar));
            }
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the time using a specified time zone. The value will be converted to
     * the local time zone.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @param calendar the calendar
     * @throws SQLException if this object is closed
     */
    public void setTime(int parameterIndex, java.sql.Time x, Calendar calendar) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setTime("+parameterIndex+", " + quoteTime(x) + ", calendar);");
            }
            if (x == null) {
                setParameter(parameterIndex, ValueNull.INSTANCE);
            } else {
                setParameter(parameterIndex, DateTimeUtils.convertTimeToUniversal(x, calendar));
            }
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the timestamp using a specified time zone. The value will be
     * converted to the local time zone.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @param calendar the calendar
     * @throws SQLException if this object is closed
     */
    public void setTimestamp(int parameterIndex, java.sql.Timestamp x, Calendar calendar) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setTimestamp("+parameterIndex+", " + quoteTimestamp(x) + ", calendar);");
            }
            if (x == null) {
                setParameter(parameterIndex, ValueNull.INSTANCE);
            } else {
                setParameter(parameterIndex, DateTimeUtils.convertTimestampToUniversal(x, calendar));
            }
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported] This feature is deprecated and not supported.
     * @deprecated
     */
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setUnicodeStream("+parameterIndex+", x, "+length+");");
            }
            throw Message.getUnsupportedException("unicodeStream");
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets a parameter to null.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param sqlType the data type (Types.x)
     * @param typeName this parameter is ignored
     * @throws SQLException if this object is closed
     */
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setNull("+parameterIndex+", "+sqlType+", "+quote(typeName)+");");
            }
            setNull(parameterIndex, sqlType);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter as a Blob.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setBlob("+parameterIndex+", x);");
            }
            checkClosedForWrite();
            Value v;
            if (x == null) {
                v = ValueNull.INSTANCE;
            } else {
                v = conn.createBlob(x.getBinaryStream(), -1);
            }
            setParameter(parameterIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter as a Blob.
     * This method does not close the stream.
     * The stream may be closed after executing the statement.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    public void setBlob(int parameterIndex, InputStream x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setBlob("+parameterIndex+", x);");
            }
            checkClosedForWrite();
            Value v = conn.createBlob(x, -1);
            setParameter(parameterIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter as a Clob.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setClob("+parameterIndex+", x);");
            }
            checkClosedForWrite();
            Value v;
            if (x == null) {
                v = ValueNull.INSTANCE;
            } else {
                v = conn.createClob(x.getCharacterStream(), -1);
            }
            setParameter(parameterIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter as a Clob.
     * This method does not close the reader.
     * The reader may be closed after executing the statement.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    public void setClob(int parameterIndex, Reader x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setClob("+parameterIndex+", x);");
            }
            checkClosedForWrite();
            Value v;
            if (x == null) {
                v = ValueNull.INSTANCE;
            } else {
                v = conn.createClob(x, -1);
            }
            setParameter(parameterIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported] Sets the value of a parameter as a Array.
     */
    public void setArray(int parameterIndex, Array x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setArray("+parameterIndex+", x);");
            }
            throw Message.getUnsupportedException("setArray");
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter as a byte array.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setBytes("+parameterIndex+", "+quoteBytes(x)+");");
            }
            Value v = x == null ? (Value) ValueNull.INSTANCE : ValueBytes.get(x);
            setParameter(parameterIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter as an input stream.
     * This method does not close the stream.
     * The stream may be closed after executing the statement.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @param length the number of bytes
     * @throws SQLException if this object is closed
     */
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setBinaryStream("+parameterIndex+", x, "+length+"L);");
            }
            checkClosedForWrite();
            Value v = conn.createBlob(x, length);
            setParameter(parameterIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter as an input stream.
     * This method does not close the stream.
     * The stream may be closed after executing the statement.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @param length the number of bytes
     * @throws SQLException if this object is closed
     */
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setBinaryStream(parameterIndex, x, (long) length);
    }

    /**
     * Sets the value of a parameter as an input stream.
     * This method does not close the stream.
     * The stream may be closed after executing the statement.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        setBinaryStream(parameterIndex, x, -1);
    }

    /**
     * Sets the value of a parameter as an ASCII stream.
     * This method does not close the stream.
     * The stream may be closed after executing the statement.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @param length the number of bytes
     * @throws SQLException if this object is closed
     */
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setAsciiStream(parameterIndex, x, (long) length);
    }

    /**
     * Sets the value of a parameter as an ASCII stream.
     * This method does not close the stream.
     * The stream may be closed after executing the statement.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @param length the number of bytes
     * @throws SQLException if this object is closed
     */
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setAsciiStream("+parameterIndex+", x, "+length+"L);");
            }
            checkClosedForWrite();
            Value v = conn.createClob(IOUtils.getAsciiReader(x), length);
            setParameter(parameterIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter as an ASCII stream.
     * This method does not close the stream.
     * The stream may be closed after executing the statement.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        setAsciiStream(parameterIndex, x, -1);
    }

    /**
     * Sets the value of a parameter as a character stream.
     * This method does not close the reader.
     * The reader may be closed after executing the statement.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @param length the number of bytes
     * @throws SQLException if this object is closed
     */
    public void setCharacterStream(int parameterIndex, Reader x, int length) throws SQLException {
        setCharacterStream(parameterIndex, x, (long) length);
    }

    /**
     * Sets the value of a parameter as a character stream.
     * This method does not close the reader.
     * The reader may be closed after executing the statement.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    public void setCharacterStream(int parameterIndex, Reader x) throws SQLException {
        setCharacterStream(parameterIndex, x, -1);
    }

    /**
     * Sets the value of a parameter as a character stream.
     * This method does not close the reader.
     * The reader may be closed after executing the statement.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @param length the number of bytes
     * @throws SQLException if this object is closed
     */
    public void setCharacterStream(int parameterIndex, Reader x, long length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setCharacterStream("+parameterIndex+", x, "+length+"L);");
            }
            checkClosedForWrite();
            Value v = conn.createClob(x, length);
            setParameter(parameterIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported]
     */
    public void setURL(int parameterIndex, URL x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setURL("+parameterIndex+", x);");
            }
            throw Message.getUnsupportedException("url");
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the result set metadata of the query returned when the statement is
     * executed. If this is not a query, this method returns null.
     *
     * @return the meta data or null if this is not a query
     * @throws SQLException if this object is closed
     */
    public ResultSetMetaData getMetaData() throws SQLException {
        try {
            debugCodeCall("getMetaData");
            checkClosed();
            ResultInterface result = command.getMetaData();
            if (result == null) {
                return null;
            }
            int id = getNextId(TraceObject.RESULT_SET_META_DATA);
            if (isDebugEnabled()) {
                debugCodeAssign("ResultSetMetaData", TraceObject.RESULT_SET_META_DATA, id, "getMetaData()");
            }
            String catalog = conn.getCatalog();
            JdbcResultSetMetaData meta = new JdbcResultSetMetaData(null, this, result, catalog, session.getTrace(), id);
            return meta;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Clears the batch.
     */
    public void clearBatch() throws SQLException {
        try {
            debugCodeCall("clearBatch");
            checkClosed();
            batchParameters = null;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Closes this statement.
     * All result sets that where created by this statement
     * become invalid after calling this method.
     */
    public void close() throws SQLException {
        try {
            super.close();
            batchParameters = null;
            if (command != null) {
                command.close();
                command = null;
            }
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Executes the batch.
     *
     * @return the array of update counts
     */
    public int[] executeBatch() throws SQLException {
        try {
            debugCodeCall("executeBatch");
            checkClosedForWrite();
            if (batchParameters == null) {
                // TODO batch: check what other database do if no parameters are set
                batchParameters = ObjectArray.newInstance();
            }
            int[] result = new int[batchParameters.size()];
            boolean error = false;
            SQLException next = null;
            for (int i = 0; i < batchParameters.size(); i++) {
                Value[] set = batchParameters.get(i);
                ObjectArray< ? extends ParameterInterface> parameters = command.getParameters();
                for (int j = 0; j < set.length; j++) {
                    Value value = set[j];
                    ParameterInterface param = parameters.get(j);
                    param.setValue(value, false);
                }
                try {
                    result[i] = executeUpdateInternal();
                } catch (SQLException e) {
                    if (next == null) {
                        next = e;
                    } else {
                        e.setNextException(next);
                        next = e;
                    }
                    logAndConvert(e);
                    //## Java 1.4 begin ##
                    result[i] = Statement.EXECUTE_FAILED;
                    //## Java 1.4 end ##
                    error = true;
                }
            }
            batchParameters = null;
            if (error) {
                JdbcBatchUpdateException e = new JdbcBatchUpdateException(next, result);
                e.setNextException(next);
                throw e;
            }
            return result;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Adds the current settings to the batch.
     */
    public void addBatch() throws SQLException {
        try {
            debugCodeCall("addBatch");
            checkClosedForWrite();
            ObjectArray< ? extends ParameterInterface> parameters = command.getParameters();
            Value[] set = new Value[parameters.size()];
            for (int i = 0; i < parameters.size(); i++) {
                ParameterInterface param = parameters.get(i);
                Value value = param.getParamValue();
                set[i] = value;
            }
            if (batchParameters == null) {
                batchParameters = ObjectArray.newInstance();
            }
            batchParameters.add(set);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Calling this method is not legal on a PreparedStatement.
     *
     * @throws SQLException Unsupported Feature
     */
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("executeUpdate("+quote(sql)+", "+autoGeneratedKeys+");");
            }
            throw Message.getSQLException(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }


    /**
     * Calling this method is not legal on a PreparedStatement.
     *
     * @throws SQLException Unsupported Feature
     */
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("executeUpdate(" + quote(sql) + ", " + quoteIntArray(columnIndexes) + ");");
            }
            throw Message.getSQLException(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Calling this method is not legal on a PreparedStatement.
     *
     * @throws SQLException Unsupported Feature
     */
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("executeUpdate(" + quote(sql) + ", " + quoteArray(columnNames) + ");");
            }
            throw Message.getSQLException(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Calling this method is not legal on a PreparedStatement.
     *
     * @throws SQLException Unsupported Feature
     */
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("execute(" + quote(sql) + ", " + autoGeneratedKeys + ");");
            }
            throw Message.getSQLException(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Calling this method is not legal on a PreparedStatement.
     *
     * @throws SQLException Unsupported Feature
     */
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("execute(" + quote(sql) + ", " + quoteIntArray(columnIndexes) + ");");
            }
            throw Message.getSQLException(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Calling this method is not legal on a PreparedStatement.
     *
     * @throws SQLException Unsupported Feature
     */
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("execute(" + quote(sql) + ", " + quoteArray(columnNames) + ");");
            }
            throw Message.getSQLException(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Get the parameter meta data of this prepared statement.
     *
     * @return the meta data
     */
//## Java 1.4 begin ##
    public ParameterMetaData getParameterMetaData() throws SQLException {
        try {
            int id = getNextId(TraceObject.PARAMETER_META_DATA);
            if (isDebugEnabled()) {
                debugCodeAssign("ParameterMetaData", TraceObject.PARAMETER_META_DATA, id, "getParameterMetaData()");
            }
            checkClosed();
            JdbcParameterMetaData meta = new JdbcParameterMetaData(session.getTrace(), this, command, id);
            return meta;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }
//## Java 1.4 end ##

    // =============================================================

    private void setParameter(int parameterIndex, Value value) throws SQLException {
        checkClosed();
        parameterIndex--;
        ObjectArray< ? extends ParameterInterface> parameters = command.getParameters();
        if (parameterIndex < 0 || parameterIndex >= parameters.size()) {
            throw Message.getInvalidValueException("" + (parameterIndex + 1), "parameterIndex");
        }
        ParameterInterface param = parameters.get(parameterIndex);
        // can only delete old temp files if they are not in the batch
        param.setValue(value, batchParameters == null);
    }

    /**
     * [Not supported] Sets the value of a parameter as a row id.
     */
/*## Java 1.6 begin ##
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw Message.getUnsupportedException("rowId");
    }
## Java 1.6 end ##*/

    /**
     * Sets the value of a parameter.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    public void setNString(int parameterIndex, String x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setNString("+parameterIndex+", "+quote(x)+");");
            }
            Value v = x == null ? (Value) ValueNull.INSTANCE : ValueString.get(x);
            setParameter(parameterIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter as a character stream.
     * This method does not close the reader.
     * The reader may be closed after executing the statement.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @param length the number of bytes
     * @throws SQLException if this object is closed
     */
    public void setNCharacterStream(int parameterIndex, Reader x, long length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setNCharacterStream("+parameterIndex+", x, "+length+"L);");
            }
            checkClosedForWrite();
            Value v = conn.createClob(x, length);
            setParameter(parameterIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter as a character stream.
     * This method does not close the reader.
     * The reader may be closed after executing the statement.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    public void setNCharacterStream(int parameterIndex, Reader x) throws SQLException {
        setNCharacterStream(parameterIndex, x, -1);
    }

    /**
     * Sets the value of a parameter as a Clob.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
/*## Java 1.6 begin ##
    public void setNClob(int parameterIndex, NClob x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setNClob("+parameterIndex+", x);");
            }
            checkClosedForWrite();
            Value v;
            if (x == null) {
                v = ValueNull.INSTANCE;
            } else {
                v = conn.createClob(x.getCharacterStream(), -1);
            }
            setParameter(parameterIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }
## Java 1.6 end ##*/

    /**
     * Sets the value of a parameter as a Clob.
     * This method does not close the reader.
     * The reader may be closed after executing the statement.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    public void setNClob(int parameterIndex, Reader x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setNClob("+parameterIndex+", x);");
            }
            checkClosedForWrite();
            Value v = conn.createClob(x, -1);
            setParameter(parameterIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter as a Clob.
     * This method does not close the reader.
     * The reader may be closed after executing the statement.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    public void setClob(int parameterIndex, Reader x, long length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setClob("+parameterIndex+", x, "+length+"L);");
            }
            checkClosedForWrite();
            Value v = conn.createClob(x, length);
            setParameter(parameterIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter as a Blob.
     * This method does not close the stream.
     * The stream may be closed after executing the statement.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    public void setBlob(int parameterIndex, InputStream x, long length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setBlob("+parameterIndex+", x, "+length+"L);");
            }
            checkClosedForWrite();
            Value v = conn.createBlob(x, length);
            setParameter(parameterIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter as a Clob.
     * This method does not close the reader.
     * The reader may be closed after executing the statement.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    public void setNClob(int parameterIndex, Reader x, long length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setNClob("+parameterIndex+", x, "+length+"L);");
            }
            checkClosedForWrite();
            Value v = conn.createClob(x, length);
            setParameter(parameterIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported] Sets the value of a parameter as a SQLXML object.
     */
/*## Java 1.6 begin ##
    public void setSQLXML(int parameterIndex, SQLXML x) throws SQLException {
        throw Message.getUnsupportedException("SQLXML");
    }
## Java 1.6 end ##*/

    /**
     * INTERNAL
     */
    public String toString() {
        return getTraceObjectName() + ": " + command;
    }

    protected boolean checkClosed(boolean write) throws SQLException {
        if (super.checkClosed(write)) {
            // if the session was re-connected, re-prepare the statement
            ObjectArray< ? extends ParameterInterface> oldParams = command.getParameters();
            command = conn.prepareCommand(sql, fetchSize);
            ObjectArray< ? extends ParameterInterface> newParams = command.getParameters();
            for (int i = 0; i < oldParams.size(); i++) {
                ParameterInterface old = oldParams.get(i);
                Value value = old.getParamValue();
                if (value != null) {
                    ParameterInterface n = newParams.get(i);
                    n.setValue(value, false);
                }
            }
            return true;
        }
        return false;
    }

}
