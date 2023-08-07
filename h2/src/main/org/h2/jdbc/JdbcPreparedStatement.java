/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
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
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLXML;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.engine.Session;
import org.h2.expression.ParameterInterface;
import org.h2.message.DbException;
import org.h2.message.TraceObject;
import org.h2.result.MergedResult;
import org.h2.result.ResultInterface;
import org.h2.result.ResultWithGeneratedKeys;
import org.h2.util.IOUtils;
import org.h2.util.LegacyDateTimeUtils;
import org.h2.util.Utils;
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
 * Represents a prepared statement.
 * <p>
 * Thread safety: the prepared statement is not thread-safe. If the same
 * prepared statement is used by multiple threads access to it must be
 * synchronized. The single synchronized block must include assignment of
 * parameters, execution of the command and all operations with its result.
 * </p>
 * <pre>
 * synchronized (prep) {
 *     prep.setInt(1, 10);
 *     try (ResultSet rs = prep.executeQuery()) {
 *         while (rs.next) {
 *             // Do something
 *         }
 *     }
 * }
 * synchronized (prep) {
 *     prep.setInt(1, 15);
 *     updateCount = prep.executeUpdate();
 * }
 * </pre>
 */
public class JdbcPreparedStatement extends JdbcStatement implements PreparedStatement {

    protected CommandInterface command;
    private ArrayList<Value[]> batchParameters;
    private MergedResult batchIdentities;
    private HashMap<String, Integer> cachedColumnLabelMap;
    private final Object generatedKeysRequest;

    JdbcPreparedStatement(JdbcConnection conn, String sql, int id, int resultSetType, int resultSetConcurrency,
            Object generatedKeysRequest) {
        super(conn, id, resultSetType, resultSetConcurrency);
        this.generatedKeysRequest = generatedKeysRequest;
        setTrace(session.getTrace(), TraceObject.PREPARED_STATEMENT, id);
        command = conn.prepareCommand(sql, fetchSize);
    }

    /**
     * Cache the column labels (looking up the column index can sometimes show
     * up on the performance profile).
     *
     * @param cachedColumnLabelMap the column map
     */
    void setCachedColumnLabelMap(HashMap<String, Integer> cachedColumnLabelMap) {
        this.cachedColumnLabelMap = cachedColumnLabelMap;
    }

    /**
     * Executes a query (select statement) and returns the result set. If
     * another result set exists for this statement, this will be closed (even
     * if this statement fails).
     *
     * @return the result set
     * @throws SQLException if this object is closed or invalid
     */
    @Override
    public ResultSet executeQuery() throws SQLException {
        try {
            int id = getNextId(TraceObject.RESULT_SET);
            debugCodeAssign("ResultSet", TraceObject.RESULT_SET, id, "executeQuery()");
            batchIdentities = null;
            final Session session = this.session;
            session.lock();
            try {
                checkClosed();
                closeOldResultSet();
                ResultInterface result;
                boolean lazy = false;
                boolean scrollable = resultSetType != ResultSet.TYPE_FORWARD_ONLY;
                boolean updatable = resultSetConcurrency == ResultSet.CONCUR_UPDATABLE;
                try {
                    setExecutingStatement(command);
                    result = command.executeQuery(maxRows, scrollable);
                    lazy = result.isLazy();
                } finally {
                    if (!lazy) {
                        setExecutingStatement(null);
                    }
                }
                resultSet = new JdbcResultSet(conn, this, command, result, id, scrollable, updatable,
                        cachedColumnLabelMap);
            } finally {
                session.unlock();
            }
            return resultSet;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Executes a statement (insert, update, delete, create, drop)
     * and returns the update count.
     * If another result set exists for this statement, this will be closed
     * (even if this statement fails).
     *
     * If auto commit is on, this statement will be committed.
     * If the statement is a DDL statement (create, drop, alter) and does not
     * throw an exception, the current transaction (if any) is committed after
     * executing the statement.
     *
     * @return the update count (number of affected rows by a DML statement or
     *         other statement able to return number of rows, or 0 if no rows
     *         were affected or the statement returns nothing, or
     *         {@link #SUCCESS_NO_INFO} if number of rows is too large for
     *         {@code int} data type)
     * @throws SQLException if this object is closed or invalid
     * @see #executeLargeUpdate()
     */
    @Override
    public int executeUpdate() throws SQLException {
        try {
            debugCodeCall("executeUpdate");
            checkClosed();
            batchIdentities = null;
            long updateCount = executeUpdateInternal();
            return updateCount <= Integer.MAX_VALUE ? (int) updateCount : SUCCESS_NO_INFO;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Executes a statement (insert, update, delete, create, drop)
     * and returns the update count.
     * If another result set exists for this statement, this will be closed
     * (even if this statement fails).
     *
     * If auto commit is on, this statement will be committed.
     * If the statement is a DDL statement (create, drop, alter) and does not
     * throw an exception, the current transaction (if any) is committed after
     * executing the statement.
     *
     * @return the update count (number of affected rows by a DML statement or
     *         other statement able to return number of rows, or 0 if no rows
     *         were affected or the statement returns nothing)
     * @throws SQLException if this object is closed or invalid
     */
    @Override
    public long executeLargeUpdate() throws SQLException {
        try {
            debugCodeCall("executeLargeUpdate");
            checkClosed();
            batchIdentities = null;
            return executeUpdateInternal();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    private long executeUpdateInternal() {
        closeOldResultSet();
        final Session session = this.session;
        session.lock();
        try {
            try {
                setExecutingStatement(command);
                ResultWithGeneratedKeys result = command.executeUpdate(generatedKeysRequest);
                updateCount = result.getUpdateCount();
                ResultInterface gk = result.getGeneratedKeys();
                if (gk != null) {
                    int id = getNextId(TraceObject.RESULT_SET);
                    generatedKeys = new JdbcResultSet(conn, this, command, gk, id, true, false, false);
                }
            } finally {
                setExecutingStatement(null);
            }
        } finally {
            session.unlock();
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
    @Override
    public boolean execute() throws SQLException {
        try {
            int id = getNextId(TraceObject.RESULT_SET);
            debugCodeCall("execute");
            checkClosed();
            boolean returnsResultSet;
            final Session session = this.session;
            session.lock();
            try {
                closeOldResultSet();
                boolean lazy = false;
                try {
                    setExecutingStatement(command);
                    if (command.isQuery()) {
                        returnsResultSet = true;
                        boolean scrollable = resultSetType != ResultSet.TYPE_FORWARD_ONLY;
                        boolean updatable = resultSetConcurrency == ResultSet.CONCUR_UPDATABLE;
                        ResultInterface result = command.executeQuery(maxRows, scrollable);
                        lazy = result.isLazy();
                        resultSet = new JdbcResultSet(conn, this, command, result, id, scrollable, updatable,
                                cachedColumnLabelMap);
                    } else {
                        returnsResultSet = false;
                        ResultWithGeneratedKeys result = command.executeUpdate(generatedKeysRequest);
                        updateCount = result.getUpdateCount();
                        ResultInterface gk = result.getGeneratedKeys();
                        if (gk != null) {
                            generatedKeys = new JdbcResultSet(conn, this, command, gk, id, true, false, false);
                        }
                    }
                } finally {
                    if (!lazy) {
                        setExecutingStatement(null);
                    }
                }
            } finally {
                session.unlock();
            }
            return returnsResultSet;
        } catch (Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Clears all parameters.
     *
     * @throws SQLException if this object is closed or invalid
     */
    @Override
    public void clearParameters() throws SQLException {
        try {
            debugCodeCall("clearParameters");
            checkClosed();
            ArrayList<? extends ParameterInterface> parameters = command.getParameters();
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
     * @param sql ignored
     * @throws SQLException Unsupported Feature
     */
    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        try {
            debugCodeCall("executeQuery", sql);
            throw DbException.get(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Calling this method is not legal on a PreparedStatement.
     *
     * @param sql ignored
     * @throws SQLException Unsupported Feature
     */
    @Override
    public void addBatch(String sql) throws SQLException {
        try {
            debugCodeCall("addBatch", sql);
            throw DbException.get(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
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
    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setNull(" + parameterIndex + ", " + sqlType + ')');
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
    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setInt(" + parameterIndex + ", " + x + ')');
            }
            setParameter(parameterIndex, ValueInteger.get(x));
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
    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setString(" + parameterIndex + ", " + quote(x) + ')');
            }
            setParameter(parameterIndex, x == null ? ValueNull.INSTANCE : ValueVarchar.get(x, conn));
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
    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setBigDecimal(" + parameterIndex + ", " + quoteBigDecimal(x) + ')');
            }
            setParameter(parameterIndex, x == null ? ValueNull.INSTANCE : ValueNumeric.getAnyScale(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter.
     * <p>
     * Usage of this method is discouraged. Use
     * {@code setObject(parameterIndex, value)} with {@link java.time.LocalDate}
     * parameter instead.
     * </p>
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     * @see #setObject(int, Object)
     */
    @Override
    public void setDate(int parameterIndex, java.sql.Date x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setDate(" + parameterIndex + ", " + quoteDate(x) + ')');
            }
            setParameter(parameterIndex, x == null ? ValueNull.INSTANCE : LegacyDateTimeUtils.fromDate(conn, null, x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter.
     * <p>
     * Usage of this method is discouraged. Use
     * {@code setObject(parameterIndex, value)} with {@link java.time.LocalTime}
     * parameter instead.
     * </p>
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     * @see #setObject(int, Object)
     */
    @Override
    public void setTime(int parameterIndex, java.sql.Time x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setTime(" + parameterIndex + ", " + quoteTime(x) + ')');
            }
            setParameter(parameterIndex, x == null ? ValueNull.INSTANCE : LegacyDateTimeUtils.fromTime(conn, null, x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter.
     * <p>
     * Usage of this method is discouraged. Use
     * {@code setObject(parameterIndex, value)} with
     * {@link java.time.LocalDateTime} parameter instead.
     * </p>
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     * @see #setObject(int, Object)
     */
    @Override
    public void setTimestamp(int parameterIndex, java.sql.Timestamp x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setTimestamp(" + parameterIndex + ", " + quoteTimestamp(x) + ')');
            }
            setParameter(parameterIndex,
                    x == null ? ValueNull.INSTANCE : LegacyDateTimeUtils.fromTimestamp(conn, null, x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter.
     * Objects of unknown classes are serialized (on the client side).
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setObject(" + parameterIndex + ", x)");
            }
            if (x == null) {
                setParameter(parameterIndex, ValueNull.INSTANCE);
            } else {
                setParameter(parameterIndex, ValueToObjectConverter.objectToValue(session, x, Value.UNKNOWN));
            }
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter. The object is converted, if required, to
     * the specified data type before sending to the database.
     * Objects of unknown classes are serialized (on the client side).
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value, null is allowed
     * @param targetSqlType the type as defined in java.sql.Types
     * @throws SQLException if this object is closed
     */
    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setObject(" + parameterIndex + ", x, " + targetSqlType + ')');
            }
            setObjectWithType(parameterIndex, x, DataType.convertSQLTypeToValueType(targetSqlType));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter. The object is converted, if required, to
     * the specified data type before sending to the database.
     * Objects of unknown classes are serialized (on the client side).
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value, null is allowed
     * @param targetSqlType the type as defined in java.sql.Types
     * @param scale is ignored
     * @throws SQLException if this object is closed
     */
    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType,
            int scale) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setObject(" + parameterIndex + ", x, " + targetSqlType + ", " + scale + ')');
            }
            setObjectWithType(parameterIndex, x, DataType.convertSQLTypeToValueType(targetSqlType));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter. The object is converted, if required, to
     * the specified data type before sending to the database.
     * Objects of unknown classes are serialized (on the client side).
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value, null is allowed
     * @param targetSqlType the SQL type
     * @throws SQLException if this object is closed
     */
    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setObject(" + parameterIndex + ", x, " + DataType.sqlTypeToString(targetSqlType) + ')');
            }
            setObjectWithType(parameterIndex, x, DataType.convertSQLTypeToValueType(targetSqlType));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter. The object is converted, if required, to
     * the specified data type before sending to the database.
     * Objects of unknown classes are serialized (on the client side).
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value, null is allowed
     * @param targetSqlType the SQL type
     * @param scaleOrLength is ignored
     * @throws SQLException if this object is closed
     */
    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setObject(" + parameterIndex + ", x, " + DataType.sqlTypeToString(targetSqlType) + ", "
                        + scaleOrLength + ')');
            }
            setObjectWithType(parameterIndex, x, DataType.convertSQLTypeToValueType(targetSqlType));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    private void setObjectWithType(int parameterIndex, Object x, int type) {
        if (x == null) {
            setParameter(parameterIndex, ValueNull.INSTANCE);
        } else {
            Value v = ValueToObjectConverter.objectToValue(conn.getSession(), x, type);
            if (type != Value.UNKNOWN) {
                v = v.convertTo(type, conn);
            }
            setParameter(parameterIndex, v);
        }
    }

    /**
     * Sets the value of a parameter.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setBoolean(" + parameterIndex + ", " + x + ')');
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
    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setByte(" + parameterIndex + ", " + x + ')');
            }
            setParameter(parameterIndex, ValueTinyint.get(x));
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
    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setShort(" + parameterIndex + ", (short) " + x + ')');
            }
            setParameter(parameterIndex, ValueSmallint.get(x));
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
    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setLong(" + parameterIndex + ", " + x + "L)");
            }
            setParameter(parameterIndex, ValueBigint.get(x));
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
    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setFloat(" + parameterIndex + ", " + x + "f)");
            }
            setParameter(parameterIndex, ValueReal.get(x));
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
    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setDouble(" + parameterIndex + ", " + x + "d)");
            }
            setParameter(parameterIndex, ValueDouble.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported] Sets the value of a column as a reference.
     */
    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw unsupported("ref");
    }

    /**
     * Sets the date using a specified time zone. The value will be converted to
     * the local time zone.
     * <p>
     * Usage of this method is discouraged. Use
     * {@code setObject(parameterIndex, value)} with {@link java.time.LocalDate}
     * parameter instead.
     * </p>
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @param calendar the calendar
     * @throws SQLException if this object is closed
     * @see #setObject(int, Object)
     */
    @Override
    public void setDate(int parameterIndex, java.sql.Date x, Calendar calendar) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setDate(" + parameterIndex + ", " + quoteDate(x) + ", calendar)");
            }
            if (x == null) {
                setParameter(parameterIndex, ValueNull.INSTANCE);
            } else {
                setParameter(parameterIndex,
                        LegacyDateTimeUtils.fromDate(conn, calendar != null ? calendar.getTimeZone() : null, x));
            }
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the time using a specified time zone. The value will be converted to
     * the local time zone.
     * <p>
     * Usage of this method is discouraged. Use
     * {@code setObject(parameterIndex, value)} with {@link java.time.LocalTime}
     * parameter instead.
     * </p>
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @param calendar the calendar
     * @throws SQLException if this object is closed
     * @see #setObject(int, Object)
     */
    @Override
    public void setTime(int parameterIndex, java.sql.Time x, Calendar calendar) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setTime(" + parameterIndex + ", " + quoteTime(x) + ", calendar)");
            }
            if (x == null) {
                setParameter(parameterIndex, ValueNull.INSTANCE);
            } else {
                setParameter(parameterIndex,
                        LegacyDateTimeUtils.fromTime(conn, calendar != null ? calendar.getTimeZone() : null, x));
            }
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the timestamp using a specified time zone. The value will be
     * converted to the local time zone.
     * <p>
     * Usage of this method is discouraged. Use
     * {@code setObject(parameterIndex, value)} with
     * {@link java.time.LocalDateTime} parameter instead.
     * </p>
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @param calendar the calendar
     * @throws SQLException if this object is closed
     * @see #setObject(int, Object)
     */
    @Override
    public void setTimestamp(int parameterIndex, java.sql.Timestamp x, Calendar calendar) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setTimestamp(" + parameterIndex + ", " + quoteTimestamp(x) + ", calendar)");
            }
            if (x == null) {
                setParameter(parameterIndex, ValueNull.INSTANCE);
            } else {
                setParameter(parameterIndex,
                        LegacyDateTimeUtils.fromTimestamp(conn, calendar != null ? calendar.getTimeZone() : null, x));
            }
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported] This feature is deprecated and not supported.
     *
     * @deprecated since JDBC 2.0, use setCharacterStream
     */
    @Deprecated
    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length)
            throws SQLException {
        throw unsupported("unicodeStream");
    }

    /**
     * Sets a parameter to null.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param sqlType the data type (Types.x)
     * @param typeName this parameter is ignored
     * @throws SQLException if this object is closed
     */
    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setNull(" + parameterIndex + ", " + sqlType + ", " + quote(typeName) + ')');
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
    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setBlob(" + parameterIndex + ", x)");
            }
            checkClosed();
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
    @Override
    public void setBlob(int parameterIndex, InputStream x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setBlob(" + parameterIndex + ", x)");
            }
            checkClosed();
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
    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setClob(" + parameterIndex + ", x)");
            }
            checkClosed();
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
    @Override
    public void setClob(int parameterIndex, Reader x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setClob(" + parameterIndex + ", x)");
            }
            checkClosed();
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
     * Sets the value of a parameter as an Array.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setArray(" + parameterIndex + ", x)");
            }
            checkClosed();
            Value v;
            if (x == null) {
                v = ValueNull.INSTANCE;
            } else {
                v = ValueToObjectConverter.objectToValue(session, x.getArray(), Value.ARRAY);
            }
            setParameter(parameterIndex, v);
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
    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setBytes(" + parameterIndex + ", " + quoteBytes(x) + ')');
            }
            setParameter(parameterIndex, x == null ? ValueNull.INSTANCE : ValueVarbinary.get(x));
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
     * @param length the maximum number of bytes
     * @throws SQLException if this object is closed
     */
    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setBinaryStream(" + parameterIndex + ", x, " + length + "L)");
            }
            checkClosed();
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
     * @param length the maximum number of bytes
     * @throws SQLException if this object is closed
     */
    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length)
            throws SQLException {
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
    @Override
    public void setBinaryStream(int parameterIndex, InputStream x)
            throws SQLException {
        setBinaryStream(parameterIndex, x, -1);
    }

    /**
     * Sets the value of a parameter as an ASCII stream.
     * This method does not close the stream.
     * The stream may be closed after executing the statement.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @param length the maximum number of bytes
     * @throws SQLException if this object is closed
     */
    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length)
            throws SQLException {
        setAsciiStream(parameterIndex, x, (long) length);
    }

    /**
     * Sets the value of a parameter as an ASCII stream.
     * This method does not close the stream.
     * The stream may be closed after executing the statement.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @param length the maximum number of bytes
     * @throws SQLException if this object is closed
     */
    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setAsciiStream(" + parameterIndex + ", x, " + length + "L)");
            }
            checkClosed();
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
    @Override
    public void setAsciiStream(int parameterIndex, InputStream x)
            throws SQLException {
        setAsciiStream(parameterIndex, x, -1);
    }

    /**
     * Sets the value of a parameter as a character stream.
     * This method does not close the reader.
     * The reader may be closed after executing the statement.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @param length the maximum number of characters
     * @throws SQLException if this object is closed
     */
    @Override
    public void setCharacterStream(int parameterIndex, Reader x, int length)
            throws SQLException {
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
    @Override
    public void setCharacterStream(int parameterIndex, Reader x)
            throws SQLException {
        setCharacterStream(parameterIndex, x, -1);
    }

    /**
     * Sets the value of a parameter as a character stream.
     * This method does not close the reader.
     * The reader may be closed after executing the statement.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @param length the maximum number of characters
     * @throws SQLException if this object is closed
     */
    @Override
    public void setCharacterStream(int parameterIndex, Reader x, long length)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setCharacterStream(" + parameterIndex + ", x, " + length + "L)");
            }
            checkClosed();
            Value v = conn.createClob(x, length);
            setParameter(parameterIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported]
     */
    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        throw unsupported("url");
    }

    /**
     * Gets the result set metadata of the query returned when the statement is
     * executed. If this is not a query, this method returns null.
     *
     * @return the meta data or null if this is not a query
     * @throws SQLException if this object is closed
     */
    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        try {
            debugCodeCall("getMetaData");
            checkClosed();
            ResultInterface result = command.getMetaData();
            if (result == null) {
                return null;
            }
            int id = getNextId(TraceObject.RESULT_SET_META_DATA);
            debugCodeAssign("ResultSetMetaData", TraceObject.RESULT_SET_META_DATA, id, "getMetaData()");
            String catalog = conn.getCatalog();
            return new JdbcResultSetMetaData(null, this, result, catalog, session.getTrace(), id);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Clears the batch.
     */
    @Override
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
    @Override
    public void close() throws SQLException {
        try {
            super.close();
            batchParameters = null;
            batchIdentities = null;
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
     * If one of the batched statements fails, this database will continue.
     *
     * @return the array of update counts
     * @see #executeLargeBatch()
     */
    @Override
    public int[] executeBatch() throws SQLException {
        try {
            debugCodeCall("executeBatch");
            if (batchParameters == null) {
                // Empty batch is allowed, see JDK-4639504 and other issues
                batchParameters = new ArrayList<>();
            }
            batchIdentities = new MergedResult();
            int size = batchParameters.size();
            int[] result = new int[size];
            SQLException exception = new SQLException();
            checkClosed();
            for (int i = 0; i < size; i++) {
                long updateCount = executeBatchElement(batchParameters.get(i), exception);
                result[i] = updateCount <= Integer.MAX_VALUE ? (int) updateCount : SUCCESS_NO_INFO;
            }
            batchParameters = null;
            exception = exception.getNextException();
            if (exception != null) {
                throw new JdbcBatchUpdateException(exception, result);
            }
            return result;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Executes the batch.
     * If one of the batched statements fails, this database will continue.
     *
     * @return the array of update counts
     */
    @Override
    public long[] executeLargeBatch() throws SQLException {
        try {
            debugCodeCall("executeLargeBatch");
            if (batchParameters == null) {
                // Empty batch is allowed, see JDK-4639504 and other issues
                batchParameters = new ArrayList<>();
            }
            batchIdentities = new MergedResult();
            int size = batchParameters.size();
            long[] result = new long[size];
            SQLException exception = new SQLException();
            checkClosed();
            for (int i = 0; i < size; i++) {
                result[i] = executeBatchElement(batchParameters.get(i), exception);
            }
            batchParameters = null;
            exception = exception.getNextException();
            if (exception != null) {
                throw new JdbcBatchUpdateException(exception, result);
            }
            return result;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    private long executeBatchElement(Value[] set, SQLException exception) {
        ArrayList<? extends ParameterInterface> parameters = command.getParameters();
        for (int i = 0, l = set.length; i < l; i++) {
            parameters.get(i).setValue(set[i], false);
        }
        long updateCount;
        try {
            updateCount = executeUpdateInternal();
            // Cannot use own implementation, it returns batch identities
            ResultSet rs = super.getGeneratedKeys();
            batchIdentities.add(((JdbcResultSet) rs).result);
        } catch (Exception e) {
            exception.setNextException(logAndConvert(e));
            updateCount = Statement.EXECUTE_FAILED;
        }
        return updateCount;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        if (batchIdentities != null) {
            try {
                int id = getNextId(TraceObject.RESULT_SET);
                debugCodeAssign("ResultSet", TraceObject.RESULT_SET, id, "getGeneratedKeys()");
                checkClosed();
                generatedKeys = new JdbcResultSet(conn, this, null, batchIdentities.getResult(), id, true, false,
                        false);
            } catch (Exception e) {
                throw logAndConvert(e);
            }
        }
        return super.getGeneratedKeys();
    }

    /**
     * Adds the current settings to the batch.
     */
    @Override
    public void addBatch() throws SQLException {
        try {
            debugCodeCall("addBatch");
            checkClosed();
            ArrayList<? extends ParameterInterface> parameters =
                    command.getParameters();
            int size = parameters.size();
            Value[] set = new Value[size];
            for (int i = 0; i < size; i++) {
                ParameterInterface param = parameters.get(i);
                param.checkSet();
                Value value = param.getParamValue();
                set[i] = value;
            }
            if (batchParameters == null) {
                batchParameters = Utils.newSmallArrayList();
            }
            batchParameters.add(set);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Get the parameter meta data of this prepared statement.
     *
     * @return the meta data
     */
    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        try {
            int id = getNextId(TraceObject.PARAMETER_META_DATA);
            debugCodeAssign("ParameterMetaData", TraceObject.PARAMETER_META_DATA, id, "getParameterMetaData()");
            checkClosed();
            return new JdbcParameterMetaData(session.getTrace(), this, command, id);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    // =============================================================

    private void setParameter(int parameterIndex, Value value) {
        checkClosed();
        parameterIndex--;
        ArrayList<? extends ParameterInterface> parameters = command.getParameters();
        if (parameterIndex < 0 || parameterIndex >= parameters.size()) {
            throw DbException.getInvalidValueException("parameterIndex",
                    parameterIndex + 1);
        }
        ParameterInterface param = parameters.get(parameterIndex);
        // can only delete old temp files if they are not in the batch
        param.setValue(value, batchParameters == null);
    }

    /**
     * [Not supported] Sets the value of a parameter as a row id.
     */
    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw unsupported("rowId");
    }

    /**
     * Sets the value of a parameter.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    @Override
    public void setNString(int parameterIndex, String x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setNString(" + parameterIndex + ", " + quote(x) + ')');
            }
            setParameter(parameterIndex, x == null ? ValueNull.INSTANCE : ValueVarchar.get(x, conn));
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
     * @param length the maximum number of characters
     * @throws SQLException if this object is closed
     */
    @Override
    public void setNCharacterStream(int parameterIndex, Reader x, long length)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setNCharacterStream(" + parameterIndex + ", x, " + length + "L)");
            }
            checkClosed();
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
    @Override
    public void setNCharacterStream(int parameterIndex, Reader x)
            throws SQLException {
        setNCharacterStream(parameterIndex, x, -1);
    }

    /**
     * Sets the value of a parameter as a Clob.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    @Override
    public void setNClob(int parameterIndex, NClob x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setNClob(" + parameterIndex + ", x)");
            }
            checkClosed();
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
    @Override
    public void setNClob(int parameterIndex, Reader x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setNClob(" + parameterIndex + ", x)");
            }
            checkClosed();
            Value v = conn.createClob(x, -1);
            setParameter(parameterIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter as a Clob. This method does not close the
     * reader. The reader may be closed after executing the statement.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @param length the maximum number of characters
     * @throws SQLException if this object is closed
     */
    @Override
    public void setClob(int parameterIndex, Reader x, long length)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setClob(" + parameterIndex + ", x, " + length + "L)");
            }
            checkClosed();
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
     * @param length the maximum number of bytes
     * @throws SQLException if this object is closed
     */
    @Override
    public void setBlob(int parameterIndex, InputStream x, long length)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setBlob(" + parameterIndex + ", x, " + length + "L)");
            }
            checkClosed();
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
     * @param length the maximum number of characters
     * @throws SQLException if this object is closed
     */
    @Override
    public void setNClob(int parameterIndex, Reader x, long length)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setNClob(" + parameterIndex + ", x, " + length + "L)");
            }
            checkClosed();
            Value v = conn.createClob(x, length);
            setParameter(parameterIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter as a SQLXML.
     *
     * @param parameterIndex the parameter index (1, 2, ...)
     * @param x the value
     * @throws SQLException if this object is closed
     */
    @Override
    public void setSQLXML(int parameterIndex, SQLXML x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("setSQLXML(" + parameterIndex + ", x)");
            }
            checkClosed();
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
     * INTERNAL
     */
    @Override
    public String toString() {
        return getTraceObjectName() + ": " + command;
    }

}
