/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
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
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Ref;
import java.sql.ResultSetMetaData;
//## Java 1.6 begin ##
import java.sql.NClob;
import java.sql.SQLXML;
import java.sql.RowId;
//## Java 1.6 end ##
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import org.h2.expression.ParameterInterface;
import org.h2.message.DbException;
import org.h2.message.TraceObject;
import org.h2.util.BitField;
import org.h2.value.ValueNull;

/**
 * Represents a callable statement.
 * 
 * @author Sergi Vladykin
 */
public class JdbcCallableStatement extends JdbcPreparedStatement implements CallableStatement {

    private BitField outParameters;
    private int maxOutParameters;
    private Map<String, Integer> namedParameters;

    JdbcCallableStatement(JdbcConnection conn, String sql, int id, int resultSetType, int resultSetConcurrency) {
        super(conn, sql, id, resultSetType, resultSetConcurrency, false);
        setTrace(session.getTrace(), TraceObject.CALLABLE_STATEMENT, id);
    }

    /**
     * @see java.sql.PreparedStatement#executeUpdate()
     */
    public int executeUpdate() throws SQLException {
        if (command.isQuery()) {
            super.executeQuery().next();
            return 0;
        }
        return super.executeUpdate();
    }

    /**
     * @see java.sql.CallableStatement#registerOutParameter(int, int)
     */
    public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
        registerOutParameter(parameterIndex, sqlType, 0);
    }

    /**
     * @see java.sql.CallableStatement#registerOutParameter(int, int, int)
     */
    public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
        registerOutParameter(parameterIndex, sqlType, scale, null);
    }

    private ResultSetMetaData getCheckedMetaData() throws SQLException {
        ResultSetMetaData meta = getMetaData();
        if (meta == null) {
            throw DbException.getUnsupportedException("Supported only for stored procedures calling.");
        }
        return meta;
    }
    
    private void checkIndexBounds(int parameterIndex) throws SQLException {
        if (parameterIndex < 1 || parameterIndex > maxOutParameters) {
            throw DbException.getInvalidValueException(Integer.toString(parameterIndex), "parameterIndex");
        }        
    }
    
    private void registerOutParameter(int parameterIndex, int sqlType, int scale, String typeName) throws SQLException {
        try {
            if (outParameters == null) {
                maxOutParameters = Math.min(getParameterMetaData().getParameterCount(), getCheckedMetaData()
                        .getColumnCount());
                outParameters = new BitField();
            }
            checkIndexBounds(parameterIndex);
            ParameterInterface param = command.getParameters().get(--parameterIndex);
            if (param.getParamValue() == null) {
                param.setValue(ValueNull.INSTANCE, false);
            }
            outParameters.set(parameterIndex);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }
    
    private void checkRegistered(int parameterIndex) throws SQLException {
        try {
            checkIndexBounds(parameterIndex);
            if (!outParameters.get(parameterIndex - 1)) {
                throw DbException.getInvalidValueException(Integer.toString(parameterIndex), "parameterIndex");
            }
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    private int getIndexForName(String parameterName) throws SQLException {
        try {
            if (namedParameters == null) {
                ResultSetMetaData meta = getCheckedMetaData();
                int columnCount = meta.getColumnCount();
                Map<String, Integer> map = new HashMap<String, Integer>(columnCount);
                for (int i = 1; i <= columnCount; i++) {
                    map.put(meta.getColumnLabel(i), i);
                }
                namedParameters = map;
            }
            Integer index = namedParameters.get(parameterName);
            if (index == null) {
                throw DbException.getInvalidValueException(parameterName, "parameterName");
            }
            return index;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * @see java.sql.CallableStatement#wasNull()
     */
    public boolean wasNull() throws SQLException {
        if (resultSet == null) {
            throw logAndConvert(DbException
                    .getUnsupportedException("Method wasNull() should be called only after statement execution and calling a getter method."));
        }
        return resultSet.wasNull();
    }

    /**
     * @see java.sql.CallableStatement#getURL(int)
     */
    public URL getURL(int parameterIndex) throws SQLException {
        checkRegistered(parameterIndex);
        return resultSet.getURL(parameterIndex);
    }

    /**
     * @see java.sql.CallableStatement#getString(int)
     */
    public String getString(int parameterIndex) throws SQLException {
        checkRegistered(parameterIndex);
        return resultSet.getString(parameterIndex);
    }

    /**
     * @see java.sql.CallableStatement#getBoolean(int)
     */
    public boolean getBoolean(int parameterIndex) throws SQLException {
        checkRegistered(parameterIndex);
        return resultSet.getBoolean(parameterIndex);
    }

    /**
     * @see java.sql.CallableStatement#getByte(int)
     */
    public byte getByte(int parameterIndex) throws SQLException {
        checkRegistered(parameterIndex);
        return resultSet.getByte(parameterIndex);
    }

    /**
     * @see java.sql.CallableStatement#getShort(int)
     */
    public short getShort(int parameterIndex) throws SQLException {
        checkRegistered(parameterIndex);
        return resultSet.getShort(parameterIndex);
    }

    /**
     * @see java.sql.CallableStatement#getInt(int)
     */
    public int getInt(int parameterIndex) throws SQLException {
        checkRegistered(parameterIndex);
        return resultSet.getInt(parameterIndex);
    }

    /**
     * @see java.sql.CallableStatement#getLong(int)
     */
    public long getLong(int parameterIndex) throws SQLException {
        checkRegistered(parameterIndex);
        return resultSet.getLong(parameterIndex);
    }

    /**
     * @see java.sql.CallableStatement#getFloat(int)
     */
    public float getFloat(int parameterIndex) throws SQLException {
        checkRegistered(parameterIndex);
        return resultSet.getFloat(parameterIndex);
    }

    /**
     * @see java.sql.CallableStatement#getDouble(int)
     */
    public double getDouble(int parameterIndex) throws SQLException {
        checkRegistered(parameterIndex);
        return resultSet.getDouble(parameterIndex);
    }

    /**
     * @see java.sql.CallableStatement#getBigDecimal(int, int)
     * @deprecated use <code>getBigDecimal(int parameterIndex)</code>
     *             or <code>getBigDecimal(String parameterName)</code>
     */
    public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
        checkRegistered(parameterIndex);
        return resultSet.getBigDecimal(parameterIndex, scale);
    }

    /**
     * @see java.sql.CallableStatement#getBytes(int)
     */
    public byte[] getBytes(int parameterIndex) throws SQLException {
        checkRegistered(parameterIndex);
        return resultSet.getBytes(parameterIndex);
    }

    /**
     * @see java.sql.CallableStatement#getDate(int)
     */
    public Date getDate(int parameterIndex) throws SQLException {
        checkRegistered(parameterIndex);
        return resultSet.getDate(parameterIndex);
    }

    /**
     * @see java.sql.CallableStatement#getTime(int)
     */
    public Time getTime(int parameterIndex) throws SQLException {
        checkRegistered(parameterIndex);
        return resultSet.getTime(parameterIndex);
    }

    /**
     * @see java.sql.CallableStatement#getTimestamp(int)
     */
    public Timestamp getTimestamp(int parameterIndex) throws SQLException {
        checkRegistered(parameterIndex);
        return resultSet.getTimestamp(parameterIndex);
    }

    /**
     * @see java.sql.CallableStatement#getObject(int)
     */
    public Object getObject(int parameterIndex) throws SQLException {
        checkRegistered(parameterIndex);
        return resultSet.getObject(parameterIndex);
    }

    /**
     * @see java.sql.CallableStatement#getBigDecimal(int)
     */
    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
        checkRegistered(parameterIndex);
        return resultSet.getBigDecimal(parameterIndex);
    }

    /**
     * @see java.sql.CallableStatement#getObject(int, java.util.Map)
     */
    public Object getObject(int parameterIndex, Map<String, Class<?>> map) throws SQLException {
        checkRegistered(parameterIndex);
        return resultSet.getObject(parameterIndex, map);
    }

    /**
     * @see java.sql.CallableStatement#getRef(int)
     */
    public Ref getRef(int parameterIndex) throws SQLException {
        checkRegistered(parameterIndex);
        return resultSet.getRef(parameterIndex);
    }

    /**
     * @see java.sql.CallableStatement#getBlob(int)
     */
    public Blob getBlob(int parameterIndex) throws SQLException {
        checkRegistered(parameterIndex);
        return resultSet.getBlob(parameterIndex);
    }

    /**
     * @see java.sql.CallableStatement#getClob(int)
     */
    public Clob getClob(int parameterIndex) throws SQLException {
        checkRegistered(parameterIndex);
        return resultSet.getClob(parameterIndex);
    }

    /**
     * @see java.sql.CallableStatement#getArray(int)
     */
    public Array getArray(int parameterIndex) throws SQLException {
        checkRegistered(parameterIndex);
        return resultSet.getArray(parameterIndex);
    }

    /**
     * @see java.sql.CallableStatement#getDate(int, java.util.Calendar)
     */
    public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
        checkRegistered(parameterIndex);
        return resultSet.getDate(parameterIndex, cal);
    }

    /**
     * @see java.sql.CallableStatement#getTime(int, java.util.Calendar)
     */
    public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
        checkRegistered(parameterIndex);
        return resultSet.getTime(parameterIndex, cal);
    }

    /**
     * @see java.sql.CallableStatement#getTimestamp(int, java.util.Calendar)
     */
    public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
        checkRegistered(parameterIndex);
        return resultSet.getTimestamp(parameterIndex, cal);
    }

    /**
     * @see java.sql.CallableStatement#registerOutParameter(int, int, java.lang.String)
     */
    public void registerOutParameter(int parameterIndex, int sqlType, String typeName) throws SQLException {
        registerOutParameter(parameterIndex, sqlType, 0, typeName);
    }

    /**
     * @see java.sql.CallableStatement#getURL(java.lang.String)
     */
    public URL getURL(String parameterName) throws SQLException {
        return getURL(getIndexForName(parameterName));
    }

    /**
     * @see java.sql.CallableStatement#getTimestamp(java.lang.String, java.util.Calendar)
     */
    public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
        return getTimestamp(getIndexForName(parameterName), cal);
    }

    /**
     * @see java.sql.CallableStatement#getTime(java.lang.String, java.util.Calendar)
     */
    public Time getTime(String parameterName, Calendar cal) throws SQLException {
        return getTime(getIndexForName(parameterName), cal);
    }

    /**
     * @see java.sql.CallableStatement#getDate(java.lang.String, java.util.Calendar)
     */
    public Date getDate(String parameterName, Calendar cal) throws SQLException {
        return getDate(getIndexForName(parameterName), cal);
    }

    /**
     * @see java.sql.CallableStatement#getArray(java.lang.String)
     */
    public Array getArray(String parameterName) throws SQLException {
        return getArray(getIndexForName(parameterName));
    }

    /**
     * @see java.sql.CallableStatement#getClob(java.lang.String)
     */
    public Clob getClob(String parameterName) throws SQLException {
        return getClob(getIndexForName(parameterName));
    }

    /**
     * @see java.sql.CallableStatement#getBlob(java.lang.String)
     */
    public Blob getBlob(String parameterName) throws SQLException {
        return getBlob(getIndexForName(parameterName));
    }

    /**
     * @see java.sql.CallableStatement#getRef(java.lang.String)
     */
    public Ref getRef(String parameterName) throws SQLException {
        return getRef(getIndexForName(parameterName));
    }

    /**
     * @see java.sql.CallableStatement#getObject(java.lang.String, java.util.Map)
     */
    public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException {
        return getObject(getIndexForName(parameterName), map);
    }

    /**
     * @see java.sql.CallableStatement#getBigDecimal(java.lang.String)
     */
    public BigDecimal getBigDecimal(String parameterName) throws SQLException {
        return getBigDecimal(getIndexForName(parameterName));
    }

    /**
     * @see java.sql.CallableStatement#getObject(java.lang.String)
     */
    public Object getObject(String parameterName) throws SQLException {
        return getObject(getIndexForName(parameterName));
    }

    /**
     * @see java.sql.CallableStatement#getTimestamp(java.lang.String)
     */
    public Timestamp getTimestamp(String parameterName) throws SQLException {
        return getTimestamp(getIndexForName(parameterName));
    }

    /**
     * @see java.sql.CallableStatement#getTime(java.lang.String)
     */
    public Time getTime(String parameterName) throws SQLException {
        return getTime(getIndexForName(parameterName));
    }

    /**
     * @see java.sql.CallableStatement#getDate(java.lang.String)
     */
    public Date getDate(String parameterName) throws SQLException {
        return getDate(getIndexForName(parameterName));
    }

    /**
     * @see java.sql.CallableStatement#getBytes(java.lang.String)
     */
    public byte[] getBytes(String parameterName) throws SQLException {
        return getBytes(getIndexForName(parameterName));
    }

    /**
     * @see java.sql.CallableStatement#getDouble(java.lang.String)
     */
    public double getDouble(String parameterName) throws SQLException {
        return getDouble(getIndexForName(parameterName));
    }

    /**
     * @see java.sql.CallableStatement#getFloat(java.lang.String)
     */
    public float getFloat(String parameterName) throws SQLException {
        return getFloat(getIndexForName(parameterName));
    }

    /**
     * @see java.sql.CallableStatement#getLong(java.lang.String)
     */
    public long getLong(String parameterName) throws SQLException {
        return getLong(getIndexForName(parameterName));
    }

    /**
     * @see java.sql.CallableStatement#getInt(java.lang.String)
     */
    public int getInt(String parameterName) throws SQLException {
        return getInt(getIndexForName(parameterName));
    }

    /**
     * @see java.sql.CallableStatement#getShort(java.lang.String)
     */
    public short getShort(String parameterName) throws SQLException {
        return getShort(getIndexForName(parameterName));
    }

    /**
     * @see java.sql.CallableStatement#getByte(java.lang.String)
     */
    public byte getByte(String parameterName) throws SQLException {
        return getByte(getIndexForName(parameterName));
    }

    /**
     * @see java.sql.CallableStatement#getBoolean(java.lang.String)
     */
    public boolean getBoolean(String parameterName) throws SQLException {
        return getBoolean(getIndexForName(parameterName));
    }

    /**
     * @see java.sql.CallableStatement#getString(java.lang.String)
     */
    public String getString(String parameterName) throws SQLException {
        return getString(getIndexForName(parameterName));
    }

    // --- setters --------------------------------------------------

    /**
     * @see java.sql.CallableStatement#setNull(java.lang.String, int, java.lang.String)
     */
    public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
        setNull(getIndexForName(parameterName), sqlType, typeName);
    }

    /**
     * @see java.sql.CallableStatement#setTimestamp(java.lang.String, java.sql.Timestamp, java.util.Calendar)
     */
    public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
        setTimestamp(getIndexForName(parameterName), x, cal);
    }

    /**
     * @see java.sql.CallableStatement#setTime(java.lang.String, java.sql.Time, java.util.Calendar)
     */
    public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
        setTime(getIndexForName(parameterName), x, cal);
    }

    /**
     * @see java.sql.CallableStatement#setDate(java.lang.String, java.sql.Date, java.util.Calendar)
     */
    public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
        setDate(getIndexForName(parameterName), x, cal);
    }

    /**
     * @see java.sql.CallableStatement#setCharacterStream(java.lang.String, java.io.Reader, int)
     */
    public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {
        setCharacterStream(getIndexForName(parameterName), reader, length);
    }

    /**
     * @see java.sql.CallableStatement#setObject(java.lang.String, java.lang.Object)
     */
    public void setObject(String parameterName, Object x) throws SQLException {
        setObject(getIndexForName(parameterName), x);
    }

    /**
     * @see java.sql.CallableStatement#setObject(java.lang.String, java.lang.Object, int)
     */
    public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
        setObject(getIndexForName(parameterName), x, targetSqlType);
    }

    /**
     * @see java.sql.CallableStatement#setObject(java.lang.String, java.lang.Object, int, int)
     */
    public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException {
        setObject(getIndexForName(parameterName), x, targetSqlType, scale);
    }

    /**
     * @see java.sql.CallableStatement#setBinaryStream(java.lang.String, java.io.InputStream, int)
     */
    public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
        setBinaryStream(getIndexForName(parameterName), x, length);
    }

    /**
     * @see java.sql.CallableStatement#setAsciiStream(java.lang.String, java.io.InputStream, long)
     */
//## Java 1.6 begin ##
    public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException {
        setAsciiStream(getIndexForName(parameterName), x, length);
    }
//## Java 1.6 end ##

    /**
     * @see java.sql.CallableStatement#setTimestamp(java.lang.String, java.sql.Timestamp)
     */
    public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
        setTimestamp(getIndexForName(parameterName), x);
    }

    /**
     * @see java.sql.CallableStatement#setTime(java.lang.String, java.sql.Time)
     */
    public void setTime(String parameterName, Time x) throws SQLException {
        setTime(getIndexForName(parameterName), x);
    }

    /**
     * @see java.sql.CallableStatement#setDate(java.lang.String, java.sql.Date)
     */
    public void setDate(String parameterName, Date x) throws SQLException {
        setDate(getIndexForName(parameterName), x);
    }

    /**
     * @see java.sql.CallableStatement#setBytes(java.lang.String, byte[])
     */
    public void setBytes(String parameterName, byte[] x) throws SQLException {
        setBytes(getIndexForName(parameterName), x);
    }

    /**
     * @see java.sql.CallableStatement#setString(java.lang.String, java.lang.String)
     */
    public void setString(String parameterName, String x) throws SQLException {
        setString(getIndexForName(parameterName), x);
    }

    /**
     * @see java.sql.CallableStatement#setBigDecimal(java.lang.String, java.math.BigDecimal)
     */
    public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
        setBigDecimal(getIndexForName(parameterName), x);
    }

    /**
     * @see java.sql.CallableStatement#setDouble(java.lang.String, double)
     */
    public void setDouble(String parameterName, double x) throws SQLException {
        setDouble(getIndexForName(parameterName), x);
    }

    /**
     * @see java.sql.CallableStatement#setFloat(java.lang.String, float)
     */
    public void setFloat(String parameterName, float x) throws SQLException {
        setFloat(getIndexForName(parameterName), x);
    }

    /**
     * @see java.sql.CallableStatement#setLong(java.lang.String, long)
     */
    public void setLong(String parameterName, long x) throws SQLException {
        setLong(getIndexForName(parameterName), x);
    }

    /**
     * @see java.sql.CallableStatement#setInt(java.lang.String, int)
     */
    public void setInt(String parameterName, int x) throws SQLException {
        setInt(getIndexForName(parameterName), x);
    }

    /**
     * @see java.sql.CallableStatement#setShort(java.lang.String, short)
     */
    public void setShort(String parameterName, short x) throws SQLException {
        setShort(getIndexForName(parameterName), x);
    }

    /**
     * @see java.sql.CallableStatement#setByte(java.lang.String, byte)
     */
    public void setByte(String parameterName, byte x) throws SQLException {
        setByte(getIndexForName(parameterName), x);
    }

    /**
     * @see java.sql.CallableStatement#setBoolean(java.lang.String, boolean)
     */
    public void setBoolean(String parameterName, boolean x) throws SQLException {
        setBoolean(getIndexForName(parameterName), x);
    }

    /**
     * @see java.sql.CallableStatement#setNull(java.lang.String, int)
     */
    public void setNull(String parameterName, int sqlType) throws SQLException {
        setNull(getIndexForName(parameterName), sqlType);
    }

    /**
     * @see java.sql.CallableStatement#setURL(java.lang.String, java.net.URL)
     */
    public void setURL(String parameterName, URL val) throws SQLException {
        setURL(getIndexForName(parameterName), val);
    }

    // --- other methods --------------------------------------------

    /**
     * @see java.sql.CallableStatement#registerOutParameter(java.lang.String, int, java.lang.String)
     */
    public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException {
        registerOutParameter(getIndexForName(parameterName), sqlType, typeName);
    }

    /**
     * @see java.sql.CallableStatement#registerOutParameter(java.lang.String, int, int)
     */
    public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException {
        registerOutParameter(getIndexForName(parameterName), sqlType, scale);
    }

    /**
     * @see java.sql.CallableStatement#registerOutParameter(java.lang.String, int)
     */
    public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
        registerOutParameter(getIndexForName(parameterName), sqlType);
    }

    // =============================================================

    /**
     * @see java.sql.CallableStatement#getRowId(int)
     */
//## Java 1.6 begin ##
    public RowId getRowId(int parameterIndex) throws SQLException {
        checkRegistered(parameterIndex);
        return resultSet.getRowId(parameterIndex);
    }
//## Java 1.6 end ##

    /**
     * @see java.sql.CallableStatement#getRowId(java.lang.String)
     */
//## Java 1.6 begin ##
    public RowId getRowId(String parameterName) throws SQLException {
        return getRowId(getIndexForName(parameterName));
    }
//## Java 1.6 end ##

    /**
     * @see java.sql.CallableStatement#setRowId(java.lang.String, java.sql.RowId)
     */
//## Java 1.6 begin ##
    public void setRowId(String parameterName, RowId x) throws SQLException {
        setRowId(getIndexForName(parameterName), x);
    }
//## Java 1.6 end ##

    /**
     * @see java.sql.CallableStatement#setNString(java.lang.String, java.lang.String)
     */
//## Java 1.6 begin ##
    public void setNString(String parameterName, String x) throws SQLException {
        setNString(getIndexForName(parameterName), x);
    }
//## Java 1.6 end ##

    /**
     * @see java.sql.CallableStatement#setNCharacterStream(java.lang.String, java.io.Reader, long)
     */
//## Java 1.6 begin ##
    public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException {
        setNCharacterStream(getIndexForName(parameterName), value, length);
    }
//## Java 1.6 end ##

    /**
     * @see java.sql.CallableStatement#setNClob(java.lang.String, java.sql.NClob)
     */
//## Java 1.6 begin ##
    public void setNClob(String parameterName, NClob value) throws SQLException {
        setNClob(getIndexForName(parameterName), value);
    }
//## Java 1.6 end ##

    /**
     * @see java.sql.CallableStatement#setClob(java.lang.String, java.io.Reader, long)
     */
//## Java 1.6 begin ##
    public void setClob(String parameterName, Reader reader, long length) throws SQLException {
        setClob(getIndexForName(parameterName), reader, length);
    }
//## Java 1.6 end ##

    /**
     * @see java.sql.CallableStatement#setBlob(java.lang.String, java.io.InputStream, long)
     */
//## Java 1.6 begin ##
    public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException {
        setBlob(getIndexForName(parameterName), inputStream, length);
    }
//## Java 1.6 end ##

    /**
     * @see java.sql.CallableStatement#setNClob(java.lang.String, java.io.Reader, long)
     */
//## Java 1.6 begin ##
    public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
        setNClob(getIndexForName(parameterName), reader, length);
    }
//## Java 1.6 end ##

    /**
     * @see java.sql.CallableStatement#getNClob(int)
     */
//## Java 1.6 begin ##
    public NClob getNClob(int parameterIndex) throws SQLException {
        checkRegistered(parameterIndex);
        return resultSet.getNClob(parameterIndex);
    }
//## Java 1.6 end ##

    /**
     * @see java.sql.CallableStatement#getNClob(java.lang.String)
     */
//## Java 1.6 begin ##
    public NClob getNClob(String parameterName) throws SQLException {
        return getNClob(getIndexForName(parameterName));
    }
//## Java 1.6 end ##

    /**
     * @see java.sql.CallableStatement#setSQLXML(java.lang.String, java.sql.SQLXML)
     */
//## Java 1.6 begin ##
    public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
        setSQLXML(getIndexForName(parameterName), xmlObject);
    }
//## Java 1.6 end ##

    /**
     * @see java.sql.CallableStatement#getSQLXML(int)
     */
//## Java 1.6 begin ##
    public SQLXML getSQLXML(int parameterIndex) throws SQLException {
        checkRegistered(parameterIndex);
        return resultSet.getSQLXML(parameterIndex);
    }
//## Java 1.6 end ##

    /**
     * @see java.sql.CallableStatement#getSQLXML(java.lang.String)
     */
//## Java 1.6 begin ##
    public SQLXML getSQLXML(String parameterName) throws SQLException {
        return getSQLXML(getIndexForName(parameterName));
    }
//## Java 1.6 end ##

    /**
     * @see java.sql.CallableStatement#getNString(int)
     */
//## Java 1.6 begin ##
    public String getNString(int parameterIndex) throws SQLException {
        checkRegistered(parameterIndex);
        return resultSet.getNString(parameterIndex);
    }
//## Java 1.6 end ##

    /**
     * @see java.sql.CallableStatement#getNString(java.lang.String)
     */
//## Java 1.6 begin ##
    public String getNString(String parameterName) throws SQLException {
        return getNString(getIndexForName(parameterName));
    }
//## Java 1.6 end ##

    /**
     * @see java.sql.CallableStatement#getNCharacterStream(int)
     */
//## Java 1.6 begin ##
    public Reader getNCharacterStream(int parameterIndex) throws SQLException {
        checkRegistered(parameterIndex);
        return resultSet.getNCharacterStream(parameterIndex);
    }
//## Java 1.6 end ##

    /**
     * @see java.sql.CallableStatement#getNCharacterStream(java.lang.String)
     */
//## Java 1.6 begin ##
    public Reader getNCharacterStream(String parameterName) throws SQLException {
        return getNCharacterStream(getIndexForName(parameterName));
    }
//## Java 1.6 end ##

    /**
     * @see java.sql.CallableStatement#getCharacterStream(int)
     */
//## Java 1.6 begin ##
    public Reader getCharacterStream(int parameterIndex) throws SQLException {
        checkRegistered(parameterIndex);
        return resultSet.getCharacterStream(parameterIndex);
    }
//## Java 1.6 end ##

    /**
     * @see java.sql.CallableStatement#getCharacterStream(java.lang.String)
     */
//## Java 1.6 begin ##
    public Reader getCharacterStream(String parameterName) throws SQLException {
        return getCharacterStream(getIndexForName(parameterName));
    }
//## Java 1.6 end ##

    /**
     * @see java.sql.CallableStatement#setBlob(java.lang.String, java.sql.Blob)
     */
//## Java 1.6 begin ##
    public void setBlob(String parameterName, Blob x) throws SQLException {
        setBlob(getIndexForName(parameterName), x);
    }
//## Java 1.6 end ##

    /**
     * @see java.sql.CallableStatement#setClob(java.lang.String, java.sql.Clob)
     */
//## Java 1.6 begin ##
    public void setClob(String parameterName, Clob x) throws SQLException {
        setClob(getIndexForName(parameterName), x);
    }
//## Java 1.6 end ##

    /**
     * @see java.sql.CallableStatement#setAsciiStream(java.lang.String, java.io.InputStream)
     */
//## Java 1.6 begin ##
    public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
        setAsciiStream(getIndexForName(parameterName), x);
    }
//## Java 1.6 end ##

    /**
     * @see java.sql.CallableStatement#setAsciiStream(java.lang.String, java.io.InputStream, int)
     */
    public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
        setAsciiStream(getIndexForName(parameterName), x, length);
    }

    /**
     * @see java.sql.CallableStatement#setBinaryStream(java.lang.String, java.io.InputStream)
     */
//## Java 1.6 begin ##
    public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
        setBinaryStream(getIndexForName(parameterName), x);
    }
//## Java 1.6 end ##

    /**
     * @see java.sql.CallableStatement#setBinaryStream(java.lang.String, java.io.InputStream, long)
     */
//## Java 1.6 begin ##
    public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException {
        setBinaryStream(getIndexForName(parameterName), x, length);
    }
//## Java 1.6 end ##

    /**
     * @see java.sql.CallableStatement#setBlob(java.lang.String, java.io.InputStream)
     */
//## Java 1.6 begin ##
    public void setBlob(String parameterName, InputStream x) throws SQLException {
        setBlob(getIndexForName(parameterName), x);
    }
//## Java 1.6 end ##

    /**
     * @see java.sql.CallableStatement#setCharacterStream(java.lang.String, java.io.Reader)
     */
//## Java 1.6 begin ##
    public void setCharacterStream(String parameterName, Reader x) throws SQLException {
        setCharacterStream(getIndexForName(parameterName), x);
    }
//## Java 1.6 end ##

    /**
     * @see java.sql.CallableStatement#setCharacterStream(java.lang.String, java.io.Reader, long)
     */
//## Java 1.6 begin ##
    public void setCharacterStream(String parameterName, Reader x, long length) throws SQLException {
        setCharacterStream(getIndexForName(parameterName), x, length);
    }
//## Java 1.6 end ##

    /**
     * @see java.sql.CallableStatement#setClob(java.lang.String, java.io.Reader)
     */
//## Java 1.6 begin ##
    public void setClob(String parameterName, Reader x) throws SQLException {
        setClob(getIndexForName(parameterName), x);
    }
//## Java 1.6 end ##

    /**
     * @see java.sql.CallableStatement#setNCharacterStream(java.lang.String, java.io.Reader)
     */
//## Java 1.6 begin ##
    public void setNCharacterStream(String parameterName, Reader x) throws SQLException {
        setNCharacterStream(getIndexForName(parameterName), x);
    }
//## Java 1.6 end ##

    /**
     * @see java.sql.CallableStatement#setNClob(java.lang.String, java.io.Reader)
     */
//## Java 1.6 begin ##
    public void setNClob(String parameterName, Reader x) throws SQLException {
        setNClob(getIndexForName(parameterName), x);
    }
//## Java 1.6 end ##
}
