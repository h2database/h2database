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
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Ref;
/*## Java 1.6 begin ##
import java.sql.NClob;
import java.sql.SQLXML;
import java.sql.RowId;
## Java 1.6 end ##*/
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

import org.h2.message.Message;
import org.h2.message.TraceObject;

/**
 * Represents a callable statement.
 */
public class JdbcCallableStatement extends JdbcPreparedStatement implements CallableStatement {

    JdbcCallableStatement(JdbcConnection conn, String sql, int id, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        super(conn, sql, id, resultSetType, resultSetConcurrency, false);
        setTrace(session.getTrace(), TraceObject.CALLABLE_STATEMENT, id);
    }

    /**
     * [Not supported]
     */
    public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
        throw Message.getUnsupportedException("registerOutParameter");
    }

    /**
     * [Not supported]
     */
    public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
        throw Message.getUnsupportedException("registerOutParameter");
    }

    /**
     * [Not supported]
     */
    public boolean wasNull() throws SQLException {
        throw Message.getUnsupportedException("wasNull");
    }

    /**
     * [Not supported]
     */
    public String getString(int parameterIndex) throws SQLException {
        throw throwGetUnsupportedException();
    }

    /**
     * [Not supported]
     */
    public boolean getBoolean(int parameterIndex) throws SQLException {
        throw throwGetUnsupportedException();
    }

    /**
     * [Not supported]
     */
    public byte getByte(int parameterIndex) throws SQLException {
        throw throwGetUnsupportedException();
    }

    /**
     * [Not supported]
     */
    public short getShort(int parameterIndex) throws SQLException {
        throw throwGetUnsupportedException();
    }

    /**
     * [Not supported]
     */
    public int getInt(int parameterIndex) throws SQLException {
        throw throwGetUnsupportedException();
    }

    /**
     * [Not supported]
     */
    public long getLong(int parameterIndex) throws SQLException {
        throw throwGetUnsupportedException();
    }

    /**
     * [Not supported]
     */
    public float getFloat(int parameterIndex) throws SQLException {
        throw throwGetUnsupportedException();
    }

    /**
     * [Not supported]
     */
    public double getDouble(int parameterIndex) throws SQLException {
        throw throwGetUnsupportedException();
    }

    /**
     * [Not supported]
     * @deprecated
     */
    public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
        throw throwGetUnsupportedException();
    }

    /**
     * [Not supported]
     */
    public byte[] getBytes(int parameterIndex) throws SQLException {
        throw throwGetUnsupportedException();
    }

    /**
     * [Not supported]
     */
    public Date getDate(int parameterIndex) throws SQLException {
        throw throwGetUnsupportedException();
    }

    /**
     * [Not supported]
     */
    public Time getTime(int parameterIndex) throws SQLException {
        throw throwGetUnsupportedException();
    }

    /**
     * [Not supported]
     */
    public Timestamp getTimestamp(int parameterIndex) throws SQLException {
        throw throwGetUnsupportedException();
    }

    /**
     * [Not supported]
     */
    public Object getObject(int parameterIndex) throws SQLException {
        throw throwGetUnsupportedException();
    }

    /**
     * [Not supported]
     */
    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
        throw throwGetUnsupportedException();
    }

    /**
     * [Not supported]
     */
    public Object getObject(int parameterIndex, Map<String, Class< ? >> map) throws SQLException {
        throw throwGetUnsupportedException();
    }

    /**
     * [Not supported]
     */
    public Ref getRef(int parameterIndex) throws SQLException {
        throw throwGetUnsupportedException();
    }

    /**
     * [Not supported]
     */
    public Blob getBlob(int parameterIndex) throws SQLException {
        throw throwGetUnsupportedException();
    }

    /**
     * [Not supported]
     */
    public Clob getClob(int parameterIndex) throws SQLException {
        throw throwGetUnsupportedException();
    }

    /**
     * [Not supported]
     */
    public Array getArray(int parameterIndex) throws SQLException {
        throw throwGetUnsupportedException();
    }

    /**
     * [Not supported]
     */
    public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
        throw throwGetUnsupportedException();
    }

    /**
     * [Not supported]
     */
    public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
        throw throwGetUnsupportedException();
    }

    /**
     * [Not supported]
     */
    public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
        throw throwGetUnsupportedException();
    }

    /**
     * [Not supported]
     */
    public void registerOutParameter(int paramIndex, int sqlType, String typeName) throws SQLException {
        throw Message.getUnsupportedException("register");
    }

    /**
     * [Not supported]
     *
     */
    public URL getURL(String parameterName) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     *
     */
    public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     */
    public Time getTime(String parameterName, Calendar cal) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     */
    public Date getDate(String parameterName, Calendar cal) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     */
    public Array getArray(String parameterName) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     */
    public Clob getClob(String parameterName) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     */
    public Blob getBlob(String parameterName) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     */
    public Ref getRef(String parameterName) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     */
    public Object getObject(String parameterName, Map<String, Class< ? >> map) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     */
    public BigDecimal getBigDecimal(String parameterName) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     */
    public Object getObject(String parameterName) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     */
    public Timestamp getTimestamp(String parameterName) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     */
    public Time getTime(String parameterName) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     */
    public Date getDate(String parameterName) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     */
    public byte[] getBytes(String parameterName) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     */
    public double getDouble(String parameterName) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     */
    public float getFloat(String parameterName) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     */
    public long getLong(String parameterName) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     */
    public int getInt(String parameterName) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     */
    public short getShort(String parameterName) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     */
    public byte getByte(String parameterName) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     */
    public boolean getBoolean(String parameterName) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     */
    public String getString(String parameterName) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    // --- setters --------------------------------------------------

    /**
     * [Not supported]
     */
    public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     */
    public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     */
    public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     */
    public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     */
    public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     */
    public void setObject(String parameterName, Object x) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     */
    public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     */
    public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     */
    public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     */
/*## Java 1.6 begin ##
    public void setAsciiStream(String parameterName, InputStream x, long length)
            throws SQLException {
        throw throwParameterNameNotSupported();
    }
## Java 1.6 end ##*/

    /**
     * [Not supported]
     */
    public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     */
    public void setTime(String parameterName, Time x) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     */
    public void setDate(String parameterName, Date x) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     */
    public void setBytes(String parameterName, byte[] x) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     */
    public void setString(String parameterName, String x) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     */
    public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     */
    public void setDouble(String parameterName, double x) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     */
    public void setFloat(String parameterName, float x) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     */
    public void setLong(String parameterName, long x) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     */
    public void setInt(String parameterName, int x) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     */
    public void setShort(String parameterName, short x) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     */
    public void setByte(String parameterName, byte x) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     */
    public void setBoolean(String parameterName, boolean x) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     */
    public void setNull(String parameterName, int sqlType) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     */
    public void setURL(String parameterName, URL val) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     */
    public URL getURL(int parameterIndex) throws SQLException {
        throw Message.getUnsupportedException("url");
    }

    // --- other methods --------------------------------------------

    /**
     * [Not supported]
     */
    public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     */
    public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     */
    public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
        throw throwParameterNameNotSupported();
    }

// =============================================================

    /**
     * [Not supported]
     */
/*## Java 1.6 begin ##
    public RowId getRowId(int parameterIndex) throws SQLException {
        throw Message.getUnsupportedException("rowId");
    }
## Java 1.6 end ##*/

    /**
     * [Not supported]
     */
/*## Java 1.6 begin ##
    public RowId getRowId(String parameterName) throws SQLException {
        throw throwParameterNameNotSupported();
    }
## Java 1.6 end ##*/

    /**
     * [Not supported]
     */
/*## Java 1.6 begin ##
    public void setRowId(String parameterName, RowId x) throws SQLException {
        throw throwParameterNameNotSupported();
    }
## Java 1.6 end ##*/

    /**
     * [Not supported]
     */
/*## Java 1.6 begin ##
    public void setNString(String parameterName, String value) throws SQLException {
        throw throwParameterNameNotSupported();
    }
## Java 1.6 end ##*/

    /**
     * [Not supported]
     */
/*## Java 1.6 begin ##
    public void setNCharacterStream(String parameterName, Reader value, long length)
            throws SQLException {
        throw throwParameterNameNotSupported();
    }
## Java 1.6 end ##*/

    /**
     * [Not supported]
     */
/*## Java 1.6 begin ##
    public void setNClob(String parameterName, NClob value)
            throws SQLException {
        throw throwParameterNameNotSupported();
    }
## Java 1.6 end ##*/

    /**
     * [Not supported]
     */
/*## Java 1.6 begin ##
    public void setClob(String parameterName, Reader reader, long length)
            throws SQLException {
        throw throwParameterNameNotSupported();
    }
## Java 1.6 end ##*/

    /**
     * [Not supported]
     */
/*## Java 1.6 begin ##
    public void setBlob(String parameterName, InputStream inputStream, long length)
            throws SQLException {
        throw throwParameterNameNotSupported();
    }
## Java 1.6 end ##*/

    /**
     * [Not supported]
     */
/*## Java 1.6 begin ##
    public void setNClob(String parameterName, Reader reader, long length)
            throws SQLException {
        throw throwParameterNameNotSupported();
    }
## Java 1.6 end ##*/

    /**
     * [Not supported]
     */
/*## Java 1.6 begin ##
    public NClob getNClob(int parameterIndex) throws SQLException {
        throw throwGetUnsupportedException();
    }
## Java 1.6 end ##*/

    /**
     * [Not supported]
     */
/*## Java 1.6 begin ##
    public NClob getNClob(String parameterName) throws SQLException {
        throw throwGetUnsupportedException();
    }
## Java 1.6 end ##*/

    /**
     * [Not supported]
     */
/*## Java 1.6 begin ##
    public void setSQLXML(String parameterName, SQLXML xmlObject)
            throws SQLException {
        throw throwParameterNameNotSupported();
    }
## Java 1.6 end ##*/

    /**
     * [Not supported]
     */
/*## Java 1.6 begin ##
    public SQLXML getSQLXML(int parameterIndex) throws SQLException {
        throw throwGetUnsupportedException();
    }
## Java 1.6 end ##*/

    /**
     * [Not supported]
     */
/*## Java 1.6 begin ##
    public SQLXML getSQLXML(String parameterName) throws SQLException {
        throw throwGetUnsupportedException();
    }
## Java 1.6 end ##*/

    /**
     * [Not supported]
     */
/*## Java 1.6 begin ##
    public String getNString(int parameterIndex) throws SQLException {
        throw throwGetUnsupportedException();
    }
## Java 1.6 end ##*/

    /**
     * [Not supported]
     */
/*## Java 1.6 begin ##
    public String getNString(String parameterName) throws SQLException {
        throw throwGetUnsupportedException();
    }
## Java 1.6 end ##*/

    /**
     * [Not supported]
     */
/*## Java 1.6 begin ##
    public Reader getNCharacterStream(int parameterIndex)
            throws SQLException {
        throw throwGetUnsupportedException();
    }
## Java 1.6 end ##*/

    /**
     * [Not supported]
     */
/*## Java 1.6 begin ##
    public Reader getNCharacterStream(String parameterName)
            throws SQLException {
        throw throwGetUnsupportedException();
    }
## Java 1.6 end ##*/

    /**
     * [Not supported]
     */
/*## Java 1.6 begin ##
    public Reader getCharacterStream(int parameterIndex) throws SQLException {
        throw throwGetUnsupportedException();
    }
## Java 1.6 end ##*/

    /**
     * [Not supported]
     */
/*## Java 1.6 begin ##
    public Reader getCharacterStream(String parameterName)
            throws SQLException {
        throw throwGetUnsupportedException();
    }
## Java 1.6 end ##*/

    /**
     * [Not supported]
     */
/*## Java 1.6 begin ##
    public void setBlob(String parameterName, Blob x) throws SQLException {
        throw throwParameterNameNotSupported();
    }
## Java 1.6 end ##*/

    /**
     * [Not supported]
     */
/*## Java 1.6 begin ##
    public void setClob(String parameterName, Clob x) throws SQLException {
        throw throwParameterNameNotSupported();
    }
## Java 1.6 end ##*/

    /**
     * [Not supported]
     */
/*## Java 1.6 begin ##
    public void setAsciiStream(String parameterName, InputStream x)
            throws SQLException {
        throw throwParameterNameNotSupported();
    }
## Java 1.6 end ##*/

    /**
     * [Not supported]
     */
    public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
        throw throwParameterNameNotSupported();
    }

    /**
     * [Not supported]
     */
/*## Java 1.6 begin ##
    public void setBinaryStream(String parameterName, InputStream x)
            throws SQLException {
        throw throwParameterNameNotSupported();
    }
## Java 1.6 end ##*/

    /**
     * [Not supported]
     */
/*## Java 1.6 begin ##
    public void setBinaryStream(String parameterName, InputStream x, long length)
            throws SQLException {
        throw throwParameterNameNotSupported();
    }
## Java 1.6 end ##*/

    /**
     * [Not supported]
     */
/*## Java 1.6 begin ##
    public void setBlob(String parameterName, InputStream x)
            throws SQLException {
        throw throwParameterNameNotSupported();
    }
## Java 1.6 end ##*/

    /**
     * [Not supported]
     */
/*## Java 1.6 begin ##
    public void setCharacterStream(String parameterName, Reader x)
            throws SQLException {
        throw throwParameterNameNotSupported();
    }
## Java 1.6 end ##*/

    /**
     * [Not supported]
     */
/*## Java 1.6 begin ##
    public void setCharacterStream(String parameterName, Reader x, long length)
            throws SQLException {
        throw throwParameterNameNotSupported();
    }
## Java 1.6 end ##*/

    /**
     * [Not supported]
     */
/*## Java 1.6 begin ##
    public void setClob(String parameterName, Reader x) throws SQLException {
        throw throwParameterNameNotSupported();
    }
## Java 1.6 end ##*/

    /**
     * [Not supported]
     */
/*## Java 1.6 begin ##
    public void setNCharacterStream(String parameterName, Reader x)
            throws SQLException {
        throw throwParameterNameNotSupported();
    }
## Java 1.6 end ##*/

    /**
     * [Not supported]
     */
/*## Java 1.6 begin ##
    public void setNClob(String parameterName, Reader x) throws SQLException {
        throw throwParameterNameNotSupported();
    }
## Java 1.6 end ##*/

    private SQLException throwParameterNameNotSupported() throws JdbcSQLException {
        throw Message.getUnsupportedException("parameterName");
    }

    private SQLException throwGetUnsupportedException() throws JdbcSQLException {
        throw Message.getUnsupportedException("get");
    }

}
