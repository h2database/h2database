/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbc;

import java.sql.*;

import org.h2.command.CommandInterface;
import org.h2.engine.SessionInterface;
import org.h2.message.Message;
import org.h2.message.TraceObject;

/**
 * Information about the parameters of a prepared statement.
 */
public class JdbcParameterMetaData extends TraceObject
// #ifdef JDK14
implements ParameterMetaData
// #endif
{

    private JdbcPreparedStatement prep;
    private int paramCount;

    /**
     * Returns the number of parameters.
     *
     * @return the number
     */
    public int getParameterCount() throws SQLException {
        try {
            debugCodeCall("getParameterCount");
            checkClosed();
            return paramCount;
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the parameter mode.
     * Always returns parameterModeIn
     *
     * @return parameterModeIn
     */
    public int getParameterMode(int param) throws SQLException {
        try {
            debugCodeCall("getParameterMode", param);
            checkParameterIndex(param);
            return parameterModeIn;
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the parameter type.
     * Always returns Types.VARCHAR everything can be passed as a VARCHAR.
     *
     * @return Types.VARCHAR
     */
    public int getParameterType(int param) throws SQLException {
        try {
            debugCodeCall("getParameterType", param);
            checkParameterIndex(param);
            return Types.VARCHAR;
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the parameter precision.
     * Always returns 0.
     *
     * @return 0
     */
    public int getPrecision(int param) throws SQLException {
        try {
            debugCodeCall("getPrecision", param);
            checkParameterIndex(param);
            return 0;
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the parameter precision.
     * Always returns 0.
     *
     * @return 0
     */
    public int getScale(int param) throws SQLException {
        try {
            debugCodeCall("getScale", param);
            checkParameterIndex(param);
            return 0;
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if this is nullable parameter.
     * Returns ResultSetMetaData.columnNullableUnknown..
     *
     * @return ResultSetMetaData.columnNullableUnknown
     */
    public int isNullable(int param) throws SQLException {
        try {
            debugCodeCall("isNullable", param);
            checkParameterIndex(param);
            return ResultSetMetaData.columnNullableUnknown;
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if this parameter is signed.
     * It always returns true.
     *
     * @return true
     */
    public boolean isSigned(int param) throws SQLException {
        try {
            debugCodeCall("isSigned", param);
            checkParameterIndex(param);
            return true;
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the parameter class name.
     * Always returns java.lang.String.
     *
     * @return "java.lang.String"
     */
    public String getParameterClassName(int param) throws SQLException {
        try {
            debugCodeCall("getParameterClassName", param);
            checkParameterIndex(param);
            return String.class.getName();
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the parameter type name.
     * Always returns VARCHAR.
     *
     * @return "VARCHAR"
     */
    public String getParameterTypeName(int param) throws SQLException {
        try {
            debugCodeCall("getParameterTypeName", param);
            checkParameterIndex(param);
            return "VARCHAR";
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    JdbcParameterMetaData(SessionInterface session, JdbcPreparedStatement prep, CommandInterface command, int id) {
        setTrace(session.getTrace(), TraceObject.PARAMETER_META_DATA, id);
        this.prep = prep;
        this.paramCount = command.getParameters().size();
    }

    void checkParameterIndex(int param) throws SQLException {
        checkClosed();
        if (param < 1 || param > paramCount) {
            throw Message.getInvalidValueException("" + param, "param");
        }
    }

    void checkClosed() throws SQLException {
        prep.checkClosed();
    }

    /**
     * Return an object of this class if possible.
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    //#ifdef JDK16
/*
    public <T> T unwrap(Class<T> iface) throws SQLException {
        debugCodeCall("unwrap");
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
        debugCodeCall("isWrapperFor");
        throw Message.getUnsupportedException();
    }
*/
    //#endif

}
