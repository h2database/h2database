/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.message;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Iterator;
import java.util.Locale;
import java.util.Properties;
import java.util.Map.Entry;

import org.h2.constant.ErrorCode;
import org.h2.jdbc.JdbcSQLException;
import org.h2.util.Resources;
import org.h2.util.StringUtils;

/**
 * Messages used in the database engine.
 * Use the PropertiesToUTF8 tool to translate properties files to UTF-8 and back.
 * If the word 'SQL' appears then the whole SQL statement must be a parameter,
 * otherwise this may be added: '; SQL statement: ' + sql
 */
public class Message {

    private static final Properties MESSAGES = new Properties();

    static {
        try {
            byte[] messages = Resources.get("/org/h2/res/_messages_en.properties");
            if (messages != null) {
                MESSAGES.load(new ByteArrayInputStream(messages));
            }
            String language = Locale.getDefault().getLanguage();
            if (!"en".equals(language)) {
                byte[] translations = Resources.get("/org/h2/res/_messages_"+language+".properties");
                // message: translated message + english (otherwise certain applications don't work)
                if (translations != null) {
                    Properties p = new Properties();
                    p.load(new ByteArrayInputStream(translations));
                    for (Iterator it = p.entrySet().iterator(); it.hasNext();) {
                        Entry e = (Entry) it.next();
                        String key = (String) e.getKey();
                        MESSAGES.put(key, e.getValue() + "\n" + MESSAGES.getProperty(key));
                    }
                }
            }
        } catch (IOException e) {
            TraceSystem.traceThrowable(e);
        }
    }

    /**
     * Gets the SQL Exception object for a specific SQLState. Supported
     * SQL states are:
     *
     * @param sqlState - the SQL State
     * @param param - the parameter of the message
     * @return the SQLException object
     */
    public static JdbcSQLException getSQLException(int sqlState, String p1) {
        return getSQLException(sqlState, new String[] { p1 });
    }

    public static String translate(String key, String[] param) {
        String message = MESSAGES.getProperty(key);
        if (message == null) {
            message = "(Message " +key+ " not found)";
        }
        if (param != null) {
            Object[] o = param;
            message = MessageFormat.format(message, o);
        }
        return message;
    }

    public static JdbcSQLException getSQLException(int errorCode, String[] param, Throwable cause) {
        String sqlstate = ErrorCode.getState(errorCode);
        String message = translate(sqlstate, param);
        return new JdbcSQLException(message, null, sqlstate, errorCode, cause, null);
    }

    public static JdbcSQLException getSQLException(int errorCode, String[] param) {
        return getSQLException(errorCode, param, null);
    }

    public static SQLException getSyntaxError(String sql, int index) {
        sql = StringUtils.addAsterisk(sql, index);
        return getSQLException(ErrorCode.SYNTAX_ERROR_1, sql);
    }

    public static SQLException getSyntaxError(String sql, int index, String expected) {
        sql = StringUtils.addAsterisk(sql, index);
        return getSQLException(ErrorCode.SYNTAX_ERROR_2, new String[]{sql, expected});
    }

    /**
     * Gets the SQL Exception object for a specific SQLState.
     *
     * @param sqlstate -
     *            the SQL State
     * @return the SQLException object
     */
    public static JdbcSQLException getSQLException(int sqlstate) {
        return getSQLException(sqlstate, (String) null);
    }

    public static JdbcSQLException getUnsupportedException() {
        return getSQLException(ErrorCode.FEATURE_NOT_SUPPORTED);
    }

    public static JdbcSQLException getInvalidValueException(String value, String param) {
        return getSQLException(ErrorCode.INVALID_VALUE_2, new String[]{value, param});
    }

    public static Error getInternalError(String s) {
        Error e = new Error(s);
        TraceSystem.traceThrowable(e);
        return e;
    }

    public static Error getInternalError(String s, Exception e) {
//#ifdef JDK14
        Error e2 = new Error(s, e);
//#endif
//#ifdef JDK13
/*
        Error e2 = new Error(s);
*/
//#endif
        TraceSystem.traceThrowable(e2);
        return e2;
    }

    public static SQLException addSQL(SQLException e, String sql) {
        if (e instanceof JdbcSQLException) {
            JdbcSQLException j = (JdbcSQLException) e;
            if (j.getSQL() != null) {
                return j;
            }
            return new JdbcSQLException(j.getOriginalMessage(), j.getSQL(), 
                    j.getSQLState(), 
                    j.getErrorCode(), j, null);
        } else {
            return new JdbcSQLException(e.getMessage(), sql, 
                    e.getSQLState(), 
                    e.getErrorCode(), e, null);
        }
    }

    public static SQLException convert(Throwable e) {
        if (e instanceof InternalException) {
            e = ((InternalException) e).getOriginalCause();
        }
        if (e instanceof SQLException) {
            return (SQLException) e;
        } else if (e instanceof InvocationTargetException) {
            InvocationTargetException te = (InvocationTargetException) e;
            Throwable t = te.getTargetException();
            if (t instanceof SQLException) {
                return (SQLException) t;
            }
            return getSQLException(ErrorCode.EXCEPTION_IN_FUNCTION, null, e);
        } else if (e instanceof IOException) {
            return getSQLException(ErrorCode.IO_EXCEPTION_1, new String[] { e.toString() }, e);
        }
        return getSQLException(ErrorCode.GENERAL_ERROR_1, new String[]{e.toString()}, e);
    }
    
    public static SQLException convertIOException(IOException e, String message) {
        if (message == null) {
            return getSQLException(ErrorCode.IO_EXCEPTION_1, new String[]{e.toString()}, e);
        } else {
            return getSQLException(ErrorCode.IO_EXCEPTION_2, new String[]{e.toString(), message}, e);
        }
    }

    public static Error getInternalError() {
        return getInternalError("unexpected code path");
    }

    public static InternalException convertToInternal(Exception e) {
        return new InternalException(e);
    }

    public static IOException convertToIOException(Throwable e) {
        if (e instanceof JdbcSQLException) {
            JdbcSQLException e2 = (JdbcSQLException) e;
            if (e2.getOriginalCause() != null) {
                e = e2.getOriginalCause();
            }
        }
        IOException io = new IOException(e.toString());
        io.fillInStackTrace();
        return io;
    }

}
