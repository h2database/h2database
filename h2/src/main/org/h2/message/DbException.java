/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.message;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.Properties;

import org.h2.api.ErrorCode;
import org.h2.engine.Constants;
import org.h2.jdbc.JdbcException;
import org.h2.jdbc.JdbcSQLDataException;
import org.h2.jdbc.JdbcSQLException;
import org.h2.jdbc.JdbcSQLFeatureNotSupportedException;
import org.h2.jdbc.JdbcSQLIntegrityConstraintViolationException;
import org.h2.jdbc.JdbcSQLInvalidAuthorizationSpecException;
import org.h2.jdbc.JdbcSQLNonTransientConnectionException;
import org.h2.jdbc.JdbcSQLNonTransientException;
import org.h2.jdbc.JdbcSQLSyntaxErrorException;
import org.h2.jdbc.JdbcSQLTimeoutException;
import org.h2.jdbc.JdbcSQLTransactionRollbackException;
import org.h2.jdbc.JdbcSQLTransientException;
import org.h2.util.SortedProperties;
import org.h2.util.StringUtils;
import org.h2.util.Utils;

/**
 * This exception wraps a checked exception.
 * It is used in methods where checked exceptions are not supported,
 * for example in a Comparator.
 */
public class DbException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * If the SQL statement contains this text, then it is never added to the
     * SQL exception. Hiding the SQL statement may be important if it contains a
     * passwords, such as a CREATE LINKED TABLE statement.
     */
    public static final String HIDE_SQL = "--hide--";

    private static final Properties MESSAGES = new Properties();

    public static final SQLException SQL_OOME =
            new SQLException("OutOfMemoryError", "HY000", ErrorCode.OUT_OF_MEMORY, new OutOfMemoryError());
    private static final DbException OOME = new DbException(SQL_OOME);

    private Object source;

    static {
        try {
            byte[] messages = Utils.getResource(
                    "/org/h2/res/_messages_en.prop");
            if (messages != null) {
                MESSAGES.load(new ByteArrayInputStream(messages));
            }
            String language = Locale.getDefault().getLanguage();
            if (!"en".equals(language)) {
                byte[] translations = Utils.getResource(
                        "/org/h2/res/_messages_" + language + ".prop");
                // message: translated message + english
                // (otherwise certain applications don't work)
                if (translations != null) {
                    Properties p = SortedProperties.fromLines(
                            new String(translations, StandardCharsets.UTF_8));
                    for (Entry<Object, Object> e : p.entrySet()) {
                        String key = (String) e.getKey();
                        String translation = (String) e.getValue();
                        if (translation != null && !translation.startsWith("#")) {
                            String original = MESSAGES.getProperty(key);
                            String message = translation + "\n" + original;
                            MESSAGES.put(key, message);
                        }
                    }
                }
            }
        } catch (OutOfMemoryError e) {
            DbException.traceThrowable(e);
        } catch (IOException e) {
            DbException.traceThrowable(e);
        }
    }

    private DbException(SQLException e) {
        super(e.getMessage(), e);
    }

    private static String translate(String key, String... params) {
        String message = null;
        if (MESSAGES != null) {
            // Tomcat sets final static fields to null sometimes
            message = MESSAGES.getProperty(key);
        }
        if (message == null) {
            message = "(Message " + key + " not found)";
        }
        if (params != null) {
            for (int i = 0; i < params.length; i++) {
                String s = params[i];
                if (s != null && s.length() > 0) {
                    params[i] = StringUtils.quoteIdentifier(s);
                }
            }
            message = MessageFormat.format(message, (Object[]) params);
        }
        return message;
    }

    /**
     * Get the SQLException object.
     *
     * @return the exception
     */
    public SQLException getSQLException() {
        return (SQLException) getCause();
    }

    /**
     * Get the error code.
     *
     * @return the error code
     */
    public int getErrorCode() {
        return getSQLException().getErrorCode();
    }

    /**
     * Set the SQL statement of the given exception.
     * This method may create a new object.
     *
     * @param sql the SQL statement
     * @return the exception
     */
    public DbException addSQL(String sql) {
        SQLException e = getSQLException();
        if (e instanceof JdbcException) {
            JdbcException j = (JdbcException) e;
            if (j.getSQL() == null) {
                j.setSQL(filterSQL(sql));
            }
            return this;
        }
        e = getJdbcSQLException(e.getMessage(), sql, e.getSQLState(), e.getErrorCode(), e, null);
        return new DbException(e);
    }

    /**
     * Create a database exception for a specific error code.
     *
     * @param errorCode the error code
     * @return the exception
     */
    public static DbException get(int errorCode) {
        return get(errorCode, (String) null);
    }

    /**
     * Create a database exception for a specific error code.
     *
     * @param errorCode the error code
     * @param p1 the first parameter of the message
     * @return the exception
     */
    public static DbException get(int errorCode, String p1) {
        return get(errorCode, new String[] { p1 });
    }

    /**
     * Create a database exception for a specific error code.
     *
     * @param errorCode the error code
     * @param cause the cause of the exception
     * @param params the list of parameters of the message
     * @return the exception
     */
    public static DbException get(int errorCode, Throwable cause,
            String... params) {
        return new DbException(getJdbcSQLException(errorCode, cause, params));
    }

    /**
     * Create a database exception for a specific error code.
     *
     * @param errorCode the error code
     * @param params the list of parameters of the message
     * @return the exception
     */
    public static DbException get(int errorCode, String... params) {
        return new DbException(getJdbcSQLException(errorCode, null, params));
    }

    /**
     * Create a database exception for an arbitrary SQLState.
     *
     * @param sqlstate the state to use
     * @param message the message to use
     * @return the exception
     */
    public static DbException fromUser(String sqlstate, String message) {
        // do not translate as sqlstate is arbitrary : avoid "message not found"
        return new DbException(getJdbcSQLException(message, null, sqlstate, 0, null, null));
    }

    /**
     * Create a syntax error exception.
     *
     * @param sql the SQL statement
     * @param index the position of the error in the SQL statement
     * @return the exception
     */
    public static DbException getSyntaxError(String sql, int index) {
        sql = StringUtils.addAsterisk(sql, index);
        return get(ErrorCode.SYNTAX_ERROR_1, sql);
    }

    /**
     * Create a syntax error exception.
     *
     * @param sql the SQL statement
     * @param index the position of the error in the SQL statement
     * @param message the message
     * @return the exception
     */
    public static DbException getSyntaxError(String sql, int index,
            String message) {
        sql = StringUtils.addAsterisk(sql, index);
        return new DbException(getJdbcSQLException(ErrorCode.SYNTAX_ERROR_2,
                null, sql, message));
    }

    /**
     * Create a syntax error exception for a specific error code.
     *
     * @param errorCode the error code
     * @param sql the SQL statement
     * @param index the position of the error in the SQL statement
     * @param params the list of parameters of the message
     * @return the exception
     */
    public static DbException getSyntaxError(int errorCode, String sql, int index, String... params) {
        sql = StringUtils.addAsterisk(sql, index);
        String sqlstate = ErrorCode.getState(errorCode);
        String message = translate(sqlstate, params);
        return new DbException(getJdbcSQLException(message, sql, sqlstate, errorCode, null, null));
    }

    /**
     * Gets a SQL exception meaning this feature is not supported.
     *
     * @param message what exactly is not supported
     * @return the exception
     */
    public static DbException getUnsupportedException(String message) {
        return get(ErrorCode.FEATURE_NOT_SUPPORTED_1, message);
    }

    /**
     * Gets a SQL exception meaning this value is invalid.
     *
     * @param param the name of the parameter
     * @param value the value passed
     * @return the IllegalArgumentException object
     */
    public static DbException getInvalidValueException(String param,
            Object value) {
        return get(ErrorCode.INVALID_VALUE_2,
                value == null ? "null" : value.toString(), param);
    }

    /**
     * Throw an internal error. This method seems to return an exception object,
     * so that it can be used instead of 'return', but in fact it always throws
     * the exception.
     *
     * @param s the message
     * @return the RuntimeException object
     * @throws RuntimeException the exception
     */
    public static RuntimeException throwInternalError(String s) {
        RuntimeException e = new RuntimeException(s);
        DbException.traceThrowable(e);
        throw e;
    }

    /**
     * Throw an internal error. This method seems to return an exception object,
     * so that it can be used instead of 'return', but in fact it always throws
     * the exception.
     *
     * @return the RuntimeException object
     */
    public static RuntimeException throwInternalError() {
        return throwInternalError("Unexpected code path");
    }

    /**
     * Convert an exception to a SQL exception using the default mapping.
     *
     * @param e the root cause
     * @return the SQL exception object
     */
    public static SQLException toSQLException(Throwable e) {
        if (e instanceof SQLException) {
            return (SQLException) e;
        }
        return convert(e).getSQLException();
    }

    /**
     * Convert a throwable to an SQL exception using the default mapping. All
     * errors except the following are re-thrown: StackOverflowError,
     * LinkageError.
     *
     * @param e the root cause
     * @return the exception object
     */
    public static DbException convert(Throwable e) {
        try {
            if (e instanceof DbException) {
                return (DbException) e;
            } else if (e instanceof SQLException) {
                return new DbException((SQLException) e);
            } else if (e instanceof InvocationTargetException) {
                return convertInvocation((InvocationTargetException) e, null);
            } else if (e instanceof IOException) {
                return get(ErrorCode.IO_EXCEPTION_1, e, e.toString());
            } else if (e instanceof OutOfMemoryError) {
                return get(ErrorCode.OUT_OF_MEMORY, e);
            } else if (e instanceof StackOverflowError || e instanceof LinkageError) {
                return get(ErrorCode.GENERAL_ERROR_1, e, e.toString());
            } else if (e instanceof Error) {
                throw (Error) e;
            }
            return get(ErrorCode.GENERAL_ERROR_1, e, e.toString());
        } catch (Throwable ex) {
            try {
                DbException dbException = new DbException(
                        new SQLException("GeneralError", "HY000", ErrorCode.GENERAL_ERROR_1, e));
                dbException.addSuppressed(ex);
                return dbException;
            } catch (OutOfMemoryError ignore) {
                return OOME;
            }
        }
    }

    /**
     * Convert an InvocationTarget exception to a database exception.
     *
     * @param te the root cause
     * @param message the added message or null
     * @return the database exception object
     */
    public static DbException convertInvocation(InvocationTargetException te,
            String message) {
        Throwable t = te.getTargetException();
        if (t instanceof SQLException || t instanceof DbException) {
            return convert(t);
        }
        message = message == null ? t.getMessage() : message + ": " + t.getMessage();
        return get(ErrorCode.EXCEPTION_IN_FUNCTION_1, t, message);
    }

    /**
     * Convert an IO exception to a database exception.
     *
     * @param e the root cause
     * @param message the message or null
     * @return the database exception object
     */
    public static DbException convertIOException(IOException e, String message) {
        if (message == null) {
            Throwable t = e.getCause();
            if (t instanceof DbException) {
                return (DbException) t;
            }
            return get(ErrorCode.IO_EXCEPTION_1, e, e.toString());
        }
        return get(ErrorCode.IO_EXCEPTION_2, e, e.toString(), message);
    }

    /**
     * Gets the SQL exception object for a specific error code.
     *
     * @param errorCode the error code
     * @return the SQLException object
     */
    public static SQLException getJdbcSQLException(int errorCode)
    {
        return getJdbcSQLException(errorCode, (Throwable)null);
    }

    /**
     * Gets the SQL exception object for a specific error code.
     *
     * @param errorCode the error code
     * @param p1 the first parameter of the message
     * @return the SQLException object
     */
    public static SQLException getJdbcSQLException(int errorCode, String p1)
    {
        return getJdbcSQLException(errorCode, null, p1);
    }

    /**
     * Gets the SQL exception object for a specific error code.
     *
     * @param errorCode the error code
     * @param cause the cause of the exception
     * @param params the list of parameters of the message
     * @return the SQLException object
     */
    public static SQLException getJdbcSQLException(int errorCode, Throwable cause, String... params) {
        String sqlstate = ErrorCode.getState(errorCode);
        String message = translate(sqlstate, params);
        return getJdbcSQLException(message, null, sqlstate, errorCode, cause, null);
    }

    /**
     * Creates a SQLException.
     *
     * @param message the reason
     * @param sql the SQL statement
     * @param state the SQL state
     * @param errorCode the error code
     * @param cause the exception that was the reason for this exception
     * @param stackTrace the stack trace
     */
    public static SQLException getJdbcSQLException(String message, String sql, String state, int errorCode,
            Throwable cause, String stackTrace) {
        sql = filterSQL(sql);
        // Use SQLState class value to detect type
        switch (errorCode / 1_000) {
        case 2:
            return new JdbcSQLNonTransientException(message, sql, state, errorCode, cause, stackTrace);
        case 7:
        case 21:
        case 42:
            return new JdbcSQLSyntaxErrorException(message, sql, state, errorCode, cause, stackTrace);
        case 8:
            return new JdbcSQLNonTransientConnectionException(message, sql, state, errorCode, cause, stackTrace);
        case 22:
            return new JdbcSQLDataException(message, sql, state, errorCode, cause, stackTrace);
        case 23:
            return new JdbcSQLIntegrityConstraintViolationException(message, sql, state, errorCode, cause, stackTrace);
        case 28:
            return new JdbcSQLInvalidAuthorizationSpecException(message, sql, state, errorCode, cause, stackTrace);
        case 40:
            return new JdbcSQLTransactionRollbackException(message, sql, state, errorCode, cause, stackTrace);
        }
        // Check error code
        switch (errorCode){
        case ErrorCode.GENERAL_ERROR_1:
        case ErrorCode.UNKNOWN_DATA_TYPE_1:
        case ErrorCode.METHOD_NOT_ALLOWED_FOR_QUERY:
        case ErrorCode.METHOD_ONLY_ALLOWED_FOR_QUERY:
        case ErrorCode.SEQUENCE_EXHAUSTED:
        case ErrorCode.OBJECT_CLOSED:
        case ErrorCode.CANNOT_DROP_CURRENT_USER:
        case ErrorCode.UNSUPPORTED_SETTING_COMBINATION:
        case ErrorCode.FILE_RENAME_FAILED_2:
        case ErrorCode.FILE_DELETE_FAILED_1:
        case ErrorCode.IO_EXCEPTION_1:
        case ErrorCode.NOT_ON_UPDATABLE_ROW:
        case ErrorCode.IO_EXCEPTION_2:
        case ErrorCode.TRACE_FILE_ERROR_2:
        case ErrorCode.ADMIN_RIGHTS_REQUIRED:
        case ErrorCode.ERROR_EXECUTING_TRIGGER_3:
        case ErrorCode.COMMIT_ROLLBACK_NOT_ALLOWED:
        case ErrorCode.FILE_CREATION_FAILED_1:
        case ErrorCode.SAVEPOINT_IS_INVALID_1:
        case ErrorCode.SAVEPOINT_IS_UNNAMED:
        case ErrorCode.SAVEPOINT_IS_NAMED:
        case ErrorCode.NOT_ENOUGH_RIGHTS_FOR_1:
        case ErrorCode.DATABASE_IS_READ_ONLY:
        case ErrorCode.WRONG_XID_FORMAT_1:
        case ErrorCode.UNSUPPORTED_COMPRESSION_OPTIONS_1:
        case ErrorCode.UNSUPPORTED_COMPRESSION_ALGORITHM_1:
        case ErrorCode.COMPRESSION_ERROR:
        case ErrorCode.EXCEPTION_IN_FUNCTION_1:
        case ErrorCode.ERROR_ACCESSING_LINKED_TABLE_2:
        case ErrorCode.FILE_NOT_FOUND_1:
        case ErrorCode.INVALID_CLASS_2:
        case ErrorCode.DATABASE_IS_NOT_PERSISTENT:
        case ErrorCode.RESULT_SET_NOT_UPDATABLE:
        case ErrorCode.RESULT_SET_NOT_SCROLLABLE:
        case ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT:
        case ErrorCode.ACCESS_DENIED_TO_CLASS_1:
        case ErrorCode.RESULT_SET_READONLY:
            return new JdbcSQLNonTransientException(message, sql, state, errorCode, cause, stackTrace);
        case ErrorCode.FEATURE_NOT_SUPPORTED_1:
            return new JdbcSQLFeatureNotSupportedException(message, sql, state, errorCode, cause, stackTrace);
        case ErrorCode.LOCK_TIMEOUT_1:
        case ErrorCode.STATEMENT_WAS_CANCELED:
        case ErrorCode.LOB_CLOSED_ON_TIMEOUT_1:
            return new JdbcSQLTimeoutException(message, sql, state, errorCode, cause, stackTrace);
        case ErrorCode.FUNCTION_MUST_RETURN_RESULT_SET_1:
        case ErrorCode.TRIGGER_SELECT_AND_ROW_BASED_NOT_SUPPORTED:
        case ErrorCode.SUM_OR_AVG_ON_WRONG_DATATYPE_1:
        case ErrorCode.MUST_GROUP_BY_COLUMN_1:
        case ErrorCode.SECOND_PRIMARY_KEY:
        case ErrorCode.FUNCTION_NOT_FOUND_1:
        case ErrorCode.COLUMN_MUST_NOT_BE_NULLABLE_1:
        case ErrorCode.USER_NOT_FOUND_1:
        case ErrorCode.USER_ALREADY_EXISTS_1:
        case ErrorCode.SEQUENCE_ALREADY_EXISTS_1:
        case ErrorCode.SEQUENCE_NOT_FOUND_1:
        case ErrorCode.VIEW_NOT_FOUND_1:
        case ErrorCode.VIEW_ALREADY_EXISTS_1:
        case ErrorCode.TRIGGER_ALREADY_EXISTS_1:
        case ErrorCode.TRIGGER_NOT_FOUND_1:
        case ErrorCode.ERROR_CREATING_TRIGGER_OBJECT_3:
        case ErrorCode.CONSTRAINT_ALREADY_EXISTS_1:
        case ErrorCode.INVALID_VALUE_SCALE_PRECISION:
        case ErrorCode.SUBQUERY_IS_NOT_SINGLE_COLUMN:
        case ErrorCode.INVALID_USE_OF_AGGREGATE_FUNCTION_1:
        case ErrorCode.CONSTRAINT_NOT_FOUND_1:
        case ErrorCode.AMBIGUOUS_COLUMN_NAME_1:
        case ErrorCode.ORDER_BY_NOT_IN_RESULT:
        case ErrorCode.ROLE_ALREADY_EXISTS_1:
        case ErrorCode.ROLE_NOT_FOUND_1:
        case ErrorCode.USER_OR_ROLE_NOT_FOUND_1:
        case ErrorCode.ROLES_AND_RIGHT_CANNOT_BE_MIXED:
        case ErrorCode.METHODS_MUST_HAVE_DIFFERENT_PARAMETER_COUNTS_2:
        case ErrorCode.ROLE_ALREADY_GRANTED_1:
        case ErrorCode.COLUMN_IS_PART_OF_INDEX_1:
        case ErrorCode.FUNCTION_ALIAS_ALREADY_EXISTS_1:
        case ErrorCode.FUNCTION_ALIAS_NOT_FOUND_1:
        case ErrorCode.SCHEMA_ALREADY_EXISTS_1:
        case ErrorCode.SCHEMA_NOT_FOUND_1:
        case ErrorCode.SCHEMA_NAME_MUST_MATCH:
        case ErrorCode.COLUMN_CONTAINS_NULL_VALUES_1:
        case ErrorCode.SEQUENCE_BELONGS_TO_A_TABLE_1:
        case ErrorCode.COLUMN_IS_REFERENCED_1:
        case ErrorCode.CANNOT_DROP_LAST_COLUMN:
        case ErrorCode.INDEX_BELONGS_TO_CONSTRAINT_2:
        case ErrorCode.CLASS_NOT_FOUND_1:
        case ErrorCode.METHOD_NOT_FOUND_1:
        case ErrorCode.COLLATION_CHANGE_WITH_DATA_TABLE_1:
        case ErrorCode.SCHEMA_CAN_NOT_BE_DROPPED_1:
        case ErrorCode.ROLE_CAN_NOT_BE_DROPPED_1:
        case ErrorCode.CANNOT_TRUNCATE_1:
        case ErrorCode.CANNOT_DROP_2:
        case ErrorCode.VIEW_IS_INVALID_2:
        case ErrorCode.COMPARING_ARRAY_TO_SCALAR:
        case ErrorCode.CONSTANT_ALREADY_EXISTS_1:
        case ErrorCode.CONSTANT_NOT_FOUND_1:
        case ErrorCode.LITERALS_ARE_NOT_ALLOWED:
        case ErrorCode.CANNOT_DROP_TABLE_1:
        case ErrorCode.USER_DATA_TYPE_ALREADY_EXISTS_1:
        case ErrorCode.USER_DATA_TYPE_NOT_FOUND_1:
        case ErrorCode.WITH_TIES_WITHOUT_ORDER_BY:
        case ErrorCode.CANNOT_MIX_INDEXED_AND_UNINDEXED_PARAMS:
        case ErrorCode.TRANSACTION_NOT_FOUND_1:
        case ErrorCode.AGGREGATE_NOT_FOUND_1:
        case ErrorCode.CAN_ONLY_ASSIGN_TO_VARIABLE_1:
        case ErrorCode.PUBLIC_STATIC_JAVA_METHOD_NOT_FOUND_1:
        case ErrorCode.JAVA_OBJECT_SERIALIZER_CHANGE_WITH_DATA_TABLE:
            return new JdbcSQLSyntaxErrorException(message, sql, state, errorCode, cause, stackTrace);
        case ErrorCode.HEX_STRING_ODD_1:
        case ErrorCode.HEX_STRING_WRONG_1:
        case ErrorCode.INVALID_VALUE_2:
        case ErrorCode.SEQUENCE_ATTRIBUTES_INVALID:
        case ErrorCode.INVALID_TO_CHAR_FORMAT:
        case ErrorCode.PARAMETER_NOT_SET_1:
        case ErrorCode.PARSE_ERROR_1:
        case ErrorCode.INVALID_TO_DATE_FORMAT:
        case ErrorCode.STRING_FORMAT_ERROR_1:
        case ErrorCode.SERIALIZATION_FAILED_1:
        case ErrorCode.DESERIALIZATION_FAILED_1:
        case ErrorCode.SCALAR_SUBQUERY_CONTAINS_MORE_THAN_ONE_ROW:
        case ErrorCode.STEP_SIZE_MUST_NOT_BE_ZERO:
            return new JdbcSQLDataException(message, sql, state, errorCode, cause, stackTrace);
        case ErrorCode.URL_RELATIVE_TO_CWD:
        case ErrorCode.DATABASE_NOT_FOUND_1:
        case ErrorCode.TRACE_CONNECTION_NOT_CLOSED:
        case ErrorCode.DATABASE_ALREADY_OPEN_1:
        case ErrorCode.FILE_CORRUPTED_1:
        case ErrorCode.URL_FORMAT_ERROR_2:
        case ErrorCode.DRIVER_VERSION_ERROR_2:
        case ErrorCode.FILE_VERSION_ERROR_1:
        case ErrorCode.FILE_ENCRYPTION_ERROR_1:
        case ErrorCode.WRONG_PASSWORD_FORMAT:
        case ErrorCode.UNSUPPORTED_CIPHER:
        case ErrorCode.UNSUPPORTED_LOCK_METHOD_1:
        case ErrorCode.EXCEPTION_OPENING_PORT_2:
        case ErrorCode.DUPLICATE_PROPERTY_1:
        case ErrorCode.CONNECTION_BROKEN_1:
        case ErrorCode.UNKNOWN_MODE_1:
        case ErrorCode.CLUSTER_ERROR_DATABASE_RUNS_ALONE:
        case ErrorCode.CLUSTER_ERROR_DATABASE_RUNS_CLUSTERED_1:
        case ErrorCode.DATABASE_IS_CLOSED:
        case ErrorCode.ERROR_SETTING_DATABASE_EVENT_LISTENER_2:
        case ErrorCode.OUT_OF_MEMORY:
        case ErrorCode.UNSUPPORTED_SETTING_1:
        case ErrorCode.REMOTE_CONNECTION_NOT_ALLOWED:
        case ErrorCode.DATABASE_CALLED_AT_SHUTDOWN:
        case ErrorCode.CANNOT_CHANGE_SETTING_WHEN_OPEN_1:
        case ErrorCode.DATABASE_IS_IN_EXCLUSIVE_MODE:
        case ErrorCode.INVALID_DATABASE_NAME_1:
        case ErrorCode.AUTHENTICATOR_NOT_AVAILABLE:
            return new JdbcSQLNonTransientConnectionException(message, sql, state, errorCode, cause, stackTrace);
        case ErrorCode.ROW_NOT_FOUND_WHEN_DELETING_1:
        case ErrorCode.CONCURRENT_UPDATE_1:
        case ErrorCode.ROW_NOT_FOUND_IN_PRIMARY_INDEX:
            return new JdbcSQLTransientException(message, sql, state, errorCode, cause, stackTrace);
        }
        // Default
        return new JdbcSQLException(message, sql, state, errorCode, cause, stackTrace);
    }

    private static String filterSQL(String sql) {
        return sql == null || !sql.contains(HIDE_SQL) ? sql : "-";
    }

    /**
     * Convert an exception to an IO exception.
     *
     * @param e the root cause
     * @return the IO exception
     */
    public static IOException convertToIOException(Throwable e) {
        if (e instanceof IOException) {
            return (IOException) e;
        }
        if (e instanceof JdbcException) {
            if (e.getCause() != null) {
                e = e.getCause();
            }
        }
        return new IOException(e.toString(), e);
    }

    /**
     * Builds message for an exception.
     *
     * @param e exception
     * @return message
     */
    public static String buildMessageForException(JdbcException e) {
        String s = e.getOriginalMessage();
        StringBuilder buff = new StringBuilder(s != null ? s : "- ");
        s = e.getSQL();
        if (s != null) {
            buff.append("; SQL statement:\n").append(s);
        }
        buff.append(" [").append(e.getErrorCode()).append('-').append(Constants.BUILD_ID).append(']');
        return buff.toString();
    }

    /**
     * Prints up to 100 next exceptions for a specified SQL exception.
     *
     * @param e SQL exception
     * @param s print writer
     */
    public static void printNextExceptions(SQLException e, PrintWriter s) {
        // getNextException().printStackTrace(s) would be very very slow
        // if many exceptions are joined
        int i = 0;
        while ((e = e.getNextException()) != null) {
            if (i++ == 100) {
                s.println("(truncated)");
                return;
            }
            s.println(e.toString());
        }
    }

    /**
     * Prints up to 100 next exceptions for a specified SQL exception.
     *
     * @param e SQL exception
     * @param s print stream
     */
    public static void printNextExceptions(SQLException e, PrintStream s) {
        // getNextException().printStackTrace(s) would be very very slow
        // if many exceptions are joined
        int i = 0;
        while ((e = e.getNextException()) != null) {
            if (i++ == 100) {
                s.println("(truncated)");
                return;
            }
            s.println(e.toString());
        }
    }

    public Object getSource() {
        return source;
    }

    public void setSource(Object source) {
        this.source = source;
    }

    /**
     * Write the exception to the driver manager log writer if configured.
     *
     * @param e the exception
     */
    public static void traceThrowable(Throwable e) {
        PrintWriter writer = DriverManager.getLogWriter();
        if (writer != null) {
            e.printStackTrace(writer);
        }
    }

}
