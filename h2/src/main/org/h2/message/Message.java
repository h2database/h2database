/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.message;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Properties;

import org.h2.jdbc.JdbcSQLException;
import org.h2.util.Resources;
import org.h2.util.StringUtils;

/**
 * @author Thomas
 */
public class Message {

    private static final Properties MESSAGES = new Properties();

    static {
        // TODO multilanguage messages
        // String language = Locale.getDefault().getLanguage();
        try {
            byte[] messages = Resources.get("/org/h2/res/messages.properties");
            if(messages != null) {
                MESSAGES.load(new ByteArrayInputStream(messages));
            }
        } catch (IOException e) {
            TraceSystem.traceThrowable(e);
        }
    }

    /**
     * Gets the SQL Exception object for a specific SQLState. Supported
     * sqlstates are:
     *
     * @param sqlstate -
     *            the SQL State
     * @param param -
     *            the parameter of the message
     * @return the SQLException object
     */
    public static JdbcSQLException getSQLException(int sqlstate, String p1) {
        return getSQLException(sqlstate, new String[] { p1 }, null);
    }

    public static String translate(String key, String[] param) {
        String message = MESSAGES.getProperty(key);
        if(message == null) {
            message = "(Message " +key+ " not found)";
        }
        if (param != null) {
            Object[] o = param;
            message = MessageFormat.format(message, o);
        }
        return message;
    }

    public static JdbcSQLException getSQLException(int errorCode, String[] param, Throwable cause) {
        String sqlstate = getState(errorCode);
        String message = translate(sqlstate, param);
        return new JdbcSQLException(message, sqlstate, errorCode, cause);
    }

    public static SQLException getSyntaxError(String sql, int index) {
        sql = StringUtils.addAsterix(sql, index);
        return Message.getSQLException(Message.SYNTAX_ERROR_1, sql);
    }

    public static SQLException getSyntaxError(String sql, int index, String expected) {
        sql = StringUtils.addAsterix(sql, index);
        return Message.getSQLException(Message.SYNTAX_ERROR_2, new String[]{sql, expected}, null);
    }

    /**
     * Gets the SQL Exception object for a specific SQLState.
     *
     * @param sqlstate -
     *            the SQL State
     * @return the SQLException object
     */
    public static JdbcSQLException getSQLException(int sqlstate) {
        return getSQLException(sqlstate, null);
    }

    public static JdbcSQLException getUnsupportedException() {
        return getSQLException(Message.FEATURE_NOT_SUPPORTED);
    }

    public static JdbcSQLException getInvalidValueException(String value, String param) {
        return getSQLException(Message.INVALID_VALUE_2, new String[]{value, param}, null);
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

    private static String getState(int errorCode) {
        switch(errorCode) {
        // 02: no data
        case NO_DATA_AVAILABLE: return "02000";

        // 07: dynamic SQL error
        case INVALID_PARAMETER_COUNT_1: return "07001";

        // 08: connection exception
        case ERROR_OPENING_DATABASE: return "08000";
        case WRONG_USER_OR_PASSWORD: return "08004";

        // 21: cardinality violation
        case COLUMN_COUNT_DOES_NOT_MATCH: return "21S02";

        // 22: data exception
        case NUMERIC_VALUE_OUT_OF_RANGE: return "22003";
        case DIVISION_BY_ZERO_1: return "22012";
        case LIKE_ESCAPE_ERROR_1: return "22025";

        // 23: integrity constraint violation
        case CHECK_CONSTRAINT_VIOLATED_1: return "23000";
        case DUPLICATE_KEY_1: return "23001"; // integrity constraint violation

        // 3B: savepoint exception

        // 42: syntax error or access rule violation
        case SYNTAX_ERROR_1: return "42000";
        case SYNTAX_ERROR_2: return "42001";
        case TABLE_OR_VIEW_ALREADY_EXISTS_1: return "42S01";
        case TABLE_OR_VIEW_NOT_FOUND_1: return "42S02";
        case INDEX_ALREADY_EXISTS_1: return "42S11";
        case INDEX_NOT_FOUND_1: return "42S12";
        case DUPLICATE_COLUMN_NAME_1: return "42S21";
        case COLUMN_NOT_FOUND_1: return "42S22";
        case SETTING_NOT_FOUND_1: return "42S32";

        // 0A: feature not supported

        // HZ: remote database access

        //
        case GENERAL_ERROR_1: return "HY000";
        case UNKNOWN_DATA_TYPE_1: return "HY004";

        case FEATURE_NOT_SUPPORTED: return "HYC00";
        case LOCK_TIMEOUT_1: return "HYT00";

        }
        return "" + errorCode;
    }

    // 02: no data
    public static final int NO_DATA_AVAILABLE = 2000;

    // 07: dynamic SQL error
    public static final int INVALID_PARAMETER_COUNT_1 = 7001;

    // 08: connection exception
    public static final int ERROR_OPENING_DATABASE = 8000;
    public static final int WRONG_USER_OR_PASSWORD = 8004;

    // 21: cardinality violation
    public static final int COLUMN_COUNT_DOES_NOT_MATCH = 21002;

    // 22: data exception
    public static final int NUMERIC_VALUE_OUT_OF_RANGE = 22003;
    public static final int DIVISION_BY_ZERO_1 = 22012;
    public static final int LIKE_ESCAPE_ERROR_1 = 22025;

    // 23: integrity constraint violation
    public static final int CHECK_CONSTRAINT_VIOLATED_1 = 23000;
    public static final int DUPLICATE_KEY_1 = 23001; // integrity constraint violation

    // 3B: savepoint exception

    // 42: syntax error or access rule violation
    public static final int SYNTAX_ERROR_1 = 42000;
    public static final int SYNTAX_ERROR_2 = 42001;
    public static final int TABLE_OR_VIEW_ALREADY_EXISTS_1 = 42101;
    public static final int TABLE_OR_VIEW_NOT_FOUND_1 = 42102;
    public static final int INDEX_ALREADY_EXISTS_1 = 42111;
    public static final int INDEX_NOT_FOUND_1 = 42112;
    public static final int DUPLICATE_COLUMN_NAME_1 = 42121;
    public static final int COLUMN_NOT_FOUND_1 = 42122;
    public static final int SETTING_NOT_FOUND_1 = 42132;

    // 0A: feature not supported

    // HZ: remote database access

    //
    public static final int GENERAL_ERROR_1 = 50000;
    public static final int UNKNOWN_DATA_TYPE_1 = 50004;

    public static final int FEATURE_NOT_SUPPORTED = 50100;
    public static final int LOCK_TIMEOUT_1 = 50200;

    public static final int FUNCTION_MUST_RETURN_RESULT_SET_1 = 90000;
    public static final int METHOD_NOT_ALLOWED_FOR_QUERY = 90001;
    public static final int METHOD_ONLY_ALLOWED_FOR_QUERY = 90002;
    public static final int HEX_STRING_ODD_1 = 90003;
    public static final int HEX_STRING_WRONG_1 = 90004;
    public static final int VALUE_TOO_LONG_1 = 90005;
    public static final int NULL_NOT_ALLOWED = 90006;
    public static final int OBJECT_CLOSED = 90007;
    public static final int INVALID_VALUE_2 = 90008;
    public static final int DATE_CONSTANT_1 = 90009;
    public static final int TIME_CONSTANT_1 = 90010;
    public static final int TIMESTAMP_CONSTANT_1 = 90011;
    public static final int PARAMETER_NOT_SET_1 = 90012;
    public static final int DATABASE_NOT_FOUND_1 = 90013;
    public static final int PARSE_ERROR_1 = 90014;
    public static final int SUM_OR_AVG_ON_WRONG_DATATYPE_1 = 90015;
    public static final int MUST_GROUP_BY_COLUMN_1 = 90016;
    public static final int SECOND_PRIMARY_KEY = 90017;
    public static final int TRACE_CONNECTION_NOT_CLOSED = 90018;
    public static final int CANT_DROP_CURRENT_USER = 90019;
    public static final int DATABASE_ALREADY_OPEN_1 = 90020;
    public static final int DATA_CONVERSION_ERROR_1 = 90021;
    public static final int FUNCTION_NOT_FOUND_1 = 90022;
    public static final int COLUMN_MUST_NOT_BE_NULLABLE_1 = 90023;
    public static final int FILE_RENAME_FAILED_2 = 90024;
    public static final int FILE_DELETE_FAILED_1 = 90025;
    public static final int SERIALIZATION_FAILED = 90026;
    public static final int DESERIALIZATION_FAILED = 90027;
    public static final int IO_EXCEPTION_1 = 90028;
    public static final int NOT_ON_UPDATABLE_ROW = 90029;
    public static final int FILE_CORRUPTED_1 = 90030;
    public static final int CONNECTION_NOT_CLOSED = 90031;
    public static final int USER_NOT_FOUND_1 = 90032;
    public static final int USER_ALREADY_EXISTS_1 = 90033;
    public static final int LOG_FILE_ERROR_1 = 90034;
    public static final int SEQUENCE_ALREADY_EXISTS_1 = 90035;
    public static final int SEQUENCE_NOT_FOUND_1 = 90036;
    public static final int VIEW_NOT_FOUND_1 = 90037;
    public static final int VIEW_ALREADY_EXISTS_1 = 90038;
    public static final int VALUE_TOO_LARGE_FOR_PRECISION_1 = 90039;
    public static final int ADMIN_RIGHTS_REQUIRED = 90040;
    public static final int TRIGGER_ALREADY_EXISTS_1 = 90041;
    public static final int TRIGGER_NOT_FOUND_1 = 90042;
    public static final int ERROR_CREATING_TRIGGER_OBJECT_2 = 90043;
    public static final int ERROR_EXECUTING_TRIGGER_2 = 90044;
    public static final int CONSTRAINT_ALREADY_EXISTS_1 = 90045;
    public static final int URL_FORMAT_ERROR_2 = 90046;
    public static final int DRIVER_VERSION_ERROR_2 = 90047;
    public static final int FILE_VERSION_ERROR_1 = 90048;
    public static final int FILE_ENCRYPTION_ERROR_1 = 90049;
    public static final int WRONG_PASSWORD_FORMAT = 90050;
    public static final int STATEMENT_WAS_CANCELLED = 90051;
    public static final int SUBQUERY_IS_NOT_SINGLE_COLUMN = 90052;
    public static final int SCALAR_SUBQUERY_CONTAINS_MORE_THAN_ONE_ROW = 90053;
    public static final int INVALID_USE_OF_AGGREGATE_FUNCTION_1 = 90054;
    public static final int UNSUPPORTED_CIPHER = 90055;
    public static final int NO_DEFAULT_SET_1 = 90056;
    public static final int CONSTRAINT_NOT_FOUND_1 = 90057;
    public static final int DUPLICATE_TABLE_ALIAS = 90058;
    public static final int AMBIGUOUS_COLUMN_NAME_1 = 90059;
    public static final int UNSUPPORTED_LOCK_METHOD_1 = 90060;
    public static final int EXCEPTION_OPENING_PORT_1 = 90061;
    public static final int FILE_CREATION_FAILED_1 = 90062;
    public static final int SAVEPOINT_IS_INVALID_1 = 90063;
    public static final int SAVEPOINT_IS_UNNAMED = 90064;
    public static final int SAVEPOINT_IS_NAMED = 90065;
    public static final int DUPLICATE_PROPERTY_1 = 90066;
    public static final int CONNECTION_BROKEN = 90067;
    public static final int ORDER_BY_NOT_IN_RESULT = 90068;
    public static final int ROLE_ALREADY_EXISTS_1 = 90069;
    public static final int ROLE_NOT_FOUND_1 = 90070;
    public static final int USER_OR_ROLE_NOT_FOUND_1 = 90071;
    public static final int ROLES_AND_RIGHT_CANNOT_BE_MIXED = 90072;
    public static final int RIGHT_NOT_FOUND = 90073;
    public static final int ROLE_ALREADY_GRANTED_1 = 90074;
    public static final int COLUMN_IS_PART_OF_INDEX_1 = 90075;
    public static final int FUNCTION_ALIAS_ALREADY_EXISTS_1 = 90076;
    public static final int FUNCTION_ALIAS_NOT_FOUND_1 = 90077;
    public static final int SCHEMA_ALREADY_EXISTS_1 = 90078;
    public static final int SCHEMA_NOT_FOUND_1 = 90079;
    public static final int SCHEMA_NAME_MUST_MATCH = 90080;
    public static final int COLUMN_CONTAINS_NULL_VALUES_1 = 90081;
    public static final int SEQUENCE_BELONGS_TO_A_TABLE_1 = 90082;
    public static final int COLUMN_MAY_BE_REFERENCED_1 = 90083;
    public static final int CANT_DROP_LAST_COLUMN = 90084;
    public static final int INDEX_BELONGS_TO_CONSTRAINT_1 = 90085;
    public static final int CLASS_NOT_FOUND_1 = 90086;
    public static final int METHOD_NOT_FOUND_1 = 90087;
    public static final int UNKNOWN_MODE_1 = 90088;
    public static final int COLLATION_CHANGE_WITH_DATA_TABLE_1 = 90089;
    public static final int SCHEMA_CAN_NOT_BE_DROPPED_1 = 90090;
    public static final int ROLE_CAN_NOT_BE_DROPPED_1 = 90091;
    public static final int UNSUPPORTED_JAVA_VERSION = 90092;
    public static final int CLUSTER_ERROR_DATABASE_RUNS_ALONE = 90093;
    public static final int CLUSTER_ERROR_DATABASE_RUNS_CLUSTERED_1 = 90094;
    public static final int STRING_FORMAT_ERROR_1 = 90095;
    public static final int NOT_ENOUGH_RIGHTS_FOR_1 = 90096;
    public static final int DATABASE_IS_READ_ONLY = 90097;
    public static final int SIMULATED_POWER_OFF = 90098;
    public static final int ERROR_SETTING_DATABASE_EVENT_LISTENER = 90099;
    public static final int NO_DISK_SPACE_AVAILABLE = 90100;
    public static final int WRONG_XID_FORMAT_1 = 90101;
    public static final int UNSUPPORTED_COMPRESSION_OPTIONS_1 = 90102;
    public static final int UNSUPPORTED_COMPRESSION_ALGORITHM_1 = 90103;
    public static final int COMPRESSION_ERROR = 90104;
    private static final int EXCEPTION_IN_FUNCTION = 90105;
    public static final int CANT_TRUNCATE_1 = 90106;
    public static final int CANT_DROP_2 = 90107;
    public static final int STACK_OVERFLOW = 90108;
    public static final int VIEW_IS_INVALID_1 = 90109;
    public static final int OVERFLOW_FOR_TYPE_1 = 90110;
    public static final int ERROR_ACCESSING_LINKED_TABLE_1 = 90111;
    public static final int ROW_NOT_FOUND_WHEN_DELETING_1 = 90112;
    public static final int UNSUPPORTED_SETTING_1 = 90113;
    public static final int CONSTANT_ALREADY_EXISTS_1 = 90114;
    public static final int CONSTANT_NOT_FOUND_1 = 90115;
    public static final int LITERALS_ARE_NOT_ALLOWED = 90116;
    public static final int REMOTE_CONNECTION_NOT_ALLOWED = 90117;
    public static final int CANT_DROP_TABLE_1  = 90118;
    public static final int USER_DATA_TYPE_ALREADY_EXISTS_1 = 90119;
    public static final int USER_DATA_TYPE_NOT_FOUND_1 = 90120;
    public static final int DATABASE_CALLED_AT_SHUTDOWN = 90121;
    public static final int OPERATION_NOT_SUPPORTED_WITH_VIEWS_2 = 90122;
    public static final int CANT_MIX_INDEXED_AND_UNINDEXED_PARAMS = 90123;
    public static final int FILE_NOT_FOUND_1 = 90124;

    public static SQLException addSQL(SQLException e, String sql) {
        if(e.getMessage().indexOf("SQL")>=0) {
            return e;
        }
        if(e instanceof JdbcSQLException) {
            JdbcSQLException j = (JdbcSQLException) e;
            return new JdbcSQLException(j.getOriginalMessage()+"; SQL statement: "+sql, j.getSQLState(), j.getErrorCode(), j);
        } else {
            return new JdbcSQLException(e.getMessage()+"; SQL statement: "+sql, e.getSQLState(), e.getErrorCode(), e);
        }
    }

    public static SQLException convert(Throwable e) {
        if(e instanceof InternalException) {
            e = ((InternalException)e).getOriginalCause();
        }
        if(e instanceof SQLException) {
            return (SQLException)e;
        } else if(e instanceof InvocationTargetException) {
            InvocationTargetException ite = (InvocationTargetException)e;
            Throwable t = ite.getTargetException();
            if(t instanceof SQLException) {
                return (SQLException)t;
            }
            return Message.getSQLException(Message.EXCEPTION_IN_FUNCTION, null, e);
        } else if(e instanceof IOException) {
            return Message.getSQLException(Message.IO_EXCEPTION_1, new String[]{e.toString()}, e);
        }
        return Message.getSQLException(Message.GENERAL_ERROR_1, new String[]{e.toString()}, e);
    }

    public static Error getInternalError() {
        return getInternalError("unexpected code path");
    }

    public static InternalException convertToInternal(Exception e) {
        return new InternalException(e);
    }

    public static IOException convertToIOException(Throwable e) {
        if(e instanceof JdbcSQLException) {
            JdbcSQLException e2 = (JdbcSQLException)e;
            if(e2.getOriginalCause() != null) {
                e = e2.getOriginalCause();
            }
        }
        IOException io = new IOException(e.toString());
        io.fillInStackTrace();
        return io;
    }

}
