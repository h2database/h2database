/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.constant;

/**
 * This class defines the error codes used for SQL exceptions.
 */
public class ErrorCode {

    // 02: no data
    /**
     * The error with code <code>2000</code> is thrown when
     * the result set is positioned before the first or after the last row, or
     * not on a valid row for the given operation.
     * Example:
     * <pre>
     * ResultSet rs = stat.executeQuery("SELECT * FROM DUAL");
     * rs.getString(1);
     * </pre>
     * Correct:
     * <pre>
     * ResultSet rs = stat.executeQuery("SELECT * FROM DUAL");
     * rs.next();
     * rs.getString(1);
     * </pre>
     */
    public static final int NO_DATA_AVAILABLE = 2000;

    // 07: dynamic SQL error
    /**
     * The error with code <code>7001</code> is thrown when
     * trying to call a function with the wrong number of parameters.
     * Example:
     * <pre>
     * CALL ABS(1, 2)
     * </pre>
     */
    public static final int INVALID_PARAMETER_COUNT_2 = 7001;

    // 08: connection exception
    /**
     * The error with code <code>8000</code> is thrown when
     * there was a problem trying to create a database lock.
     * See the cause for details.
     */
    public static final int ERROR_OPENING_DATABASE = 8000;

    /**
     * The error with code <code>8004</code> is thrown when
     * there is no such user registered in the database, when
     * the user password does not match, or when the database encryption password
     * does not match (if database encryption is used).
     */
    public static final int WRONG_USER_OR_PASSWORD = 8004;

    // 21: cardinality violation
    /**
     * The error with code <code>21002</code> is thrown when
     * the number of columns does not match.
     * Possible reasons are: for an INSERT or MERGE statement, the column count does not match
     * the table or the column list specified. For a SELECT UNION statement, both queries
     * return a different number of columns. For a constraint, the number of referenced and referencing
     * columns does not match.
     * Example:
     * <pre>
     * CREATE TABLE TEST(ID INT, NAME VARCHAR);
     * INSERT INTO TEST VALUES('Hello');
     * </pre>
     */
    public static final int COLUMN_COUNT_DOES_NOT_MATCH = 21002;

    // 22: data exception
    /**
     * The error with code <code>22003</code> is thrown when
     * a value is out of range when converting to another data type.
     * Example:
     * <pre>
     * CALL CAST(1000000 AS TINYINT);
     * </pre>
     */
    public static final int NUMERIC_VALUE_OUT_OF_RANGE = 22003;

    /**
     * The error with code <code>22012</code> is thrown when
     * trying to divide a value by zero.
     * Example:
     * <pre>
     * CALL 1/0;
     * </pre>
     */
    public static final int DIVISION_BY_ZERO_1 = 22012;

    /**
     * The error with code <code>22025</code> is thrown when
     * using an invalid escape character sequence for LIKE or REGEXP.
     * Example:
     * <pre>
     * CALL 'Hello' LIKE '1+' ESCAPE '+';
     * </pre>
     */
    public static final int LIKE_ESCAPE_ERROR_1 = 22025;

    // 23: integrity constraint violation
    /**
     * The error with code <code>23000</code> is thrown when
     * a check constraint is violated.
     * Example:
     * <pre>
     * CREATE TABLE TEST(ID INT CHECK ID>0);
     * INSERT INTO TEST VALUES(0);
     * </pre>
     */
    public static final int CHECK_CONSTRAINT_VIOLATED_1 = 23000;

    /**
     * The error with code <code>23001</code> is thrown when
     * trying to insert a row that would violate a unique index or primary key.
     * Example:
     * <pre>
     * CREATE TABLE TEST(ID INT PRIMARY KEY);
     * INSERT INTO TEST VALUES(1);
     * INSERT INTO TEST VALUES(1);
     * </pre>
     */
    public static final int DUPLICATE_KEY_1 = 23001;

    /**
     * The error with code <code>23002</code> is thrown when
     * trying to insert or update a row that would violate a referential constraint,
     * because the referenced row does not exist.
     * Example:
     * <pre>
     * CREATE TABLE PARENT(ID INT);
     * CREATE TABLE CHILD(P_ID INT REFERENCES PARENT(ID));
     * INSERT INTO CHILD VALUES(1);
     * </pre>
     */
    public static final int REFERENTIAL_INTEGRITY_VIOLATED_PARENT_MISSING_1 = 23002;

    /**
     * The error with code <code>23003</code> is thrown when
     * trying to delete or update a row when this would violate a referential constraint,
     * because there is a child row that would become an orphan.
     * Example:
     * <pre>
     * CREATE TABLE PARENT(ID INT);
     * CREATE TABLE CHILD(P_ID INT REFERENCES PARENT(ID));
     * INSERT INTO PARENT VALUES(1);
     * INSERT INTO CHILD VALUES(1);
     * DELETE FROM PARENT;
     * </pre>
     */
    public static final int REFERENTIAL_INTEGRITY_VIOLATED_CHILD_EXISTS_1 = 23003;

    // 3B: savepoint exception

    // 42: syntax error or access rule violation
    /**
     * The error with code <code>42000</code> is thrown when
     * trying to execute an invalid SQL statement.
     * Example:
     * <pre>
     * CREATE ALIAS REMAINDER FOR "IEEEremainder";
     * </pre>
     */
    public static final int SYNTAX_ERROR_1 = 42000;

    /**
     * The error with code <code>42001</code> is thrown when
     * trying to execute an invalid SQL statement.
     * Example:
     * <pre>
     * CREATE TABLE TEST(ID INT);
     * INSERT INTO TEST(1);
     * </pre>
     */
    public static final int SYNTAX_ERROR_2 = 42001;

    /**
     * The error with code <code>42101</code> is thrown when
     * trying to create a table or view if an object with this name already exists.
     * Example:
     * <pre>
     * CREATE TABLE TEST(ID INT);
     * CREATE TABLE TEST(ID INT PRIMARY KEY);
     * </pre>
     */
    public static final int TABLE_OR_VIEW_ALREADY_EXISTS_1 = 42101;

    /**
     * The error with code <code>42102</code> is thrown when
     * trying to query, modify or drop a table or view that does not exists.
     * Example:
     * <pre>
     * SELECT * FROM ABC;
     * </pre>
     */
    public static final int TABLE_OR_VIEW_NOT_FOUND_1 = 42102;

    /**
     * The error with code <code>42111</code> is thrown when
     * trying to create an index if an index with the same name already exists.
     * Example:
     * <pre>
     * CREATE TABLE TEST(ID INT, NAME VARCHAR);
     * CREATE INDEX IDX_ID ON TEST(ID);
     * CREATE TABLE ADDRESS(ID INT);
     * CREATE INDEX IDX_ID ON ADDRESS(ID);
     * </pre>
     */
    public static final int INDEX_ALREADY_EXISTS_1 = 42111;

    /**
     * The error with code <code>42112</code> is thrown when
     * trying to drop or reference an index that does not exist.
     * Example:
     * <pre>
     * DROP INDEX ABC;
     * </pre>
     */
    public static final int INDEX_NOT_FOUND_1 = 42112;

    /**
     * The error with code <code>42121</code> is thrown when
     * trying to create a table or insert into a table and use the same column name twice.
     * Example:
     * <pre>
     * CREATE TABLE TEST(ID INT, ID INT);
     * </pre>
     */
    public static final int DUPLICATE_COLUMN_NAME_1 = 42121;

    /**
     * The error with code <code>42122</code> is thrown when
     * referencing an non-existing column.
     * Example:
     * <pre>
     * CREATE TABLE TEST(ID INT);
     * SELECT NAME FROM TEST;
     * </pre>
     */
    public static final int COLUMN_NOT_FOUND_1 = 42122;


    public static final int SETTING_NOT_FOUND_1 = 42132;

    // 0A: feature not supported

    // HZ: remote database access

    //
    /**
     * The error with code <code>50000</code> is thrown when
     * something unexpected occurs, for example an internal stack
     * overflow. For details about the problem, see the cause of the
     * exception in the stack trace.
     */
    public static final int GENERAL_ERROR_1 = 50000;

    /**
     * The error with code <code>50004</code> is thrown when
     * creating a table with an unsupported data type, or
     * when the data type is unknown because parameters are used.
     * Example:
     * <pre>
     * CREATE TABLE TEST(ID VERYSMALLINT);
     * </pre>
     */
    public static final int UNKNOWN_DATA_TYPE_1 = 50004;

    /**
     * The error with code <code>50100</code> is thrown when
     * calling an unsupported JDBC method.
     */
    public static final int FEATURE_NOT_SUPPORTED = 50100;

    /**
     * The error with code <code>50200</code> is thrown when
     * another connection locked an object longer than the lock timeout
     * set for this connection, or when a deadlock occured.
     * Example:
     * <pre>
     * CREATE TABLE TEST(ID INT);
     * -- connection 1:
     * SET AUTOCOMMIT FALSE;
     * INSERT INTO TEST VALUES(1);
     * -- connection 2:
     * SET AUTOCOMMIT FALSE;
     * INSERT INTO TEST VALUES(1);
     * </pre>
     */
    public static final int LOCK_TIMEOUT_1 = 50200;

    /**
     * The error with code <code>90000</code> is thrown when
     * a function that does not return a result set was used in the FROM clause.
     * Example:
     * <pre>
     * SELECT * FROM SIN(1);
     * </pre>
     */
    public static final int FUNCTION_MUST_RETURN_RESULT_SET_1 = 90000;

    /**
     * The error with code <code>90001</code> is thrown when
     * Statement.executeUpdate() was called for a SELECT statement.
     * This is not allowed according to the JDBC specs.
     */
    public static final int METHOD_NOT_ALLOWED_FOR_QUERY = 90001;

    /**
     * The error with code <code>90002</code> is thrown when
     * Statement.executeQuery() was called for a statement that does
     * not return a result set (for example, an UPDATE statement).
     * This is not allowed according to the JDBC specs.
     */
    public static final int METHOD_ONLY_ALLOWED_FOR_QUERY = 90002;

    /**
     * The error with code <code>90003</code> is thrown when
     * trying to convert a String to a binary value. Two hex digits
     * per byte are required. Example:
     * <pre>
     * CALL X'00023';
     * Hexadecimal string with odd number of characters: 00023
     * </pre>
     * Correct:
     * <pre>
     * CALL X'000023';
     * </pre>
     */
    public static final int HEX_STRING_ODD_1 = 90003;

    /**
     * The error with code <code>90004</code> is thrown when
     * trying to convert a text to binary, but the expression contains
     * a non-hexadecimal character.
     * Example:
     * <pre>
     * CALL X'ABCDEFGH';
     * CALL CAST('ABCDEFGH' AS BINARY);
     * </pre>
     * Conversion from text to binary is supported, but the text must
     * represent the hexadecimal encoded bytes.
     */
    public static final int HEX_STRING_WRONG_1 = 90004;

    /**
     * The error with code <code>90005</code> is thrown when
     * trying to insert a value that is too long for the column.
     * Example:
     * <pre>
     * CREATE TABLE TEST(ID INT, NAME VARCHAR(2));
     * INSERT INTO TEST VALUES(1, 'Hello');
     * </pre>
     */
    public static final int VALUE_TOO_LONG_2 = 90005;

    /**
     * The error with code <code>90006</code> is thrown when
     * trying to insert NULL into a column that does not allow NULL.
     * Example:
     * <pre>
     * CREATE TABLE TEST(ID INT, NAME VARCHAR NOT NULL);
     * INSERT INTO TEST(ID) VALUES(1);
     * </pre>
     */
    public static final int NULL_NOT_ALLOWED = 90006;

    /**
     * The error with code <code>90007</code> is thrown when
     * trying to call a JDBC method on an object that has been closed.
     */
    public static final int OBJECT_CLOSED = 90007;

    /**
     * The error with code <code>90008</code> is thrown when
     * trying to use a value that is not valid for the given operation.
     * Example:
     * <pre>
     * CREATE SEQUENCE TEST INCREMENT 0;
     * </pre>
     */
    public static final int INVALID_VALUE_2 = 90008;

    /**
     * The error with code <code>90009</code> is thrown when
     * a text can not be converted to a date constant.
     * Example:
     * <pre>
     * CALL DATE '2007-January-01';
     * </pre>
     */
    public static final int DATE_CONSTANT_2 = 90009;

    /**
     * The error with code <code>90010</code> is thrown when
     * a text can not be converted to a time constant.
     * Example:
     * <pre>
     * CALL TIME '14:61:00';
     * </pre>
     */
    public static final int TIME_CONSTANT_2 = 90010;

    /**
     * The error with code <code>90011</code> is thrown when
     * a text can not be converted to a timestamp constant.
     * Example:
     * <pre>
     * CALL TIMESTAMP '2001-02-30 12:00:00';
     * </pre>
     */
    public static final int TIMESTAMP_CONSTANT_2 = 90011;

    /**
     * The error with code <code>90012</code> is thrown when
     * trying to execute a statement with an parameter.
     * Example:
     * <pre>
     * CALL SIN(?);
     * </pre>
     */
    public static final int PARAMETER_NOT_SET_1 = 90012;
    public static final int DATABASE_NOT_FOUND_1 = 90013;
    public static final int PARSE_ERROR_1 = 90014;
    public static final int SUM_OR_AVG_ON_WRONG_DATATYPE_1 = 90015;

    /**
     * The error with code <code>90016</code> is thrown when
     * a column was used in the expression list or the order by clause
     * of a group or aggregate query, and that column is not in the GROUP BY clause.
     * Example:
     * <pre>
     * CREATE TABLE TEST(ID INT, NAME VARCHAR);
     * INSERT INTO TEST VALUES(1, 'Hello'), (2, 'World');
     * SELECT ID, MAX(NAME) FROM TEST;
     * Column ID must be in the GROUP BY list.
     * </pre>
     * Correct:
     * <pre>
     * SELECT ID, MAX(NAME) FROM TEST GROUP BY ID;
     * </pre>
     */
    public static final int MUST_GROUP_BY_COLUMN_1 = 90016;
    public static final int SECOND_PRIMARY_KEY = 90017;

    /**
     * The error with code <code>90018</code> is thrown when
     * the connection was opened, but never closed. In the finalizer of the
     * connection, this forgotten close was detected and the connection was
     * closed automatically, but relying on the finalizer is not good practice
     * as it is not guaranteed and behavior is virtual machine dependent. The
     * application should close the connection. This exception only appears in
     * the .trace.db file. Example:
     * <pre>
     * Connection conn = DriverManager.getConnection(&quot;jdbc:h2:&tilde;/test&quot;);
     * conn = null;
     * The connection was not closed by the application and is garbage collected
     * </pre>
     * Correct:
     * <pre>
     * conn.close();
     * </pre>
     */
    public static final int TRACE_CONNECTION_NOT_CLOSED = 90018;
    public static final int CANNOT_DROP_CURRENT_USER = 90019;
    public static final int DATABASE_ALREADY_OPEN_1 = 90020;
    public static final int DATA_CONVERSION_ERROR_1 = 90021;
    public static final int FUNCTION_NOT_FOUND_1 = 90022;
    public static final int COLUMN_MUST_NOT_BE_NULLABLE_1 = 90023;
    public static final int FILE_RENAME_FAILED_2 = 90024;
    public static final int FILE_DELETE_FAILED_1 = 90025;
    public static final int SERIALIZATION_FAILED_1 = 90026;
    public static final int DESERIALIZATION_FAILED_1 = 90027;
    public static final int IO_EXCEPTION_1 = 90028;
    public static final int NOT_ON_UPDATABLE_ROW = 90029;
    public static final int FILE_CORRUPTED_1 = 90030;
    public static final int IO_EXCEPTION_2 = 90031;
    public static final int USER_NOT_FOUND_1 = 90032;
    public static final int USER_ALREADY_EXISTS_1 = 90033;
    public static final int LOG_FILE_ERROR_2 = 90034;
    public static final int SEQUENCE_ALREADY_EXISTS_1 = 90035;
    public static final int SEQUENCE_NOT_FOUND_1 = 90036;
    public static final int VIEW_NOT_FOUND_1 = 90037;
    public static final int VIEW_ALREADY_EXISTS_1 = 90038;
    public static final int VALUE_TOO_LARGE_FOR_PRECISION_1 = 90039;
    public static final int ADMIN_RIGHTS_REQUIRED = 90040;
    public static final int TRIGGER_ALREADY_EXISTS_1 = 90041;
    public static final int TRIGGER_NOT_FOUND_1 = 90042;
    public static final int ERROR_CREATING_TRIGGER_OBJECT_3 = 90043;
    public static final int ERROR_EXECUTING_TRIGGER_3 = 90044;
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
    public static final int EXCEPTION_OPENING_PORT_2 = 90061;
    public static final int FILE_CREATION_FAILED_1 = 90062;
    public static final int SAVEPOINT_IS_INVALID_1 = 90063;
    public static final int SAVEPOINT_IS_UNNAMED = 90064;
    public static final int SAVEPOINT_IS_NAMED = 90065;
    public static final int DUPLICATE_PROPERTY_1 = 90066;
    public static final int CONNECTION_BROKEN = 90067;

    /**
     * The error with code <code>90068</code> is thrown when
     * the given expression that is used in the ORDER BY is not in the result list.
     * This is required for distinct queries, otherwise the result would be ambiguous.
     * <pre>
     * CREATE TABLE TEST(ID INT, NAME VARCHAR);
     * INSERT INTO TEST VALUES(2, 'Hello'), (1, 'Hello');
     * SELECT DISTINCT NAME FROM TEST ORDER BY ID;
     * Order by expression ID must be in the result list in this case
     * </pre>
     * Correct:
     * <pre>
     * SELECT DISTINCT ID, NAME FROM TEST ORDER BY ID;
     * </pre>
     */
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
    public static final int CANNOT_DROP_LAST_COLUMN = 90084;

    /**
     * The error with code <code>90085</code> is thrown when
     * trying to manually drop an index that was generated by the system because of a unique constraint.
     * <pre>
     * CREATE TABLE TEST(ID INT, CONSTRAINT UID UNIQUE(ID));
     * DROP INDEX UID_INDEX_0;
     * Index UID_INDEX_0 belongs to a constraint
     * </pre>
     * Correct:
     * <pre>
     * ALTER TABLE TEST DROP CONSTRAINT UID;
     * </pre>
     */
    public static final int INDEX_BELONGS_TO_CONSTRAINT_1 = 90085;
    public static final int CLASS_NOT_FOUND_1 = 90086;
    public static final int METHOD_NOT_FOUND_1 = 90087;
    public static final int UNKNOWN_MODE_1 = 90088;

    /**
     * The error with code <code>90089</code> is thrown when
     * trying to change the collation while there was already data in
     * the database. The collation of the database must be set when the
     * database is empty.
     * <pre>
     * CREATE TABLE TEST(NAME VARCHAR PRIMARY KEY);
     * INSERT INTO TEST VALUES('Hello', 'World');
     * SET COLLATION DE;
     * Collation cannot be changed because there is a data table: PUBLIC.TEST
     * </pre>
     * Correct:
     * <pre>
     * SET COLLATION DE;
     * CREATE TABLE TEST(NAME VARCHAR PRIMARY KEY);
     * INSERT INTO TEST VALUES('Hello', 'World');
     * </pre>
     */
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
    public static final int ERROR_SETTING_DATABASE_EVENT_LISTENER_2 = 90099;
    public static final int NO_DISK_SPACE_AVAILABLE = 90100;
    public static final int WRONG_XID_FORMAT_1 = 90101;
    public static final int UNSUPPORTED_COMPRESSION_OPTIONS_1 = 90102;
    public static final int UNSUPPORTED_COMPRESSION_ALGORITHM_1 = 90103;
    public static final int COMPRESSION_ERROR = 90104;
    public static final int EXCEPTION_IN_FUNCTION = 90105;
    public static final int CANNOT_TRUNCATE_1 = 90106;
    public static final int CANNOT_DROP_2 = 90107;
    public static final int STACK_OVERFLOW = 90108;
    public static final int VIEW_IS_INVALID_2 = 90109;
    public static final int OVERFLOW_FOR_TYPE_1 = 90110;
    public static final int ERROR_ACCESSING_LINKED_TABLE_2 = 90111;

    /**
     * The error with code <code>90112</code> is thrown when
     * a row was deleted twice while locking was disabled.
     * This is an intern exception that should never be thrown to the application,
     * because such deleted should be detected and the resulting exception ignored inside the database engine.
     * <pre>
     * Row not found when trying to delete from index UID_INDEX_0
     * </pre>
     */
    public static final int ROW_NOT_FOUND_WHEN_DELETING_1 = 90112;
    public static final int UNSUPPORTED_SETTING_1 = 90113;
    public static final int CONSTANT_ALREADY_EXISTS_1 = 90114;
    public static final int CONSTANT_NOT_FOUND_1 = 90115;
    public static final int LITERALS_ARE_NOT_ALLOWED = 90116;
    public static final int REMOTE_CONNECTION_NOT_ALLOWED = 90117;
    public static final int CANNOT_DROP_TABLE_1  = 90118;
    public static final int USER_DATA_TYPE_ALREADY_EXISTS_1 = 90119;
    public static final int USER_DATA_TYPE_NOT_FOUND_1 = 90120;
    public static final int DATABASE_CALLED_AT_SHUTDOWN = 90121;
    public static final int OPERATION_NOT_SUPPORTED_WITH_VIEWS_2 = 90122;
    public static final int CANNOT_MIX_INDEXED_AND_UNINDEXED_PARAMS = 90123;
    public static final int FILE_NOT_FOUND_1 = 90124;

    /**
     * The error with code <code>90125</code> is thrown when
     * PreparedStatement.setBigDecimal is called
     * with object that extends the class BigDecimal, and the system property
     * h2.allowBigDecimalExtensions is not set. Using extensions of BigDecimal is
     * dangerous because the database relies on the behavior of BigDecimal.
     * <pre>
     * BigDecimal bd = new MyDecimal("$10.3");
     * prep.setBigDecimal(1, bd);
     * Invalid class, expected java.math.BigDecimal but got MyDecimal
     * </pre>
     * Correct:
     * <pre>
     * BigDecimal bd = new BigDecimal("10.3");
     * prep.setBigDecimal(1, bd);
     * </pre>
     */
    public static final int INVALID_CLASS_2 = 90125;
    public static final int DATABASE_IS_NOT_PERSISTENT = 90126;
    public static final int RESULT_SET_NOT_UPDATABLE = 90127;
    public static final int RESULT_SET_NOT_SCROLLABLE = 90128;
    public static final int TRANSACTION_NOT_FOUND_1 = 90129;
    public static final int METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT = 90130;
    public static final int CONCURRENT_UPDATE_1 = 90131;
    public static final int AGGREGATE_NOT_FOUND_1 = 90132;
    public static final int CANNOT_CHANGE_SETTING_WHEN_OPEN_1 = 90133;
    public static final int ACCESS_DENIED_TO_CLASS_1 = 90134;
    public static final int DATABASE_IS_IN_EXCLUSIVE_MODE = 90135;
    public static final int UNSUPPORTED_OUTER_JOIN_CONDITION_1 = 90136;

    /**
     * The error with code <code>90137</code> is thrown when
     * trying to assign a value to something that is not a variable.
     * <pre>
     * SELECT AMOUNT, SET(@V, IFNULL(@V, 0)+AMOUNT) FROM TEST;
     * </pre>
     */
    public static final int CAN_ONLY_ASSIGN_TO_VARIABLE_1 = 90137;

    /**
     * INTERNAL
     */
    public static String getState(int errorCode) {
        // To convert SQLState to error code, replace
        // 21S: 210, 42S: 421, HY: 50, C: 1, T: 2

        switch (errorCode) {
        // 02: no data
        case NO_DATA_AVAILABLE: return "02000";

        // 07: dynamic SQL error
        case INVALID_PARAMETER_COUNT_2: return "07001";

        // 08: connection exception
        case ERROR_OPENING_DATABASE: return "08000";
        case WRONG_USER_OR_PASSWORD: return "08004";

        // 21: cardinality violation
        case COLUMN_COUNT_DOES_NOT_MATCH: return "21S02";

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

        // HY
        case GENERAL_ERROR_1: return "HY000";
        case UNKNOWN_DATA_TYPE_1: return "HY004";

        case FEATURE_NOT_SUPPORTED: return "HYC00";
        case LOCK_TIMEOUT_1: return "HYT00";
        default:
            return "" + errorCode;
        }
    }

}
