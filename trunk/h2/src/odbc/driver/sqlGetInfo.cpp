/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 */

#include "h2odbc.h"

SQLRETURN  SQL_API SQLGetInfo(SQLHDBC ConnectionHandle, 
           SQLUSMALLINT InfoType, SQLPOINTER InfoValuePtr, 
           SQLSMALLINT BufferLength, SQLSMALLINT* StringLengthPtr) {
    trace("SQLGetInfo");
    Connection* conn;
    conn=Connection::cast(ConnectionHandle);
    if(conn==0) {
        return SQL_INVALID_HANDLE;
    }
    conn->setError(0);
    const char* string=0;
    switch(InfoType) {
    case SQL_ALTER_TABLE:
        trace(" SQL_ALTER_TABLE");
        // todo
        returnInt(InfoValuePtr, StringLengthPtr, 0);
        break;
    case SQL_FETCH_DIRECTION:
        trace(" SQL_FETCH_DIRECTION");
        returnInt(InfoValuePtr, StringLengthPtr, SQL_FD_FETCH_NEXT);
        break;
    case SQL_ODBC_API_CONFORMANCE:
        trace(" SQL_ODBC_API_CONFORMANCE");
        returnSmall(InfoValuePtr, StringLengthPtr, SQL_OAC_LEVEL1);
        break;
    case SQL_LOCK_TYPES:
        trace(" SQL_LOCK_TYPES");
        returnInt(InfoValuePtr, StringLengthPtr, SQL_LCK_NO_CHANGE);
        break;
    case SQL_POS_OPERATIONS:
        trace(" SQL_POS_OPERATIONS");
        returnInt(InfoValuePtr, StringLengthPtr, 0);
        break;
    case SQL_POSITIONED_STATEMENTS:
        trace(" SQL_POSITIONED_STATEMENTS");
        returnInt(InfoValuePtr, StringLengthPtr, 0);
        break;
    case SQL_SCROLL_CONCURRENCY:
        trace(" SQL_SCROLL_CONCURRENCY");
        returnInt(InfoValuePtr, StringLengthPtr, SQL_SCCO_READ_ONLY);
        break;
    case SQL_STATIC_SENSITIVITY:
        trace(" SQL_STATIC_SENSITIVITY");
        returnInt(InfoValuePtr, StringLengthPtr, 0);
        break;
    case SQL_ACCESSIBLE_PROCEDURES:
        trace(" SQL_ACCESSIBLE_PROCEDURES");
        string="Y";
        break;
    case SQL_ACCESSIBLE_TABLES:
        trace(" SQL_ACCESSIBLE_TABLES");
        string="Y";
        break;
    case SQL_ACTIVE_ENVIRONMENTS:
        trace(" SQL_ACTIVE_ENVIRONMENTS");
        returnSmall(InfoValuePtr, StringLengthPtr, 0);
        break;
    case SQL_AGGREGATE_FUNCTIONS:
        trace(" SQL_AGGREGATE_FUNCTIONS");
        returnInt(InfoValuePtr, StringLengthPtr, SQL_AF_ALL);
        break;
    case SQL_ALTER_DOMAIN:
        trace(" SQL_ALTER_DOMAIN");
        returnInt(InfoValuePtr, StringLengthPtr, 0);
        break;
    case SQL_ASYNC_MODE:
        trace(" SQL_ASYNC_MODE");
        returnInt(InfoValuePtr, StringLengthPtr, SQL_AM_NONE);
        // todo SQL_AM_STATEMENT
        break;
    case SQL_BATCH_ROW_COUNT:
        trace(" SQL_BATCH_ROW_COUNT");
        returnInt(InfoValuePtr, StringLengthPtr, SQL_BRC_EXPLICIT);
        break;
    case SQL_BATCH_SUPPORT:
        trace(" SQL_BATCH_SUPPORT");
        returnInt(InfoValuePtr, StringLengthPtr, 0);
        // todo
        break;
    case SQL_BOOKMARK_PERSISTENCE:
        trace(" SQL_BOOKMARK_PERSISTENCE");
        returnInt(InfoValuePtr, StringLengthPtr, 0);
        // todo
        break;
    case SQL_CATALOG_LOCATION:
        trace(" SQL_CATALOG_LOCATION");
        returnSmall(InfoValuePtr, StringLengthPtr, SQL_CL_START);
        break;
    case SQL_CATALOG_NAME:
        trace(" SQL_CATALOG_NAME");
        string="Y";
        break;
    case SQL_CATALOG_NAME_SEPARATOR:
        trace(" SQL_CATALOG_NAME_SEPARATOR");
        string=".";
        break;
    case SQL_CATALOG_TERM:
        trace(" SQL_CATALOG_TERM");
        string="catalog";
        break;
    case SQL_CATALOG_USAGE:
        trace(" SQL_CATALOG_USAGE");
        returnInt(InfoValuePtr, StringLengthPtr, SQL_CU_DML_STATEMENTS | SQL_CU_PROCEDURE_INVOCATION | SQL_CU_TABLE_DEFINITION | SQL_CU_INDEX_DEFINITION | SQL_CU_PRIVILEGE_DEFINITION);
        break;
    case SQL_COLLATION_SEQ:
        trace(" SQL_COLLATION_SEQ");
        string="";
        // todo: ISO 8859-1 ?
        break;
    case SQL_CONCAT_NULL_BEHAVIOR:
        trace(" SQL_CONCAT_NULL_BEHAVIOR TODO");
        returnSmall(InfoValuePtr, StringLengthPtr, SQL_CB_NULL);
        break;
    case SQL_CONVERT_BIGINT:
    case SQL_CONVERT_BINARY:
    case SQL_CONVERT_BIT:
    case SQL_CONVERT_CHAR:
    case SQL_CONVERT_DATE:
    case SQL_CONVERT_DECIMAL:
    case SQL_CONVERT_DOUBLE:
    case SQL_CONVERT_FLOAT:
    case SQL_CONVERT_INTEGER:
    case SQL_CONVERT_INTERVAL_YEAR_MONTH:
    case SQL_CONVERT_INTERVAL_DAY_TIME:
    case SQL_CONVERT_LONGVARBINARY:
    case SQL_CONVERT_LONGVARCHAR:
    case SQL_CONVERT_NUMERIC:
    case SQL_CONVERT_REAL:
    case SQL_CONVERT_SMALLINT:
    case SQL_CONVERT_TIME:
    case SQL_CONVERT_TIMESTAMP:
    case SQL_CONVERT_TINYINT:
    case SQL_CONVERT_VARBINARY:
    case SQL_CONVERT_VARCHAR:
        trace(" SQL_CONVERT_ %d", InfoType);
        returnInt(InfoValuePtr, StringLengthPtr, 
            SQL_CVT_BIGINT |
            SQL_CVT_BINARY |
            SQL_CVT_BIT |
            SQL_CVT_CHAR |
            SQL_CVT_DATE |
            SQL_CVT_DECIMAL |
            SQL_CVT_DOUBLE |
            SQL_CVT_FLOAT |
            SQL_CVT_INTEGER |
            SQL_CVT_INTERVAL_YEAR_MONTH |
            SQL_CVT_INTERVAL_DAY_TIME |
            SQL_CVT_LONGVARBINARY |
            SQL_CVT_LONGVARCHAR |
            SQL_CVT_NUMERIC |
            SQL_CVT_REAL |
            SQL_CVT_SMALLINT |
            SQL_CVT_TIME |
            SQL_CVT_TIMESTAMP |
            SQL_CVT_TINYINT |
            SQL_CVT_VARBINARY |
            SQL_CVT_VARCHAR);
        break;
    case SQL_CONVERT_FUNCTIONS:
        trace(" SQL_CONVERT_FUNCTIONS");
        returnInt(InfoValuePtr, StringLengthPtr, SQL_FN_CVT_CAST);
        // todo
        break;
    case SQL_CORRELATION_NAME:
        trace(" SQL_CORRELATION_NAME");
        returnInt(InfoValuePtr, StringLengthPtr, SQL_CN_ANY);
        break;
    case SQL_CREATE_ASSERTION:
    case SQL_CREATE_CHARACTER_SET:
    case SQL_CREATE_COLLATION:
    case SQL_CREATE_DOMAIN:
    case SQL_CREATE_SCHEMA:
        trace(" SQL_CREATE_ %d TODO", InfoType);
        returnInt(InfoValuePtr, StringLengthPtr, 0);
        break;
    case SQL_CREATE_TABLE:
        trace(" SQL_CREATE_TABLE TODO");
        returnInt(InfoValuePtr, StringLengthPtr, SQL_CT_CREATE_TABLE | SQL_CT_TABLE_CONSTRAINT);
        // todo: SQL_CT_CONSTRAINT_NAME_DEFINITION
        break;
    case SQL_CREATE_TRANSLATION:
        trace(" SQL_CREATE_TRANSLATION");
        returnInt(InfoValuePtr, StringLengthPtr, 0);
        break;
    case SQL_CREATE_VIEW:
        trace(" SQL_CREATE_VIEW");
        returnInt(InfoValuePtr, StringLengthPtr, SQL_CV_CREATE_VIEW | SQL_CV_CHECK_OPTION);
        break;
    case SQL_CURSOR_COMMIT_BEHAVIOR:
        trace(" SQL_CURSOR_COMMIT_BEHAVIOR");
        returnSmall(InfoValuePtr, StringLengthPtr, SQL_CB_CLOSE);
        break;        
    case SQL_CURSOR_ROLLBACK_BEHAVIOR:
        trace(" SQL_CURSOR_ROLLBACK_BEHAVIOR");    
        returnSmall(InfoValuePtr, StringLengthPtr, SQL_CB_CLOSE);
        break;
    case SQL_CURSOR_SENSITIVITY:
        trace(" SQL_CURSOR_SENSITIVITY");
        returnInt(InfoValuePtr, StringLengthPtr, SQL_UNSPECIFIED);
        break;
    case SQL_DATA_SOURCE_NAME:
        trace(" SQL_DATA_SOURCE_NAME %s", conn->getDataSourceName().data());
        string=conn->getDataSourceName().data();
        break;
    case SQL_DATA_SOURCE_READ_ONLY:
        trace(" SQL_DATA_SOURCE_READ_ONLY");
        string="N";
        // todo
        break;
    case SQL_DATABASE_NAME:
        trace(" SQL_DATABASE_NAME");
        string="H2";
        // todo
        break;
    case SQL_DATETIME_LITERALS:
        trace(" SQL_DATETIME_LITERALS");
        returnInt(InfoValuePtr, StringLengthPtr, 
            SQL_DL_SQL92_DATE | 
            SQL_DL_SQL92_TIME | 
            SQL_DL_SQL92_TIMESTAMP);
        // todo
    case SQL_DBMS_NAME:
        trace(" SQL_DBMS_NAME");
        string="h2";
        // todo
        break;
    case SQL_DBMS_VER:
        trace(" SQL_DBMS_VER");
        string="1.0";
        // todo
        break;
    case SQL_DDL_INDEX:
        trace(" SQL_DDL_INDEX");
        returnInt(InfoValuePtr, StringLengthPtr, SQL_DI_CREATE_INDEX | SQL_DI_DROP_INDEX);
        break;
    case SQL_DEFAULT_TXN_ISOLATION:
        trace(" SQL_DEFAULT_TXN_ISOLATION");
        returnInt(InfoValuePtr, StringLengthPtr, SQL_TXN_READ_COMMITTED);
        break;
    case SQL_DESCRIBE_PARAMETER:
        trace(" SQL_DESCRIBE_PARAMETER");
        string="N";
        break;
    case SQL_DM_VER:
    case SQL_DRIVER_HDBC:
    case SQL_DRIVER_HENV:
    case SQL_DRIVER_HDESC:
    case SQL_DRIVER_HLIB:
    case SQL_DRIVER_HSTMT:
        trace(" SQL_DRIVER_ %d", InfoType);
        // implemented by the DriverManager
        break;
    case SQL_DRIVER_NAME:
        trace(" SQL_DRIVER_NAME");
        string="h2";
        break;
    case SQL_DRIVER_ODBC_VER:
        trace(" SQL_DRIVER_ODBC_VER");
        string="03.00";
        break;
    case SQL_DRIVER_VER:
        trace(" SQL_DRIVER_VER");
        string="01.00.0000";
        break;
    case SQL_DROP_ASSERTION:
    case SQL_DROP_CHARACTER_SET:
    case SQL_DROP_COLLATION:
    case SQL_DROP_DOMAIN:
    case SQL_DROP_SCHEMA:
        trace(" SQL_DROP_ %d", InfoType);
        returnInt(InfoValuePtr, StringLengthPtr, 0);
        break;
    case SQL_DROP_TABLE:
        trace(" SQL_DROP_TABLE");
        returnInt(InfoValuePtr, StringLengthPtr, SQL_DT_DROP_TABLE);
        break;
    case SQL_DROP_TRANSLATION:
    case SQL_DROP_VIEW:
        trace(" SQL_DROP_ %d", InfoType);
        returnInt(InfoValuePtr, StringLengthPtr, 0);
        break;
    case SQL_DYNAMIC_CURSOR_ATTRIBUTES1:
        trace(" SQL_DYNAMIC_CURSOR_ATTRIBUTES1");
        returnInt(InfoValuePtr, StringLengthPtr, 0);
        //SQL_CA1_NEXT | SQL_CA1_ABSOLUTE | SQL_CA1_RELATIVE;
        break;
    case SQL_DYNAMIC_CURSOR_ATTRIBUTES2:
        trace(" SQL_DYNAMIC_CURSOR_ATTRIBUTES2");
        returnInt(InfoValuePtr, StringLengthPtr, 0);
        break;
    case SQL_EXPRESSIONS_IN_ORDERBY:
        trace(" SQL_EXPRESSIONS_IN_ORDERBY");
        string="Y";
        break;
    case SQL_FILE_USAGE:
        trace(" SQL_FILE_USAGE");
        returnSmall(InfoValuePtr, StringLengthPtr, SQL_FILE_NOT_SUPPORTED);
        break;
    case SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1:
        trace(" SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1");
        returnInt(InfoValuePtr, StringLengthPtr, SQL_CA1_NEXT);
        break;
    case SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2:
        trace(" SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2");
        returnInt(InfoValuePtr, StringLengthPtr, SQL_CA2_READ_ONLY_CONCURRENCY);
        break;
    case SQL_GETDATA_EXTENSIONS:
        trace(" SQL_GETDATA_EXTENSIONS");
        returnInt(InfoValuePtr, StringLengthPtr, SQL_GD_ANY_COLUMN | SQL_GD_ANY_ORDER);
        break;
    case SQL_GROUP_BY:
        trace(" SQL_GROUP_BY");
        returnSmall(InfoValuePtr, StringLengthPtr, SQL_GB_GROUP_BY_EQUALS_SELECT);
        break;
    case SQL_IDENTIFIER_CASE:
        trace(" SQL_IDENTIFIER_CASE");
        returnSmall(InfoValuePtr, StringLengthPtr, SQL_IC_UPPER);
        break;
    case SQL_IDENTIFIER_QUOTE_CHAR:
        trace(" SQL_IDENTIFIER_QUOTE_CHAR");
        string="\"";
        break;
    case SQL_INDEX_KEYWORDS:
        trace(" SQL_INDEX_KEYWORDS");
        returnInt(InfoValuePtr, StringLengthPtr, SQL_IK_NONE);
        break;
    case SQL_INFO_SCHEMA_VIEWS:
        trace(" SQL_INFO_SCHEMA_VIEWS");
        returnInt(InfoValuePtr, StringLengthPtr, 0);
        break;
    case SQL_INSERT_STATEMENT:
        trace(" SQL_INSERT_STATEMENT");
        returnInt(InfoValuePtr, StringLengthPtr, SQL_IS_INSERT_LITERALS | SQL_IS_INSERT_SEARCHED | SQL_IS_SELECT_INTO);
        break;
    case SQL_INTEGRITY:
        trace(" SQL_INTEGRITY");
        string="Y";
        break;
    case SQL_KEYSET_CURSOR_ATTRIBUTES1:
        trace(" SQL_KEYSET_CURSOR_ATTRIBUTES1");
        returnInt(InfoValuePtr, StringLengthPtr, SQL_CA1_NEXT);
        break;
    case SQL_KEYSET_CURSOR_ATTRIBUTES2:
        trace(" SQL_KEYSET_CURSOR_ATTRIBUTES2");
        returnInt(InfoValuePtr, StringLengthPtr, SQL_CA2_READ_ONLY_CONCURRENCY);
        break;
    case SQL_KEYWORDS:
        trace(" SQL_KEYWORDS");
        string=SQL_ODBC_KEYWORDS;
        break;
    case SQL_LIKE_ESCAPE_CLAUSE:
        trace(" SQL_LIKE_ESCAPE_CLAUSE");
        string="Y";
        break;
    case SQL_MAX_ASYNC_CONCURRENT_STATEMENTS:
    case SQL_MAX_BINARY_LITERAL_LEN:
    case SQL_MAX_CHAR_LITERAL_LEN:
    case SQL_MAX_INDEX_SIZE:
    case SQL_MAX_ROW_SIZE:
    case SQL_MAX_STATEMENT_LEN:
        trace(" SQL_MAX_ %d", InfoType);
        returnInt(InfoValuePtr, StringLengthPtr, 0);
        break;
    case SQL_MAX_CATALOG_NAME_LEN:
    case SQL_MAX_COLUMN_NAME_LEN:
    case SQL_MAX_COLUMNS_IN_GROUP_BY:
    case SQL_MAX_COLUMNS_IN_INDEX:
    case SQL_MAX_COLUMNS_IN_ORDER_BY:
    case SQL_MAX_COLUMNS_IN_SELECT:
    case SQL_MAX_COLUMNS_IN_TABLE:
    case SQL_MAX_CONCURRENT_ACTIVITIES:
    case SQL_MAX_CURSOR_NAME_LEN:
    case SQL_MAX_DRIVER_CONNECTIONS:
    case SQL_MAX_IDENTIFIER_LEN:
    case SQL_MAX_PROCEDURE_NAME_LEN:
    case SQL_MAX_SCHEMA_NAME_LEN:
    case SQL_MAX_TABLE_NAME_LEN:
    case SQL_MAX_TABLES_IN_SELECT:
    case SQL_MAX_USER_NAME_LEN:
        trace(" SQL_MAX_ %d", InfoType);
        returnSmall(InfoValuePtr, StringLengthPtr, 0);
        break;
    case SQL_MAX_ROW_SIZE_INCLUDES_LONG:
        trace(" SQL_MAX_ROW_SIZE_INCLUDES_LONG");
        string="Y";
        break;
    case SQL_MULT_RESULT_SETS:
        trace(" SQL_MULT_RESULT_SETS");
        string="N";
        break;
    case SQL_MULTIPLE_ACTIVE_TXN:
        trace(" SQL_MULTIPLE_ACTIVE_TXN");
        string="Y";
        break;
    case SQL_NEED_LONG_DATA_LEN:
        trace(" SQL_NEED_LONG_DATA_LEN");
        string="Y";
        break;
    case SQL_NON_NULLABLE_COLUMNS:
        trace(" SQL_NON_NULLABLE_COLUMNS");
        returnSmall(InfoValuePtr, StringLengthPtr, SQL_NNC_NON_NULL);
        break;
    case SQL_NULL_COLLATION:
        trace(" SQL_NULL_COLLATION");
        returnSmall(InfoValuePtr, StringLengthPtr, SQL_NC_LOW);
        break;
    case SQL_NUMERIC_FUNCTIONS:
        trace(" SQL_NUMERIC_FUNCTIONS");
        returnInt(InfoValuePtr, StringLengthPtr, 
            SQL_FN_NUM_ABS |
            SQL_FN_NUM_ACOS |
            SQL_FN_NUM_ASIN |
            SQL_FN_NUM_ATAN |
            SQL_FN_NUM_ATAN2 |
            SQL_FN_NUM_CEILING |
            SQL_FN_NUM_COS |
            SQL_FN_NUM_COT |
            SQL_FN_NUM_DEGREES |
            SQL_FN_NUM_EXP |
            SQL_FN_NUM_FLOOR |
            SQL_FN_NUM_LOG |
            SQL_FN_NUM_LOG10 |
            SQL_FN_NUM_MOD |
            SQL_FN_NUM_PI |
            SQL_FN_NUM_POWER |
            SQL_FN_NUM_RADIANS |
            SQL_FN_NUM_RAND |
            SQL_FN_NUM_ROUND |
            SQL_FN_NUM_SIGN |
            SQL_FN_NUM_SIN |
            SQL_FN_NUM_SQRT |
            SQL_FN_NUM_TAN |
            SQL_FN_NUM_TRUNCATE);
        break;
    case SQL_ODBC_INTERFACE_CONFORMANCE:
        trace(" SQL_ODBC_INTERFACE_CONFORMANCE");
        returnInt(InfoValuePtr, StringLengthPtr, SQL_OIC_CORE);
        break;
    case SQL_ODBC_VER:
        trace(" SQL_ODBC_VER");
        // Driver Manager only
        break;
    case SQL_OJ_CAPABILITIES:
        trace(" SQL_OJ_CAPABILITIES");
        returnInt(InfoValuePtr, StringLengthPtr, SQL_OJ_LEFT | SQL_OJ_RIGHT | SQL_OJ_INNER);
        break;
    case SQL_ORDER_BY_COLUMNS_IN_SELECT:
        trace(" SQL_ORDER_BY_COLUMNS_IN_SELECT");
        string="N";
        break;
    case SQL_PARAM_ARRAY_ROW_COUNTS:
    case SQL_PARAM_ARRAY_SELECTS:
        trace(" SQL_PARAM_ARRAY_ %d", InfoType);
        returnInt(InfoValuePtr, StringLengthPtr, 0);
        break;
    case SQL_PROCEDURE_TERM:
        trace(" SQL_PROCEDURE_TERM");
        string="procedure";
        break;
    case SQL_PROCEDURES:
        trace(" SQL_PROCEDURES");
        string="Y";
        break;
    case SQL_QUOTED_IDENTIFIER_CASE:
        trace(" SQL_QUOTED_IDENTIFIER_CASE");
        returnSmall(InfoValuePtr, StringLengthPtr, SQL_IC_SENSITIVE);
        break;
    case SQL_ROW_UPDATES:
        trace(" SQL_ROW_UPDATES");
        string="N";
        break;
    case SQL_SCHEMA_TERM:
        trace(" SQL_SCHEMA_TERM");
        string="schema";
        break;
    case SQL_SCHEMA_USAGE:
        trace(" SQL_SCHEMA_USAGE");
        returnInt(InfoValuePtr, StringLengthPtr, SQL_SU_DML_STATEMENTS);
        break;
    case SQL_SCROLL_OPTIONS:
        trace(" SQL_SCROLL_OPTIONS");
        returnInt(InfoValuePtr, StringLengthPtr, SQL_SO_FORWARD_ONLY);
        break;
    case SQL_SEARCH_PATTERN_ESCAPE:
        trace(" SQL_SEARCH_PATTERN_ESCAPE");
        string="\\";
        break;
    case SQL_SERVER_NAME:
        trace(" SQL_SERVER_NAME TODO");
        string="h2";
        // todo
        break;
    case SQL_SPECIAL_CHARACTERS:
        trace(" SQL_SPECIAL_CHARACTERS");
        string="";
        break;
    case SQL_SQL_CONFORMANCE:
        trace(" SQL_SQL_CONFORMANCE");
        returnInt(InfoValuePtr, StringLengthPtr, SQL_SC_SQL92_ENTRY);
        break;
    case SQL_SQL92_DATETIME_FUNCTIONS:
        trace(" SQL_SQL92_DATETIME_FUNCTIONS");
        returnInt(InfoValuePtr, StringLengthPtr, 
            SQL_SDF_CURRENT_DATE | SQL_SDF_CURRENT_TIME | SQL_SDF_CURRENT_TIMESTAMP);
        break;
    case SQL_SQL92_FOREIGN_KEY_DELETE_RULE:
        trace(" SQL_SQL92_FOREIGN_KEY_DELETE_RULE");
        returnInt(InfoValuePtr, StringLengthPtr, SQL_SFKD_NO_ACTION);
        break;
    case SQL_SQL92_FOREIGN_KEY_UPDATE_RULE:
        trace(" SQL_SQL92_FOREIGN_KEY_UPDATE_RULE");
        returnInt(InfoValuePtr, StringLengthPtr, SQL_SFKU_NO_ACTION);
        break;
    case SQL_SQL92_GRANT:
        trace(" SQL_SQL92_GRANT");
        returnInt(InfoValuePtr, StringLengthPtr, 0);
        break;
    case SQL_SQL92_NUMERIC_VALUE_FUNCTIONS:
        trace(" SQL_SQL92_NUMERIC_VALUE_FUNCTIONS");
        returnInt(InfoValuePtr, StringLengthPtr, 
            SQL_SNVF_BIT_LENGTH |
            SQL_SNVF_CHAR_LENGTH |
            SQL_SNVF_CHARACTER_LENGTH |
            SQL_SNVF_EXTRACT |
            SQL_SNVF_OCTET_LENGTH |
            SQL_SNVF_POSITION);
        break;
    case SQL_SQL92_PREDICATES:
        trace(" SQL_SQL92_PREDICATES");
        returnInt(InfoValuePtr, StringLengthPtr, 
            SQL_SP_BETWEEN |
            SQL_SP_COMPARISON |
            SQL_SP_EXISTS |
            SQL_SP_IN |
            SQL_SP_ISNOTNULL |
            SQL_SP_ISNULL |
            SQL_SP_LIKE |
            SQL_SP_QUANTIFIED_COMPARISON |
            SQL_SP_UNIQUE);
        break;
    case SQL_SQL92_RELATIONAL_JOIN_OPERATORS:
        trace(" SQL_SQL92_RELATIONAL_JOIN_OPERATORS");
        returnInt(InfoValuePtr, StringLengthPtr, 
            SQL_SRJO_INNER_JOIN | 
            SQL_SRJO_LEFT_OUTER_JOIN | 
            SQL_SRJO_RIGHT_OUTER_JOIN);
        break;
    case SQL_SQL92_REVOKE:
        trace(" SQL_SQL92_REVOKE");
        returnInt(InfoValuePtr, StringLengthPtr, 0);
        break;
    case SQL_SQL92_ROW_VALUE_CONSTRUCTOR:
        trace(" SQL_SQL92_ROW_VALUE_CONSTRUCTOR");
        returnInt(InfoValuePtr, StringLengthPtr, 
            SQL_SRVC_VALUE_EXPRESSION | 
            SQL_SRVC_NULL | 
            SQL_SRVC_ROW_SUBQUERY);
        break;
    case SQL_SQL92_STRING_FUNCTIONS:
        trace(" SQL_SQL92_STRING_FUNCTIONS");
        returnInt(InfoValuePtr, StringLengthPtr, 
            SQL_SSF_CONVERT |
            SQL_SSF_LOWER |
            SQL_SSF_UPPER |
            SQL_SSF_SUBSTRING |
            SQL_SSF_TRANSLATE |
            SQL_SSF_TRIM_BOTH |
            SQL_SSF_TRIM_LEADING |
            SQL_SSF_TRIM_TRAILING);
        break;
    case SQL_SQL92_VALUE_EXPRESSIONS:
        trace(" SQL_SQL92_VALUE_EXPRESSIONS");
        returnInt(InfoValuePtr, StringLengthPtr, 0);
        break;
    case SQL_STANDARD_CLI_CONFORMANCE:
        trace(" SQL_STANDARD_CLI_CONFORMANCE");
        returnInt(InfoValuePtr, StringLengthPtr, SQL_SCC_XOPEN_CLI_VERSION1);
        break;
    case SQL_STATIC_CURSOR_ATTRIBUTES1:
        trace(" SQL_STATIC_CURSOR_ATTRIBUTES1");
        returnInt(InfoValuePtr, StringLengthPtr, 
            SQL_CA1_NEXT /* | SQL_CA1_ABSOLUTE | SQL_CA1_RELATIVE */);
        break;
    case SQL_STATIC_CURSOR_ATTRIBUTES2:
        trace(" SQL_STATIC_CURSOR_ATTRIBUTES2");
        returnInt(InfoValuePtr, StringLengthPtr, 0);
        break;
    case SQL_STRING_FUNCTIONS:
        trace(" SQL_STRING_FUNCTIONS");
        returnInt(InfoValuePtr, StringLengthPtr, 
            SQL_FN_STR_ASCII |
            SQL_FN_STR_BIT_LENGTH |
            SQL_FN_STR_CHAR |
            SQL_FN_STR_CHAR_LENGTH |
            SQL_FN_STR_CHARACTER_LENGTH |
            SQL_FN_STR_CONCAT |
            SQL_FN_STR_DIFFERENCE |
            SQL_FN_STR_INSERT |
            SQL_FN_STR_LCASE |
            SQL_FN_STR_LEFT |
            SQL_FN_STR_LENGTH |
            SQL_FN_STR_LOCATE |
            SQL_FN_STR_LTRIM |
            SQL_FN_STR_OCTET_LENGTH |
            SQL_FN_STR_POSITION |
            SQL_FN_STR_REPEAT |
            SQL_FN_STR_REPLACE |
            SQL_FN_STR_RIGHT |
            SQL_FN_STR_RTRIM |
            SQL_FN_STR_SOUNDEX |
            SQL_FN_STR_SPACE |
            SQL_FN_STR_SUBSTRING |
            SQL_FN_STR_UCASE);
        break;
    case SQL_SUBQUERIES:
        trace(" SQL_SUBQUERIES");
        returnInt(InfoValuePtr, StringLengthPtr, 
            SQL_SQ_CORRELATED_SUBQUERIES |
            SQL_SQ_COMPARISON |
            SQL_SQ_EXISTS |
            SQL_SQ_IN |
            SQL_SQ_QUANTIFIED);
        break;
    case SQL_SYSTEM_FUNCTIONS:
        trace(" SQL_SYSTEM_FUNCTIONS");
        returnInt(InfoValuePtr, StringLengthPtr, 
            SQL_FN_SYS_DBNAME |
            SQL_FN_SYS_IFNULL |
            SQL_FN_SYS_USERNAME);
        break;
    case SQL_TABLE_TERM:
        trace(" SQL_TABLE_TERM");
        string="table";
        break;
    case SQL_TIMEDATE_ADD_INTERVALS:
        trace(" SQL_TIMEDATE_ADD_INTERVALS");
        returnInt(InfoValuePtr, StringLengthPtr, 0);
        break;
    case SQL_TIMEDATE_DIFF_INTERVALS:
        trace(" SQL_TIMEDATE_DIFF_INTERVALS");
        returnInt(InfoValuePtr, StringLengthPtr, 0);
        break;
    case SQL_TIMEDATE_FUNCTIONS:
        trace(" SQL_TIMEDATE_FUNCTIONS");
        returnInt(InfoValuePtr, StringLengthPtr, 
            SQL_FN_TD_CURRENT_DATE |
            SQL_FN_TD_CURRENT_TIME |
            SQL_FN_TD_CURRENT_TIMESTAMP |
            SQL_FN_TD_CURDATE |
            SQL_FN_TD_CURTIME);
        break;
    case SQL_TXN_CAPABLE:
        trace(" SQL_TXN_CAPABLE");
        returnSmall(InfoValuePtr, StringLengthPtr, SQL_TC_DDL_COMMIT);
        break;
    case SQL_TXN_ISOLATION_OPTION:
        trace(" SQL_TXN_ISOLATION_OPTION");
        returnInt(InfoValuePtr, StringLengthPtr, 
            SQL_TXN_SERIALIZABLE | 
            SQL_TXN_REPEATABLE_READ | 
            SQL_TXN_READ_COMMITTED);
        break;
    case SQL_UNION:
        trace(" SQL_UNION");
        returnInt(InfoValuePtr, StringLengthPtr, SQL_U_UNION | SQL_U_UNION_ALL);
        break;
    case SQL_USER_NAME:
        trace(" SQL_USER_NAME TODO");
        string="sa";
        // todo
        break;
    case SQL_XOPEN_CLI_YEAR:
        trace(" SQL_XOPEN_CLI_YEAR");
        string="2002";
        break;
    default:
        trace(" ? %d TODO", InfoType);
        conn->setError(E_HY096);
        return SQL_ERROR;
/*        
SQL_ACCESSIBLE_PROCEDURES
SQL_ACCESSIBLE_TABLES
SQL_ACTIVE_ENVIRONMENTS
SQL_AGGREGATE_FUNCTIONS
SQL_ALTER_DOMAIN
SQL_ALTER_TABLE
SQL_ASYNC_MODE
SQL_BATCH_ROW_COUNT
SQL_BATCH_SUPPORT
SQL_BOOKMARK_PERSISTENCE
SQL_CATALOG_LOCATION
SQL_CATALOG_NAME
SQL_CATALOG_NAME_SEPARATOR
SQL_CATALOG_TERM
SQL_CATALOG_USAGE
SQL_COLLATION_SEQ
SQL_COLUMN_ALIAS
SQL_CONCAT_NULL_BEHAVIOR
SQL_CONVERT_BIGINT
SQL_CONVERT_BINARY
SQL_CONVERT_BIT
SQL_CONVERT_CHAR
SQL_CONVERT_DATE
SQL_CONVERT_DECIMAL
SQL_CONVERT_DOUBLE
SQL_CONVERT_FLOAT
SQL_CONVERT_FUNCTIONS
SQL_CONVERT_GUID
SQL_CONVERT_INTEGER
SQL_CONVERT_INTERVAL_DAY_TIME
SQL_CONVERT_INTERVAL_YEAR_MONTH
SQL_CONVERT_LONGVARBINARY
SQL_CONVERT_LONGVARCHAR
SQL_CONVERT_NUMERIC
SQL_CONVERT_REAL
SQL_CONVERT_SMALLINT
SQL_CONVERT_TIME
SQL_CONVERT_TIMESTAMP
SQL_CONVERT_TINYINT
SQL_CONVERT_VARBINARY
SQL_CONVERT_VARCHAR
SQL_CORRELATION_NAME
SQL_CREATE_ASSERTION
SQL_CREATE_CHARACTER_SET
SQL_CREATE_COLLATION
SQL_CREATE_DOMAIN
SQL_CREATE_SCHEMA
SQL_CREATE_TABLE
SQL_CREATE_TRANSLATION
SQL_CREATE_VIEW
SQL_CURSOR_COMMIT_BEHAVIOR
SQL_CURSOR_ROLLBACK_BEHAVIOR
SQL_CURSOR_ROLLBACK_SQL_CURSOR_SENSITIVITY
SQL_DATA_SOURCE_NAME
SQL_DATA_SOURCE_READ_ONLY
SQL_DATABASE_NAME
SQL_DATETIME_LITERALS
SQL_DBMS_NAME
SQL_DBMS_VER
SQL_DDL_INDEX
SQL_DEFAULT_TXN_ISOLATION
SQL_DESCRIBE_PARAMETER
SQL_DM_VER
SQL_DRIVER_HDBC
SQL_DRIVER_HDESC
SQL_DRIVER_HENV
SQL_DRIVER_HLIB
SQL_DRIVER_HSTMT
SQL_DRIVER_NAME
SQL_DRIVER_ODBC_VER
SQL_DRIVER_VER
SQL_DROP_ASSERTION
SQL_DROP_CHARACTER_SET
SQL_DROP_COLLATION
SQL_DROP_DOMAIN
SQL_DROP_SCHEMA
SQL_DROP_TABLE
SQL_DROP_TRANSLATION
SQL_DROP_VIEW
SQL_DYNAMIC_CURSOR_ATTRIBUTES1
SQL_DYNAMIC_CURSOR_ATTRIBUTES2
SQL_EXPRESSIONS_IN_ORDERBY
SQL_FILE_USAGE
SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1
SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2
SQL_GETDATA_EXTENSIONS
SQL_GROUP_BY
SQL_IDENTIFIER_CASE
SQL_IDENTIFIER_QUOTE_CHAR
SQL_INDEX_KEYWORDS
SQL_INFO_SCHEMA_VIEWS
SQL_INSERT_STATEMENT
SQL_INTEGRITY
SQL_KEYSET_CURSOR_ATTRIBUTES1
SQL_KEYSET_CURSOR_ATTRIBUTES2
SQL_KEYWORDS
SQL_LIKE_ESCAPE_CLAUSE
SQL_MAX_ASYNC_CONCURRENT_STATEMENTS
SQL_MAX_BINARY_LITERAL_LEN
SQL_MAX_CATALOG_NAME_LEN
SQL_MAX_CHAR_LITERAL_LEN
SQL_MAX_COLUMN_NAME_LEN
SQL_MAX_COLUMNS_IN_GROUP_BY
SQL_MAX_COLUMNS_IN_INDEX
SQL_MAX_COLUMNS_IN_ORDER_BY
SQL_MAX_COLUMNS_IN_SELECT
SQL_MAX_COLUMNS_IN_TABLE
SQL_MAX_CONCURRENT_ACTIVITIES
SQL_MAX_CURSOR_NAME_LEN
SQL_MAX_DRIVER_CONNECTIONS
SQL_MAX_IDENTIFIER_LEN
SQL_MAX_INDEX_SIZE
SQL_MAX_PROCEDURE_NAME_LEN
SQL_MAX_ROW_SIZE
SQL_MAX_ROW_SIZE_INCLUDES_LONG
SQL_MAX_SCHEMA_NAME_LEN
SQL_MAX_STATEMENT_LEN
SQL_MAX_TABLE_NAME_LEN
SQL_MAX_TABLES_IN_SELECT
SQL_MAX_USER_NAME_LEN
SQL_MULT_RESULT_SETS
SQL_MULTIPLE_ACTIVE_TXN
SQL_NEED_LONG_DATA_LEN
SQL_NON_NULLABLE_COLUMNS
SQL_NULL_COLLATION
SQL_NUMERIC_FUNCTIONS
SQL_ODBC_INTERFACE_CONFORMANCE
SQL_ODBC_VER
SQL_OJ_CAPABILITIES
SQL_ORDER_BY_COLUMNS_IN_SELECT
SQL_PARAM_ARRAY_ROW_COUNTS
SQL_PARAM_ARRAY_SELECTS
SQL_POS_OPERATIONS
SQL_PROCEDURE_TERM
SQL_PROCEDURES
SQL_QUOTED_IDENTIFIER_CASE
SQL_ROW_UPDATES
SQL_SCHEMA_TERM
SQL_SCHEMA_USAGE
SQL_SCROLL_OPTIONS
SQL_SEARCH_PATTERN_ESCAPE
SQL_SERVER_NAME
SQL_SPECIAL_CHARACTERS
SQL_SQL_CONFORMANCE
SQL_SQL92_DATETIME_FUNCTIONS
SQL_SQL92_FOREIGN_KEY_DELETE_RULE
SQL_SQL92_FOREIGN_KEY_UPDATE_RULE
SQL_SQL92_GRANT
SQL_SQL92_NUMERIC_InfoValuePtr_FUNCTIONS
SQL_SQL92_PREDICATES
SQL_SQL92_RELATIONAL_JOIN_OPERATORS
SQL_SQL92_REVOKE
SQL_SQL92_ROW_InfoValuePtr_CONSTRUCTOR
SQL_SQL92_STRING_FUNCTIONS
SQL_SQL92_InfoValuePtr_EXPRESSIONS
SQL_STANDARD_CLI_CONFORMANCE
SQL_STATIC_CURSOR_ATTRIBUTES1
SQL_STATIC_CURSOR_ATTRIBUTES2
SQL_STRING_FUNCTIONS
SQL_SUBQUERIES
SQL_SYSTEM_FUNCTIONS
SQL_TABLE_TERM
SQL_TIMEDATE_ADD_INTERVALS
SQL_TIMEDATE_DIFF_INTERVALS
SQL_TIMEDATE_FUNCTIONS
SQL_TXN_CAPABLE
SQL_TXN_ISOLATION_OPTION
SQL_UNION
SQL_USER_NAME
SQL_XOPEN_CLI_YEAR
    
        
*/        
    }
    if(string!=0) {
        trace("   =%s", string);
        returnString((SQLCHAR*)InfoValuePtr, BufferLength, StringLengthPtr, string);
    }
    return SQL_SUCCESS;
}
