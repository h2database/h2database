/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.h2.command.Parser;
import org.h2.command.Token;
import org.h2.command.Tokenizer;
import org.h2.message.DbException;
import org.h2.test.TestBase;
import org.h2.util.ParserUtil;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

/**
 * Tests keywords.
 */
public class TestKeywords extends TestBase {

    private enum TokenType {
        IDENTIFIER,

        KEYWORD,

        CONTEXT_SENSITIVE_KEYWORD;
    }

    private static final HashSet<String> SQL92_RESERVED_WORDS = toSet(new String[] {

            "ABSOLUTE", "ACTION", "ADD", "ALL", "ALLOCATE", "ALTER", "AND", "ANY", "ARE", "AS", "ASC", "ASSERTION",
            "AT", "AUTHORIZATION", "AVG",

            "BEGIN", "BETWEEN", "BIT", "BIT_LENGTH", "BOTH", "BY",

            "CASCADE", "CASCADED", "CASE", "CAST", "CATALOG", "CHAR", "CHARACTER", "CHAR_LENGTH", "CHARACTER_LENGTH",
            "CHECK", "CLOSE", "COALESCE", "COLLATE", "COLLATION", "COLUMN", "COMMIT", "CONNECT", "CONNECTION",
            "CONSTRAINT", "CONSTRAINTS", "CONTINUE", "CONVERT", "CORRESPONDING", "COUNT", "CREATE", "CROSS", "CURRENT",
            "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "CURSOR",

            "DATE", "DAY", "DEALLOCATE", "DEC", "DECIMAL", "DECLARE", "DEFAULT", "DEFERRABLE", "DEFERRED", "DELETE",
            "DESC", "DESCRIBE", "DESCRIPTOR", "DIAGNOSTICS", "DISCONNECT", "DISTINCT", "DOMAIN", "DOUBLE", "DROP",

            "ELSE", "END", "END-EXEC", "ESCAPE", "EXCEPT", "EXCEPTION", "EXEC", "EXECUTE", "EXISTS", "EXTERNAL",
            "EXTRACT",

            "FALSE", "FETCH", "FIRST", "FLOAT", "FOR", "FOREIGN", "FOUND", "FROM", "FULL",

            "GET", "GLOBAL", "GO", "GOTO", "GRANT", "GROUP",

            "HAVING", "HOUR",

            "IDENTITY", "IMMEDIATE", "IN", "INDICATOR", "INITIALLY", "INNER", "INPUT", "INSENSITIVE", "INSERT", "INT",
            "INTEGER", "INTERSECT", "INTERVAL", "INTO", "IS", "ISOLATION",

            "JOIN",

            "KEY",

            "LANGUAGE", "LAST", "LEADING", "LEFT", "LEVEL", "LIKE", "LOCAL", "LOWER",

            "MATCH", "MAX", "MIN", "MINUTE", "MODULE", "MONTH",

            "NAMES", "NATIONAL", "NATURAL", "NCHAR", "NEXT", "NO", "NOT", "NULL", "NULLIF", "NUMERIC",

            "OCTET_LENGTH", "OF", "ON", "ONLY", "OPEN", "OPTION", "OR", "ORDER", "OUTER", "OUTPUT", "OVERLAPS",

            "PAD", "PARTIAL", "POSITION", "PRECISION", "PREPARE", "PRESERVE", "PRIMARY", "PRIOR", "PRIVILEGES",
            "PROCEDURE", "PUBLIC",

            "READ", "REAL", "REFERENCES", "RELATIVE", "RESTRICT", "REVOKE", "RIGHT", "ROLLBACK", "ROWS",

            "SCHEMA", "SCROLL", "SECOND", "SECTION", "SELECT", "SESSION", "SESSION_USER", "SET", "SIZE", "SMALLINT",
            "SOME", "SPACE", "SQL", "SQLCODE", "SQLERROR", "SQLSTATE", "SUBSTRING", "SUM", "SYSTEM_USER",

            "TABLE", "TEMPORARY", "THEN", "TIME", "TIMESTAMP", "TIMEZONE_HOUR", "TIMEZONE_MINUTE", "TO", "TRAILING",
            "TRANSACTION", "TRANSLATE", "TRANSLATION", "TRIM", "TRUE",

            "UNION", "UNIQUE", "UNKNOWN", "UPDATE", "UPPER", "USAGE", "USER", "USING",

            "VALUE", "VALUES", "VARCHAR", "VARYING", "VIEW",

            "WHEN", "WHENEVER", "WHERE", "WITH", "WORK", "WRITE",

            "YEAR",

            "ZONE",

    });

    private static final HashSet<String> SQL1999_RESERVED_WORDS = toSet(new String[] {

            "ABSOLUTE", "ACTION", "ADD", "ADMIN", "AFTER", "AGGREGATE", "ALIAS", "ALL", "ALLOCATE", "ALTER", "AND",
            "ANY", "ARE", "ARRAY", "AS", "ASC", "ASSERTION", "AT", "AUTHORIZATION",

            "BEFORE", "BEGIN", "BINARY", "BIT", "BLOB", "BOOLEAN", "BOTH", "BREADTH", "BY",

            "CALL", "CASCADE", "CASCADED", "CASE", "CAST", "CATALOG", "CHAR", "CHARACTER", "CHECK", "CLASS", "CLOB",
            "CLOSE", "COLLATE", "COLLATION", "COLUMN", "COMMIT", "COMPLETION", "CONNECT", "CONNECTION", "CONSTRAINT",
            "CONSTRAINTS", "CONSTRUCTOR", "CONTINUE", "CORRESPONDING", "CREATE", "CROSS", "CUBE", "CURRENT",
            "CURRENT_DATE", "CURRENT_PATH", "CURRENT_ROLE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER",
            "CURSOR", "CYCLE",

            "DATA", "DATE", "DAY", "DEALLOCATE", "DEC", "DECIMAL", "DECLARE", "DEFAULT", "DEFERRABLE", "DEFERRED",
            "DELETE", "DEPTH", "DEREF", "DESC", "DESCRIBE", "DESCRIPTOR", "DESTROY", "DESTRUCTOR", "DETERMINISTIC",
            "DICTIONARY", "DIAGNOSTICS", "DISCONNECT", "DISTINCT", "DOMAIN", "DOUBLE", "DROP", "DYNAMIC",

            "EACH", "ELSE", "END", "END-EXEC", "EQUALS", "ESCAPE", "EVERY", "EXCEPT", "EXCEPTION", "EXEC", "EXECUTE",
            "EXTERNAL",

            "FALSE", "FETCH", "FIRST", "FLOAT", "FOR", "FOREIGN", "FOUND", "FROM", "FREE", "FULL", "FUNCTION",

            "GENERAL", "GET", "GLOBAL", "GO", "GOTO", "GRANT", "GROUP", "GROUPING",

            "HAVING", "HOST", "HOUR",

            "IDENTITY", "IGNORE", "IMMEDIATE", "IN", "INDICATOR", "INITIALIZE", "INITIALLY", "INNER", "INOUT", "INPUT",
            "INSERT", "INT", "INTEGER", "INTERSECT", "INTERVAL", "INTO", "IS", "ISOLATION", "ITERATE",

            "JOIN",

            "KEY",

            "LANGUAGE", "LARGE", "LAST", "LATERAL", "LEADING", "LEFT", "LESS", "LEVEL", "LIKE", "LIMIT", "LOCAL",
            "LOCALTIME", "LOCALTIMESTAMP", "LOCATOR",

            "MAP", "MATCH", "MINUTE", "MODIFIES", "MODIFY", "MODULE", "MONTH",

            "NAMES", "NATIONAL", "NATURAL", "NCHAR", "NCLOB", "NEW", "NEXT", "NO", "NONE", "NOT", "NULL", "NUMERIC",

            "OBJECT", "OF", "OFF", "OLD", "ON", "ONLY", "OPEN", "OPERATION", "OPTION", "OR", "ORDER", "ORDINALITY",
            "OUT", "OUTER", "OUTPUT",

            "PAD", "PARAMETER", "PARAMETERS", "PARTIAL", "PATH", "POSTFIX", "PRECISION", "PREFIX", "PREORDER",
            "PREPARE", "PRESERVE", "PRIMARY", "PRIOR", "PRIVILEGES", "PROCEDURE", "PUBLIC",

            "READ", "READS", "REAL", "RECURSIVE", "REF", "REFERENCES", "REFERENCING", "RELATIVE", "RESTRICT", "RESULT",
            "RETURN", "RETURNS", "REVOKE", "RIGHT", "ROLE", "ROLLBACK", "ROLLUP", "ROUTINE", "ROW", "ROWS",

            "SAVEPOINT", "SCHEMA", "SCROLL", "SCOPE", "SEARCH", "SECOND", "SECTION", "SELECT", "SEQUENCE", "SESSION",
            "SESSION_USER", "SET", "SETS", "SIZE", "SMALLINT", "SOME", "SPACE", "SPECIFIC", "SPECIFICTYPE", "SQL",
            "SQLEXCEPTION", "SQLSTATE", "SQLWARNING", "START", "STATE", "STATEMENT", "STATIC", "STRUCTURE",
            "SYSTEM_USER",

            "TABLE", "TEMPORARY", "TERMINATE", "THAN", "THEN", "TIME", "TIMESTAMP", "TIMEZONE_HOUR", "TIMEZONE_MINUTE",
            "TO", "TRAILING", "TRANSACTION", "TRANSLATION", "TREAT", "TRIGGER", "TRUE",

            "UNDER", "UNION", "UNIQUE", "UNKNOWN", "UNNEST", "UPDATE", "USAGE", "USER", "USING",

            "VALUE", "VALUES", "VARCHAR", "VARIABLE", "VARYING", "VIEW",

            "WHEN", "WHENEVER", "WHERE", "WITH", "WITHOUT", "WORK", "WRITE",

            "YEAR", "ZONE",

    });

    private static final HashSet<String> SQL2003_RESERVED_WORDS = toSet(new String[] {

            "ABS", "ALL", "ALLOCATE", "ALTER", "AND", "ANY", "ARE", "ARRAY", "AS", "ASENSITIVE", "ASYMMETRIC", "AT",
            "ATOMIC", "AUTHORIZATION", "AVG",

            "BEGIN", "BETWEEN", "BIGINT", "BINARY", "BLOB", "BOOLEAN", "BOTH", "BY",

            "CALL", "CALLED", "CARDINALITY", "CASCADED", "CASE", "CAST", "CEIL", "CEILING", "CHAR", "CHAR_LENGTH",
            "CHARACTER", "CHARACTER_LENGTH", "CHECK", "CLOB", "CLOSE", "COALESCE", "COLLATE", "COLLECT", "COLUMN",
            "COMMIT", "CONDITION", "CONNECT", "CONSTRAINT", "CONVERT", "CORR", "CORRESPONDING", "COUNT", "COVAR_POP",
            "COVAR_SAMP", "CREATE", "CROSS", "CUBE", "CUME_DIST", "CURRENT", "CURRENT_DATE",
            "CURRENT_DEFAULT_TRANSFORM_GROUP", "CURRENT_PATH", "CURRENT_ROLE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
            "CURRENT_TRANSFORM_GROUP_FOR_TYPE", "CURRENT_USER", "CURSOR", "CYCLE",

            "DATE", "DAY", "DEALLOCATE", "DEC", "DECIMAL", "DECLARE", "DEFAULT", "DELETE", "DENSE_RANK", "DEREF",
            "DESCRIBE", "DETERMINISTIC", "DISCONNECT", "DISTINCT", "DOUBLE", "DROP", "DYNAMIC",

            "EACH", "ELEMENT", "ELSE", "END", "END-EXEC", "ESCAPE", "EVERY", "EXCEPT", "EXEC", "EXECUTE", "EXISTS",
            "EXP", "EXTERNAL", "EXTRACT",

            "FALSE", "FETCH", "FILTER", "FLOAT", "FLOOR", "FOR", "FOREIGN", "FREE", "FROM", "FULL", "FUNCTION",
            "FUSION",

            "GET", "GLOBAL", "GRANT", "GROUP", "GROUPING",

            "HAVING", "HOLD", "HOUR", "IDENTITY", "IN", "INDICATOR", "INNER", "INOUT", "INSENSITIVE",

            "INSERT", "INT", "INTEGER", "INTERSECT", "INTERSECTION", "INTERVAL", "INTO", "IS",

            "JOIN",

            "LANGUAGE", "LARGE", "LATERAL", "LEADING", "LEFT", "LIKE", "LN", "LOCAL", "LOCALTIME", "LOCALTIMESTAMP",
            "LOWER",

            "MATCH", "MAX", "MEMBER", "MERGE", "METHOD", "MIN", "MINUTE", "MOD", "MODIFIES", "MODULE", "MONTH",
            "MULTISET",

            "NATIONAL", "NATURAL", "NCHAR", "NCLOB", "NEW", "NO", "NONE", "NORMALIZE", "NOT", "NULL", "NULLIF",
            "NUMERIC",

            "OCTET_LENGTH", "OF", "OLD", "ON", "ONLY", "OPEN", "OR", "ORDER", "OUT", "OUTER", "OVER", "OVERLAPS",
            "OVERLAY",

            "PARAMETER", "PARTITION", "PERCENT_RANK", "PERCENTILE_CONT", "PERCENTILE_DISC", "POSITION", "POWER",
            "PRECISION", "PREPARE", "PRIMARY", "PROCEDURE",

            "RANGE", "RANK", "READS", "REAL", "RECURSIVE", "REF", "REFERENCES", "REFERENCING", "REGR_AVGX", //
            "REGR_AVGY", "REGR_COUNT", "REGR_INTERCEPT", "REGR_R2", "REGR_SLOPE", "REGR_SXX", "REGR_SXY", "REGR_SYY",
            "RELEASE", "RESULT", "RETURN", "RETURNS", "REVOKE", "RIGHT", "ROLLBACK", "ROLLUP", "ROW", "ROW_NUMBER",
            "ROWS",

            "SAVEPOINT", "SCOPE", "SCROLL", "SEARCH", "SECOND", "SELECT", "SENSITIVE", "SESSION_USER", "SET", //
            "SIMILAR", "SMALLINT", "SOME", "SPECIFIC", "SPECIFICTYPE", "SQL", "SQLEXCEPTION", "SQLSTATE", "SQLWARNING",
            "SQRT", "START", "STATIC", "STDDEV_POP", "STDDEV_SAMP", "SUBMULTISET", "SUBSTRING", "SUM", "SYMMETRIC",
            "SYSTEM", "SYSTEM_USER",

            "TABLE", "TABLESAMPLE", "THEN", "TIME", "TIMESTAMP", "TIMEZONE_HOUR", "TIMEZONE_MINUTE", "TO", "TRAILING",
            "TRANSLATE", "TRANSLATION", "TREAT", "TRIGGER", "TRIM", "TRUE",

            "UESCAPE", "UNION", "UNIQUE", "UNKNOWN", "UNNEST", "UPDATE", "UPPER", "USER", "USING",

            "VALUE", "VALUES", "VAR_POP", "VAR_SAMP", "VARCHAR", "VARYING",

            "WHEN", "WHENEVER", "WHERE", "WIDTH_BUCKET", "WINDOW", "WITH", "WITHIN", "WITHOUT",

            "YEAR",

    });

    private static final HashSet<String> SQL2008_RESERVED_WORDS = toSet(new String[] {

            "ABS", "ALL", "ALLOCATE", "ALTER", "AND", "ANY", "ARE", "ARRAY", "AS", "ASENSITIVE", "ASYMMETRIC", "AT",
            "ATOMIC", "AUTHORIZATION", "AVG",

            "BEGIN", "BETWEEN", "BIGINT", "BINARY", "BLOB", "BOOLEAN", "BOTH", "BY",

            "CALL", "CALLED", "CARDINALITY", "CASCADED", "CASE", "CAST", "CEIL", "CEILING", "CHAR", "CHAR_LENGTH",
            "CHARACTER", "CHARACTER_LENGTH", "CHECK", "CLOB", "CLOSE", "COALESCE", "COLLATE", "COLLECT", "COLUMN",
            "COMMIT", "CONDITION", "CONNECT", "CONSTRAINT", "CONVERT", "CORR", "CORRESPONDING", "COUNT", "COVAR_POP",
            "COVAR_SAMP", "CREATE", "CROSS", "CUBE", "CUME_DIST", "CURRENT", "CURRENT_CATALOG", "CURRENT_DATE",
            "CURRENT_DEFAULT_TRANSFORM_GROUP", "CURRENT_PATH", "CURRENT_ROLE", "CURRENT_SCHEMA", "CURRENT_TIME",
            "CURRENT_TIMESTAMP", "CURRENT_TRANSFORM_GROUP_FOR_TYPE", "CURRENT_USER", "CURSOR", "CYCLE",

            "DATE", "DAY", "DEALLOCATE", "DEC", "DECIMAL", "DECLARE", "DEFAULT", "DELETE", "DENSE_RANK", "DEREF",
            "DESCRIBE", "DETERMINISTIC", "DISCONNECT", "DISTINCT", "DOUBLE", "DROP", "DYNAMIC",

            "EACH", "ELEMENT", "ELSE", "END", "END-EXEC", "ESCAPE", "EVERY", "EXCEPT", "EXEC", "EXECUTE", "EXISTS",
            "EXP", "EXTERNAL", "EXTRACT",

            "FALSE", "FETCH", "FILTER", "FLOAT", "FLOOR", "FOR", "FOREIGN", "FREE", "FROM", "FULL", "FUNCTION",
            "FUSION",

            "GET", "GLOBAL", "GRANT", "GROUP", "GROUPING",

            "HAVING", "HOLD", "HOUR",

            "IDENTITY", "IN", "INDICATOR", "INNER", "INOUT", "INSENSITIVE", "INSERT", "INT", "INTEGER", "INTERSECT",
            "INTERSECTION", "INTERVAL", "INTO", "IS",

            "JOIN",

            "LANGUAGE", "LARGE", "LATERAL", "LEADING", "LEFT", "LIKE", "LIKE_REGEX", "LN", "LOCAL", "LOCALTIME",
            "LOCALTIMESTAMP", "LOWER",

            "MATCH", "MAX", "MEMBER", "MERGE", "METHOD", "MIN", "MINUTE", "MOD", "MODIFIES", "MODULE", "MONTH",
            "MULTISET",

            "NATIONAL", "NATURAL", "NCHAR", "NCLOB", "NEW", "NO", "NONE", "NORMALIZE", "NOT", "NULL", "NULLIF",
            "NUMERIC",

            "OCTET_LENGTH", "OCCURRENCES_REGEX", "OF", "OLD", "ON", "ONLY", "OPEN", "OR", "ORDER", "OUT", "OUTER",
            "OVER", "OVERLAPS", "OVERLAY",

            "PARAMETER", "PARTITION", "PERCENT_RANK", "PERCENTILE_CONT", "PERCENTILE_DISC", "POSITION",
            "POSITION_REGEX", "POWER", "PRECISION", "PREPARE", "PRIMARY", "PROCEDURE",

            "RANGE", "RANK", "READS", "REAL", "RECURSIVE", "REF", "REFERENCES", "REFERENCING", "REGR_AVGX", //
            "REGR_AVGY", "REGR_COUNT", "REGR_INTERCEPT", "REGR_R2", "REGR_SLOPE", "REGR_SXX", "REGR_SXY", "REGR_SYY",
            "RELEASE", "RESULT", "RETURN", "RETURNS", "REVOKE", "RIGHT", "ROLLBACK", "ROLLUP", "ROW", "ROW_NUMBER",
            "ROWS",

            "SAVEPOINT", "SCOPE", "SCROLL", "SEARCH", "SECOND", "SELECT", "SENSITIVE", "SESSION_USER", "SET", //
            "SIMILAR", "SMALLINT", "SOME", "SPECIFIC", "SPECIFICTYPE", "SQL", "SQLEXCEPTION", "SQLSTATE", "SQLWARNING",
            "SQRT", "START", "STATIC", "STDDEV_POP", "STDDEV_SAMP", "SUBMULTISET", "SUBSTRING", "SUBSTRING_REGEX",
            "SUM", "SYMMETRIC", "SYSTEM", "SYSTEM_USER",

            "TABLE", "TABLESAMPLE", "THEN", "TIME", "TIMESTAMP", "TIMEZONE_HOUR", "TIMEZONE_MINUTE", "TO", "TRAILING",
            "TRANSLATE", "TRANSLATE_REGEX", "TRANSLATION", "TREAT", "TRIGGER", "TRIM", "TRUE",

            "UESCAPE", "UNION", "UNIQUE", "UNKNOWN", "UNNEST", "UPDATE", "UPPER", "USER", "USING",

            "VALUE", "VALUES", "VAR_POP", "VAR_SAMP", "VARBINARY", "VARCHAR", "VARYING",

            "WHEN", "WHENEVER", "WHERE", "WIDTH_BUCKET", "WINDOW", "WITH", "WITHIN", "WITHOUT",

            "YEAR",

    });

    private static final HashSet<String> SQL2011_RESERVED_WORDS = toSet(new String[] {

            "ABS", "ALL", "ALLOCATE", "ALTER", "AND", "ANY", "ARE", "ARRAY", "ARRAY_AGG", "ARRAY_MAX_CARDINALITY", //
            "AS", "ASENSITIVE", "ASYMMETRIC", "AT", "ATOMIC", "AUTHORIZATION", "AVG",

            "BEGIN", "BEGIN_FRAME", "BEGIN_PARTITION", "BETWEEN", "BIGINT", "BINARY", "BLOB", "BOOLEAN", "BOTH", "BY",

            "CALL", "CALLED", "CARDINALITY", "CASCADED", "CASE", "CAST", "CEIL", "CEILING", "CHAR", "CHAR_LENGTH",
            "CHARACTER", "CHARACTER_LENGTH", "CHECK", "CLOB", "CLOSE", "COALESCE", "COLLATE", "COLLECT", "COLUMN",
            "COMMIT", "CONDITION", "CONNECT", "CONSTRAINT", "CONTAINS", "CONVERT", "CORR", "CORRESPONDING", "COUNT",
            "COVAR_POP", "COVAR_SAMP", "CREATE", "CROSS", "CUBE", "CUME_DIST", "CURRENT", "CURRENT_CATALOG",
            "CURRENT_DATE", "CURRENT_DEFAULT_TRANSFORM_GROUP", "CURRENT_PATH", "CURRENT_ROLE", "CURRENT_ROW",
            "CURRENT_SCHEMA", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_TRANSFORM_GROUP_FOR_TYPE", "CURRENT_USER",
            "CURSOR", "CYCLE",

            "DATE", "DAY", "DEALLOCATE", "DEC", "DECIMAL", "DECLARE", "DEFAULT", "DELETE", "DENSE_RANK", "DEREF",
            "DESCRIBE", "DETERMINISTIC", "DISCONNECT", "DISTINCT", "DOUBLE", "DROP", "DYNAMIC",

            "EACH", "ELEMENT", "ELSE", "END", "END_FRAME", "END_PARTITION", "END-EXEC", "EQUALS", "ESCAPE", "EVERY",
            "EXCEPT", "EXEC", "EXECUTE", "EXISTS", "EXP", "EXTERNAL", "EXTRACT",

            "FALSE", "FETCH", "FILTER", "FIRST_VALUE", "FLOAT", "FLOOR", "FOR", "FOREIGN", "FRAME_ROW", "FREE", "FROM",
            "FULL", "FUNCTION", "FUSION",

            "GET", "GLOBAL", "GRANT", "GROUP", "GROUPING", "GROUPS",

            "HAVING", "HOLD", "HOUR",

            "IDENTITY", "IN", "INDICATOR", "INNER", "INOUT", "INSENSITIVE", "INSERT", "INT", "INTEGER", "INTERSECT",
            "INTERSECTION", "INTERVAL", "INTO", "IS",

            "JOIN",

            "LAG", "LANGUAGE", "LARGE", "LAST_VALUE", "LATERAL", "LEAD", "LEADING", "LEFT", "LIKE", "LIKE_REGEX", "LN",
            "LOCAL", "LOCALTIME", "LOCALTIMESTAMP", "LOWER",

            "MATCH", "MAX", "MEMBER", "MERGE", "METHOD", "MIN", "MINUTE", "MOD", "MODIFIES", "MODULE", "MONTH",
            "MULTISET",

            "NATIONAL", "NATURAL", "NCHAR", "NCLOB", "NEW", "NO", "NONE", "NORMALIZE", "NOT", "NTH_VALUE", "NTILE",
            "NULL", "NULLIF", "NUMERIC",

            "OCTET_LENGTH", "OCCURRENCES_REGEX", "OF", "OFFSET", "OLD", "ON", "ONLY", "OPEN", "OR", "ORDER", "OUT",
            "OUTER", "OVER", "OVERLAPS", "OVERLAY",

            "PARAMETER", "PARTITION", "PERCENT", "PERCENT_RANK", "PERCENTILE_CONT", "PERCENTILE_DISC", "PERIOD",
            "PORTION", "POSITION", "POSITION_REGEX", "POWER", "PRECEDES", "PRECISION", "PREPARE", "PRIMARY",
            "PROCEDURE",

            "RANGE", "RANK", "READS", "REAL", "RECURSIVE", "REF", "REFERENCES", "REFERENCING", "REGR_AVGX", //
            "REGR_AVGY", "REGR_COUNT", "REGR_INTERCEPT", "REGR_R2", "REGR_SLOPE", "REGR_SXX", "REGR_SXY", "REGR_SYY",
            "RELEASE", "RESULT", "RETURN", "RETURNS", "REVOKE", "RIGHT", "ROLLBACK", "ROLLUP", "ROW", "ROW_NUMBER",
            "ROWS",

            "SAVEPOINT", "SCOPE", "SCROLL", "SEARCH", "SECOND", "SELECT", "SENSITIVE", "SESSION_USER", "SET", //
            "SIMILAR", "SMALLINT", "SOME", "SPECIFIC", "SPECIFICTYPE", "SQL", "SQLEXCEPTION", "SQLSTATE", "SQLWARNING",
            "SQRT", "START", "STATIC", "STDDEV_POP", "STDDEV_SAMP", "SUBMULTISET", "SUBSTRING", "SUBSTRING_REGEX",
            "SUCCEEDS", "SUM", "SYMMETRIC", "SYSTEM", "SYSTEM_TIME", "SYSTEM_USER",

            "TABLE", "TABLESAMPLE", "THEN", "TIME", "TIMESTAMP", "TIMEZONE_HOUR", "TIMEZONE_MINUTE", "TO", "TRAILING",
            "TRANSLATE", "TRANSLATE_REGEX", "TRANSLATION", "TREAT", "TRIGGER", "TRUNCATE", "TRIM", "TRIM_ARRAY", //
            "TRUE",

            "UESCAPE", "UNION", "UNIQUE", "UNKNOWN", "UNNEST", "UPDATE", "UPPER", "USER", "USING",

            "VALUE", "VALUES", "VALUE_OF", "VAR_POP", "VAR_SAMP", "VARBINARY", "VARCHAR", "VARYING", "VERSIONING",

            "WHEN", "WHENEVER", "WHERE", "WIDTH_BUCKET", "WINDOW", "WITH", "WITHIN", "WITHOUT",

            "YEAR",

    });

    private static final HashSet<String> SQL2016_RESERVED_WORDS = toSet(new String[] {

            "ABS", "ACOS", "ALL", "ALLOCATE", "ALTER", "AND", "ANY", "ARE", "ARRAY", "ARRAY_AGG",
            "ARRAY_MAX_CARDINALITY", "AS", "ASENSITIVE", "ASIN", "ASYMMETRIC", "AT", "ATAN", "ATOMIC", "AUTHORIZATION",
            "AVG",

            "BEGIN", "BEGIN_FRAME", "BEGIN_PARTITION", "BETWEEN", "BIGINT", "BINARY", "BLOB", "BOOLEAN", "BOTH", "BY",

            "CALL", "CALLED", "CARDINALITY", "CASCADED", "CASE", "CAST", "CEIL", "CEILING", "CHAR", "CHAR_LENGTH",
            "CHARACTER", "CHARACTER_LENGTH", "CHECK", "CLASSIFIER", "CLOB", "CLOSE", "COALESCE", "COLLATE", "COLLECT",
            "COLUMN", "COMMIT", "CONDITION", "CONNECT", "CONSTRAINT", "CONTAINS", "CONVERT", "COPY", "CORR",
            "CORRESPONDING", "COS", "COSH", "COUNT", "COVAR_POP", "COVAR_SAMP", "CREATE", "CROSS", "CUBE", "CUME_DIST",
            "CURRENT", "CURRENT_CATALOG", "CURRENT_DATE", "CURRENT_DEFAULT_TRANSFORM_GROUP", "CURRENT_PATH",
            "CURRENT_ROLE", "CURRENT_ROW", "CURRENT_SCHEMA", "CURRENT_TIME", "CURRENT_TIMESTAMP",
            "CURRENT_TRANSFORM_GROUP_FOR_TYPE", "CURRENT_USER", "CURSOR", "CYCLE",

            "DATE", "DAY", "DEALLOCATE", "DEC", "DECIMAL", "DECFLOAT", "DECLARE", "DEFAULT", "DEFINE", "DELETE",
            "DENSE_RANK", "DEREF", "DESCRIBE", "DETERMINISTIC", "DISCONNECT", "DISTINCT", "DOUBLE", "DROP", "DYNAMIC",

            "EACH", "ELEMENT", "ELSE", "EMPTY", "END", "END_FRAME", "END_PARTITION", "END-EXEC", "EQUALS", "ESCAPE",
            "EVERY", "EXCEPT", "EXEC", "EXECUTE", "EXISTS", "EXP", "EXTERNAL", "EXTRACT",

            "FALSE", "FETCH", "FILTER", "FIRST_VALUE", "FLOAT", "FLOOR", "FOR", "FOREIGN", "FRAME_ROW", "FREE", "FROM",
            "FULL", "FUNCTION", "FUSION",

            "GET", "GLOBAL", "GRANT", "GROUP", "GROUPING", "GROUPS",

            "HAVING", "HOLD", "HOUR",

            "IDENTITY", "IN", "INDICATOR", "INITIAL", "INNER", "INOUT", "INSENSITIVE", "INSERT", "INT", "INTEGER",
            "INTERSECT", "INTERSECTION", "INTERVAL", "INTO", "IS",

            "JOIN", "JSON_ARRAY", "JSON_ARRAYAGG", "JSON_EXISTS", "JSON_OBJECT", "JSON_OBJECTAGG", "JSON_QUERY",
            "JSON_TABLE", "JSON_TABLE_PRIMITIVE", "JSON_VALUE",

            "LAG", "LANGUAGE", "LARGE", "LAST_VALUE", "LATERAL", "LEAD", "LEADING", "LEFT", "LIKE", "LIKE_REGEX",
            "LISTAGG", "LN", "LOCAL", "LOCALTIME", "LOCALTIMESTAMP", "LOG", "LOG10", "LOWER",

            "MATCH", "MATCH_NUMBER", "MATCH_RECOGNIZE", "MATCHES", "MAX", "MEMBER", "MERGE", "METHOD", "MIN", "MINUTE",
            "MOD", "MODIFIES", "MODULE", "MONTH", "MULTISET",

            "NATIONAL", "NATURAL", "NCHAR", "NCLOB", "NEW", "NO", "NONE", "NORMALIZE", "NOT", "NTH_VALUE", "NTILE",
            "NULL", "NULLIF", "NUMERIC",

            "OCTET_LENGTH", "OCCURRENCES_REGEX", "OF", "OFFSET", "OLD", "OMIT", "ON", "ONE", "ONLY", "OPEN", "OR",
            "ORDER", "OUT", "OUTER", "OVER", "OVERLAPS", "OVERLAY",

            "PARAMETER", "PARTITION", "PATTERN", "PER", "PERCENT", "PERCENT_RANK", "PERCENTILE_CONT", //
            "PERCENTILE_DISC", "PERIOD", "PORTION", "POSITION", "POSITION_REGEX", "POWER", "PRECEDES", "PRECISION",
            "PREPARE", "PRIMARY", "PROCEDURE", "PTF",

            "RANGE", "RANK", "READS", "REAL", "RECURSIVE", "REF", "REFERENCES", "REFERENCING", "REGR_AVGX", //
            "REGR_AVGY", "REGR_COUNT", "REGR_INTERCEPT", "REGR_R2", "REGR_SLOPE", "REGR_SXX", "REGR_SXY", "REGR_SYY",
            "RELEASE", "RESULT", "RETURN", "RETURNS", "REVOKE", "RIGHT", "ROLLBACK", "ROLLUP", "ROW", "ROW_NUMBER",
            "ROWS", "RUNNING",

            "SAVEPOINT", "SCOPE", "SCROLL", "SEARCH", "SECOND", "SEEK", "SELECT", "SENSITIVE", "SESSION_USER", "SET",
            "SHOW", "SIMILAR", "SIN", "SINH", "SKIP", "SMALLINT", "SOME", "SPECIFIC", "SPECIFICTYPE", "SQL",
            "SQLEXCEPTION", "SQLSTATE", "SQLWARNING", "SQRT", "START", "STATIC", "STDDEV_POP", "STDDEV_SAMP",
            "SUBMULTISET", "SUBSET", "SUBSTRING", "SUBSTRING_REGEX", "SUCCEEDS", "SUM", "SYMMETRIC", "SYSTEM",
            "SYSTEM_TIME", "SYSTEM_USER",

            "TABLE", "TABLESAMPLE", "TAN", "TANH", "THEN", "TIME", "TIMESTAMP", "TIMEZONE_HOUR", "TIMEZONE_MINUTE",
            "TO", "TRAILING", "TRANSLATE", "TRANSLATE_REGEX", "TRANSLATION", "TREAT", "TRIGGER", "TRIM", "TRIM_ARRAY",
            "TRUE", "TRUNCATE",

            "UESCAPE", "UNION", "UNIQUE", "UNKNOWN", "UNNEST", "UPDATE", "UPPER", "USER", "USING",

            "VALUE", "VALUES", "VALUE_OF", "VAR_POP", "VAR_SAMP", "VARBINARY", "VARCHAR", "VARYING", "VERSIONING",

            "WHEN", "WHENEVER", "WHERE", "WIDTH_BUCKET", "WINDOW", "WITH", "WITHIN", "WITHOUT",

            "YEAR",

    });

    private static final HashSet<String> STRICT_MODE_NON_KEYWORDS = toSet(new String[] { "LIMIT", "MINUS", "TOP" });

    private static final HashSet<String> ALL_RESEVED_WORDS;

    private static final HashMap<String, TokenType> TOKENS;

    static {
        HashSet<String> set = new HashSet<>(1024);
        set.addAll(SQL92_RESERVED_WORDS);
        set.addAll(SQL1999_RESERVED_WORDS);
        set.addAll(SQL2003_RESERVED_WORDS);
        set.addAll(SQL2008_RESERVED_WORDS);
        set.addAll(SQL2011_RESERVED_WORDS);
        set.addAll(SQL2016_RESERVED_WORDS);
        ALL_RESEVED_WORDS = set;
        HashMap<String, TokenType> tokens = new HashMap<>();
        processClass(Parser.class, tokens);
        processClass(ParserUtil.class, tokens);
        processClass(Token.class, tokens);
        processClass(Tokenizer.class, tokens);
        TOKENS = tokens;
    }

    private static void processClass(Class<?> clazz, HashMap<String, TokenType> tokens) {
        ClassReader r;
        try {
            r = new ClassReader(clazz.getResourceAsStream(clazz.getSimpleName() + ".class"));
        } catch (IOException e) {
            throw DbException.convert(e);
        }
        r.accept(new ClassVisitor(Opcodes.ASM8) {
            @Override
            public FieldVisitor visitField(int access, String name, String descriptor, String signature, //
                    Object value) {
                add(value);
                return null;
            }

            @Override
            public MethodVisitor visitMethod(int access, String name, String descriptor, String signature,
                    String[] exceptions) {
                return new MethodVisitor(Opcodes.ASM8) {
                    @Override
                    public void visitLdcInsn(Object value) {
                        add(value);
                    }
                };
            }

            void add(Object value) {
                if (!(value instanceof String)) {
                    return;
                }
                String s = (String) value;
                int l = s.length();
                if (l == 0) {
                    return;
                }
                for (int i = 0; i < l; i++) {
                    char ch = s.charAt(i);
                    if ((ch < 'A' || ch > 'Z') && ch != '_') {
                        return;
                    }
                }
                final TokenType type;
                switch (ParserUtil.getTokenType(s, false, true)) {
                case ParserUtil.IDENTIFIER:
                    type = TokenType.IDENTIFIER;
                    break;
                case ParserUtil.KEYWORD:
                    type = TokenType.CONTEXT_SENSITIVE_KEYWORD;
                    break;
                default:
                    type = TokenType.KEYWORD;
                }
                tokens.put(s, type);
            }
        }, ClassReader.SKIP_DEBUG | ClassReader.SKIP_FRAMES);
    }

    private static HashSet<String> toSet(String[] array) {
        HashSet<String> set = new HashSet<>((int) Math.ceil(array.length / .75));
        for (String reservedWord : array) {
            if (!set.add(reservedWord)) {
                throw new AssertionError(reservedWord);
            }
        }
        return set;
    }

    /**
     * Run just this test.
     *
     * @param a
     *            ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().testFromMain();
    }

    @Override
    public void test() throws Exception {
        testParser();
        testInformationSchema();
        testMetaData();
    }

    private void testParser() throws Exception {
        testParser(false);
        testParser(true);
    }

    private void testParser(boolean strictMode) throws Exception {
        try (Connection conn = DriverManager
                .getConnection("jdbc:h2:mem:keywords;MODE=" + (strictMode ? "STRICT" : "REGULAR"))) {
            Statement stat = conn.createStatement();
            for (Entry<String, TokenType> entry : TOKENS.entrySet()) {
                String s = entry.getKey();
                TokenType type = entry.getValue();
                if (strictMode && STRICT_MODE_NON_KEYWORDS.contains(s)) {
                    type = TokenType.IDENTIFIER;
                }
                Throwable exception1 = null, exception2 = null;
                try {
                    stat.execute("CREATE TABLE " + s + '(' + s + " INT)");
                    stat.execute("INSERT INTO " + s + '(' + s + ") VALUES (10)");
                } catch (Throwable t) {
                    exception1 = t;
                }
                if (exception1 == null) {
                    try {
                        try (ResultSet rs = stat.executeQuery("SELECT " + s + " FROM " + s)) {
                            assertTrue(rs.next());
                            assertEquals(10, rs.getInt(1));
                            assertFalse(rs.next());
                        }
                        try (ResultSet rs = stat.executeQuery("SELECT SUM(" + s + ") " + s + " FROM " + s + ' ' + s)) {
                            assertTrue(rs.next());
                            assertEquals(10, rs.getInt(1));
                            assertFalse(rs.next());
                            assertEquals(s, rs.getMetaData().getColumnLabel(1));
                        }
                        try (ResultSet rs = stat.executeQuery("SELECT CASE " + s + " WHEN 10 THEN 1 END FROM " + s)) {
                            assertTrue(rs.next());
                            assertEquals(1, rs.getInt(1));
                            assertFalse(rs.next());
                        }
                        stat.execute("DROP TABLE " + s);
                        stat.execute("CREATE TABLE TEST(" + s + " VARCHAR) AS VALUES '-'");
                        String str;
                        try (ResultSet rs = stat.executeQuery("SELECT TRIM(" + s + " FROM '--a--') FROM TEST")) {
                            assertTrue(rs.next());
                            str = rs.getString(1);
                        }
                        stat.execute("DROP TABLE TEST");
                        stat.execute("CREATE TABLE TEST(" + s + " INT) AS (VALUES 10)");
                        try (ResultSet rs = stat.executeQuery("SELECT " + s + " V FROM TEST")) {
                            assertTrue(rs.next());
                            assertEquals(10, rs.getInt(1));
                        }
                        try (ResultSet rs = stat.executeQuery("SELECT TEST." + s + " FROM TEST")) {
                            assertTrue(rs.next());
                            assertEquals(10, rs.getInt(1));
                        }
                        stat.execute("DROP TABLE TEST");
                        stat.execute("CREATE TABLE TEST(" + s + " INT, _VALUE_ INT DEFAULT 1) AS VALUES (2, 2)");
                        stat.execute("UPDATE TEST SET _VALUE_ = " + s);
                        try (ResultSet rs = stat.executeQuery("SELECT _VALUE_ FROM TEST")) {
                            assertTrue(rs.next());
                            assertEquals(2, rs.getInt(1));
                        }
                        stat.execute("DROP TABLE TEST");
                        try (ResultSet rs = stat.executeQuery("SELECT 1 DAY " + s)) {
                            assertEquals(s, rs.getMetaData().getColumnLabel(1));
                            assertTrue(rs.next());
                            assertEquals(Duration.ofDays(1L), rs.getObject(1, Duration.class));
                        }
                        try (ResultSet rs = stat.executeQuery("SELECT 1 = " + s + " FROM (VALUES 1) T(" + s + ')')) {
                            rs.next();
                            assertTrue(rs.getBoolean(1));
                        }
                        try (ResultSet rs = stat
                                .executeQuery("SELECT ROW_NUMBER() OVER(" + s + ") WINDOW " + s + " AS ()")) {
                        }
                        if (!"a".equals(str)) {
                            exception2 = new AssertionError();
                        }
                    } catch (Throwable t) {
                        exception2 = t;
                        stat.execute("DROP TABLE IF EXISTS TEST");
                    }
                }
                switch (type) {
                case IDENTIFIER:
                    if (exception1 != null) {
                        throw new AssertionError(s + " must be a keyword.", exception1);
                    }
                    if (exception2 != null) {
                        throw new AssertionError(s + " must be a context-sensitive keyword.", exception2);
                    }
                    break;
                case KEYWORD:
                    if (exception1 == null && exception2 == null) {
                        throw new AssertionError(s + " may be removed from a list of keywords.");
                    }
                    if (exception1 == null) {
                        throw new AssertionError(s + " may be a context-sensitive keyword.");
                    }
                    break;
                case CONTEXT_SENSITIVE_KEYWORD:
                    if (exception1 != null) {
                        throw new AssertionError(s + " must be a keyword.", exception1);
                    }
                    if (exception2 == null) {
                        throw new AssertionError(s + " may be removed from a list of context-sensitive keywords.");
                    }
                    break;
                default:
                    fail();
                }
            }
        }
    }

    private void testInformationSchema() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:")) {
            Statement stat = conn.createStatement();
            try (ResultSet rs = stat.executeQuery("SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS")) {
                while (rs.next()) {
                    String table = rs.getString(1);
                    if (isKeyword(table) && !table.equals("PARAMETERS")) {
                        fail("Table INFORMATION_SCHEMA.\"" + table
                                + "\" uses a keyword or SQL reserved word as its name.");
                    }
                    String column = rs.getString(2);
                    if (isKeyword(column)) {
                        fail("Column INFORMATION_SCHEMA." + table + ".\"" + column
                                + "\" uses a keyword or SQL reserved word as its name.");
                    }
                }
            }
        }
    }

    private static boolean isKeyword(String identifier) {
        return ALL_RESEVED_WORDS.contains(identifier) || ParserUtil.isKeyword(identifier, false);
    }

    @SuppressWarnings("incomplete-switch")
    private void testMetaData() throws Exception {
        TreeSet<String> set = new TreeSet<>();
        for (Entry<String, TokenType> entry : TOKENS.entrySet()) {
            switch (entry.getValue()) {
            case KEYWORD:
            case CONTEXT_SENSITIVE_KEYWORD: {
                String s = entry.getKey();
                if (!SQL2003_RESERVED_WORDS.contains(s)) {
                    set.add(s);
                }
            }
            }
        }
        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:")) {
            assertEquals(setToString(set), conn.getMetaData().getSQLKeywords());
        }
        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:;MODE=STRICT")) {
            TreeSet<String> set2 = new TreeSet<>(set);
            set2.removeAll(STRICT_MODE_NON_KEYWORDS);
            assertEquals(setToString(set2), conn.getMetaData().getSQLKeywords());
        }
        set.add("INTERSECTS");
        set.add("SYSDATE");
        set.add("SYSTIME");
        set.add("SYSTIMESTAMP");
        set.add("TODAY");
        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:;OLD_INFORMATION_SCHEMA=TRUE")) {
            assertEquals(setToString(set), conn.getMetaData().getSQLKeywords());
        }
    }

    private static String setToString(TreeSet<String> set) {
        Iterator<String> i = set.iterator();
        if (i.hasNext()) {
            StringBuilder builder = new StringBuilder(i.next());
            while (i.hasNext()) {
                builder.append(',').append(i.next());
            }
            return builder.toString();
        }
        return "";
    }

}
