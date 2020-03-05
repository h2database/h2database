/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.h2.api.ErrorCode;
import org.h2.command.Command;
import org.h2.command.Parser;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Mode;
import org.h2.engine.Mode.ModeEnum;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.ExpressionWithFlags;
import org.h2.expression.Format;
import org.h2.expression.Subquery;
import org.h2.expression.TypedValueExpression;
import org.h2.expression.ValueExpression;
import org.h2.expression.Variable;
import org.h2.index.Index;
import org.h2.message.DbException;
import org.h2.mode.FunctionsMSSQLServer;
import org.h2.mode.FunctionsMySQL;
import org.h2.mode.FunctionsOracle;
import org.h2.mode.FunctionsPostgreSQL;
import org.h2.mvstore.db.MVSpatialIndex;
import org.h2.schema.Domain;
import org.h2.schema.Schema;
import org.h2.schema.Sequence;
import org.h2.security.BlockCipher;
import org.h2.security.CipherFactory;
import org.h2.security.SHA3;
import org.h2.store.fs.FileUtils;
import org.h2.table.Column;
import org.h2.table.ColumnResolver;
import org.h2.table.LinkSchema;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.tools.CompressTool;
import org.h2.tools.Csv;
import org.h2.util.Bits;
import org.h2.util.DateTimeUtils;
import org.h2.util.IOUtils;
import org.h2.util.JdbcUtils;
import org.h2.util.LegacyDateTimeUtils;
import org.h2.util.MathUtils;
import org.h2.util.StringUtils;
import org.h2.util.Utils;
import org.h2.util.json.JSONByteArrayTarget;
import org.h2.util.json.JSONBytesSource;
import org.h2.util.json.JSONStringTarget;
import org.h2.util.json.JSONValidationTargetWithUniqueKeys;
import org.h2.value.DataType;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueBigint;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueCollectionBase;
import org.h2.value.ValueDate;
import org.h2.value.ValueDouble;
import org.h2.value.ValueInteger;
import org.h2.value.ValueJson;
import org.h2.value.ValueLob;
import org.h2.value.ValueNull;
import org.h2.value.ValueNumeric;
import org.h2.value.ValueReal;
import org.h2.value.ValueResultSet;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueTimestampTimeZone;
import org.h2.value.ValueUuid;
import org.h2.value.ValueVarbinary;
import org.h2.value.ValueVarchar;

/**
 * This class implements most built-in functions of this database.
 */
public class Function extends Expression implements FunctionCall, ExpressionWithFlags {
    public static final int ABS = 0, ACOS = 1, ASIN = 2, ATAN = 3, ATAN2 = 4,
            BITAND = 5, BITOR = 6, BITXOR = 7, CEILING = 8, COS = 9, COT = 10,
            DEGREES = 11, EXP = 12, FLOOR = 13, LOG = 14, LOG10 = 15, MOD = 16,
            PI = 17, POWER = 18, RADIANS = 19, RAND = 20, ROUND = 21,
            ROUNDMAGIC = 22, SIGN = 23, SIN = 24, SQRT = 25, TAN = 26,
            TRUNCATE = 27, SECURE_RAND = 28, HASH = 29, ENCRYPT = 30,
            DECRYPT = 31, COMPRESS = 32, EXPAND = 33, ZERO = 34,
            RANDOM_UUID = 35, COSH = 36, SINH = 37, TANH = 38, LN = 39,
            BITGET = 40, ORA_HASH = 41, BITNOT = 42, LSHIFT = 43, RSHIFT = 44;

    public static final int ASCII = 50, BIT_LENGTH = 51, CHAR = 52,
            CHAR_LENGTH = 53, CONCAT = 54, DIFFERENCE = 55, HEXTORAW = 56,
            INSERT = 57, INSTR = 58, LCASE = 59, LEFT = 60, LENGTH = 61,
            LOCATE = 62, LTRIM = 63, OCTET_LENGTH = 64, RAWTOHEX = 65,
            REPEAT = 66, REPLACE = 67, RIGHT = 68, RTRIM = 69, SOUNDEX = 70,
            SPACE = 71, /* 72 */ SUBSTRING = 73, UCASE = 74, LOWER = 75,
            UPPER = 76, POSITION = 77, TRIM = 78, STRINGENCODE = 79,
            STRINGDECODE = 80, STRINGTOUTF8 = 81, UTF8TOSTRING = 82,
            XMLATTR = 83, XMLNODE = 84, XMLCOMMENT = 85, XMLCDATA = 86,
            XMLSTARTDOC = 87, XMLTEXT = 88, REGEXP_REPLACE = 89, RPAD = 90,
            LPAD = 91, CONCAT_WS = 92, TO_CHAR = 93, TRANSLATE = 94, QUOTE_IDENT = 95,
            TO_DATE = 96, TO_TIMESTAMP = 97, ADD_MONTHS = 98, TO_TIMESTAMP_TZ = 99;

    public static final int CURRENT_DATE = 100, CURRENT_TIME = 101, LOCALTIME = 102,
            CURRENT_TIMESTAMP = 103, LOCALTIMESTAMP = 104,
            DATEADD = 105, DATEDIFF = 106, DAY_NAME = 107, DAY_OF_MONTH = 108,
            DAY_OF_WEEK = 109, DAY_OF_YEAR = 110, HOUR = 111, MINUTE = 112,
            MONTH = 113, MONTH_NAME = 114, QUARTER = 115,
            SECOND = 116, WEEK = 117, YEAR = 118, EXTRACT = 119,
            FORMATDATETIME = 120, PARSEDATETIME = 121, ISO_YEAR = 122,
            ISO_WEEK = 123, ISO_DAY_OF_WEEK = 124, DATE_TRUNC = 125;

    /**
     * Pseudo functions for DATEADD, DATEDIFF, and EXTRACT.
     */
    public static final int MILLISECOND = 126, EPOCH = 127, MICROSECOND = 128, NANOSECOND = 129,
            TIMEZONE_HOUR = 130, TIMEZONE_MINUTE = 131, TIMEZONE_SECOND = 132, DECADE = 133, CENTURY = 134,
            MILLENNIUM = 135, DOW = 136;

    public static final int CURRENT_CATALOG = 150, USER = 151, CURRENT_USER = 152,
            IDENTITY = 153, SCOPE_IDENTITY = 154, AUTOCOMMIT = 155,
            READONLY = 156, DATABASE_PATH = 157, LOCK_TIMEOUT = 158,
            DISK_SPACE_USED = 159, SIGNAL = 160, ESTIMATED_ENVELOPE = 161;

    private static final Pattern SIGNAL_PATTERN = Pattern.compile("[0-9A-Z]{5}");

    public static final int IFNULL = 200, CASEWHEN = 201, CONVERT = 202,
            CAST = 203, COALESCE = 204, NULLIF = 205, CASE = 206,
            NEXTVAL = 207, CURRVAL = 208, ARRAY_GET = 209, CSVREAD = 210,
            CSVWRITE = 211, MEMORY_FREE = 212, MEMORY_USED = 213,
            LOCK_MODE = 214, CURRENT_SCHEMA = 215, SESSION_ID = 216,
            CARDINALITY = 217, LINK_SCHEMA = 218, GREATEST = 219, LEAST = 220,
            CANCEL_SESSION = 221, SET = 222, TABLE = 223, TABLE_DISTINCT = 224,
            FILE_READ = 225, TRANSACTION_ID = 226, TRUNCATE_VALUE = 227,
            NVL2 = 228, DECODE = 229, ARRAY_CONTAINS = 230, FILE_WRITE = 232,
            UNNEST = 233, ARRAY_CONCAT = 234, ARRAY_APPEND = 235, ARRAY_SLICE = 236,
            ABORT_SESSION = 237;

    public static final int REGEXP_LIKE = 240;

    /**
     * Used in MySQL-style INSERT ... ON DUPLICATE KEY UPDATE ... VALUES
     */
    public static final int VALUES = 250;

    public static final int JSON_OBJECT = 251, JSON_ARRAY = 252;

    /**
     * This is called H2VERSION() and not VERSION(), because we return a fake
     * value for VERSION() when running under the PostgreSQL ODBC driver.
     */
    public static final int H2VERSION = 231;

    private static final int COUNT = JSON_ARRAY + 1;

    /**
     * The flag for TRIM(LEADING ...) function.
     */
    public static final int TRIM_LEADING = 1;

    /**
     * The flag for TRIM(TRAILING ...) function.
     */
    public static final int TRIM_TRAILING = 2;

    /**
     * The ABSENT ON NULL flag for JSON_ARRAY and JSON_OBJECT functions.
     */
    public static final int JSON_ABSENT_ON_NULL = 1;

    /**
     * The WITH UNIQUE KEYS flag for JSON_OBJECT function.
     */
    public static final int JSON_WITH_UNIQUE_KEYS = 2;

    protected static final int VAR_ARGS = -1;

    private static final FunctionInfo[] FUNCTIONS_BY_ID = new FunctionInfo[COUNT];
    private static final HashMap<String, FunctionInfo> FUNCTIONS_BY_NAME = new HashMap<>(256);
    private static final char[] SOUNDEX_INDEX = new char[128];

    protected Expression[] args;
    private int argsCount;

    protected final FunctionInfo info;
    private int flags;
    protected TypeInfo type;
    private Domain domain;

    private final Database database;

    static {
        // SOUNDEX_INDEX
        String index = "7AEIOUY8HW1BFPV2CGJKQSXZ3DT4L5MN6R";
        char number = 0;
        for (int i = 0, length = index.length(); i < length; i++) {
            char c = index.charAt(i);
            if (c < '9') {
                number = c;
            } else {
                SOUNDEX_INDEX[c] = number;
                SOUNDEX_INDEX[Character.toLowerCase(c)] = number;
            }
        }

        // FUNCTIONS
        addFunction("ABS", ABS, 1, Value.NULL);
        addFunction("ACOS", ACOS, 1, Value.DOUBLE);
        addFunction("ASIN", ASIN, 1, Value.DOUBLE);
        addFunction("ATAN", ATAN, 1, Value.DOUBLE);
        addFunction("ATAN2", ATAN2, 2, Value.DOUBLE);
        addFunction("BITAND", BITAND, 2, Value.BIGINT);
        addFunction("BITGET", BITGET, 2, Value.BOOLEAN);
        addFunction("BITNOT", BITNOT, 1, Value.BIGINT);
        addFunction("BITOR", BITOR, 2, Value.BIGINT);
        addFunction("BITXOR", BITXOR, 2, Value.BIGINT);
        addFunction("CEILING", CEILING, 1, Value.NULL);
        addFunction("CEIL", CEILING, 1, Value.NULL);
        addFunction("COS", COS, 1, Value.DOUBLE);
        addFunction("COSH", COSH, 1, Value.DOUBLE);
        addFunction("COT", COT, 1, Value.DOUBLE);
        addFunction("DEGREES", DEGREES, 1, Value.DOUBLE);
        addFunction("EXP", EXP, 1, Value.DOUBLE);
        addFunction("FLOOR", FLOOR, 1, Value.NULL);
        addFunction("LOG", LOG, VAR_ARGS, Value.DOUBLE);
        addFunction("LN", LN, 1, Value.DOUBLE);
        addFunction("LOG10", LOG10, 1, Value.DOUBLE);
        addFunction("LSHIFT", LSHIFT, 2, Value.BIGINT);
        addFunction("MOD", MOD, 2, Value.BIGINT);
        addFunction("PI", PI, 0, Value.DOUBLE);
        addFunction("POWER", POWER, 2, Value.DOUBLE);
        addFunction("RADIANS", RADIANS, 1, Value.DOUBLE);
        // RAND without argument: get the next value
        // RAND with one argument: seed the random generator
        addFunctionNotDeterministic("RAND", RAND, VAR_ARGS, Value.DOUBLE);
        addFunctionNotDeterministic("RANDOM", RAND, VAR_ARGS, Value.DOUBLE);
        addFunction("ROUND", ROUND, VAR_ARGS, Value.NULL);
        addFunction("ROUNDMAGIC", ROUNDMAGIC, 1, Value.DOUBLE);
        addFunction("RSHIFT", RSHIFT, 2, Value.BIGINT);
        addFunction("SIGN", SIGN, 1, Value.INTEGER);
        addFunction("SIN", SIN, 1, Value.DOUBLE);
        addFunction("SINH", SINH, 1, Value.DOUBLE);
        addFunction("SQRT", SQRT, 1, Value.DOUBLE);
        addFunction("TAN", TAN, 1, Value.DOUBLE);
        addFunction("TANH", TANH, 1, Value.DOUBLE);
        addFunction("TRUNCATE", TRUNCATE, VAR_ARGS, Value.NULL);
        // same as TRUNCATE
        addFunction("TRUNC", TRUNCATE, VAR_ARGS, Value.NULL);
        addFunction("HASH", HASH, VAR_ARGS, Value.VARBINARY);
        addFunction("ENCRYPT", ENCRYPT, 3, Value.VARBINARY);
        addFunction("DECRYPT", DECRYPT, 3, Value.VARBINARY);
        addFunctionNotDeterministic("SECURE_RAND", SECURE_RAND, 1, Value.VARBINARY);
        addFunction("COMPRESS", COMPRESS, VAR_ARGS, Value.VARBINARY);
        addFunction("EXPAND", EXPAND, 1, Value.VARBINARY);
        addFunction("ZERO", ZERO, 0, Value.INTEGER);
        addFunctionNotDeterministic("RANDOM_UUID", RANDOM_UUID, 0, Value.UUID);
        addFunctionNotDeterministic("UUID", RANDOM_UUID, 0, Value.UUID);
        addFunction("ORA_HASH", ORA_HASH, VAR_ARGS, Value.BIGINT);
        // string
        addFunction("ASCII", ASCII, 1, Value.INTEGER);
        addFunction("BIT_LENGTH", BIT_LENGTH, 1, Value.BIGINT);
        addFunction("CHAR", CHAR, 1, Value.VARCHAR);
        addFunction("CHR", CHAR, 1, Value.VARCHAR);
        addFunction("CHAR_LENGTH", CHAR_LENGTH, 1, Value.INTEGER);
        // same as CHAR_LENGTH
        addFunction("CHARACTER_LENGTH", CHAR_LENGTH, 1, Value.INTEGER);
        addFunctionWithNull("CONCAT", CONCAT, VAR_ARGS, Value.VARCHAR);
        addFunctionWithNull("CONCAT_WS", CONCAT_WS, VAR_ARGS, Value.VARCHAR);
        addFunction("DIFFERENCE", DIFFERENCE, 2, Value.INTEGER);
        addFunction("HEXTORAW", HEXTORAW, 1, Value.NULL);
        addFunctionWithNull("INSERT", INSERT, 4, Value.VARCHAR);
        addFunction("LCASE", LCASE, 1, Value.VARCHAR);
        addFunction("LEFT", LEFT, 2, Value.VARCHAR);
        addFunction("LENGTH", LENGTH, 1, Value.BIGINT);
        // 2 or 3 arguments
        addFunction("LOCATE", LOCATE, VAR_ARGS, Value.INTEGER);
        // same as LOCATE with 2 arguments
        addFunction("POSITION", LOCATE, 2, Value.INTEGER);
        addFunction("INSTR", INSTR, VAR_ARGS, Value.INTEGER);
        addFunction("LTRIM", LTRIM, VAR_ARGS, Value.VARCHAR);
        addFunction("OCTET_LENGTH", OCTET_LENGTH, 1, Value.BIGINT);
        addFunction("RAWTOHEX", RAWTOHEX, 1, Value.VARCHAR);
        addFunction("REPEAT", REPEAT, 2, Value.VARCHAR);
        addFunctionWithNull("REPLACE", REPLACE, VAR_ARGS, Value.VARCHAR);
        addFunction("RIGHT", RIGHT, 2, Value.VARCHAR);
        addFunction("RTRIM", RTRIM, VAR_ARGS, Value.VARCHAR);
        addFunction("SOUNDEX", SOUNDEX, 1, Value.VARCHAR);
        addFunction("SPACE", SPACE, 1, Value.VARCHAR);
        addFunction("SUBSTR", SUBSTRING, VAR_ARGS, Value.NULL);
        addFunction("SUBSTRING", SUBSTRING, VAR_ARGS, Value.NULL);
        addFunction("UCASE", UCASE, 1, Value.VARCHAR);
        addFunction("LOWER", LOWER, 1, Value.VARCHAR);
        addFunction("UPPER", UPPER, 1, Value.VARCHAR);
        addFunction("POSITION", POSITION, 2, Value.INTEGER);
        addFunction("TRIM", TRIM, VAR_ARGS, Value.VARCHAR);
        addFunction("STRINGENCODE", STRINGENCODE, 1, Value.VARCHAR);
        addFunction("STRINGDECODE", STRINGDECODE, 1, Value.VARCHAR);
        addFunction("STRINGTOUTF8", STRINGTOUTF8, 1, Value.VARBINARY);
        addFunction("UTF8TOSTRING", UTF8TOSTRING, 1, Value.VARCHAR);
        addFunction("XMLATTR", XMLATTR, 2, Value.VARCHAR);
        addFunctionWithNull("XMLNODE", XMLNODE, VAR_ARGS, Value.VARCHAR);
        addFunction("XMLCOMMENT", XMLCOMMENT, 1, Value.VARCHAR);
        addFunction("XMLCDATA", XMLCDATA, 1, Value.VARCHAR);
        addFunction("XMLSTARTDOC", XMLSTARTDOC, 0, Value.VARCHAR);
        addFunction("XMLTEXT", XMLTEXT, VAR_ARGS, Value.VARCHAR);
        addFunction("REGEXP_REPLACE", REGEXP_REPLACE, VAR_ARGS, Value.VARCHAR);
        addFunction("RPAD", RPAD, VAR_ARGS, Value.VARCHAR);
        addFunction("LPAD", LPAD, VAR_ARGS, Value.VARCHAR);
        addFunction("TO_CHAR", TO_CHAR, VAR_ARGS, Value.VARCHAR);
        addFunction("TRANSLATE", TRANSLATE, 3, Value.VARCHAR);
        addFunction("QUOTE_IDENT", QUOTE_IDENT, 1, Value.VARCHAR);
        addFunction("REGEXP_LIKE", REGEXP_LIKE, VAR_ARGS, Value.BOOLEAN);

        // date
        addFunctionNotDeterministic("CURRENT_DATE", CURRENT_DATE, 0, Value.DATE, false);
        addFunctionNotDeterministic("CURDATE", CURRENT_DATE, 0, Value.DATE);
        addFunctionNotDeterministic("SYSDATE", CURRENT_DATE, 0, Value.DATE, false);
        addFunctionNotDeterministic("TODAY", CURRENT_DATE, 0, Value.DATE, false);

        addFunctionNotDeterministic("CURRENT_TIME", CURRENT_TIME, VAR_ARGS, Value.TIME_TZ, false);

        addFunctionNotDeterministic("LOCALTIME", LOCALTIME, VAR_ARGS, Value.TIME, false);
        addFunctionNotDeterministic("SYSTIME", LOCALTIME, 0, Value.TIME, false);
        addFunctionNotDeterministic("CURTIME", LOCALTIME, VAR_ARGS, Value.TIME);

        addFunctionNotDeterministic("CURRENT_TIMESTAMP", CURRENT_TIMESTAMP, VAR_ARGS, Value.TIMESTAMP_TZ, false);
        addFunctionNotDeterministic("SYSTIMESTAMP", CURRENT_TIMESTAMP, VAR_ARGS, Value.TIMESTAMP_TZ, false);

        addFunctionNotDeterministic("LOCALTIMESTAMP", LOCALTIMESTAMP, VAR_ARGS, Value.TIMESTAMP, false);
        addFunctionNotDeterministic("NOW", LOCALTIMESTAMP, VAR_ARGS, Value.TIMESTAMP);

        addFunction("TO_DATE", TO_DATE, VAR_ARGS, Value.TIMESTAMP);
        addFunction("TO_TIMESTAMP", TO_TIMESTAMP, VAR_ARGS, Value.TIMESTAMP);
        addFunction("ADD_MONTHS", ADD_MONTHS, 2, Value.TIMESTAMP);
        addFunction("TO_TIMESTAMP_TZ", TO_TIMESTAMP_TZ, VAR_ARGS, Value.TIMESTAMP_TZ);
        addFunction("DATEADD", DATEADD, 3, Value.TIMESTAMP);
        addFunction("TIMESTAMPADD", DATEADD, 3, Value.TIMESTAMP);
        addFunction("DATEDIFF", DATEDIFF, 3, Value.BIGINT);
        addFunction("TIMESTAMPDIFF", DATEDIFF, 3, Value.BIGINT);
        addFunction("DAYNAME", DAY_NAME,
                1, Value.VARCHAR);
        addFunction("DAYNAME", DAY_NAME,
                1, Value.VARCHAR);
        addFunction("DAY", DAY_OF_MONTH,
                1, Value.INTEGER);
        addFunction("DAY_OF_MONTH", DAY_OF_MONTH,
                1, Value.INTEGER);
        addFunction("DAY_OF_WEEK", DAY_OF_WEEK,
                1, Value.INTEGER);
        addFunction("DAY_OF_YEAR", DAY_OF_YEAR,
                1, Value.INTEGER);
        addFunction("DAYOFMONTH", DAY_OF_MONTH,
                1, Value.INTEGER);
        addFunction("DAYOFWEEK", DAY_OF_WEEK,
                1, Value.INTEGER);
        addFunction("DAYOFYEAR", DAY_OF_YEAR,
                1, Value.INTEGER);
        addFunction("HOUR", HOUR,
                1, Value.INTEGER);
        addFunction("MINUTE", MINUTE,
                1, Value.INTEGER);
        addFunction("MONTH", MONTH,
                1, Value.INTEGER);
        addFunction("MONTHNAME", MONTH_NAME,
                1, Value.VARCHAR);
        addFunction("QUARTER", QUARTER,
                1, Value.INTEGER);
        addFunction("SECOND", SECOND,
                1, Value.INTEGER);
        addFunction("WEEK", WEEK,
                1, Value.INTEGER);
        addFunction("YEAR", YEAR,
                1, Value.INTEGER);
        addFunction("EXTRACT", EXTRACT,
                2, Value.INTEGER);
        addFunctionWithNull("FORMATDATETIME", FORMATDATETIME,
                VAR_ARGS, Value.VARCHAR);
        addFunctionWithNull("PARSEDATETIME", PARSEDATETIME,
                VAR_ARGS, Value.TIMESTAMP);
        addFunction("ISO_YEAR", ISO_YEAR,
                1, Value.INTEGER);
        addFunction("ISO_WEEK", ISO_WEEK,
                1, Value.INTEGER);
        addFunction("ISO_DAY_OF_WEEK", ISO_DAY_OF_WEEK,
                1, Value.INTEGER);
        addFunction("DATE_TRUNC", DATE_TRUNC, 2, Value.NULL);
        // system
        addFunctionNotDeterministic("CURRENT_CATALOG", CURRENT_CATALOG, 0, Value.VARCHAR, false);
        addFunctionNotDeterministic("DATABASE", CURRENT_CATALOG, 0, Value.VARCHAR);
        addFunctionNotDeterministic("USER", USER,
                0, Value.VARCHAR);
        addFunctionNotDeterministic("CURRENT_USER", CURRENT_USER, 0, Value.VARCHAR, false);
        addFunctionNotDeterministic("IDENTITY", IDENTITY,
                0, Value.BIGINT);
        addFunctionNotDeterministic("SCOPE_IDENTITY", SCOPE_IDENTITY,
                0, Value.BIGINT);
        addFunctionNotDeterministic("IDENTITY_VAL_LOCAL", IDENTITY,
                0, Value.BIGINT);
        addFunctionNotDeterministic("LASTVAL", IDENTITY,
                0, Value.BIGINT);
        addFunctionNotDeterministic("AUTOCOMMIT", AUTOCOMMIT,
                0, Value.BOOLEAN);
        addFunctionNotDeterministic("READONLY", READONLY,
                0, Value.BOOLEAN);
        addFunction("DATABASE_PATH", DATABASE_PATH,
                0, Value.VARCHAR);
        addFunctionNotDeterministic("LOCK_TIMEOUT", LOCK_TIMEOUT,
                0, Value.INTEGER);
        addFunctionWithNull("IFNULL", IFNULL,
                2, Value.NULL);
        addFunctionWithNull("ISNULL", IFNULL,
                2, Value.NULL);
        addFunctionWithNull("CASEWHEN", CASEWHEN,
                3, Value.NULL);
        addFunctionWithNull("CONVERT", CONVERT,
                1, Value.NULL);
        addFunctionWithNull("CAST", CAST,
                1, Value.NULL);
        addFunctionWithNull("TRUNCATE_VALUE", TRUNCATE_VALUE,
                3, Value.NULL);
        addFunctionWithNull("COALESCE", COALESCE,
                VAR_ARGS, Value.NULL);
        addFunctionWithNull("NVL", COALESCE,
                VAR_ARGS, Value.NULL);
        addFunctionWithNull("NVL2", NVL2,
                3, Value.NULL);
        addFunctionWithNull("NULLIF", NULLIF,
                2, Value.NULL);
        addFunctionWithNull("CASE", CASE,
                VAR_ARGS, Value.NULL);
        addFunctionNotDeterministic("NEXTVAL", NEXTVAL, VAR_ARGS, Value.NULL);
        addFunctionNotDeterministic("CURRVAL", CURRVAL, VAR_ARGS, Value.NULL);
        addFunction("ARRAY_GET", ARRAY_GET,
                2, Value.NULL);
        addFunctionWithNull("ARRAY_CONTAINS", ARRAY_CONTAINS, 2, Value.BOOLEAN);
        addFunction("ARRAY_CAT", ARRAY_CONCAT, 2, Value.ARRAY);
        addFunction("ARRAY_APPEND", ARRAY_APPEND, 2, Value.ARRAY);
        addFunction("ARRAY_SLICE", ARRAY_SLICE, 3, Value.ARRAY);
        addFunction("CSVREAD", CSVREAD,
                VAR_ARGS, Value.RESULT_SET, false, false, true, false);
        addFunction("CSVWRITE", CSVWRITE,
                VAR_ARGS, Value.INTEGER, false, false, true, false);
        addFunctionNotDeterministic("MEMORY_FREE", MEMORY_FREE,
                0, Value.INTEGER);
        addFunctionNotDeterministic("MEMORY_USED", MEMORY_USED,
                0, Value.INTEGER);
        addFunctionNotDeterministic("LOCK_MODE", LOCK_MODE,
                0, Value.INTEGER);
        addFunctionNotDeterministic("CURRENT_SCHEMA", CURRENT_SCHEMA, 0, Value.VARCHAR, false);
        addFunctionNotDeterministic("SCHEMA", CURRENT_SCHEMA, 0, Value.VARCHAR);
        addFunctionNotDeterministic("SESSION_ID", SESSION_ID,
                0, Value.INTEGER);
        addFunction("CARDINALITY", CARDINALITY, 1, Value.INTEGER);
        addFunction("ARRAY_LENGTH", CARDINALITY, 1, Value.INTEGER);
        addFunctionNotDeterministic("LINK_SCHEMA", LINK_SCHEMA,
                6, Value.RESULT_SET);
        addFunctionWithNull("LEAST", LEAST,
                VAR_ARGS, Value.NULL);
        addFunctionWithNull("GREATEST", GREATEST,
                VAR_ARGS, Value.NULL);
        addFunctionNotDeterministic("CANCEL_SESSION", CANCEL_SESSION,
                1, Value.BOOLEAN);
        addFunctionNotDeterministic("ABORT_SESSION", ABORT_SESSION,
                1, Value.BOOLEAN);
        addFunction("SET", SET,
                2, Value.NULL, false, false, true, false);
        addFunction("FILE_READ", FILE_READ,
                VAR_ARGS, Value.NULL, false, false, true, false);
        addFunction("FILE_WRITE", FILE_WRITE,
                2, Value.BIGINT, false, false, true, false);
        addFunctionNotDeterministic("TRANSACTION_ID", TRANSACTION_ID,
                0, Value.VARCHAR);
        addFunctionWithNull("DECODE", DECODE,
                VAR_ARGS, Value.NULL);
        addFunctionNotDeterministic("DISK_SPACE_USED", DISK_SPACE_USED,
                1, Value.BIGINT);
        addFunctionWithNull("SIGNAL", SIGNAL, 2, Value.NULL);
        addFunctionNotDeterministic("ESTIMATED_ENVELOPE", ESTIMATED_ENVELOPE, 2, Value.BIGINT);
        addFunction("H2VERSION", H2VERSION, 0, Value.VARCHAR);

        // TableFunction
        addFunctionWithNull("TABLE", TABLE, VAR_ARGS, Value.RESULT_SET);
        addFunctionWithNull("TABLE_DISTINCT", TABLE_DISTINCT, VAR_ARGS, Value.RESULT_SET);
        addFunctionWithNull("UNNEST", UNNEST, VAR_ARGS, Value.RESULT_SET);

        // ON DUPLICATE KEY VALUES function
        addFunction("VALUES", VALUES, 1, Value.NULL, false, true, true, false);

        addFunction("JSON_ARRAY", JSON_ARRAY, VAR_ARGS, Value.JSON, false, true, true, true);
        addFunction("JSON_OBJECT", JSON_OBJECT, VAR_ARGS, Value.JSON, false, true, true, true);
    }

    private static void addFunction(String name, int type, int parameterCount,
            int returnDataType, boolean nullIfParameterIsNull, boolean deterministic,
            boolean requireParentheses, boolean specialArguments) {
        FunctionInfo info = new FunctionInfo(name, type, parameterCount, returnDataType, nullIfParameterIsNull,
                deterministic, requireParentheses, specialArguments);
        if (FUNCTIONS_BY_ID[type] == null) {
            FUNCTIONS_BY_ID[type] = info;
        }
        FUNCTIONS_BY_NAME.put(name, info);
    }

    private static void addFunctionNotDeterministic(String name, int type,
            int parameterCount, int returnDataType) {
        addFunctionNotDeterministic(name, type, parameterCount, returnDataType, true);
    }

    private static void addFunctionNotDeterministic(String name, int type,
            int parameterCount, int returnDataType, boolean requireParentheses) {
        addFunction(name, type, parameterCount, returnDataType, true, false, requireParentheses, false);
    }

    private static void addFunction(String name, int type, int parameterCount,
            int returnDataType) {
        addFunction(name, type, parameterCount, returnDataType, true, true, true, false);
    }

    private static void addFunctionWithNull(String name, int type,
            int parameterCount, int returnDataType) {
        addFunction(name, type, parameterCount, returnDataType, false, true, true, false);
    }

    /**
     * Get an instance of the given function for this database.
     *
     * @param database the database
     * @param id the function number
     * @return the function object
     */
    public static Function getFunction(Database database, int id) {
        return createFunction(database, FUNCTIONS_BY_ID[id], null);
    }

    /**
     * Get an instance of the given function for this database.
     *
     * @param database the database
     * @param id the function number
     * @param arguments the arguments
     * @return the function object
     */
    public static Function getFunctionWithArgs(Database database, int id, Expression... arguments) {
        return createFunction(database, FUNCTIONS_BY_ID[id], arguments);
    }

    /**
     * Get an instance of the given function for this database.
     * If no function with this name is found, null is returned.
     *
     * @param database the database
     * @param name the function name
     * @return the function object or null
     */
    public static Function getFunction(Database database, String name) {
        if (!database.getSettings().databaseToUpper) {
            // if not yet converted to uppercase, do it now
            name = StringUtils.toUpperEnglish(name);
        }
        FunctionInfo info = FUNCTIONS_BY_NAME.get(name);
        if (info == null) {
            switch (database.getMode().getEnum()) {
            case MSSQLServer:
                return FunctionsMSSQLServer.getFunction(database, name);
            case MySQL:
                return FunctionsMySQL.getFunction(database, name);
            case Oracle:
                return FunctionsOracle.getFunction(database, name);
            case PostgreSQL:
                return FunctionsPostgreSQL.getFunction(database, name);
            default:
                return null;
            }
        }
        return createFunction(database, info, null);
    }

    private static Function createFunction(Database database, FunctionInfo info, Expression[] arguments) {
        switch (info.type) {
        case TABLE:
        case TABLE_DISTINCT:
        case UNNEST:
            assert arguments == null;
            return new TableFunction(database, info, Long.MAX_VALUE);
        default:
            return arguments != null ? new Function(database, info, arguments) : new Function(database, info);
        }
    }

    /**
     * Returns function information for the specified function name.
     *
     * @param upperName the function name in upper case
     * @return the function information or {@code null}
     */
    public static FunctionInfo getFunctionInfo(String upperName) {
        return FUNCTIONS_BY_NAME.get(upperName);
    }

    /**
     * Creates a new instance of function.
     *
     * @param database database
     * @param info function information
     */
    public Function(Database database, FunctionInfo info) {
        this.database = database;
        this.info = info;
        int count = info.parameterCount;
        args = new Expression[count != VAR_ARGS ? count : 4];
    }

    /**
     * Creates a new instance of function.
     *
     * @param database database
     * @param info function information
     * @param arguments the arguments
     */
    public Function(Database database, FunctionInfo info, Expression[] arguments) {
        this.database = database;
        this.info = info;
        int expected = info.parameterCount, len = arguments.length;
        if (expected == VAR_ARGS) {
            checkParameterCount(len);
        } else if (expected != len) {
            throw DbException.get(ErrorCode.INVALID_PARAMETER_COUNT_2, info.name, Integer.toString(expected));
        }
        args = arguments;
    }

    /**
     * Adds the parameter expression.
     * @param param the expression
     */
    public void addParameter(Expression param) {
        int capacity = args.length;
        if (argsCount >= capacity) {
            if (info.parameterCount != VAR_ARGS) {
                throw DbException.get(ErrorCode.INVALID_PARAMETER_COUNT_2, info.name, Integer.toString(capacity));
            }
            args = Arrays.copyOf(args, capacity * 2);
        }
        args[argsCount++] = param;
    }

    @Override
    public void setFlags(int flags) {
        this.flags = flags;
    }

    @Override
    public int getFlags() {
        return flags;
    }

    @Override
    public Value getValue(Session session) {
        return getValueWithArgs(session, args);
    }

    private Value getSimpleValue(Session session, Value v0, Expression[] args,
            Value[] values) {
        Value result;
        switch (info.type) {
        case ABS:
            result = v0.getSignum() >= 0 ? v0 : v0.negate();
            break;
        case ACOS:
            result = ValueDouble.get(Math.acos(v0.getDouble()));
            break;
        case ASIN:
            result = ValueDouble.get(Math.asin(v0.getDouble()));
            break;
        case ATAN:
            result = ValueDouble.get(Math.atan(v0.getDouble()));
            break;
        case CEILING:
            result = getCeilOrFloor(v0, false);
            break;
        case COS:
            result = ValueDouble.get(Math.cos(v0.getDouble()));
            break;
        case COSH:
            result = ValueDouble.get(Math.cosh(v0.getDouble()));
            break;
        case COT: {
            double d = Math.tan(v0.getDouble());
            if (d == 0.0) {
                throw DbException.get(ErrorCode.DIVISION_BY_ZERO_1, getTraceSQL());
            }
            result = ValueDouble.get(1. / d);
            break;
        }
        case DEGREES:
            result = ValueDouble.get(Math.toDegrees(v0.getDouble()));
            break;
        case EXP:
            result = ValueDouble.get(Math.exp(v0.getDouble()));
            break;
        case FLOOR:
            result = getCeilOrFloor(v0, true);
            break;
        case LN: {
            double arg = v0.getDouble();
            if (arg <= 0) {
                throw DbException.getInvalidValueException("LN() argument", arg);
            }
            result = ValueDouble.get(Math.log(arg));
            break;
        }
        case LOG:
            result = log(v0, getNullOrValue(session, args, values, 1));
            break;
        case LOG10: {
            double arg = v0.getDouble();
            if (arg <= 0) {
                throw DbException.getInvalidValueException("LOG10() argument", arg);
            }
            result = ValueDouble.get(Math.log10(arg));
            break;
        }
        case PI:
            result = ValueDouble.get(Math.PI);
            break;
        case RADIANS:
            result = ValueDouble.get(Math.toRadians(v0.getDouble()));
            break;
        case RAND: {
            if (v0 != null) {
                session.getRandom().setSeed(v0.getInt());
            }
            result = ValueDouble.get(session.getRandom().nextDouble());
            break;
        }
        case ROUNDMAGIC:
            result = ValueDouble.get(roundMagic(v0.getDouble()));
            break;
        case SIGN:
            result = ValueInteger.get(v0.getSignum());
            break;
        case SIN:
            result = ValueDouble.get(Math.sin(v0.getDouble()));
            break;
        case SINH:
            result = ValueDouble.get(Math.sinh(v0.getDouble()));
            break;
        case SQRT:
            result = ValueDouble.get(Math.sqrt(v0.getDouble()));
            break;
        case TAN:
            result = ValueDouble.get(Math.tan(v0.getDouble()));
            break;
        case TANH:
            result = ValueDouble.get(Math.tanh(v0.getDouble()));
            break;
        case SECURE_RAND:
            result = ValueVarbinary.getNoCopy(
                    MathUtils.secureRandomBytes(v0.getInt()));
            break;
        case EXPAND:
            result = ValueVarbinary.getNoCopy(
                    CompressTool.getInstance().expand(v0.getBytesNoCopy()));
            break;
        case ZERO:
            result = ValueInteger.get(0);
            break;
        case RANDOM_UUID:
            result = ValueUuid.getNewRandom();
            break;
            // string
        case ASCII: {
            String s = v0.getString();
            if (s.isEmpty()) {
                result = ValueNull.INSTANCE;
            } else {
                result = ValueInteger.get(s.charAt(0));
            }
            break;
        }
        case BIT_LENGTH:
            result = ValueBigint.get(16 * length(v0));
            break;
        case CHAR:
            result = ValueVarchar.get(String.valueOf((char) v0.getInt()), database);
            break;
        case CHAR_LENGTH:
        case LENGTH:
            result = ValueBigint.get(length(v0));
            break;
        case OCTET_LENGTH:
            result = ValueBigint.get(2 * length(v0));
            break;
        case CONCAT_WS:
        case CONCAT: {
            result = ValueNull.INSTANCE;
            int start = 0;
            String separator = "";
            if (info.type == CONCAT_WS) {
                start = 1;
                separator = getNullOrValue(session, args, values, 0).getString();
            }
            for (int i = start; i < args.length; i++) {
                Value v = getNullOrValue(session, args, values, i);
                if (v == ValueNull.INSTANCE) {
                    continue;
                }
                if (result == ValueNull.INSTANCE) {
                    result = v;
                } else {
                    String tmp = v.getString();
                    if (!StringUtils.isNullOrEmpty(separator)
                            && !StringUtils.isNullOrEmpty(tmp)) {
                        tmp = separator + tmp;
                    }
                    result = ValueVarchar.get(result.getString() + tmp, database);
                }
            }
            if (info.type == CONCAT_WS) {
                if (separator != null && result == ValueNull.INSTANCE) {
                    result = ValueVarchar.get("", database);
                }
            }
            break;
        }
        case HEXTORAW:
            result = hexToRaw(v0.getString(), database);
            break;
        case LOWER:
        case LCASE:
            // TODO this is locale specific, need to document or provide a way
            // to set the locale
            result = ValueVarchar.get(v0.getString().toLowerCase(), database);
            break;
        case RAWTOHEX:
            result = ValueVarchar.get(rawToHex(v0, database.getMode()), database);
            break;
        case SOUNDEX:
            result = ValueVarchar.get(getSoundex(v0.getString()), database);
            break;
        case SPACE: {
            int len = Math.max(0, v0.getInt());
            char[] chars = new char[len];
            for (int i = len - 1; i >= 0; i--) {
                chars[i] = ' ';
            }
            result = ValueVarchar.get(new String(chars), database);
            break;
        }
        case UPPER:
        case UCASE:
            // TODO this is locale specific, need to document or provide a way
            // to set the locale
            result = ValueVarchar.get(v0.getString().toUpperCase(), database);
            break;
        case STRINGENCODE:
            result = ValueVarchar.get(StringUtils.javaEncode(v0.getString()), database);
            break;
        case STRINGDECODE:
            result = ValueVarchar.get(StringUtils.javaDecode(v0.getString()), database);
            break;
        case STRINGTOUTF8:
            result = ValueVarbinary.getNoCopy(v0.getString().
                    getBytes(StandardCharsets.UTF_8));
            break;
        case UTF8TOSTRING:
            result = ValueVarchar.get(new String(v0.getBytesNoCopy(), StandardCharsets.UTF_8), database);
            break;
        case XMLCOMMENT:
            result = ValueVarchar.get(StringUtils.xmlComment(v0.getString()), database);
            break;
        case XMLCDATA:
            result = ValueVarchar.get(StringUtils.xmlCData(v0.getString()), database);
            break;
        case XMLSTARTDOC:
            result = ValueVarchar.get(StringUtils.xmlStartDoc(), database);
            break;
        case CURRENT_DATE:
            result = session.currentTimestamp().convertToDate(session);
            break;
        case CURRENT_TIME:
            result = session.currentTimestamp().castTo(
                    TypeInfo.getTypeInfo(Value.TIME_TZ, 0L, v0 == null ? ValueTime.DEFAULT_SCALE : v0.getInt(), null),
                    session);
            break;
        case LOCALTIME:
            result = session.currentTimestamp().castTo(
                    TypeInfo.getTypeInfo(Value.TIME, 0L, v0 == null ? ValueTime.DEFAULT_SCALE : v0.getInt(), null),
                    session);
            break;
        case CURRENT_TIMESTAMP:
            result = session.currentTimestamp().castTo(TypeInfo.getTypeInfo(Value.TIMESTAMP_TZ, 0L,
                    v0 == null ? ValueTimestamp.DEFAULT_SCALE : v0.getInt(), null), session);
            break;
        case LOCALTIMESTAMP:
            result = session.currentTimestamp().castTo(TypeInfo.getTypeInfo(Value.TIMESTAMP, 0L,
                    v0 == null ? ValueTimestamp.DEFAULT_SCALE : v0.getInt(), null), session);
            break;
        case DAY_NAME: {
            int dayOfWeek = DateTimeUtils.getSundayDayOfWeek(DateTimeUtils.dateAndTimeFromValue(v0, session)[0]);
            result = ValueVarchar.get(DateTimeFunctions.getMonthsAndWeeks(1)[dayOfWeek], database);
            break;
        }
        case DAY_OF_MONTH:
        case DAY_OF_WEEK:
        case DAY_OF_YEAR:
        case HOUR:
        case MINUTE:
        case MONTH:
        case QUARTER:
        case ISO_YEAR:
        case ISO_WEEK:
        case ISO_DAY_OF_WEEK:
        case SECOND:
        case WEEK:
        case YEAR:
            result = ValueInteger.get(DateTimeFunctions.getIntDatePart(session, v0, info.type));
            break;
        case MONTH_NAME: {
            int month = DateTimeUtils.monthFromDateValue(DateTimeUtils.dateAndTimeFromValue(v0, session)[0]);
            result = ValueVarchar.get(DateTimeFunctions.getMonthsAndWeeks(0)[month - 1], database);
            break;
        }
        case CURRENT_CATALOG:
            result = ValueVarchar.get(database.getShortName(), database);
            break;
        case USER:
        case CURRENT_USER:
            result = ValueVarchar.get(session.getUser().getName(), database);
            break;
        case IDENTITY:
            result = session.getLastIdentity();
            break;
        case SCOPE_IDENTITY:
            result = session.getLastScopeIdentity();
            break;
        case AUTOCOMMIT:
            result = ValueBoolean.get(session.getAutoCommit());
            break;
        case READONLY:
            result = ValueBoolean.get(database.isReadOnly());
            break;
        case DATABASE_PATH: {
            String path = database.getDatabasePath();
            result = path == null ? (Value) ValueNull.INSTANCE : ValueVarchar.get(path, database);
            break;
        }
        case LOCK_TIMEOUT:
            result = ValueInteger.get(session.getLockTimeout());
            break;
        case DISK_SPACE_USED:
            result = ValueBigint.get(getDiskSpaceUsed(session, v0));
            break;
        case ESTIMATED_ENVELOPE:
            result = getEstimatedEnvelope(session, v0, values[1]);
            break;
        case CAST:
        case CONVERT:
            result = v0.castTo(type, session);
            if (domain != null) {
                domain.checkConstraints(session, result);
            }
            break;
        case MEMORY_FREE:
            session.getUser().checkAdmin();
            result = ValueInteger.get(Utils.getMemoryFree());
            break;
        case MEMORY_USED:
            session.getUser().checkAdmin();
            result = ValueInteger.get(Utils.getMemoryUsed());
            break;
        case LOCK_MODE:
            result = ValueInteger.get(database.getLockMode());
            break;
        case CURRENT_SCHEMA:
            result = ValueVarchar.get(session.getCurrentSchemaName(), database);
            break;
        case SESSION_ID:
            result = ValueInteger.get(session.getId());
            break;
        case IFNULL: {
            result = v0;
            if (v0 == ValueNull.INSTANCE) {
                result = getNullOrValue(session, args, values, 1);
            }
            result = result.convertTo(type, session);
            break;
        }
        case CASEWHEN: {
            Value v;
            if (!v0.getBoolean()) {
                v = getNullOrValue(session, args, values, 2);
            } else {
                v = getNullOrValue(session, args, values, 1);
            }
            result = v.convertTo(type, session);
            break;
        }
        case DECODE: {
            int index = -1;
            for (int i = 1, len = args.length - 1; i < len; i += 2) {
                if (session.areEqual(v0, getNullOrValue(session, args, values, i))) {
                    index = i + 1;
                    break;
                }
            }
            if (index < 0 && args.length % 2 == 0) {
                index = args.length - 1;
            }
            Value v = index < 0 ? ValueNull.INSTANCE :
                    getNullOrValue(session, args, values, index);
            result = v.convertTo(type, session);
            break;
        }
        case NVL2: {
            Value v;
            if (v0 == ValueNull.INSTANCE) {
                v = getNullOrValue(session, args, values, 2);
            } else {
                v = getNullOrValue(session, args, values, 1);
            }
            result = v.convertTo(type, session);
            break;
        }
        case COALESCE: {
            result = v0;
            for (int i = 0; i < args.length; i++) {
                Value v = getNullOrValue(session, args, values, i);
                if (v != ValueNull.INSTANCE) {
                    result = v.convertTo(type, session);
                    break;
                }
            }
            break;
        }
        case GREATEST:
        case LEAST: {
            result = ValueNull.INSTANCE;
            for (int i = 0; i < args.length; i++) {
                Value v = getNullOrValue(session, args, values, i);
                if (v != ValueNull.INSTANCE) {
                    v = v.convertTo(type, session);
                    if (result == ValueNull.INSTANCE) {
                        result = v;
                    } else {
                        int comp = session.compareTypeSafe(result, v);
                        if (info.type == GREATEST && comp < 0) {
                            result = v;
                        } else if (info.type == LEAST && comp > 0) {
                            result = v;
                        }
                    }
                }
            }
            break;
        }
        case CASE: {
            Expression then = null;
            if (v0 == null) {
                // Searched CASE expression
                // (null, when, then)
                // (null, when, then, else)
                // (null, when, then, when, then)
                // (null, when, then, when, then, else)
                for (int i = 1, len = args.length - 1; i < len; i += 2) {
                    Value when = args[i].getValue(session);
                    if (when.getBoolean()) {
                        then = args[i + 1];
                        break;
                    }
                }
            } else {
                // Simple CASE expression
                // (expr, when, then)
                // (expr, when, then, else)
                // (expr, when, then, when, then)
                // (expr, when, then, when, then, else)
                if (v0 != ValueNull.INSTANCE) {
                    for (int i = 1, len = args.length - 1; i < len; i += 2) {
                        Value when = args[i].getValue(session);
                        if (session.areEqual(v0, when)) {
                            then = args[i + 1];
                            break;
                        }
                    }
                }
            }
            if (then == null && args.length % 2 == 0) {
                // then = elsePart
                then = args[args.length - 1];
            }
            Value v = then == null ? ValueNull.INSTANCE : then.getValue(session);
            result = v.convertTo(type, session);
            break;
        }
        case ARRAY_GET: {
            Value[] list = getArray(v0);
            if (list != null) {
                Value v1 = getNullOrValue(session, args, values, 1);
                int element = v1.getInt();
                if (element < 1 || element > list.length) {
                    result = ValueNull.INSTANCE;
                } else {
                    result = list[element - 1];
                }
            } else {
                result = ValueNull.INSTANCE;
            }
            break;
        }
        case CARDINALITY: {
            Value[] list = getArray(v0);
            if (list != null) {
                result = ValueInteger.get(list.length);
            } else {
                result = ValueNull.INSTANCE;
            }
            break;
        }
        case ARRAY_CONTAINS: {
            result = ValueBoolean.FALSE;
            Value[] list = getArray(v0);
            if (list != null) {
                Value v1 = getNullOrValue(session, args, values, 1);
                for (Value v : list) {
                    if (session.areEqual(v, v1)) {
                        result = ValueBoolean.TRUE;
                        break;
                    }
                }
            } else {
                result = ValueNull.INSTANCE;
            }
            break;
        }
        case CANCEL_SESSION: {
            result = ValueBoolean.get(cancelStatement(session, v0.getInt()));
            break;
        }
        case ABORT_SESSION: {
            result = ValueBoolean.get(abortSession(session, v0.getInt()));
            break;
        }
        case TRANSACTION_ID: {
            result = session.getTransactionId();
            break;
        }
        case JSON_OBJECT:
            result = jsonObject(session, args);
            break;
        case JSON_ARRAY:
            result = jsonArray(session, args);
            break;
        default:
            result = null;
        }
        return result;
    }

    private static Value getCeilOrFloor(Value v0, boolean floor) {
        Value result;
        int t = v0.getValueType();
        if (t == Value.DOUBLE || t == Value.REAL) {
            double v = v0.getDouble();
            v = floor ? Math.floor(v) : Math.ceil(v);
            result = t == Value.DOUBLE ? ValueDouble.get(v) : ValueReal.get((float) v);
        } else {
            result = ValueNumeric
                    .get(v0.getBigDecimal().setScale(0, floor ? RoundingMode.FLOOR : RoundingMode.CEILING));
        }
        return result;
    }

    private static Value[] getArray(Value v0) {
        int t = v0.getValueType();
        Value[] list;
        if (t == Value.ARRAY || t == Value.ROW) {
            list = ((ValueCollectionBase) v0).getList();
        } else {
            list = null;
        }
        return list;
    }

    private static boolean cancelStatement(Session session, int targetSessionId) {
        session.getUser().checkAdmin();
        Session[] sessions = session.getDatabase().getSessions(false);
        for (Session s : sessions) {
            if (s.getId() == targetSessionId) {
                Command c = s.getCurrentCommand();
                if (c == null) {
                    return false;
                }
                c.cancel();
                return true;
            }
        }
        return false;
    }

    private static boolean abortSession(Session session, int targetSessionId) {
        session.getUser().checkAdmin();
        Session[] sessions = session.getDatabase().getSessions(false);
        for (Session s : sessions) {
            if (s.getId() == targetSessionId) {
                Command c = s.getCurrentCommand();
                if (c != null) {
                    c.cancel();
                }
                s.close();
                return true;
            }
        }
        return false;
    }

    private static long getDiskSpaceUsed(Session session, Value tableName) {
        return getTable(session, tableName).getDiskSpaceUsed();
    }

    private static Value getEstimatedEnvelope(Session session, Value tableName, Value columnName) {
        Table table = getTable(session, tableName);
        Column column = table.getColumn(columnName.getString());
        ArrayList<Index> indexes = table.getIndexes();
        if (indexes != null) {
            for (int i = 1, size = indexes.size(); i < size; i++) {
                Index index = indexes.get(i);
                if (index instanceof MVSpatialIndex && index.isFirstColumn(column)) {
                    return ((MVSpatialIndex) index).getEstimatedBounds(session);
                }
            }
        }
        return ValueNull.INSTANCE;
    }

    private static Table getTable(Session session, Value tableName) {
        return new Parser(session).parseTableName(tableName.getString());
    }

    /**
     * Get value transformed by expression, or null if i is out of range or
     * the input value is null.
     *
     * @param session database session
     * @param args expressions
     * @param values array of input values
     * @param i index of value of transform
     * @return value or null
     */
    protected static Value getNullOrValue(Session session, Expression[] args,
            Value[] values, int i) {
        if (i >= args.length) {
            return null;
        }
        Value v = values[i];
        if (v == null) {
            Expression e = args[i];
            if (e == null) {
                return null;
            }
            v = values[i] = e.getValue(session);
        }
        return v;
    }

    /**
     * Return the resulting value for the given expression arguments.
     *
     * @param session the session
     * @param args argument expressions
     * @return the result
     */
    protected Value getValueWithArgs(Session session, Expression[] args) {
        Value[] values = new Value[args.length];
        if (info.nullIfParameterIsNull) {
            for (int i = 0; i < args.length; i++) {
                Expression e = args[i];
                Value v = e.getValue(session);
                if (v == ValueNull.INSTANCE) {
                    return ValueNull.INSTANCE;
                }
                values[i] = v;
            }
        }
        Value v0 = info.specialArguments ? null : getNullOrValue(session, args, values, 0);
        Value resultSimple = getSimpleValue(session, v0, args, values);
        if (resultSimple != null) {
            return resultSimple;
        }
        Value v1 = getNullOrValue(session, args, values, 1);
        Value v2 = getNullOrValue(session, args, values, 2);
        Value v3 = getNullOrValue(session, args, values, 3);
        Value v4 = getNullOrValue(session, args, values, 4);
        Value v5 = getNullOrValue(session, args, values, 5);
        Value result;
        switch (info.type) {
        case ATAN2:
            result = ValueDouble.get(
                    Math.atan2(v0.getDouble(), v1.getDouble()));
            break;
        case BITAND:
            result = ValueBigint.get(v0.getLong() & v1.getLong());
            break;
        case BITGET:
            result = ValueBoolean.get((v0.getLong() & (1L << v1.getInt())) != 0);
            break;
        case BITNOT:
            result = ValueBigint.get(~v0.getLong());
            break;
        case BITOR:
            result = ValueBigint.get(v0.getLong() | v1.getLong());
            break;
        case BITXOR:
            result = ValueBigint.get(v0.getLong() ^ v1.getLong());
            break;
        case LSHIFT:
            result = ValueBigint.get(v0.getLong() << v1.getInt());
            break;
        case RSHIFT:
            result = ValueBigint.get(v0.getLong() >> v1.getInt());
            break;
        case MOD: {
            long x = v1.getLong();
            if (x == 0) {
                throw DbException.get(ErrorCode.DIVISION_BY_ZERO_1, getTraceSQL());
            }
            result = ValueBigint.get(v0.getLong() % x);
            break;
        }
        case POWER:
            result = ValueDouble.get(Math.pow(
                    v0.getDouble(), v1.getDouble()));
            break;
        case ROUND:
            result = round(v0, v1);
            break;
        case TRUNCATE:
            result = truncate(session, v0, v1);
            break;
        case HASH:
            result = getHash(v0.getString(), v1, v2 == null ? 1 : v2.getInt());
            break;
        case ENCRYPT:
            result = ValueVarbinary.getNoCopy(encrypt(v0.getString(),
                    v1.getBytesNoCopy(), v2.getBytesNoCopy()));
            break;
        case DECRYPT:
            result = ValueVarbinary.getNoCopy(decrypt(v0.getString(),
                    v1.getBytesNoCopy(), v2.getBytesNoCopy()));
            break;
        case COMPRESS: {
            String algorithm = null;
            if (v1 != null) {
                algorithm = v1.getString();
            }
            result = ValueVarbinary.getNoCopy(CompressTool.getInstance().
                    compress(v0.getBytesNoCopy(), algorithm));
            break;
        }
        case ORA_HASH:
            result = oraHash(v0,
                    v1 == null ? 0xffff_ffffL : v1.getLong(),
                    v2 == null ? 0L : v2.getLong());
            break;
        case DIFFERENCE:
            result = ValueInteger.get(getDifference(
                    v0.getString(), v1.getString()));
            break;
        case INSERT: {
            if (v1 == ValueNull.INSTANCE || v2 == ValueNull.INSTANCE) {
                result = v1;
            } else {
                result = ValueVarchar.get(insert(v0.getString(), v1.getInt(), v2.getInt(), v3.getString()), database);
            }
            break;
        }
        case LEFT:
            result = ValueVarchar.get(left(v0.getString(), v1.getInt()), database);
            break;
        case LOCATE: {
            int start = v2 == null ? 0 : v2.getInt();
            result = ValueInteger.get(locate(v0.getString(), v1.getString(), start));
            break;
        }
        case INSTR: {
            int start = v2 == null ? 0 : v2.getInt();
            result = ValueInteger.get(locate(v1.getString(), v0.getString(), start));
            break;
        }
        case REPEAT: {
            int count = Math.max(0, v1.getInt());
            result = ValueVarchar.get(repeat(v0.getString(), count), database);
            break;
        }
        case REPLACE:
            if (v0 == ValueNull.INSTANCE || v1 == ValueNull.INSTANCE
                    || v2 == ValueNull.INSTANCE && database.getMode().getEnum() != Mode.ModeEnum.Oracle) {
                result = ValueNull.INSTANCE;
            } else {
                String s0 = v0.getString();
                String s1 = v1.getString();
                String s2 = (v2 == null) ? "" : v2.getString();
                if (s2 == null) {
                    s2 = "";
                }
                result = ValueVarchar.get(StringUtils.replaceAll(s0, s1, s2), database);
            }
            break;
        case RIGHT:
            result = ValueVarchar.get(right(v0.getString(), v1.getInt()), database);
            break;
        case LTRIM:
            result = ValueVarchar.get(StringUtils.trim(v0.getString(), true, false, v1 == null ? " " : v1.getString()),
                    database);
            break;
        case TRIM:
            result = ValueVarchar.get(StringUtils.trim(v0.getString(),
                    (flags & TRIM_LEADING) != 0, (flags & TRIM_TRAILING) != 0, v1 == null ? " " : v1.getString()),
                    database);
            break;
        case RTRIM:
            result = ValueVarchar.get(StringUtils.trim(v0.getString(), false, true, v1 == null ? " " : v1.getString()),
                    database);
            break;
        case SUBSTRING:
            result = substring(v0, v1, v2);
            break;
        case POSITION:
            result = ValueInteger.get(locate(v0.getString(), v1.getString(), 0));
            break;
        case XMLATTR:
            result = ValueVarchar.get(StringUtils.xmlAttr(v0.getString(), v1.getString()), database);
            break;
        case XMLNODE: {
            String attr = v1 == null ?
                    null : v1 == ValueNull.INSTANCE ? null : v1.getString();
            String content = v2 == null ?
                    null : v2 == ValueNull.INSTANCE ? null : v2.getString();
            boolean indent = v3 == null ?
                    true : v3.getBoolean();
            result = ValueVarchar.get(StringUtils.xmlNode(v0.getString(), attr, content, indent), database);
            break;
        }
        case REGEXP_REPLACE: {
            String input = v0.getString();
            String regexp = v1.getString();
            String replacement = v2.getString();
            String regexpMode = v3 != null ? v3.getString() : null;
            result = regexpReplace(input, regexp, replacement, regexpMode);
            break;
        }
        case RPAD:
            result = ValueVarchar.get(
                    StringUtils.pad(v0.getString(), v1.getInt(), v2 == null ? null : v2.getString(), true),
                    database);
            break;
        case LPAD:
            result = ValueVarchar.get(
                    StringUtils.pad(v0.getString(), v1.getInt(), v2 == null ? null : v2.getString(), false),
                    database);
            break;
        case TO_CHAR:
            switch (v0.getValueType()){
            case Value.TIME:
            case Value.DATE:
            case Value.TIMESTAMP:
            case Value.TIMESTAMP_TZ:
                result = ValueVarchar.get(
                        ToChar.toCharDateTime(session,
                        v0,
                        v1 == null ? null : v1.getString(),
                        v2 == null ? null : v2.getString()),
                        database);
                break;
            case Value.SMALLINT:
            case Value.INTEGER:
            case Value.BIGINT:
            case Value.NUMERIC:
            case Value.DOUBLE:
            case Value.REAL:
                result = ValueVarchar.get(ToChar.toChar(v0.getBigDecimal(),
                        v1 == null ? null : v1.getString(),
                        v2 == null ? null : v2.getString()),
                        database);
                break;
            default:
                result = ValueVarchar.get(v0.getString(), database);
            }
            break;
        case TO_DATE:
            result = ToDateParser.toDate(session, v0.getString(), v1 == null ? null : v1.getString());
            break;
        case TO_TIMESTAMP:
            result = ToDateParser.toTimestamp(session, v0.getString(), v1 == null ? null : v1.getString());
            break;
        case ADD_MONTHS:
            result = DateTimeFunctions.dateadd(session, "MONTH", v1.getInt(), v0);
            break;
        case TO_TIMESTAMP_TZ:
            result = ToDateParser.toTimestampTz(session, v0.getString(), v1 == null ? null : v1.getString());
            break;
        case TRANSLATE: {
            String matching = v1.getString();
            String replacement = v2.getString();
            if (database.getMode().getEnum() == ModeEnum.DB2) {
                String t = matching;
                matching = replacement;
                replacement = t;
            }
            result = ValueVarchar.get(translate(v0.getString(), matching, replacement), database);
            break;
        }
        case QUOTE_IDENT:
            result = ValueVarchar.get(StringUtils.quoteIdentifier(v0.getString()), database);
            break;
        case H2VERSION:
            result = ValueVarchar.get(Constants.VERSION, database);
            break;
        case DATEADD:
            result = DateTimeFunctions.dateadd(session, v0.getString(), v1.getLong(), v2);
            break;
        case DATEDIFF:
            result = ValueBigint.get(DateTimeFunctions.datediff(session, v0.getString(), v1, v2));
            break;
        case DATE_TRUNC:
            result = DateTimeFunctions.truncateDate(session, v0.getString(), v1);
            break;
        case EXTRACT:
            result = DateTimeFunctions.extract(session, v0.getString(), v1);
            break;
        case FORMATDATETIME: {
            if (v0 == ValueNull.INSTANCE || v1 == ValueNull.INSTANCE) {
                result = ValueNull.INSTANCE;
            } else {
                String locale = v2 == null ?
                        null : v2 == ValueNull.INSTANCE ? null : v2.getString();
                String tz = v3 == null ?
                        null : v3 == ValueNull.INSTANCE ? null : v3.getString();
                if (v0 instanceof ValueTimestampTimeZone) {
                    tz = DateTimeUtils.timeZoneNameFromOffsetSeconds(
                            ((ValueTimestampTimeZone) v0).getTimeZoneOffsetSeconds());
                }
                result = ValueVarchar.get(DateTimeFunctions.formatDateTime(
                        LegacyDateTimeUtils.toTimestamp(session, null, v0), v1.getString(), locale, tz), database);
            }
            break;
        }
        case PARSEDATETIME: {
            if (v0 == ValueNull.INSTANCE || v1 == ValueNull.INSTANCE) {
                result = ValueNull.INSTANCE;
            } else {
                String locale = v2 == null ?
                        null : v2 == ValueNull.INSTANCE ? null : v2.getString();
                String tz = v3 == null ?
                        null : v3 == ValueNull.INSTANCE ? null : v3.getString();
                java.util.Date d = DateTimeFunctions.parseDateTime(
                        v0.getString(), v1.getString(), locale, tz);
                result = LegacyDateTimeUtils.fromTimestamp(session, d.getTime(), 0);
            }
            break;
        }
        case NULLIF:
            result = session.areEqual(v0, v1) ? ValueNull.INSTANCE : v0;
            break;
            // system
        case NEXTVAL:
            result = session.getNextValueFor(getSequence(session, v0, v1), null);
            break;
        case CURRVAL:
            result = session.getCurrentValueFor(getSequence(session, v0, v1));
            break;
        case CSVREAD: {
            String fileName = v0.getString();
            String columnList = v1 == null ? null : v1.getString();
            Csv csv = new Csv();
            String options = v2 == null ? null : v2.getString();
            String charset = null;
            if (options != null && options.indexOf('=') >= 0) {
                charset = csv.setOptions(options);
            } else {
                charset = options;
                String fieldSeparatorRead = v3 == null ? null : v3.getString();
                String fieldDelimiter = v4 == null ? null : v4.getString();
                String escapeCharacter = v5 == null ? null : v5.getString();
                Value v6 = getNullOrValue(session, args, values, 6);
                String nullString = v6 == null ? null : v6.getString();
                setCsvDelimiterEscape(csv, fieldSeparatorRead, fieldDelimiter,
                        escapeCharacter);
                csv.setNullString(nullString);
            }
            char fieldSeparator = csv.getFieldSeparatorRead();
            String[] columns = StringUtils.arraySplit(columnList,
                    fieldSeparator, true);
            try {
                result = ValueResultSet.get(session, csv.read(fileName, columns, charset), Integer.MAX_VALUE);
            } catch (SQLException e) {
                throw DbException.convert(e);
            }
            break;
        }
        case ARRAY_CONCAT: {
            final ValueArray array = (ValueArray) v0.convertTo(TypeInfo.TYPE_ARRAY);
            final ValueArray array2 = (ValueArray) v1.convertTo(TypeInfo.TYPE_ARRAY);
            final Value[] res = Arrays.copyOf(array.getList(), array.getList().length + array2.getList().length);
            System.arraycopy(array2.getList(), 0, res, array.getList().length, array2.getList().length);
            result = ValueArray.get(res);
            break;
        }
        case ARRAY_APPEND: {
            final ValueArray array = (ValueArray) v0.convertTo(TypeInfo.TYPE_ARRAY);
            final Value[] res = Arrays.copyOf(array.getList(), array.getList().length + 1);
            res[array.getList().length] = v1;
            result = ValueArray.get(res);
            break;
        }
        case ARRAY_SLICE: {
            result = null;
            final ValueArray array = (ValueArray) v0.convertTo(TypeInfo.TYPE_ARRAY);
            // SQL is 1-based
            int index1 = v1.getInt() - 1;
            // 1-based and inclusive as postgreSQL (-1+1)
            int index2 = v2.getInt();
            // https://www.postgresql.org/docs/current/arrays.html#ARRAYS-ACCESSING
            // For historical reasons postgreSQL ignore invalid indexes
            final boolean isPG = database.getMode().getEnum() == ModeEnum.PostgreSQL;
            if (index1 > index2) {
                if (isPG)
                    result = ValueArray.get(array.getComponentType(), Value.EMPTY_VALUES);
                else
                    result = ValueNull.INSTANCE;
            } else {
                if (index1 < 0) {
                    if (isPG)
                        index1 = 0;
                    else
                        result = ValueNull.INSTANCE;
                }
                if (index2 > array.getList().length) {
                    if (isPG)
                        index2 = array.getList().length;
                    else
                        result = ValueNull.INSTANCE;
                }
            }
            if (result == null)
                result = ValueArray.get(array.getComponentType(), Arrays.copyOfRange(array.getList(), index1, index2));
            break;
        }
        case LINK_SCHEMA: {
            session.getUser().checkAdmin();
            Connection conn = session.createConnection(false);
            ResultSet rs = LinkSchema.linkSchema(conn, v0.getString(),
                    v1.getString(), v2.getString(), v3.getString(),
                    v4.getString(), v5.getString());
            result = ValueResultSet.get(session, rs, Integer.MAX_VALUE);
            break;
        }
        case CSVWRITE: {
            session.getUser().checkAdmin();
            Connection conn = session.createConnection(false);
            Csv csv = new Csv();
            String options = v2 == null ? null : v2.getString();
            String charset = null;
            if (options != null && options.indexOf('=') >= 0) {
                charset = csv.setOptions(options);
            } else {
                charset = options;
                String fieldSeparatorWrite = v3 == null ? null : v3.getString();
                String fieldDelimiter = v4 == null ? null : v4.getString();
                String escapeCharacter = v5 == null ? null : v5.getString();
                Value v6 = getNullOrValue(session, args, values, 6);
                String nullString = v6 == null ? null : v6.getString();
                Value v7 = getNullOrValue(session, args, values, 7);
                String lineSeparator = v7 == null ? null : v7.getString();
                setCsvDelimiterEscape(csv, fieldSeparatorWrite, fieldDelimiter,
                        escapeCharacter);
                csv.setNullString(nullString);
                if (lineSeparator != null) {
                    csv.setLineSeparator(lineSeparator);
                }
            }
            try {
                int rows = csv.write(conn, v0.getString(), v1.getString(),
                        charset);
                result = ValueInteger.get(rows);
            } catch (SQLException e) {
                throw DbException.convert(e);
            }
            break;
        }
        case SET: {
            Variable var = (Variable) args[0];
            session.setVariable(var.getName(), v1);
            result = v1;
            break;
        }
        case FILE_READ: {
            session.getUser().checkAdmin();
            String fileName = v0.getString();
            boolean blob = args.length == 1;
            ValueLob lob;
            try {
                long fileLength = FileUtils.size(fileName);
                final InputStream in = FileUtils.newInputStream(fileName);
                try {
                    if (blob) {
                        lob = database.getLobStorage().createBlob(in, fileLength);
                    } else {
                        Reader reader;
                        if (v1 == ValueNull.INSTANCE) {
                            reader = new InputStreamReader(in);
                        } else {
                            reader = new InputStreamReader(in, v1.getString());
                        }
                        lob = database.getLobStorage().createClob(reader, fileLength);
                    }
                } finally {
                    IOUtils.closeSilently(in);
                }
                result = session.addTemporaryLob(lob);
            } catch (IOException e) {
                throw DbException.convertIOException(e, fileName);
            }
            break;
        }
        case FILE_WRITE: {
            session.getUser().checkAdmin();
            result = ValueNull.INSTANCE;
            String fileName = v1.getString();
            try {
                OutputStream fileOutputStream = Files.newOutputStream(Paths.get(fileName));
                try (InputStream in = v0.getInputStream()) {
                    result = ValueBigint.get(IOUtils.copyAndClose(in,
                            fileOutputStream));
                }
            } catch (IOException e) {
                throw DbException.convertIOException(e, fileName);
            }
            break;
        }
        case TRUNCATE_VALUE:
            result = truncateValue(session, v0, v1.getLong(), v2.getBoolean());
            break;
        case XMLTEXT:
            if (v1 == null) {
                result = ValueVarchar.get(StringUtils.xmlText(v0.getString()), database);
            } else {
                result = ValueVarchar.get(StringUtils.xmlText(v0.getString(), v1.getBoolean()), database);
            }
            break;
        case REGEXP_LIKE: {
            String regexp = v1.getString();
            String regexpMode = v2 != null ? v2.getString() : null;
            int flags = makeRegexpFlags(regexpMode, false);
            try {
                result = ValueBoolean.get(Pattern.compile(regexp, flags)
                        .matcher(v0.getString()).find());
            } catch (PatternSyntaxException e) {
                throw DbException.get(ErrorCode.LIKE_ESCAPE_ERROR_1, e, regexp);
            }
            break;
        }
        case VALUES: {
            Expression a0 = args[0];
            StringBuilder builder = new StringBuilder();
            Parser.quoteIdentifier(builder, a0.getSchemaName(), DEFAULT_SQL_FLAGS).append('.');
            Parser.quoteIdentifier(builder, a0.getTableName(), DEFAULT_SQL_FLAGS).append('.');
            Parser.quoteIdentifier(builder, a0.getColumnName(), DEFAULT_SQL_FLAGS);
            result = session.getVariable(builder.toString());
            break;
        }
        case SIGNAL: {
            String sqlState = v0.getString();
            if (sqlState.startsWith("00") || !SIGNAL_PATTERN.matcher(sqlState).matches()) {
                throw DbException.getInvalidValueException("SQLSTATE", sqlState);
            }
            String msgText = v1.getString();
            throw DbException.fromUser(sqlState, msgText);
        }
        default:
            throw DbException.throwInternalError("type=" + info.type);
        }
        return result;
    }

    private Value round(Value v0, Value v1) {
        BigDecimal bd = v0.getBigDecimal().setScale(v1 == null ? 0 : v1.getInt(), RoundingMode.HALF_UP);
        Value result;
        switch (type.getValueType()) {
        case Value.DOUBLE:
            result = ValueDouble.get(bd.doubleValue());
            break;
        case Value.REAL:
            result = ValueReal.get(bd.floatValue());
            break;
        default:
            result = ValueNumeric.get(bd);
        }
        return result;
    }

    private static Value truncate(Session session, Value v0, Value v1) {
        Value result;
        int t = v0.getValueType();
        switch (t) {
        case Value.TIMESTAMP:
            result = ValueTimestamp.fromDateValueAndNanos(((ValueTimestamp) v0).getDateValue(), 0);
            break;
        case Value.DATE:
            result = ValueTimestamp.fromDateValueAndNanos(((ValueDate) v0).getDateValue(), 0);
            break;
        case Value.TIMESTAMP_TZ: {
            ValueTimestampTimeZone ts = (ValueTimestampTimeZone) v0;
            result = ValueTimestampTimeZone.fromDateValueAndNanos(ts.getDateValue(), 0,
                    ts.getTimeZoneOffsetSeconds());
            break;
        }
        case Value.VARCHAR:
            result = ValueTimestamp.fromDateValueAndNanos(
                    ValueTimestamp.parse(v0.getString(), session).getDateValue(), 0);
            break;
        default:
            int scale = v1 == null ? 0 : v1.getInt();
            if (t == Value.DOUBLE || t == Value.REAL) {
                double d = v0.getDouble();
                if (scale == 0) {
                    d = d < 0 ? Math.ceil(d) : Math.floor(d);
                } else {
                    double f = Math.pow(10, scale);
                    d *= f;
                    d = (d < 0 ? Math.ceil(d) : Math.floor(d)) / f;
                }
                result = t == Value.DOUBLE ? ValueDouble.get(d) : ValueReal.get((float) d);
            } else {
                result = ValueNumeric.get(v0.getBigDecimal().setScale(scale, RoundingMode.DOWN));
            }
            break;
        }
        return result;
    }

    private static Value truncateValue(Session session, Value value, long precision, boolean force) {
        if (precision <= 0) {
            throw DbException.get(ErrorCode.INVALID_VALUE_PRECISION, Long.toString(precision), "1",
                    "" + Integer.MAX_VALUE);
        }
        TypeInfo t = value.getType();
        int valueType = t.getValueType();
        if (DataType.getDataType(valueType).supportsPrecision) {
            if (precision < t.getPrecision()) {
                if (valueType == Value.NUMERIC) {
                    return ValueNumeric.get(value.getBigDecimal().round(new MathContext(
                            MathUtils.convertLongToInt(precision))));
                } else {
                    return value.castTo(TypeInfo.getTypeInfo(valueType, precision, t.getScale(), t.getExtTypeInfo()),
                            session);
                }
            }
        } else if (force) {
            BigDecimal bd;
            switch (valueType) {
            case Value.TINYINT:
            case Value.SMALLINT:
            case Value.INTEGER:
                bd = BigDecimal.valueOf(value.getInt());
                break;
            case Value.BIGINT:
                bd = BigDecimal.valueOf(value.getLong());
                break;
            case Value.REAL:
            case Value.DOUBLE:
                bd = value.getBigDecimal();
                break;
            default:
                return value;
            }
            bd = bd.round(new MathContext(MathUtils.convertLongToInt(precision)));
            return ValueNumeric.get(bd).convertTo(valueType);
        }
        return value;
    }

    private Sequence getSequence(Session session, Value v0, Value v1) {
        String schemaName, sequenceName;
        if (v1 == null) {
            Parser p = new Parser(session);
            String sql = v0.getString();
            Expression expr = p.parseExpression(sql);
            if (expr instanceof ExpressionColumn) {
                ExpressionColumn seq = (ExpressionColumn) expr;
                schemaName = seq.getOriginalTableAliasName();
                if (schemaName == null) {
                    schemaName = session.getCurrentSchemaName();
                    sequenceName = sql;
                } else {
                    sequenceName = seq.getColumnName();
                }
            } else {
                throw DbException.getSyntaxError(sql, 1);
            }
        } else {
            schemaName = v0.getString();
            sequenceName = v1.getString();
        }
        Schema s = database.findSchema(schemaName);
        if (s == null) {
            schemaName = StringUtils.toUpperEnglish(schemaName);
            s = database.getSchema(schemaName);
        }
        Sequence seq = s.findSequence(sequenceName);
        if (seq == null) {
            sequenceName = StringUtils.toUpperEnglish(sequenceName);
            seq = s.getSequence(sequenceName);
        }
        return seq;
    }

    private static long length(Value v) {
        switch (v.getValueType()) {
        case Value.BLOB:
        case Value.CLOB:
        case Value.VARBINARY:
        case Value.BINARY:
        case Value.JAVA_OBJECT:
            return v.getType().getPrecision();
        default:
            return v.getString().length();
        }
    }

    private Value log(Value v0, Value v1) {
        double arg = v0.getDouble();
        double r;
        Mode mode = database.getMode();
        if (v1 == null) {
            if (arg <= 0) {
                throw DbException.getInvalidValueException("LOG() argument", arg);
            }
            r = mode.logIsLogBase10 ? Math.log10(arg) : Math.log(arg);
        } else {
            double base = v1.getDouble();
            if (!mode.swapLogFunctionParameters) {
                double t = arg;
                arg = base;
                base = t;
            }
            if (arg <= 0) {
                throw DbException.getInvalidValueException("LOG() argument", arg);
            }
            if (base <= 0 || base == 1) {
                throw DbException.getInvalidValueException("LOG() base", base);
            }
            if (base == Math.E) {
                r = Math.log(arg);
            } else if (base == 10d) {
                r = Math.log10(arg);
            } else {
                r = Math.log(arg) / Math.log(base);
            }
        }
        return ValueDouble.get(r);
    }

    private static byte[] getPaddedArrayCopy(byte[] data, int blockSize) {
        int size = MathUtils.roundUpInt(data.length, blockSize);
        return Utils.copyBytes(data, size);
    }

    private static byte[] decrypt(String algorithm, byte[] key, byte[] data) {
        BlockCipher cipher = CipherFactory.getBlockCipher(algorithm);
        byte[] newKey = getPaddedArrayCopy(key, cipher.getKeyLength());
        cipher.setKey(newKey);
        byte[] newData = getPaddedArrayCopy(data, BlockCipher.ALIGN);
        cipher.decrypt(newData, 0, newData.length);
        return newData;
    }

    private static byte[] encrypt(String algorithm, byte[] key, byte[] data) {
        BlockCipher cipher = CipherFactory.getBlockCipher(algorithm);
        byte[] newKey = getPaddedArrayCopy(key, cipher.getKeyLength());
        cipher.setKey(newKey);
        byte[] newData = getPaddedArrayCopy(data, BlockCipher.ALIGN);
        cipher.encrypt(newData, 0, newData.length);
        return newData;
    }

    private static Value getHash(String algorithm, Value value, int iterations) {
        if (iterations <= 0) {
            throw DbException.getInvalidValueException("iterations", iterations);
        }
        MessageDigest md;
        switch (StringUtils.toUpperEnglish(algorithm)) {
        case "MD5":
        case "SHA-1":
        case "SHA-224":
        case "SHA-256":
        case "SHA-384":
        case "SHA-512":
            md = hashImpl(value, algorithm);
            break;
        case "SHA256":
            md = hashImpl(value, "SHA-256");
            break;
        case "SHA3-224":
            md = hashImpl(value, SHA3.getSha3_224());
            break;
        case "SHA3-256":
            md = hashImpl(value, SHA3.getSha3_256());
            break;
        case "SHA3-384":
            md = hashImpl(value, SHA3.getSha3_384());
            break;
        case "SHA3-512":
            md = hashImpl(value, SHA3.getSha3_512());
            break;
        default:
            throw DbException.getInvalidValueException("algorithm", algorithm);
        }
        byte[] b = md.digest();
        for (int i = 1; i < iterations; i++) {
            b = md.digest(b);
        }
        return ValueVarbinary.getNoCopy(b);
    }

    private Value substring(Value stringValue, Value startValue, Value lengthValue) {
        if (type.getValueType() == Value.VARBINARY) {
            byte[] s = stringValue.getBytesNoCopy();
            int sl = s.length;
            int start = startValue.getInt();
            // These compatibility conditions violate the Standard
            if (start == 0) {
                start = 1;
            } else if (start < 0) {
                start = sl + start + 1;
            }
            int end = lengthValue == null ? Math.max(sl + 1, start) : start + lengthValue.getInt();
            // SQL Standard requires "data exception - substring error" when
            // end < start but H2 does not throw it for compatibility
            start = Math.max(start, 1);
            end = Math.min(end, sl + 1);
            if (start > sl || end <= start) {
                return ValueVarbinary.EMPTY;
            }
            start--;
            end--;
            if (start == 0 && end == s.length) {
                return stringValue.convertTo(TypeInfo.TYPE_VARBINARY);
            }
            return ValueVarbinary.getNoCopy(Arrays.copyOfRange(s, start, end));
        } else {
            String s = stringValue.getString();
            int sl = s.length();
            int start = startValue.getInt();
            // These compatibility conditions violate the Standard
            if (start == 0) {
                start = 1;
            } else if (start < 0) {
                start = sl + start + 1;
            }
            int end = lengthValue == null ? Math.max(sl + 1, start) : start + lengthValue.getInt();
            // SQL Standard requires "data exception - substring error" when
            // end < start but H2 does not throw it for compatibility
            start = Math.max(start, 1);
            end = Math.min(end, sl + 1);
            if (start > sl || end <= start) {
                return database.getMode().treatEmptyStringsAsNull ? ValueNull.INSTANCE : ValueVarchar.EMPTY;
            }
            return ValueVarchar.get(s.substring(start - 1, end - 1), null);
        }
    }

    private static String repeat(String s, int count) {
        StringBuilder buff = new StringBuilder(s.length() * count);
        while (count-- > 0) {
            buff.append(s);
        }
        return buff.toString();
    }

    private static String rawToHex(Value v, Mode mode) {
        if (DataType.isBinaryStringOrSpecialBinaryType(v.getValueType())) {
            return StringUtils.convertBytesToHex(v.getBytesNoCopy());
        }
        String s = v.getString();
        if (mode.getEnum() == ModeEnum.Oracle) {
            return StringUtils.convertBytesToHex(s.getBytes(StandardCharsets.UTF_8));
        }
        int length = s.length();
        StringBuilder buff = new StringBuilder(4 * length);
        for (int i = 0; i < length; i++) {
            String hex = Integer.toHexString(s.charAt(i) & 0xffff);
            for (int j = hex.length(); j < 4; j++) {
                buff.append('0');
            }
            buff.append(hex);
        }
        return buff.toString();
    }

    private static int locate(String search, String s, int start) {
        if (start < 0) {
            int i = s.length() + start;
            return s.lastIndexOf(search, i) + 1;
        }
        int i = (start == 0) ? 0 : start - 1;
        return s.indexOf(search, i) + 1;
    }

    private static String right(String s, int count) {
        if (count < 0) {
            count = 0;
        } else if (count > s.length()) {
            count = s.length();
        }
        return s.substring(s.length() - count);
    }

    private static String left(String s, int count) {
        if (count < 0) {
            count = 0;
        } else if (count > s.length()) {
            count = s.length();
        }
        return s.substring(0, count);
    }

    private static String insert(String s1, int start, int length, String s2) {
        if (s1 == null) {
            return s2;
        }
        if (s2 == null) {
            return s1;
        }
        int len1 = s1.length();
        int len2 = s2.length();
        start--;
        if (start < 0 || length <= 0 || len2 == 0 || start > len1) {
            return s1;
        }
        if (start + length > len1) {
            length = len1 - start;
        }
        return s1.substring(0, start) + s2 + s1.substring(start + length);
    }

    private static Value hexToRaw(String s, Database database) {
        if (database.getMode().getEnum() == ModeEnum.Oracle) {
            return ValueVarbinary.get(StringUtils.convertHexToBytes(s));
        }
        int len = s.length();
        if (len % 4 != 0) {
            throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, s);
        }
        StringBuilder buff = new StringBuilder(len / 4);
        for (int i = 0; i < len; i += 4) {
            try {
                char raw = (char) Integer.parseInt(s.substring(i, i + 4), 16);
                buff.append(raw);
            } catch (NumberFormatException e) {
                throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, s);
            }
        }
        return ValueVarchar.get(buff.toString(), database);
    }

    private static int getDifference(String s1, String s2) {
        // TODO function difference: compatibility with SQL Server and HSQLDB
        s1 = getSoundex(s1);
        s2 = getSoundex(s2);
        int e = 0;
        for (int i = 0; i < 4; i++) {
            if (s1.charAt(i) == s2.charAt(i)) {
                e++;
            }
        }
        return e;
    }

    private static String translate(String original, String findChars,
            String replaceChars) {
        if (StringUtils.isNullOrEmpty(original) ||
                StringUtils.isNullOrEmpty(findChars)) {
            return original;
        }
        // if it stays null, then no replacements have been made
        StringBuilder buff = null;
        // if shorter than findChars, then characters are removed
        // (if null, we don't access replaceChars at all)
        int replaceSize = replaceChars == null ? 0 : replaceChars.length();
        for (int i = 0, size = original.length(); i < size; i++) {
            char ch = original.charAt(i);
            int index = findChars.indexOf(ch);
            if (index >= 0) {
                if (buff == null) {
                    buff = new StringBuilder(size);
                    if (i > 0) {
                        buff.append(original, 0, i);
                    }
                }
                if (index < replaceSize) {
                    ch = replaceChars.charAt(index);
                }
            }
            if (buff != null) {
                buff.append(ch);
            }
        }
        return buff == null ? original : buff.toString();
    }

    private static double roundMagic(double d) {
        if ((d < 0.000_000_000_000_1) && (d > -0.000_000_000_000_1)) {
            return 0.0;
        }
        if ((d > 1_000_000_000_000d) || (d < -1_000_000_000_000d)) {
            return d;
        }
        StringBuilder s = new StringBuilder();
        s.append(d);
        if (s.toString().indexOf('E') >= 0) {
            return d;
        }
        int len = s.length();
        if (len < 16) {
            return d;
        }
        if (s.toString().indexOf('.') > len - 3) {
            return d;
        }
        s.delete(len - 2, len);
        len -= 2;
        char c1 = s.charAt(len - 2);
        char c2 = s.charAt(len - 3);
        char c3 = s.charAt(len - 4);
        if ((c1 == '0') && (c2 == '0') && (c3 == '0')) {
            s.setCharAt(len - 1, '0');
        } else if ((c1 == '9') && (c2 == '9') && (c3 == '9')) {
            s.setCharAt(len - 1, '9');
            s.append('9');
            s.append('9');
            s.append('9');
        }
        return Double.parseDouble(s.toString());
    }

    private static String getSoundex(String s) {
        int len = s.length();
        char[] chars = { '0', '0', '0', '0' };
        char lastDigit = '0';
        for (int i = 0, j = 0; i < len && j < 4; i++) {
            char c = s.charAt(i);
            char newDigit = c > SOUNDEX_INDEX.length ?
                    0 : SOUNDEX_INDEX[c];
            if (newDigit != 0) {
                if (j == 0) {
                    chars[j++] = c;
                    lastDigit = newDigit;
                } else if (newDigit <= '6') {
                    if (newDigit != lastDigit) {
                        chars[j++] = newDigit;
                        lastDigit = newDigit;
                    }
                } else if (newDigit == '7') {
                    lastDigit = newDigit;
                }
            }
        }
        return new String(chars);
    }

    private static Value oraHash(Value value, long bucket, long seed) {
        if ((bucket & 0xffff_ffff_0000_0000L) != 0L) {
            throw DbException.getInvalidValueException("bucket", bucket);
        }
        if ((seed & 0xffff_ffff_0000_0000L) != 0L) {
            throw DbException.getInvalidValueException("seed", seed);
        }
        MessageDigest md = hashImpl(value, "SHA-1");
        if (md == null) {
            return ValueNull.INSTANCE;
        }
        if (seed != 0L) {
            byte[] b = new byte[4];
            Bits.writeInt(b, 0, (int) seed);
            md.update(b);
        }
        long hc = Bits.readLong(md.digest(), 0);
        // Strip sign and use modulo operation to get value from 0 to bucket inclusive
        return ValueBigint.get((hc & Long.MAX_VALUE) % (bucket + 1));
    }

    private static MessageDigest hashImpl(Value value, String algorithm) {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance(algorithm);
        } catch (Exception ex) {
            throw DbException.convert(ex);
        }
        return hashImpl(value, md);
    }

    private static MessageDigest hashImpl(Value value, MessageDigest md) {
        try {
            switch (value.getValueType()) {
            case Value.VARCHAR:
            case Value.CHAR:
            case Value.VARCHAR_IGNORECASE:
                md.update(value.getString().getBytes(StandardCharsets.UTF_8));
                break;
            case Value.BLOB:
            case Value.CLOB: {
                byte[] buf = new byte[4096];
                try (InputStream is = value.getInputStream()) {
                    for (int r; (r = is.read(buf)) > 0; ) {
                        md.update(buf, 0, r);
                    }
                }
                break;
            }
            default:
                md.update(value.getBytesNoCopy());
            }
            return md;
        } catch (Exception ex) {
            throw DbException.convert(ex);
        }
    }

    private Value regexpReplace(String input, String regexp, String replacement, String regexpMode) {
        Mode mode = database.getMode();
        if (mode.regexpReplaceBackslashReferences) {
            if ((replacement.indexOf('\\') >= 0) || (replacement.indexOf('$') >= 0)) {
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < replacement.length(); i++) {
                    char c = replacement.charAt(i);
                    if (c == '$') {
                        sb.append('\\');
                    } else if (c == '\\' && ++i < replacement.length()) {
                        c = replacement.charAt(i);
                        sb.append(c >= '0' && c <= '9' ? '$' : '\\');
                    }
                    sb.append(c);
                }
                replacement = sb.toString();
            }
        }
        boolean isInPostgreSqlMode = Mode.ModeEnum.PostgreSQL.equals(mode.getEnum());
        int flags = makeRegexpFlags(regexpMode, isInPostgreSqlMode);
        try {
            Matcher matcher = Pattern.compile(regexp, flags).matcher(input);
            return ValueVarchar.get(isInPostgreSqlMode && (regexpMode == null || regexpMode.indexOf('g') < 0) ?
                    matcher.replaceFirst(replacement) : matcher.replaceAll(replacement),
                    database);
        } catch (PatternSyntaxException e) {
            throw DbException.get(ErrorCode.LIKE_ESCAPE_ERROR_1, e, regexp);
        } catch (StringIndexOutOfBoundsException | IllegalArgumentException e) {
            throw DbException.get(ErrorCode.LIKE_ESCAPE_ERROR_1, e, replacement);
        }
    }

    private static int makeRegexpFlags(String stringFlags, boolean ignoreGlobalFlag) {
        int flags = Pattern.UNICODE_CASE;
        if (stringFlags != null) {
            for (int i = 0; i < stringFlags.length(); ++i) {
                switch (stringFlags.charAt(i)) {
                    case 'i':
                        flags |= Pattern.CASE_INSENSITIVE;
                        break;
                    case 'c':
                        flags &= ~Pattern.CASE_INSENSITIVE;
                        break;
                    case 'n':
                        flags |= Pattern.DOTALL;
                        break;
                    case 'm':
                        flags |= Pattern.MULTILINE;
                        break;
                    case 'g':
                        if (ignoreGlobalFlag) {
                            break;
                        }
                    //$FALL-THROUGH$
                    default:
                        throw DbException.get(ErrorCode.INVALID_VALUE_2, stringFlags);
                }
            }
        }
        return flags;
    }

    private Value jsonObject(Session session, Expression[] args) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.write('{');
        for (int i = 0, l = args.length; i < l;) {
            String name = args[i++].getValue(session).getString();
            if (name == null) {
                throw DbException.getInvalidValueException("JSON_OBJECT key", "NULL");
            }
            Value value = args[i++].getValue(session);
            if (value == ValueNull.INSTANCE) {
                if ((flags & JSON_ABSENT_ON_NULL) != 0) {
                    continue;
                } else {
                    value = ValueJson.NULL;
                }
            }
            jsonObjectAppend(baos, name, value);
        }
        return jsonObjectFinish(baos, flags);
    }

    /**
     * Appends a value to a JSON object in the specified string builder.
     *
     * @param baos the output stream to append to
     * @param key the name of the property
     * @param value the value of the property
     */
    public static void jsonObjectAppend(ByteArrayOutputStream baos, String key, Value value) {
        if (baos.size() > 1) {
            baos.write(',');
        }
        JSONByteArrayTarget.encodeString(baos, key).write(':');
        byte[] b = value.convertTo(TypeInfo.TYPE_JSON).getBytesNoCopy();
        baos.write(b, 0, b.length);
    }

    /**
     * Appends trailing closing brace to the specified string builder with a
     * JSON object, validates it, and converts to a JSON value.
     *
     * @param baos the output stream with the object
     * @param flags the flags ({@link #JSON_WITH_UNIQUE_KEYS})
     * @return the JSON value
     * @throws DbException
     *             if {@link #JSON_WITH_UNIQUE_KEYS} is specified and keys are
     *             not unique
     */
    public static Value jsonObjectFinish(ByteArrayOutputStream baos, int flags) {
        baos.write('}');
        byte[] result = baos.toByteArray();
        if ((flags & JSON_WITH_UNIQUE_KEYS) != 0) {
            try {
                JSONBytesSource.parse(result, new JSONValidationTargetWithUniqueKeys());
            } catch (RuntimeException ex) {
                String s = JSONBytesSource.parse(result, new JSONStringTarget());
                throw DbException.getInvalidValueException("JSON WITH UNIQUE KEYS",
                        s.length() < 128 ? result : s.substring(0, 128) + "...");
            }
        }
        return ValueJson.getInternal(result);
    }

    private Value jsonArray(Session session, Expression[] args) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.write('[');
        int l = args.length;
        evaluate: {
            if (l == 1) {
                Expression arg0 = args[0];
                if (arg0 instanceof Subquery) {
                    Subquery q = (Subquery) arg0;
                    for (Value value : q.getAllRows(session)) {
                        jsonArrayAppend(baos, value, flags);
                    }
                    break evaluate;
                } else if (arg0 instanceof Format) {
                    Format format = (Format) arg0;
                    arg0 = format.getSubexpression(0);
                    if (arg0 instanceof Subquery) {
                        Subquery q = (Subquery) arg0;
                        for (Value value : q.getAllRows(session)) {
                            jsonArrayAppend(baos, format.getValue(value), flags);
                        }
                        break evaluate;
                    }
                }
            }
            for (int i = 0; i < l;) {
                jsonArrayAppend(baos, args[i++].getValue(session), flags);
            }
        }
        baos.write(']');
        return ValueJson.getInternal(baos.toByteArray());
    }

    /**
     * Appends a value to a JSON array in the specified string builder.
     *
     * @param baos the output stream to append to
     * @param value the value
     * @param flags the flags ({@link #JSON_ABSENT_ON_NULL})
     */
    public static void jsonArrayAppend(ByteArrayOutputStream baos, Value value, int flags) {
        if (value == ValueNull.INSTANCE) {
            if ((flags & JSON_ABSENT_ON_NULL) != 0) {
                return;
            } else {
                value = ValueJson.NULL;
            }
        }
        if (baos.size() > 1) {
            baos.write(',');
        }
        byte[] b = value.convertTo(TypeInfo.TYPE_JSON).getBytesNoCopy();
        baos.write(b, 0, b.length);
    }

    @Override
    public TypeInfo getType() {
        return type;
    }

    @Override
    public int getValueType() {
        return type.getValueType();
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level, int state) {
        for (Expression e : args) {
            if (e != null) {
                e.mapColumns(resolver, level, state);
            }
        }
    }

    /**
     * Check if the parameter count is correct.
     *
     * @param len the number of parameters set
     * @throws DbException if the parameter count is incorrect
     */
    protected void checkParameterCount(int len) {
        int min = 0, max = Integer.MAX_VALUE;
        switch (info.type) {
        case COALESCE:
        case CSVREAD:
        case LEAST:
        case GREATEST:
            min = 1;
            break;
        case CURRENT_TIME:
        case LOCALTIME:
        case CURRENT_TIMESTAMP:
        case LOCALTIMESTAMP:
        case RAND:
            max = 1;
            break;
        case LOG:
        case COMPRESS:
        case LTRIM:
        case RTRIM:
        case TRIM:
        case FILE_READ:
        case ROUND:
        case XMLTEXT:
        case TRUNCATE:
        case TO_TIMESTAMP:
        case TO_TIMESTAMP_TZ:
        case CURRVAL:
        case NEXTVAL:
            min = 1;
            max = 2;
            break;
        case DATE_TRUNC:
            min = 2;
            max = 2;
            break;
        case TO_CHAR:
        case TO_DATE:
        case ORA_HASH:
            min = 1;
            max = 3;
            break;
        case HASH:
        case REPLACE:
        case LOCATE:
        case INSTR:
        case SUBSTRING:
        case LPAD:
        case RPAD:
        case REGEXP_LIKE:
            min = 2;
            max = 3;
            break;
        case CONCAT:
        case CONCAT_WS:
        case CSVWRITE:
            min = 2;
            break;
        case XMLNODE:
            min = 1;
            max = 4;
            break;
        case FORMATDATETIME:
        case PARSEDATETIME:
            min = 2;
            max = 4;
            break;
        case DECODE:
        case CASE:
            min = 3;
            break;
        case REGEXP_REPLACE:
            min = 3;
            max = 4;
            break;
        case JSON_OBJECT: // Ensured by Parser
        case JSON_ARRAY:
            break;
        default:
            DbException.throwInternalError("type=" + info.type);
        }
        if (len < min || len > max) {
            throw DbException.get(ErrorCode.INVALID_PARAMETER_COUNT_2, info.name, min + ".." + max);
        }
    }

    /**
     * This method is called after all the parameters have been set.
     * It checks if the parameter count is correct.
     *
     * @throws DbException if the parameter count is incorrect.
     */
    public void doneWithParameters() {
        int count = info.parameterCount;
        if (count == VAR_ARGS) {
            checkParameterCount(argsCount);
            if (args.length != argsCount) {
                args = Arrays.copyOf(args, argsCount);
            }
        } else if (count != argsCount) {
            throw DbException.get(ErrorCode.INVALID_PARAMETER_COUNT_2, info.name, Integer.toString(argsCount));
        }
    }

    public void setDataType(TypeInfo type) {
        this.type = type;
        this.domain = null;
    }

    public void setDataType(Column column) {
        this.type = column.getType();
        this.domain = column.getDomain();
    }

    @Override
    public Expression optimize(Session session) {
        boolean allConst = info.deterministic;
        for (int i = 0; i < args.length; i++) {
            Expression e = args[i];
            if (e == null) {
                continue;
            }
            e = e.optimize(session);
            args[i] = e;
            if (!e.isConstant()) {
                allConst = false;
            }
        }
        TypeInfo typeInfo;
        Expression p0 = args.length < 1 ? null : args[0];
        switch (info.type) {
        case DATEADD: {
            typeInfo = TypeInfo.TYPE_TIMESTAMP;
            if (p0.isConstant()) {
                Expression p2 = args[2];
                switch (p2.getType().getValueType()) {
                case Value.TIME:
                    typeInfo = TypeInfo.TYPE_TIME;
                    break;
                case Value.DATE: {
                    int field = DateTimeFunctions.getDatePart(p0.getValue(session).getString());
                    switch (field) {
                    case HOUR:
                    case MINUTE:
                    case SECOND:
                    case EPOCH:
                    case MILLISECOND:
                    case MICROSECOND:
                    case NANOSECOND:
                        // TIMESTAMP result
                        break;
                    default:
                        type = TypeInfo.TYPE_DATE;
                    }
                    break;
                }
                case Value.TIMESTAMP_TZ:
                    type = TypeInfo.TYPE_TIMESTAMP_TZ;
                }
            }
            break;
        }
        case EXTRACT: {
            if (p0.isConstant() && DateTimeFunctions.getDatePart(p0.getValue(session).getString()) == Function.EPOCH) {
                typeInfo = TypeInfo.getTypeInfo(Value.NUMERIC, ValueBigint.PRECISION + ValueTimestamp.MAXIMUM_SCALE,
                        ValueTimestamp.MAXIMUM_SCALE, null);
            } else {
                typeInfo = TypeInfo.TYPE_INTEGER;
            }
            break;
        }
        case DATE_TRUNC:
            typeInfo = args[1].getType();
            // TODO set scale when possible
            if (typeInfo.getValueType() != Value.TIMESTAMP_TZ) {
                typeInfo = TypeInfo.TYPE_TIMESTAMP;
            }
            break;
        case IFNULL:
        case NULLIF:
        case COALESCE:
        case LEAST:
        case GREATEST: {
            typeInfo = TypeInfo.TYPE_UNKNOWN;
            for (Expression e : args) {
                if (!e.isNullConstant()) {
                    TypeInfo type = e.getType();
                    int valueType = type.getValueType();
                    if (valueType != Value.UNKNOWN && valueType != Value.NULL) {
                        typeInfo = Value.getHigherType(typeInfo, type);
                    }
                }
            }
            if (typeInfo.getValueType() == Value.UNKNOWN) {
                typeInfo = TypeInfo.TYPE_VARCHAR;
            }
            break;
        }
        case CASE:
        case DECODE: {
            typeInfo = TypeInfo.TYPE_UNKNOWN;
            // (expr, when, then)
            // (expr, when, then, else)
            // (expr, when, then, when, then)
            // (expr, when, then, when, then, else)
            for (int i = 2, len = args.length; i < len; i += 2) {
                Expression then = args[i];
                if (!then.isNullConstant()) {
                    TypeInfo type = then.getType();
                    int valueType = type.getValueType();
                    if (valueType != Value.UNKNOWN && valueType != Value.NULL) {
                        typeInfo = Value.getHigherType(typeInfo, type);
                    }
                }
            }
            if (args.length % 2 == 0) {
                Expression elsePart = args[args.length - 1];
                if (!elsePart.isNullConstant()) {
                    TypeInfo type = elsePart.getType();
                    int valueType = type.getValueType();
                    if (valueType != Value.UNKNOWN && valueType != Value.NULL) {
                        typeInfo = Value.getHigherType(typeInfo, type);
                    }
                }
            }
            if (typeInfo.getValueType() == Value.UNKNOWN) {
                typeInfo = TypeInfo.TYPE_VARCHAR;
            }
            break;
        }
        case CASEWHEN:
            typeInfo = Value.getHigherType(args[1].getType(), args[2].getType());
            break;
        case NVL2: {
            TypeInfo t1 = args[1].getType(), t2 = args[2].getType();
            switch (t1.getValueType()) {
            case Value.VARCHAR:
            case Value.CLOB:
            case Value.CHAR:
            case Value.VARCHAR_IGNORECASE:
                typeInfo = TypeInfo.getTypeInfo(t1.getValueType(), -1, 0, null);
                break;
            default:
                typeInfo = Value.getHigherType(t1, t2);
                break;
            }
            break;
        }
        case CAST:
        case CONVERT:
            typeInfo = type;
            if (allConst) {
                Value v = getValue(session);
                if (v == ValueNull.INSTANCE) {
                    return TypedValueExpression.get(ValueNull.INSTANCE, typeInfo);
                }
                int src = p0.getType().getValueType(), dst = typeInfo.getValueType();
                if (canOptimizeCast(src, dst)) {
                    DataType dt = DataType.getDataType(dst);
                    TypeInfo vt = v.getType();
                    if (dt.supportsPrecision && typeInfo.getPrecision() != vt.getPrecision()
                            || dt.supportsScale && typeInfo.getScale() != vt.getScale()) {
                        return TypedValueExpression.get(v, typeInfo);
                    }
                    break;
                }
                return this;
            }
            break;
        case TRUNCATE_VALUE:
            if (type != null) {
                // data type, precision and scale is already set
                typeInfo = type;
            } else {
                typeInfo = TypeInfo.TYPE_UNKNOWN;
            }
            break;
        case CEILING:
        case FLOOR:
        case ROUND:
            switch (p0.getType().getValueType()) {
            case Value.DOUBLE:
                typeInfo = TypeInfo.TYPE_DOUBLE;
                break;
            case Value.REAL:
                typeInfo = TypeInfo.TYPE_REAL;
                break;
            default:
                typeInfo = getRoundNumericType(session);
            }
            break;
        case TRUNCATE:
            switch (p0.getType().getValueType()) {
            case Value.DOUBLE:
                typeInfo = TypeInfo.TYPE_DOUBLE;
                break;
            case Value.REAL:
                typeInfo = TypeInfo.TYPE_REAL;
                break;
            case Value.VARCHAR:
            case Value.DATE:
            case Value.TIMESTAMP:
                if (args.length > 1) {
                    throw DbException.get(ErrorCode.INVALID_PARAMETER_COUNT_2, info.name, "1");
                }
                typeInfo = TypeInfo.getTypeInfo(Value.TIMESTAMP, -1, 0, null);
                break;
            case Value.TIMESTAMP_TZ:
                if (args.length > 1) {
                    throw DbException.get(ErrorCode.INVALID_PARAMETER_COUNT_2, info.name, "1");
                }
                typeInfo = TypeInfo.getTypeInfo(Value.TIMESTAMP_TZ, -1, 0, null);
                break;
            default:
                typeInfo = getRoundNumericType(session);
            }
            break;
        case ABS: {
            TypeInfo type = p0.getType();
            typeInfo = type;
            if (typeInfo.getValueType() == Value.NULL) {
                typeInfo = TypeInfo.TYPE_INTEGER;
            }
            break;
        }
        case SET:
            typeInfo = args[1].getType();
            if (!(p0 instanceof Variable)) {
                throw DbException.get(ErrorCode.CAN_ONLY_ASSIGN_TO_VARIABLE_1, p0.getTraceSQL());
            }
            break;
        case FILE_READ: {
            if (args.length == 1) {
                typeInfo = TypeInfo.getTypeInfo(Value.BLOB, Integer.MAX_VALUE, 0, null);
            } else {
                typeInfo = TypeInfo.getTypeInfo(Value.CLOB, Integer.MAX_VALUE, 0, null);
            }
            break;
        }
        case SUBSTRING: {
            TypeInfo argType = args[0].getType();
            long p = argType.getPrecision();
            if (args[1].isConstant()) {
                // if only two arguments are used,
                // subtract offset from first argument length
                p -= args[1].getValue(session).getLong() - 1;
            }
            if (args.length == 3 && args[2].isConstant()) {
                // if the third argument is constant it is at most this value
                p = Math.min(p, args[2].getValue(session).getLong());
            }
            p = Math.max(0, p);
            typeInfo = TypeInfo.getTypeInfo(DataType.isBinaryStringType(argType.getValueType())
                    ? Value.VARBINARY : Value.VARCHAR, p, 0, null);
            break;
        }
        case ENCRYPT:
        case DECRYPT:
            typeInfo = TypeInfo.getTypeInfo(info.returnDataType, args[2].getType().getPrecision(), 0, null);
            break;
        case COMPRESS:
            typeInfo = TypeInfo.getTypeInfo(info.returnDataType, args[0].getType().getPrecision(), 0, null);
            break;
        case CHAR:
            typeInfo = TypeInfo.getTypeInfo(info.returnDataType, 1, 0, null);
            break;
        case CONCAT: {
            long p = 0;
            for (Expression e : args) {
                TypeInfo type = e.getType();
                p += type.getPrecision();
                if (p < 0) {
                    p = Long.MAX_VALUE;
                }
            }
            typeInfo = TypeInfo.getTypeInfo(info.returnDataType, p, 0, null);
            break;
        }
        case HEXTORAW: {
            TypeInfo t = args[0].getType();
            if (database.getMode().getEnum() == ModeEnum.Oracle) {
                if (DataType.isStringType(t.getValueType())) {
                    typeInfo = TypeInfo.getTypeInfo(Value.VARBINARY, t.getPrecision() / 2, 0, null);
                } else {
                    typeInfo = TypeInfo.TYPE_VARBINARY;
                }
            } else {
                if (DataType.isStringType(t.getValueType())) {
                    typeInfo = TypeInfo.getTypeInfo(Value.VARCHAR, t.getPrecision() / 4, 0, null);
                } else {
                    typeInfo = TypeInfo.TYPE_VARCHAR;
                }
            }
            break;
        }
        case LCASE:
        case LTRIM:
        case RIGHT:
        case RTRIM:
        case UCASE:
        case LOWER:
        case UPPER:
        case TRIM:
        case STRINGDECODE:
        case UTF8TOSTRING:
            typeInfo = TypeInfo.getTypeInfo(info.returnDataType, args[0].getType().getPrecision(), 0, null);
            break;
        case RAWTOHEX: {
            TypeInfo t = args[0].getType();
            long precision = t.getPrecision();
            int mul = DataType.isBinaryStringOrSpecialBinaryType(t.getValueType()) ? 2
                    : database.getMode().getEnum() == ModeEnum.Oracle ? 6 : 4;
            typeInfo = TypeInfo.getTypeInfo(info.returnDataType,
                    precision <= Long.MAX_VALUE / mul ? precision * mul : Long.MAX_VALUE, 0, null);
            break;
        }
        case SOUNDEX:
            typeInfo = TypeInfo.getTypeInfo(info.returnDataType, 4, 0, null);
            break;
        case DAY_NAME:
        case MONTH_NAME:
            // day and month names may be long in some languages
            typeInfo = TypeInfo.getTypeInfo(info.returnDataType, 20, 0, null);
            break;
        case NEXTVAL:
        case CURRVAL:
            typeInfo = database.getMode().decimalSequences ? TypeInfo.TYPE_NUMERIC_BIGINT : TypeInfo.TYPE_BIGINT;
            break;
        default:
            typeInfo = TypeInfo.getTypeInfo(info.returnDataType, -1, -1, null);
        }
        type = typeInfo;
        if (allConst) {
            return ValueExpression.get(getValue(session));
        }
        return this;
    }

    private static boolean canOptimizeCast(int src, int dst) {
        switch (src) {
        case Value.TIME:
            switch (dst) {
            case Value.TIME_TZ:
            case Value.TIMESTAMP:
            case Value.TIMESTAMP_TZ:
                return false;
            }
            break;
        case Value.TIME_TZ:
            switch (dst) {
            case Value.TIME:
            case Value.TIMESTAMP:
            case Value.TIMESTAMP_TZ:
                return false;
            }
            break;
        case Value.DATE:
            if (dst == Value.TIMESTAMP_TZ) {
                return false;
            }
            break;
        case Value.TIMESTAMP:
            switch (dst) {
            case Value.TIME_TZ:
            case Value.TIMESTAMP_TZ:
                return false;
            }
            break;
        case Value.TIMESTAMP_TZ:
            switch (dst) {
            case Value.TIME:
            case Value.DATE:
            case Value.TIMESTAMP:
                return false;
            }
        }
        return true;
    }

    private TypeInfo getRoundNumericType(Session session) {
        int scale = 0;
        if (args.length > 1) {
            Expression scaleExpr = args[1];
            if (scaleExpr.isConstant()) {
                Value scaleValue = scaleExpr.getValue(session);
                if (scaleValue != ValueNull.INSTANCE) {
                    scale = scaleValue.getInt();
                }
            } else {
                scale = Integer.MAX_VALUE;
            }
        }
        return TypeInfo.getTypeInfo(Value.NUMERIC, Integer.MAX_VALUE, scale, null);
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        for (Expression e : args) {
            if (e != null) {
                e.setEvaluatable(tableFilter, b);
            }
        }
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        builder.append(info.name);
        if (info.type == CASE) {
            if (args[0] != null) {
                builder.append(' ');
                args[0].getSQL(builder, sqlFlags);
            }
            for (int i = 1, len = args.length - 1; i < len; i += 2) {
                builder.append(" WHEN ");
                args[i].getSQL(builder, sqlFlags);
                builder.append(" THEN ");
                args[i + 1].getSQL(builder, sqlFlags);
            }
            if (args.length % 2 == 0) {
                builder.append(" ELSE ");
                args[args.length - 1].getSQL(builder, sqlFlags);
            }
            return builder.append(" END");
        }
        boolean addParentheses = args.length > 0 || info.requireParentheses;
        if (addParentheses) {
            builder.append('(');
        }
        switch (info.type) {
        case SUBSTRING: {
            args[0].getSQL(builder, sqlFlags).append(" FROM ");
            args[1].getSQL(builder, sqlFlags);
            if (args.length > 2) {
                builder.append(" FOR ");
                args[2].getSQL(builder, sqlFlags);
            }
            break;
        }
        case TRIM: {
            switch (flags) {
            case TRIM_LEADING:
                builder.append("LEADING ");
                break;
            case TRIM_TRAILING:
                builder.append("TRAILING ");
                break;
            }
            if (args.length > 1) {
                args[1].getSQL(builder, sqlFlags).append(" FROM ");
            }
            args[0].getSQL(builder, sqlFlags);
            break;
        }
        case CAST: {
            Expression a = args[0];
            a.getSQL(builder, a instanceof ValueExpression ? sqlFlags | NO_CASTS : sqlFlags).append(" AS ");
            if (domain != null) {
                domain.getSQL(builder, sqlFlags);
            } else {
                type.getSQL(builder);
            }
            break;
        }
        case CONVERT: {
            if (database.getMode().swapConvertFunctionParameters) {
                type.getSQL(builder).append(", ");
                args[0].getSQL(builder, sqlFlags);
            } else {
                args[0].getSQL(builder, sqlFlags).append(", ");
                type.getSQL(builder);
            }
            break;
        }
        case EXTRACT: {
            ValueVarchar v = (ValueVarchar) ((ValueExpression) args[0]).getValue(null);
            builder.append(v.getString()).append(" FROM ");
            args[1].getSQL(builder, sqlFlags);
            break;
        }
        case JSON_OBJECT: {
            for (int i = 0, l = args.length; i < l;) {
                if (i > 0) {
                    builder.append(", ");
                }
                args[i++].getSQL(builder, sqlFlags).append(": ");
                args[i++].getSQL(builder, sqlFlags);
            }
            getJsonFunctionFlagsSQL(builder, flags, false);
            break;
        }
        case JSON_ARRAY: {
            writeExpressions(builder, args, sqlFlags);
            getJsonFunctionFlagsSQL(builder, flags, true);
            break;
        }
        default:
            writeExpressions(builder, args, sqlFlags);
        }
        if (addParentheses) {
            builder.append(')');
        }
        return builder;
    }

    /**
     * Appends flags of a JSON function to the specified string builder.
     *
     * @param builder string builder to append to
     * @param flags flags to append
     * @param forArray whether the function is an array function
     */
    public static void getJsonFunctionFlagsSQL(StringBuilder builder, int flags, boolean forArray) {
        if ((flags & JSON_ABSENT_ON_NULL) != 0) {
            if (!forArray) {
                builder.append(" ABSENT ON NULL");
            }
        } else if (forArray) {
            builder.append(" NULL ON NULL");
        }
        if (!forArray && (flags & JSON_WITH_UNIQUE_KEYS) != 0) {
            builder.append(" WITH UNIQUE KEYS");
        }
    }

    @Override
    public void updateAggregate(Session session, int stage) {
        for (Expression e : args) {
            if (e != null) {
                e.updateAggregate(session, stage);
            }
        }
    }

    public int getFunctionType() {
        return info.type;
    }

    @Override
    public String getName() {
        return info.name;
    }

    @Override
    public ValueResultSet getValueForColumnList(Session session,
            Expression[] argList) {
        switch (info.type) {
        case CSVREAD: {
            String fileName = argList[0].getValue(session).getString();
            if (fileName == null) {
                throw DbException.get(ErrorCode.PARAMETER_NOT_SET_1, "fileName");
            }
            String columnList = argList.length < 2 ?
                    null : argList[1].getValue(session).getString();
            Csv csv = new Csv();
            String options = argList.length < 3 ?
                    null : argList[2].getValue(session).getString();
            String charset = null;
            if (options != null && options.indexOf('=') >= 0) {
                charset = csv.setOptions(options);
            } else {
                charset = options;
                String fieldSeparatorRead = argList.length < 4 ?
                        null : argList[3].getValue(session).getString();
                String fieldDelimiter = argList.length < 5 ?
                        null : argList[4].getValue(session).getString();
                String escapeCharacter = argList.length < 6 ?
                        null : argList[5].getValue(session).getString();
                setCsvDelimiterEscape(csv, fieldSeparatorRead, fieldDelimiter,
                        escapeCharacter);
            }
            char fieldSeparator = csv.getFieldSeparatorRead();
            String[] columns = StringUtils.arraySplit(columnList, fieldSeparator, true);
            ResultSet rs = null;
            ValueResultSet x;
            try {
                rs = csv.read(fileName, columns, charset);
                x = ValueResultSet.get(session, rs, 0);
            } catch (SQLException e) {
                throw DbException.convert(e);
            } finally {
                csv.close();
                JdbcUtils.closeSilently(rs);
            }
            return x;
        }
        default:
            break;
        }
        return (ValueResultSet) getValueWithArgs(session, argList);
    }

    private static void setCsvDelimiterEscape(Csv csv, String fieldSeparator,
            String fieldDelimiter, String escapeCharacter) {
        if (fieldSeparator != null) {
            csv.setFieldSeparatorWrite(fieldSeparator);
            if (!fieldSeparator.isEmpty()) {
                char fs = fieldSeparator.charAt(0);
                csv.setFieldSeparatorRead(fs);
            }
        }
        if (fieldDelimiter != null) {
            char fd = fieldDelimiter.isEmpty() ? 0 : fieldDelimiter.charAt(0);
            csv.setFieldDelimiter(fd);
        }
        if (escapeCharacter != null) {
            char ec = escapeCharacter.isEmpty() ? 0 : escapeCharacter.charAt(0);
            csv.setEscapeCharacter(ec);
        }
    }

    @Override
    public Expression[] getArgs() {
        return args;
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        for (Expression e : args) {
            if (e != null && !e.isEverything(visitor)) {
                return false;
            }
        }
        switch (visitor.getType()) {
        case ExpressionVisitor.DETERMINISTIC:
        case ExpressionVisitor.QUERY_COMPARABLE:
        case ExpressionVisitor.READONLY:
            return info.deterministic;
        case ExpressionVisitor.EVALUATABLE:
        case ExpressionVisitor.GET_DEPENDENCIES:
        case ExpressionVisitor.INDEPENDENT:
        case ExpressionVisitor.NOT_FROM_RESOLVER:
        case ExpressionVisitor.OPTIMIZABLE_AGGREGATE:
        case ExpressionVisitor.SET_MAX_DATA_MODIFICATION_ID:
        case ExpressionVisitor.GET_COLUMNS1:
        case ExpressionVisitor.GET_COLUMNS2:
            return true;
        default:
            throw DbException.throwInternalError("type=" + visitor.getType());
        }
    }

    @Override
    public int getCost() {
        int cost = 3;
        for (Expression e : args) {
            if (e != null) {
                cost += e.getCost();
            }
        }
        return cost;
    }

    @Override
    public boolean isDeterministic() {
        return info.deterministic;
    }

    @Override
    public int getSubexpressionCount() {
        return args.length;
    }

    @Override
    public Expression getSubexpression(int index) {
        return args[index];
    }

}
