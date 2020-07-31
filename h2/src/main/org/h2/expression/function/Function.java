/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Paths;
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
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.ExpressionWithFlags;
import org.h2.expression.OperationN;
import org.h2.expression.ValueExpression;
import org.h2.expression.Variable;
import org.h2.index.Index;
import org.h2.message.DbException;
import org.h2.mode.FunctionsDB2Derby;
import org.h2.mode.FunctionsMSSQLServer;
import org.h2.mode.FunctionsMySQL;
import org.h2.mode.FunctionsOracle;
import org.h2.mode.FunctionsPostgreSQL;
import org.h2.mvstore.db.MVSpatialIndex;
import org.h2.store.fs.FileUtils;
import org.h2.table.Column;
import org.h2.table.LinkSchema;
import org.h2.table.Table;
import org.h2.tools.Csv;
import org.h2.util.IOUtils;
import org.h2.util.JdbcUtils;
import org.h2.util.MathUtils;
import org.h2.util.StringUtils;
import org.h2.util.Utils;
import org.h2.value.DataType;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueBigint;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueCollectionBase;
import org.h2.value.ValueDate;
import org.h2.value.ValueDecfloat;
import org.h2.value.ValueDouble;
import org.h2.value.ValueInteger;
import org.h2.value.ValueLob;
import org.h2.value.ValueNull;
import org.h2.value.ValueNumeric;
import org.h2.value.ValueReal;
import org.h2.value.ValueResultSet;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueTimestampTimeZone;
import org.h2.value.ValueVarbinary;
import org.h2.value.ValueVarchar;

/**
 * This class implements most built-in functions of this database.
 */
public class Function extends OperationN implements FunctionCall, ExpressionWithFlags {
    public static final int
            ROUND = 21,
            ROUNDMAGIC = 22,
            TRUNCATE = 27;

    public static final int ASCII = 50, CHAR = 52,
            INSERT = 57, INSTR = 58, LEFT = 60,
            LOCATE = 62,
            REPEAT = 66, REPLACE = 67, RIGHT = 68,
            SUBSTRING = 73,
            POSITION = 77, TRIM = 78,
            XMLATTR = 83, XMLNODE = 84, XMLCOMMENT = 85, XMLCDATA = 86,
            XMLSTARTDOC = 87, XMLTEXT = 88, REGEXP_REPLACE = 89, RPAD = 90,
            LPAD = 91, TO_CHAR = 93, TRANSLATE = 94;

    public static final int
            AUTOCOMMIT = 155,
            READONLY = 156, DATABASE_PATH = 157, LOCK_TIMEOUT = 158,
            DISK_SPACE_USED = 159, SIGNAL = 160, ESTIMATED_ENVELOPE = 161;

    private static final Pattern SIGNAL_PATTERN = Pattern.compile("[0-9A-Z]{5}");

    public static final int
            CSVREAD = 210, CSVWRITE = 211,
            MEMORY_FREE = 212, MEMORY_USED = 213,
            LOCK_MODE = 214, SESSION_ID = 216,
            LINK_SCHEMA = 218,
            CANCEL_SESSION = 221, SET = 222, TABLE = 223, TABLE_DISTINCT = 224,
            FILE_READ = 225, TRANSACTION_ID = 226, TRUNCATE_VALUE = 227,
            ARRAY_CONTAINS = 230, FILE_WRITE = 232,
            UNNEST = 233, TRIM_ARRAY = 235, ARRAY_SLICE = 236,
            ABORT_SESSION = 237;

    public static final int REGEXP_LIKE = 240;
    public static final int REGEXP_SUBSTR = 241;

    /**
     * This is called H2VERSION() and not VERSION(), because we return a fake
     * value for VERSION() when running under the PostgreSQL ODBC driver.
     */
    public static final int H2VERSION = 231;

    private static final int COUNT = REGEXP_SUBSTR + 1;

    /**
     * The flag for TRIM(LEADING ...) function.
     */
    public static final int TRIM_LEADING = 1;

    /**
     * The flag for TRIM(TRAILING ...) function.
     */
    public static final int TRIM_TRAILING = 2;

    protected static final int VAR_ARGS = -1;

    private static final FunctionInfo[] FUNCTIONS_BY_ID = new FunctionInfo[COUNT];
    private static final HashMap<String, FunctionInfo> FUNCTIONS_BY_NAME = new HashMap<>(128);

    protected final FunctionInfo info;
    private int flags;

    static {
        addFunction("ROUND", ROUND, VAR_ARGS, Value.NULL);
        addFunction("ROUNDMAGIC", ROUNDMAGIC, 1, Value.DOUBLE);
        addFunction("TRUNCATE", TRUNCATE, VAR_ARGS, Value.NULL);
        // same as TRUNCATE
        addFunction("TRUNC", TRUNCATE, VAR_ARGS, Value.NULL);
        // string
        addFunction("ASCII", ASCII, 1, Value.INTEGER);
        addFunction("CHAR", CHAR, 1, Value.VARCHAR);
        addFunction("CHR", CHAR, 1, Value.VARCHAR);
        addFunctionWithNull("INSERT", INSERT, 4, Value.VARCHAR);
        addFunction("LEFT", LEFT, 2, Value.VARCHAR);
        // 2 or 3 arguments
        addFunction("LOCATE", LOCATE, VAR_ARGS, Value.INTEGER);
        // same as LOCATE with 2 arguments
        addFunction("POSITION", LOCATE, 2, Value.INTEGER);
        addFunction("INSTR", INSTR, VAR_ARGS, Value.INTEGER);
        addFunction("REPEAT", REPEAT, 2, Value.VARCHAR);
        addFunctionWithNull("REPLACE", REPLACE, VAR_ARGS, Value.VARCHAR);
        addFunction("RIGHT", RIGHT, 2, Value.VARCHAR);
        addFunction("SUBSTRING", SUBSTRING, VAR_ARGS, Value.NULL);
        addFunction("POSITION", POSITION, 2, Value.INTEGER);
        addFunction("TRIM", TRIM, VAR_ARGS, Value.VARCHAR);
        addFunction("XMLATTR", XMLATTR, 2, Value.VARCHAR);
        addFunctionWithNull("XMLNODE", XMLNODE, VAR_ARGS, Value.VARCHAR);
        addFunction("XMLCOMMENT", XMLCOMMENT, 1, Value.VARCHAR);
        addFunction("XMLCDATA", XMLCDATA, 1, Value.VARCHAR);
        addFunction("XMLSTARTDOC", XMLSTARTDOC, 0, Value.VARCHAR);
        addFunction("XMLTEXT", XMLTEXT, VAR_ARGS, Value.VARCHAR);
        addFunctionWithNull("REGEXP_REPLACE", REGEXP_REPLACE, VAR_ARGS, Value.VARCHAR);
        addFunction("RPAD", RPAD, VAR_ARGS, Value.VARCHAR);
        addFunction("LPAD", LPAD, VAR_ARGS, Value.VARCHAR);
        addFunction("TO_CHAR", TO_CHAR, VAR_ARGS, Value.VARCHAR);
        addFunction("TRANSLATE", TRANSLATE, 3, Value.VARCHAR);
        addFunction("REGEXP_LIKE", REGEXP_LIKE, VAR_ARGS, Value.BOOLEAN);
        addFunctionWithNull("REGEXP_SUBSTR", REGEXP_SUBSTR, VAR_ARGS, Value.VARCHAR);

        // system
        addFunctionNotDeterministic("AUTOCOMMIT", AUTOCOMMIT,
                0, Value.BOOLEAN);
        addFunctionNotDeterministic("READONLY", READONLY,
                0, Value.BOOLEAN);
        addFunction("DATABASE_PATH", DATABASE_PATH,
                0, Value.VARCHAR);
        addFunctionNotDeterministic("LOCK_TIMEOUT", LOCK_TIMEOUT,
                0, Value.INTEGER);
        addFunctionWithNull("TRUNCATE_VALUE", TRUNCATE_VALUE,
                3, Value.NULL);
        addFunctionWithNull("ARRAY_CONTAINS", ARRAY_CONTAINS, 2, Value.BOOLEAN);
        addFunctionWithNull("TRIM_ARRAY", TRIM_ARRAY, 2, Value.ARRAY);
        addFunction("ARRAY_SLICE", ARRAY_SLICE, 3, Value.ARRAY);
        addFunction("CSVREAD", CSVREAD, VAR_ARGS, Value.RESULT_SET, false, false);
        addFunction("CSVWRITE", CSVWRITE, VAR_ARGS, Value.INTEGER, false, false);
        addFunctionNotDeterministic("MEMORY_FREE", MEMORY_FREE,
                0, Value.INTEGER);
        addFunctionNotDeterministic("MEMORY_USED", MEMORY_USED,
                0, Value.INTEGER);
        addFunctionNotDeterministic("LOCK_MODE", LOCK_MODE,
                0, Value.INTEGER);
        addFunctionNotDeterministic("SESSION_ID", SESSION_ID,
                0, Value.INTEGER);
        addFunctionNotDeterministic("LINK_SCHEMA", LINK_SCHEMA,
                6, Value.RESULT_SET);
        addFunctionNotDeterministic("CANCEL_SESSION", CANCEL_SESSION,
                1, Value.BOOLEAN);
        addFunctionNotDeterministic("ABORT_SESSION", ABORT_SESSION,
                1, Value.BOOLEAN);
        addFunction("SET", SET, 2, Value.NULL, false, false);
        addFunction("FILE_READ", FILE_READ, VAR_ARGS, Value.NULL, false, false);
        addFunction("FILE_WRITE", FILE_WRITE, 2, Value.BIGINT, false, false);
        addFunctionNotDeterministic("TRANSACTION_ID", TRANSACTION_ID,
                0, Value.VARCHAR);
        addFunctionNotDeterministic("DISK_SPACE_USED", DISK_SPACE_USED,
                1, Value.BIGINT);
        addFunctionWithNull("SIGNAL", SIGNAL, 2, Value.NULL);
        addFunctionNotDeterministic("ESTIMATED_ENVELOPE", ESTIMATED_ENVELOPE, 2, Value.BIGINT);
        addFunctionNotDeterministic("H2VERSION", H2VERSION, 0, Value.VARCHAR);

        // TableFunction
        addFunctionWithNull("TABLE", TABLE, VAR_ARGS, Value.RESULT_SET);
        addFunctionWithNull("TABLE_DISTINCT", TABLE_DISTINCT, VAR_ARGS, Value.RESULT_SET);
        addFunctionWithNull("UNNEST", UNNEST, VAR_ARGS, Value.RESULT_SET);
    }

    private static void addFunction(String name, int type, int parameterCount,
            int returnDataType, boolean nullIfParameterIsNull, boolean deterministic) {
        FunctionInfo info = new FunctionInfo(name, type, parameterCount, returnDataType, nullIfParameterIsNull,
                deterministic);
        if (FUNCTIONS_BY_ID[type] == null) {
            FUNCTIONS_BY_ID[type] = info;
        }
        FUNCTIONS_BY_NAME.put(name, info);
    }

    private static void addFunctionNotDeterministic(String name, int type,
            int parameterCount, int returnDataType) {
        addFunction(name, type, parameterCount, returnDataType, true, false);
    }

    private static void addFunction(String name, int type, int parameterCount,
            int returnDataType) {
        addFunction(name, type, parameterCount, returnDataType, true, true);
    }

    private static void addFunctionWithNull(String name, int type,
            int parameterCount, int returnDataType) {
        addFunction(name, type, parameterCount, returnDataType, false, true);
    }

    /**
     * Get an instance of the given function for this database.
     *
     * @param id the function number
     * @return the function object
     */
    public static Function getFunction(int id) {
        return createFunction(FUNCTIONS_BY_ID[id], null);
    }

    /**
     * Get an instance of the given function for this database.
     *
     * @param id the function number
     * @param arguments the arguments
     * @return the function object
     */
    public static Function getFunctionWithArgs(int id, Expression... arguments) {
        return createFunction(FUNCTIONS_BY_ID[id], arguments);
    }

    /**
     * Get an instance of the given function for this database.
     * If no function with this name is found, null is returned.
     *
     * @param database the database
     * @param name the upper case function name
     * @return the function object or null
     */
    public static Function getFunction(Database database, String name) {
        FunctionInfo info = FUNCTIONS_BY_NAME.get(name);
        if (info == null) {
            ModeEnum modeEnum = database.getMode().getEnum();
            if (modeEnum != ModeEnum.REGULAR) {
                return getCompatibilityModeFunction(name, modeEnum);
            }
            return null;
        }
        return createFunction(info, null);
    }

    private static Function getCompatibilityModeFunction(String name, ModeEnum modeEnum) {
        switch (modeEnum) {
        case DB2:
        case Derby:
            return FunctionsDB2Derby.getFunction(name);
        case MSSQLServer:
            return FunctionsMSSQLServer.getFunction(name);
        case MySQL:
            return FunctionsMySQL.getFunction(name);
        case Oracle:
            return FunctionsOracle.getFunction(name);
        case PostgreSQL:
            return FunctionsPostgreSQL.getFunction(name);
        default:
            return null;
        }
    }

    private static Function createFunction(FunctionInfo info, Expression[] arguments) {
        switch (info.type) {
        case TABLE:
        case TABLE_DISTINCT:
        case UNNEST:
            assert arguments == null;
            return new TableFunction(info);
        default:
            return arguments != null ? new Function(info, arguments) : new Function(info);
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
     * @param info function information
     */
    public Function(FunctionInfo info) {
        super(new Expression[info.parameterCount != VAR_ARGS ? info.parameterCount : 4]);
        this.info = info;
    }

    /**
     * Creates a new instance of function.
     *
     * @param info function information
     * @param arguments the arguments
     */
    public Function(FunctionInfo info, Expression[] arguments) {
        super(arguments);
        this.info = info;
        int expected = info.parameterCount, len = arguments.length;
        if (expected == VAR_ARGS) {
            checkParameterCount(len);
        } else if (expected != len) {
            throw DbException.get(ErrorCode.INVALID_PARAMETER_COUNT_2, info.name, Integer.toString(expected));
        }
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
    public Value getValue(SessionLocal session) {
        return getValueWithArgs(session, args);
    }

    private Value getSimpleValue(SessionLocal session, Value v0, Expression[] args,
            Value[] values) {
        Value result;
        switch (info.type) {
        case ROUNDMAGIC:
            result = ValueDouble.get(roundMagic(v0.getDouble()));
            break;
        case ASCII: {
            String s = v0.getString();
            if (s.isEmpty()) {
                result = ValueNull.INSTANCE;
            } else {
                result = ValueInteger.get(s.charAt(0));
            }
            break;
        }
        case CHAR:
            result = ValueVarchar.get(String.valueOf((char) v0.getInt()), session);
            break;
        case XMLCOMMENT:
            result = ValueVarchar.get(StringUtils.xmlComment(v0.getString()), session);
            break;
        case XMLCDATA:
            result = ValueVarchar.get(StringUtils.xmlCData(v0.getString()), session);
            break;
        case XMLSTARTDOC:
            result = ValueVarchar.get(StringUtils.xmlStartDoc(), session);
            break;
        case AUTOCOMMIT:
            result = ValueBoolean.get(session.getAutoCommit());
            break;
        case READONLY:
            result = ValueBoolean.get(session.getDatabase().isReadOnly());
            break;
        case DATABASE_PATH: {
            String path = session.getDatabase().getDatabasePath();
            result = path == null ? (Value) ValueNull.INSTANCE : ValueVarchar.get(path, session);
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
        case MEMORY_FREE:
            session.getUser().checkAdmin();
            result = ValueInteger.get(Utils.getMemoryFree());
            break;
        case MEMORY_USED:
            session.getUser().checkAdmin();
            result = ValueInteger.get(Utils.getMemoryUsed());
            break;
        case LOCK_MODE:
            result = ValueInteger.get(session.getDatabase().getLockMode());
            break;
        case SESSION_ID:
            result = ValueInteger.get(session.getId());
            break;
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
        default:
            result = null;
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

    private static boolean cancelStatement(SessionLocal session, int targetSessionId) {
        session.getUser().checkAdmin();
        SessionLocal[] sessions = session.getDatabase().getSessions(false);
        for (SessionLocal s : sessions) {
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

    private static boolean abortSession(SessionLocal session, int targetSessionId) {
        session.getUser().checkAdmin();
        SessionLocal[] sessions = session.getDatabase().getSessions(false);
        for (SessionLocal s : sessions) {
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

    private static long getDiskSpaceUsed(SessionLocal session, Value tableName) {
        return getTable(session, tableName).getDiskSpaceUsed();
    }

    private static Value getEstimatedEnvelope(SessionLocal session, Value tableName, Value columnName) {
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

    private static Table getTable(SessionLocal session, Value tableName) {
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
    protected static Value getNullOrValue(SessionLocal session, Expression[] args,
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
    protected Value getValueWithArgs(SessionLocal session, Expression[] args) {
        Value[] values = getArgumentsValues(session, args);
        if (values == null) {
            return ValueNull.INSTANCE;
        }
        Value v0 = getNullOrValue(session, args, values, 0);
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
        case ROUND:
            result = round(v0, v1);
            break;
        case TRUNCATE:
            result = truncate(session, v0, v1);
            break;
        case INSERT: {
            if (v1 == ValueNull.INSTANCE || v2 == ValueNull.INSTANCE) {
                result = v1;
            } else {
                result = ValueVarchar.get(insert(v0.getString(), v1.getInt(), v2.getInt(), v3.getString()), session);
            }
            break;
        }
        case LEFT:
            result = ValueVarchar.get(left(v0.getString(), v1.getInt()), session);
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
            result = ValueVarchar.get(repeat(v0.getString(), count), session);
            break;
        }
        case REPLACE:
            if (v0 == ValueNull.INSTANCE || v1 == ValueNull.INSTANCE
                    || v2 == ValueNull.INSTANCE && session.getMode().getEnum() != Mode.ModeEnum.Oracle) {
                result = ValueNull.INSTANCE;
            } else {
                String s0 = v0.getString();
                String s1 = v1.getString();
                String s2 = (v2 == null) ? "" : v2.getString();
                if (s2 == null) {
                    s2 = "";
                }
                result = ValueVarchar.get(StringUtils.replaceAll(s0, s1, s2), session);
            }
            break;
        case RIGHT:
            result = ValueVarchar.get(right(v0.getString(), v1.getInt()), session);
            break;
        case TRIM:
            result = ValueVarchar.get(StringUtils.trim(v0.getString(),
                    (flags & TRIM_LEADING) != 0, (flags & TRIM_TRAILING) != 0, v1 == null ? " " : v1.getString()),
                    session);
            break;
        case SUBSTRING:
            result = substring(session, v0, v1, v2);
            break;
        case POSITION:
            result = ValueInteger.get(locate(v0.getString(), v1.getString(), 0));
            break;
        case XMLATTR:
            result = ValueVarchar.get(StringUtils.xmlAttr(v0.getString(), v1.getString()), session);
            break;
        case XMLNODE: {
            String attr = v1 == null ?
                    null : v1 == ValueNull.INSTANCE ? null : v1.getString();
            String content = v2 == null ?
                    null : v2 == ValueNull.INSTANCE ? null : v2.getString();
            boolean indent = v3 == null ?
                    true : v3.getBoolean();
            result = ValueVarchar.get(StringUtils.xmlNode(v0.getString(), attr, content, indent), session);
            break;
        }
        case REGEXP_REPLACE: {
            String input = v0.getString();
            if (ModeEnum.Oracle == session.getMode().getEnum()) {
                if (input == null) {
                    result = ValueNull.INSTANCE;
                } else {
                    String regexp = v1.getString() != null ? v1.getString() : "";
                    String replacement = v2.getString() != null ? v2.getString() : "";
                    int position = v3 != null ? v3.getInt() : 1;
                    int occurrence = v4 != null ? v4.getInt() : 0;
                    String regexpMode = v5 != null ? v5.getString() : null;
                    result = regexpReplace(session, input, regexp, replacement, position, occurrence, regexpMode);
                }
            } else {
                if (v4 != null) {
                    throw DbException.get(ErrorCode.INVALID_PARAMETER_COUNT_2, info.name, "3..4");
                }
                if (v0 == ValueNull.INSTANCE || v1 == ValueNull.INSTANCE || v2 == ValueNull.INSTANCE || v3 == ValueNull.INSTANCE) {
                    result = ValueNull.INSTANCE;
                } else {
                    String regexp = v1.getString();
                    String replacement = v2.getString();
                    String regexpMode = v3 != null ? v3.getString() : null;
                    result = regexpReplace(session, input, regexp, replacement, 1, 0, regexpMode);
                }
            }
            break;
        }
        case RPAD:
            result = ValueVarchar.get(
                    StringUtils.pad(v0.getString(), v1.getInt(), v2 == null ? null : v2.getString(), true),
                    session);
            break;
        case LPAD:
            result = ValueVarchar.get(
                    StringUtils.pad(v0.getString(), v1.getInt(), v2 == null ? null : v2.getString(), false),
                    session);
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
                        session);
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
                        session);
                break;
            default:
                result = ValueVarchar.get(v0.getString(), session);
            }
            break;
        case TRANSLATE: {
            String matching = v1.getString();
            String replacement = v2.getString();
            if (session.getMode().getEnum() == ModeEnum.DB2) {
                String t = matching;
                matching = replacement;
                replacement = t;
            }
            result = ValueVarchar.get(translate(v0.getString(), matching, replacement), session);
            break;
        }
        case H2VERSION:
            result = ValueVarchar.get(Constants.VERSION, session);
            break;
            // system
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
        case TRIM_ARRAY: {
            if (v1 == ValueNull.INSTANCE) {
                result = ValueNull.INSTANCE;
                break;
            }
            int trim = v1.getInt();
            if (trim < 0) {
                // This exception should be thrown even when array is null
                throw DbException.get(ErrorCode.ARRAY_ELEMENT_ERROR_2, Integer.toString(trim),
                        "0..CARDINALITY(array)");
            }
            if (v0 == ValueNull.INSTANCE) {
                result = ValueNull.INSTANCE;
                break;
            }
            final ValueArray array = v0.convertToAnyArray(session);
            Value[] elements = array.getList();
            int length = elements.length;
            if (trim > length) {
                throw DbException.get(ErrorCode.ARRAY_ELEMENT_ERROR_2, Integer.toString(trim), "0.." + length);
            }
            if (trim == 0) {
                result = array;
            } else {
                result = ValueArray.get(array.getComponentType(), Arrays.copyOf(elements, length - trim), session);
            }
            break;
        }
        case ARRAY_SLICE: {
            result = null;
            final ValueArray array = v0.convertToAnyArray(session);
            // SQL is 1-based
            int index1 = v1.getInt() - 1;
            // 1-based and inclusive as postgreSQL (-1+1)
            int index2 = v2.getInt();
            // https://www.postgresql.org/docs/current/arrays.html#ARRAYS-ACCESSING
            // For historical reasons postgreSQL ignore invalid indexes
            final boolean isPG = session.getMode().getEnum() == ModeEnum.PostgreSQL;
            if (index1 > index2) {
                if (isPG)
                    result = ValueArray.get(array.getComponentType(), Value.EMPTY_VALUES, session);
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
                result = ValueArray.get(array.getComponentType(), Arrays.copyOfRange(array.getList(), index1, index2),
                        session);
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
                Database database = session.getDatabase();
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
                result = ValueVarchar.get(StringUtils.xmlText(v0.getString()), session);
            } else {
                result = ValueVarchar.get(StringUtils.xmlText(v0.getString(), v1.getBoolean()), session);
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
        case REGEXP_SUBSTR: {
            result = regexpSubstr(v0, v1, v2, v3, v4, v5, session);
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

    private static Value regexpSubstr(Value inputString, Value regexpArg, Value positionArg,
            Value occurrenceArg, Value regexpModeArg, Value subexpressionArg, SessionLocal session) {
        String regexp = regexpArg.getString();

        if (inputString == ValueNull.INSTANCE || regexpArg == ValueNull.INSTANCE || positionArg == ValueNull.INSTANCE
            || occurrenceArg == ValueNull.INSTANCE || subexpressionArg == ValueNull.INSTANCE) {
            return ValueNull.INSTANCE;
        }

        int position = positionArg != null ? positionArg.getInt() - 1 : 0;
        int requestedOccurrence = occurrenceArg != null ? occurrenceArg.getInt() : 1;
        String regexpMode = regexpModeArg != null ? regexpModeArg.getString() : null;
        int subexpression = subexpressionArg != null ? subexpressionArg.getInt() : 0;
        int flags = makeRegexpFlags(regexpMode, false);
        try {
            Matcher m = Pattern.compile(regexp, flags).matcher(inputString.getString());

            boolean found = m.find(position);
            for(int occurrence = 1; occurrence < requestedOccurrence && found; occurrence++) {
                found = m.find();
            }

            if (!found) {
                return ValueNull.INSTANCE;
            }
            else {
                return ValueVarchar.get(m.group(subexpression), session);
            }
        } catch (PatternSyntaxException e) {
            throw DbException.get(ErrorCode.LIKE_ESCAPE_ERROR_1, e, regexp);
        }
        catch (IndexOutOfBoundsException e) {
            return ValueNull.INSTANCE;
        }
    }

    /**
     * Gets values of arguments and checks them for NULL values if function
     * returns NULL on NULL argument.
     *
     * @param session
     *            the session
     * @param args
     *            the arguments
     * @return the values, or {@code null} if function should return NULL due to
     *         NULL argument
     */
    protected final Value[] getArgumentsValues(SessionLocal session, Expression[] args) {
        Value[] values = new Value[args.length];
        if (info.nullIfParameterIsNull) {
            for (int i = 0, l = args.length; i < l; i++) {
                Value v = args[i].getValue(session);
                if (v == ValueNull.INSTANCE) {
                    return null;
                }
                values[i] = v;
            }
        }
        return values;
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
        case Value.DECFLOAT:
            result = ValueDecfloat.get(bd);
            break;
        default:
            result = ValueNumeric.get(bd);
        }
        return result;
    }

    private static Value truncate(SessionLocal session, Value v0, Value v1) {
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
            switch (t) {
            case Value.DOUBLE:
            case Value.REAL:
                double d = v0.getDouble();
                if (scale == 0) {
                    d = d < 0 ? Math.ceil(d) : Math.floor(d);
                } else {
                    double f = Math.pow(10, scale);
                    d *= f;
                    d = (d < 0 ? Math.ceil(d) : Math.floor(d)) / f;
                }
                result = t == Value.DOUBLE ? ValueDouble.get(d) : ValueReal.get((float) d);
                break;
            case Value.DECFLOAT:
                result = ValueDecfloat.get(v0.getBigDecimal().setScale(scale, RoundingMode.DOWN));
                break;
            default:
                result = ValueNumeric.get(v0.getBigDecimal().setScale(scale, RoundingMode.DOWN));
                break;
            }
            break;
        }
        return result;
    }

    private static Value truncateValue(SessionLocal session, Value value, long precision, boolean force) {
        if (precision <= 0) {
            throw DbException.get(ErrorCode.INVALID_VALUE_PRECISION, Long.toString(precision), "1",
                    "" + Integer.MAX_VALUE);
        }
        TypeInfo t = value.getType();
        int valueType = t.getValueType();
        if (DataType.getDataType(valueType).supportsPrecision) {
            if (precision < t.getPrecision()) {
                switch (valueType) {
                case Value.NUMERIC:
                    return ValueNumeric.get(value.getBigDecimal().round(new MathContext(
                            MathUtils.convertLongToInt(precision))));
                case Value.DECFLOAT:
                    return ValueDecfloat.get(value.getBigDecimal().round(new MathContext(
                            MathUtils.convertLongToInt(precision))));
                default:
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


    private Value substring(SessionLocal session, Value stringValue, Value startValue, Value lengthValue) {
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
                return session.getMode().treatEmptyStringsAsNull ? ValueNull.INSTANCE : ValueVarchar.EMPTY;
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

    private static Value regexpReplace(SessionLocal session, String input, String regexp,
            String replacement, int position, int occurrence, String regexpMode) {
        Mode mode = session.getMode();
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
        if (isInPostgreSqlMode && ( regexpMode == null || regexpMode.isEmpty() || !regexpMode.contains("g"))) {
            occurrence = 1;
        }
        try {
            Matcher matcher = Pattern.compile(regexp, flags).matcher(input).region(position - 1, input.length());
            if (occurrence == 0) {
                return ValueVarchar.get(matcher.replaceAll(replacement), session);
            } else {
                StringBuffer sb = new StringBuffer();
                int index = 1;
                while (matcher.find()) {
                    if (index == occurrence) {
                        matcher.appendReplacement(sb, replacement);
                        break;
                    }
                    index++;
                }
                matcher.appendTail(sb);
                return ValueVarchar.get(sb.toString(), session);
            }
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

    @Override
    public int getValueType() {
        return type.getValueType();
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
        case CSVREAD:
            min = 1;
            break;
        case TRIM:
        case FILE_READ:
        case ROUND:
        case XMLTEXT:
        case TRUNCATE:
            min = 1;
            max = 2;
            break;
        case TO_CHAR:
            min = 1;
            max = 3;
            break;
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
        case REGEXP_SUBSTR:
            min = 2;
            max = 6;
            break;
        case CSVWRITE:
            min = 2;
            break;
        case XMLNODE:
            min = 1;
            max = 4;
            break;
        case REGEXP_REPLACE:
            min = 3;
            max = 6;
            break;
        default:
            DbException.throwInternalError("type=" + info.type);
        }
        if (len < min || len > max) {
            throw DbException.get(ErrorCode.INVALID_PARAMETER_COUNT_2, info.name, min + ".." + max);
        }
    }

    @Override
    public void doneWithParameters() {
        int count = info.parameterCount;
        if (count == VAR_ARGS) {
            checkParameterCount(argsCount);
            super.doneWithParameters();
        } else if (count != argsCount) {
            throw DbException.get(ErrorCode.INVALID_PARAMETER_COUNT_2, info.name, Integer.toString(argsCount));
        }
    }

    @Override
    public Expression optimize(SessionLocal session) {
        boolean allConst = optimizeArguments(session);
        TypeInfo typeInfo;
        Expression p0 = args.length < 1 ? null : args[0];
        switch (info.type) {
        case TRUNCATE_VALUE:
            if (type != null) {
                // data type, precision and scale is already set
                typeInfo = type;
            } else {
                typeInfo = TypeInfo.TYPE_UNKNOWN;
            }
            break;
        case ROUND:
            switch (p0.getType().getValueType()) {
            case Value.DOUBLE:
            case Value.REAL:
            case Value.DECFLOAT:
                typeInfo = p0.getType();
                break;
            default:
                typeInfo = getRoundNumericType(session);
            }
            break;
        case TRUNCATE:
            switch (p0.getType().getValueType()) {
            case Value.DOUBLE:
            case Value.REAL:
            case Value.DECFLOAT:
                typeInfo = p0.getType();
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
            TypeInfo argType = p0.getType();
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
        case CHAR:
            typeInfo = TypeInfo.getTypeInfo(info.returnDataType, 1, 0, null);
            break;
        case RIGHT:
        case TRIM:
            typeInfo = TypeInfo.getTypeInfo(info.returnDataType, p0.getType().getPrecision(), 0, null);
            break;
        case TRIM_ARRAY:
        case ARRAY_SLICE: {
            typeInfo = p0.getType();
            int t = typeInfo.getValueType();
            if (t != Value.ARRAY && t != Value.NULL) {
                throw DbException.getInvalidValueException(getName() + " array argument", typeInfo.getTraceSQL());
            }
            break;
        }
        default:
            typeInfo = TypeInfo.getTypeInfo(info.returnDataType, -1, -1, null);
        }
        type = typeInfo;
        if (allConst) {
            return ValueExpression.get(getValue(session));
        }
        return this;
    }

    /**
     * Optimizes arguments.
     *
     * @param session
     *            the session
     * @return whether all arguments are constants and function is deterministic
     */
    protected final boolean optimizeArguments(SessionLocal session) {
        return optimizeArguments(session, info.deterministic);
    }

    private TypeInfo getRoundNumericType(SessionLocal session) {
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
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        builder.append(info.name).append('(');
        switch (info.type) {
        case SUBSTRING: {
            args[0].getUnenclosedSQL(builder, sqlFlags).append(" FROM ");
            args[1].getUnenclosedSQL(builder, sqlFlags);
            if (args.length > 2) {
                builder.append(" FOR ");
                args[2].getUnenclosedSQL(builder, sqlFlags);
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
                args[1].getUnenclosedSQL(builder, sqlFlags).append(" FROM ");
            }
            args[0].getUnenclosedSQL(builder, sqlFlags);
            break;
        }
        default:
            writeExpressions(builder, args, sqlFlags);
        }
        return builder.append(')');
    }

    public int getFunctionType() {
        return info.type;
    }

    @Override
    public String getName() {
        return info.name;
    }

    @Override
    public ValueResultSet getValueForColumnList(SessionLocal session,
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
        if (!super.isEverything(visitor)) {
            return false;
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
    public boolean isDeterministic() {
        return info.deterministic;
    }

}
