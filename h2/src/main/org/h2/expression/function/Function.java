/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function;

import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.regex.Pattern;

import org.h2.api.ErrorCode;
import org.h2.engine.Database;
import org.h2.engine.Mode.ModeEnum;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.ValueExpression;
import org.h2.expression.Variable;
import org.h2.message.DbException;
import org.h2.mode.FunctionsDB2Derby;
import org.h2.mode.FunctionsMSSQLServer;
import org.h2.mode.FunctionsMySQL;
import org.h2.mode.FunctionsOracle;
import org.h2.mode.FunctionsPostgreSQL;
import org.h2.table.LinkSchema;
import org.h2.tools.Csv;
import org.h2.util.JdbcUtils;
import org.h2.util.MathUtils;
import org.h2.util.StringUtils;
import org.h2.value.DataType;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueDecfloat;
import org.h2.value.ValueInteger;
import org.h2.value.ValueNull;
import org.h2.value.ValueNumeric;
import org.h2.value.ValueResultSet;
import org.h2.value.ValueVarchar;

/**
 * This class implements most built-in functions of this database.
 */
public class Function extends FunctionN implements FunctionCall {

    public static final int
            XMLATTR = 83, XMLNODE = 84, XMLCOMMENT = 85, XMLCDATA = 86,
            XMLSTARTDOC = 87, XMLTEXT = 88;

    public static final int
            SIGNAL = 160;

    private static final Pattern SIGNAL_PATTERN = Pattern.compile("[0-9A-Z]{5}");

    public static final int
            CSVREAD = 210, CSVWRITE = 211,
            LINK_SCHEMA = 218,
            SET = 222, TABLE = 223, TABLE_DISTINCT = 224,
            TRUNCATE_VALUE = 227,
            UNNEST = 233;

    private static final int COUNT = UNNEST + 1;

    protected static final int VAR_ARGS = -1;

    private static final FunctionInfo[] FUNCTIONS_BY_ID = new FunctionInfo[COUNT];
    private static final HashMap<String, FunctionInfo> FUNCTIONS_BY_NAME = new HashMap<>(128);

    protected final FunctionInfo info;

    static {
        addFunction("XMLATTR", XMLATTR, 2, Value.VARCHAR);
        addFunctionWithNull("XMLNODE", XMLNODE, VAR_ARGS, Value.VARCHAR);
        addFunction("XMLCOMMENT", XMLCOMMENT, 1, Value.VARCHAR);
        addFunction("XMLCDATA", XMLCDATA, 1, Value.VARCHAR);
        addFunction("XMLSTARTDOC", XMLSTARTDOC, 0, Value.VARCHAR);
        addFunction("XMLTEXT", XMLTEXT, VAR_ARGS, Value.VARCHAR);

        // system
        addFunctionWithNull("TRUNCATE_VALUE", TRUNCATE_VALUE,
                3, Value.NULL);
        addFunction("CSVREAD", CSVREAD, VAR_ARGS, Value.RESULT_SET, false, false);
        addFunction("CSVWRITE", CSVWRITE, VAR_ARGS, Value.INTEGER, false, false);
        addFunctionNotDeterministic("LINK_SCHEMA", LINK_SCHEMA,
                6, Value.RESULT_SET);
        addFunction("SET", SET, 2, Value.NULL, false, false);
        addFunctionWithNull("SIGNAL", SIGNAL, 2, Value.NULL);

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
    public Value getValue(SessionLocal session) {
        return getValueWithArgs(session, args);
    }

    private Value getSimpleValue(SessionLocal session, Value v0, Expression[] args,
            Value[] values) {
        Value result;
        switch (info.type) {
        case XMLCOMMENT:
            result = ValueVarchar.get(StringUtils.xmlComment(v0.getString()), session);
            break;
        case XMLCDATA:
            result = ValueVarchar.get(StringUtils.xmlCData(v0.getString()), session);
            break;
        case XMLSTARTDOC:
            result = ValueVarchar.get(StringUtils.xmlStartDoc(), session);
            break;
        default:
            result = null;
        }
        return result;
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
        case XMLTEXT:
            min = 1;
            max = 2;
            break;
        case CSVWRITE:
            min = 2;
            break;
        case XMLNODE:
            min = 1;
            max = 4;
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
        case SET:
            typeInfo = args[1].getType();
            if (!(p0 instanceof Variable)) {
                throw DbException.get(ErrorCode.CAN_ONLY_ASSIGN_TO_VARIABLE_1, p0.getTraceSQL());
            }
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
