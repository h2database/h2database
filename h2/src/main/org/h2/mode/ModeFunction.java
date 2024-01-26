/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mode;

import org.h2.api.ErrorCode;
import org.h2.engine.Database;
import org.h2.engine.Mode.ModeEnum;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.function.CurrentDateTimeValueFunction;
import org.h2.expression.function.FunctionN;
import org.h2.message.DbException;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * Base class for mode-specific functions.
 */
public abstract class ModeFunction extends FunctionN {

    /**
     * Constant for variable number of arguments.
     */
    protected static final int VAR_ARGS = -1;

    /**
     * The information about this function.
     */
    protected final FunctionInfo info;

    /**
     * Get an instance of the given function for this database.
     * If no function with this name is found, null is returned.
     *
     * @param database the database
     * @param name the upper case function name
     * @return the function object or null
     */
    public static ModeFunction getFunction(Database database, String name) {
        ModeEnum modeEnum = database.getMode().getEnum();
        if (modeEnum != ModeEnum.REGULAR) {
            return getCompatibilityModeFunction(name, modeEnum);
        }
        return null;
    }

    private static ModeFunction getCompatibilityModeFunction(String name, ModeEnum modeEnum) {
        switch (modeEnum) {
        case LEGACY:
            return FunctionsLegacy.getFunction(name);
        case DB2:
        case Derby:
            return FunctionsDB2Derby.getFunction(name);
        case MSSQLServer:
            return FunctionsMSSQLServer.getFunction(name);
        case MariaDB:
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

    /**
     * Get an instance of the given function without parentheses for this
     * database. If no function with this name is found, null is returned.
     *
     * @param database the database
     * @param name the upper case function name
     * @param scale the scale, or {@code -1}
     * @return the function object or null
     */
    @SuppressWarnings("incomplete-switch")
    public static Expression getCompatibilityDateTimeValueFunction(Database database, String name, int scale) {
        switch (name) {
        case "SYSDATE":
            switch (database.getMode().getEnum()) {
            case LEGACY:
            case HSQLDB:
            case Oracle:
                return new CompatibilityDateTimeValueFunction(CompatibilityDateTimeValueFunction.SYSDATE, -1);
            }
            break;
        case "SYSTIMESTAMP":
            switch (database.getMode().getEnum()) {
            case LEGACY:
            case Oracle:
                return new CompatibilityDateTimeValueFunction(CompatibilityDateTimeValueFunction.SYSTIMESTAMP, scale);
            }
            break;
        case "TODAY":
            switch (database.getMode().getEnum()) {
            case LEGACY:
            case HSQLDB:
                return new CurrentDateTimeValueFunction(CurrentDateTimeValueFunction.CURRENT_DATE, scale);
            }
            break;
        }
        return null;
    }

    /**
     * Creates a new instance of function.
     *
     * @param info function information
     */
    ModeFunction(FunctionInfo info) {
        super(new Expression[info.parameterCount != VAR_ARGS ? info.parameterCount : 4]);
        this.info = info;
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
    static Value getNullOrValue(SessionLocal session, Expression[] args,
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
    final Value[] getArgumentsValues(SessionLocal session, Expression[] args) {
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

    /**
     * Check if the parameter count is correct.
     *
     * @param len the number of parameters set
     * @throws DbException if the parameter count is incorrect
     */
    void checkParameterCount(int len) {
        throw DbException.getInternalError("type=" + info.type);
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

    /**
     * Optimizes arguments.
     *
     * @param session
     *            the session
     * @return whether all arguments are constants and function is deterministic
     */
    final boolean optimizeArguments(SessionLocal session) {
        return optimizeArguments(session, info.deterministic);
    }

    @Override
    public String getName() {
        return info.name;
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
        default:
            return true;
        }
    }

}
