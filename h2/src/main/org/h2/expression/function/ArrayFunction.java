/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function;

import java.util.Arrays;

import org.h2.api.ErrorCode;
import org.h2.engine.SessionLocal;
import org.h2.engine.Mode.ModeEnum;
import org.h2.expression.Expression;
import org.h2.expression.TypedValueExpression;
import org.h2.message.DbException;
import org.h2.mvstore.db.Store;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueCollectionBase;
import org.h2.value.ValueNull;

/**
 * An array function.
 */
public final class ArrayFunction extends FunctionN {

    /**
     * TRIM_ARRAY().
     */
    public static final int TRIM_ARRAY = 0;

    /**
     * ARRAY_CONTAINS() (non-standard).
     */
    public static final int ARRAY_CONTAINS = TRIM_ARRAY + 1;

    /**
     * ARRAY_SLICE() (non-standard).
     */
    public static final int ARRAY_SLICE = ARRAY_CONTAINS + 1;

    private static final String[] NAMES = { //
            "TRIM_ARRAY", "ARRAY_CONTAINS", "ARRAY_SLICE" //
    };

    private final int function;

    public ArrayFunction(Expression arg1, Expression arg2, Expression arg3, int function) {
        super(arg3 == null ? new Expression[] { arg1, arg2 } : new Expression[] { arg1, arg2, arg3 });
        this.function = function;
    }

    @Override
    public Value getValue(SessionLocal session) {
        Value v1 = args[0].getValue(session), v2 = args[1].getValue(session);
        switch (function) {
        case TRIM_ARRAY: {
            if (v2 == ValueNull.INSTANCE) {
                v1 = ValueNull.INSTANCE;
                break;
            }
            int trim = v2.getInt();
            if (trim < 0) {
                // This exception should be thrown even when array is null
                throw DbException.get(ErrorCode.ARRAY_ELEMENT_ERROR_2, Integer.toString(trim), //
                        "0..CARDINALITY(array)");
            }
            if (v1 == ValueNull.INSTANCE) {
                break;
            }
            final ValueArray array = v1.convertToAnyArray(session);
            Value[] elements = array.getList();
            int length = elements.length;
            if (trim > length) {
                throw DbException.get(ErrorCode.ARRAY_ELEMENT_ERROR_2, Integer.toString(trim), "0.." + length);
            } else if (trim == 0) {
                v1 = array;
            } else {
                v1 = ValueArray.get(array.getComponentType(), Arrays.copyOf(elements, length - trim), session);
            }
            break;
        }
        case ARRAY_CONTAINS: {
            int t = v1.getValueType();
            if (t == Value.ARRAY || t == Value.ROW) {
                Value[] list = ((ValueCollectionBase) v1).getList();
                v1 = ValueBoolean.FALSE;
                for (Value v : list) {
                    if (session.areEqual(v, v2)) {
                        v1 = ValueBoolean.TRUE;
                        break;
                    }
                }
            } else {
                v1 = ValueNull.INSTANCE;
            }
            break;
        }
        case ARRAY_SLICE: {
            Value v3;
            if (v1 == ValueNull.INSTANCE || v2 == ValueNull.INSTANCE
                    || (v3 = args[2].getValue(session)) == ValueNull.INSTANCE) {
                v1 = ValueNull.INSTANCE;
                break;
            }
            ValueArray array = v1.convertToAnyArray(session);
            // SQL is 1-based
            int index1 = v2.getInt() - 1;
            // 1-based and inclusive as postgreSQL (-1+1)
            int index2 = v3.getInt();
            // https://www.postgresql.org/docs/current/arrays.html#ARRAYS-ACCESSING
            // For historical reasons postgreSQL ignore invalid indexes
            final boolean isPG = session.getMode().getEnum() == ModeEnum.PostgreSQL;
            if (index1 > index2) {
                v1 = isPG ? ValueArray.get(array.getComponentType(), Value.EMPTY_VALUES, session) : ValueNull.INSTANCE;
                break;
            }
            if (index1 < 0) {
                if (isPG) {
                    index1 = 0;
                } else {
                    v1 = ValueNull.INSTANCE;
                    break;
                }
            }
            if (index2 > array.getList().length) {
                if (isPG) {
                    index2 = array.getList().length;
                } else {
                    v1 = ValueNull.INSTANCE;
                    break;
                }
            }
            v1 = ValueArray.get(array.getComponentType(), Arrays.copyOfRange(array.getList(), index1, index2), //
                    session);
            break;
        }
        default:
            throw DbException.getInternalError("function=" + function);
        }
        return v1;
    }

    @Override
    public Expression optimize(SessionLocal session) {
        boolean allConst = optimizeArguments(session, true);
        switch (function) {
        case TRIM_ARRAY:
        case ARRAY_SLICE: {
            Expression arg = args[0];
            type = arg.getType();
            int t = type.getValueType();
            if (t != Value.ARRAY && t != Value.NULL) {
                throw Store.getInvalidExpressionTypeException(getName() + " array argument", arg);
            }
            break;
        }
        case ARRAY_CONTAINS:
            type = TypeInfo.TYPE_BOOLEAN;
            break;
        default:
            throw DbException.getInternalError("function=" + function);
        }
        if (allConst) {
            return TypedValueExpression.getTypedIfNull(getValue(session), type);
        }
        return this;
    }

    @Override
    public String getName() {
        return NAMES[function];
    }

}
