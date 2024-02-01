/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function;

import java.io.ByteArrayOutputStream;

import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionWithFlags;
import org.h2.expression.Format;
import org.h2.expression.OperationN;
import org.h2.expression.Subquery;
import org.h2.expression.TypedValueExpression;
import org.h2.message.DbException;
import org.h2.util.json.JsonConstructorUtils;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueJson;
import org.h2.value.ValueNull;

/**
 * JSON constructor function.
 */
public final class JsonConstructorFunction extends OperationN implements ExpressionWithFlags, NamedExpression {

    private final boolean array;

    private int flags;

    /**
     * Creates a new instance of JSON constructor function.
     *
     * @param array
     *            {@code false} for {@code JSON_OBJECT}, {@code true} for
     *            {@code JSON_ARRAY}.
     */
    public JsonConstructorFunction(boolean array) {
        super(new Expression[4]);
        this.array = array;
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
        return array ? jsonArray(session, args) : jsonObject(session, args);
    }

    private Value jsonObject(SessionLocal session, Expression[] args) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.write('{');
        for (int i = 0, l = args.length; i < l;) {
            String name = args[i++].getValue(session).getString();
            if (name == null) {
                throw DbException.getInvalidValueException("JSON_OBJECT key", "NULL");
            }
            Value value = args[i++].getValue(session);
            if (value == ValueNull.INSTANCE || value == ValueJson.NULL) {
                if ((flags & JsonConstructorUtils.JSON_ABSENT_ON_NULL) != 0) {
                    continue;
                } else {
                    value = ValueJson.NULL;
                }
            }
            JsonConstructorUtils.jsonObjectAppend(baos, name, value);
        }
        return JsonConstructorUtils.jsonObjectFinish(baos, flags);
    }

    private Value jsonArray(SessionLocal session, Expression[] args) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.write('[');
        int l = args.length;
        evaluate: {
            if (l == 1) {
                Expression arg0 = args[0];
                if (arg0 instanceof Subquery) {
                    Subquery q = (Subquery) arg0;
                    for (Value value : q.getAllRows(session)) {
                        JsonConstructorUtils.jsonArrayAppend(baos, value, flags);
                    }
                    break evaluate;
                } else if (arg0 instanceof Format) {
                    Format format = (Format) arg0;
                    arg0 = format.getSubexpression(0);
                    if (arg0 instanceof Subquery) {
                        Subquery q = (Subquery) arg0;
                        for (Value value : q.getAllRows(session)) {
                            JsonConstructorUtils.jsonArrayAppend(baos, format.getValue(value), flags);
                        }
                        break evaluate;
                    }
                }
            }
            for (int i = 0; i < l;) {
                JsonConstructorUtils.jsonArrayAppend(baos, args[i++].getValue(session), flags);
            }
        }
        baos.write(']');
        return ValueJson.getInternal(baos.toByteArray());
    }

    @Override
    public Expression optimize(SessionLocal session) {
        boolean allConst = optimizeArguments(session, true);
        type = TypeInfo.TYPE_JSON;
        if (allConst) {
            return TypedValueExpression.getTypedIfNull(getValue(session), type);
        }
        return this;
    }

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        builder.append(getName()).append('(');
        if (array) {
            writeExpressions(builder, args, sqlFlags);
        } else {
            for (int i = 0, l = args.length; i < l;) {
                if (i > 0) {
                    builder.append(", ");
                }
                args[i++].getUnenclosedSQL(builder, sqlFlags).append(": ");
                args[i++].getUnenclosedSQL(builder, sqlFlags);
            }
        }
        return getJsonFunctionFlagsSQL(builder, flags, array).append(')');
    }

    /**
     * Appends flags of a JSON function to the specified string builder.
     *
     * @param builder
     *            string builder to append to
     * @param flags
     *            flags to append
     * @param forArray
     *            whether the function is an array function
     * @return the specified string builder
     */
    public static StringBuilder getJsonFunctionFlagsSQL(StringBuilder builder, int flags, boolean forArray) {
        if ((flags & JsonConstructorUtils.JSON_ABSENT_ON_NULL) != 0) {
            if (!forArray) {
                builder.append(" ABSENT ON NULL");
            }
        } else if (forArray) {
            builder.append(" NULL ON NULL");
        }
        if (!forArray && (flags & JsonConstructorUtils.JSON_WITH_UNIQUE_KEYS) != 0) {
            builder.append(" WITH UNIQUE KEYS");
        }
        return builder;
    }

    @Override
    public String getName() {
        return array ? "JSON_ARRAY" : "JSON_OBJECT";
    }

}
