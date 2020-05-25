/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function;

import java.io.ByteArrayOutputStream;

import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionWithFlags;
import org.h2.expression.Format;
import org.h2.expression.OperationN;
import org.h2.expression.Subquery;
import org.h2.expression.TypedValueExpression;
import org.h2.message.DbException;
import org.h2.util.json.JSONByteArrayTarget;
import org.h2.util.json.JSONBytesSource;
import org.h2.util.json.JSONStringTarget;
import org.h2.util.json.JSONValidationTargetWithUniqueKeys;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueJson;
import org.h2.value.ValueNull;

/**
 * JSON constructor function.
 */
public class JsonConstructorFunction extends OperationN implements ExpressionWithFlags {

    /**
     * The ABSENT ON NULL flag.
     */
    public static final int JSON_ABSENT_ON_NULL = 1;

    /**
     * The WITH UNIQUE KEYS flag.
     */
    public static final int JSON_WITH_UNIQUE_KEYS = 2;

    /**
     * Returns whether specified function is known by this class.
     *
     * @param upperName
     *            the name of the function in upper case
     * @return {@code true} if it exists
     */
    public static boolean exists(String upperName) {
        return upperName.equals("JSON_OBJECT") || upperName.equals("JSON_ARRAY");
    }

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
    public Value getValue(Session session) {
        return array ? jsonArray(session, args) : jsonObject(session, args);
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
     * @param baos
     *            the output stream to append to
     * @param key
     *            the name of the property
     * @param value
     *            the value of the property
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
     * @param baos
     *            the output stream with the object
     * @param flags
     *            the flags ({@link #JSON_WITH_UNIQUE_KEYS})
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
     * @param baos
     *            the output stream to append to
     * @param value
     *            the value
     * @param flags
     *            the flags ({@link #JSON_ABSENT_ON_NULL})
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
    public Expression optimize(Session session) {
        boolean allConst = true;
        for (int i = 0, l = args.length; i < l; i++) {
            Expression e = args[i].optimize(session);
            args[i] = e;
            if (!e.isConstant()) {
                allConst = false;
            }
        }
        type = TypeInfo.TYPE_JSON;
        if (allConst) {
            return TypedValueExpression.getTypedIfNull(getValue(session), type);
        }
        return this;
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        if (array) {
            writeExpressions(builder.append("JSON_ARRAY").append('('), args, sqlFlags);
        } else {
            builder.append("JSON_OBJECT").append('(');
            for (int i = 0, l = args.length; i < l;) {
                if (i > 0) {
                    builder.append(", ");
                }
                args[i++].getSQL(builder, sqlFlags).append(": ");
                args[i++].getSQL(builder, sqlFlags);
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
        return builder;
    }

}
