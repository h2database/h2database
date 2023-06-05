/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function;

import org.h2.engine.Mode.ModeEnum;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.TypedValueExpression;
import org.h2.message.DbException;
import org.h2.util.StringUtils;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueInteger;
import org.h2.value.ValueNull;
import org.h2.value.ValueVarchar;

/**
 * An string function with multiple arguments.
 */
public final class StringFunction extends FunctionN {

    /**
     * LOCATE() (non-standard).
     */
    public static final int LOCATE = 0;

    /**
     * INSERT() (non-standard).
     */
    public static final int INSERT = LOCATE + 1;

    /**
     * REPLACE() (non-standard).
     */
    public static final int REPLACE = INSERT + 1;

    /**
     * LPAD().
     */
    public static final int LPAD = REPLACE + 1;

    /**
     * RPAD().
     */
    public static final int RPAD = LPAD + 1;

    /**
     * TRANSLATE() (non-standard).
     */
    public static final int TRANSLATE = RPAD + 1;

    private static final String[] NAMES = { //
            "LOCATE", "INSERT", "REPLACE", "LPAD", "RPAD", "TRANSLATE" //
    };

    private final int function;

    public StringFunction(Expression arg1, Expression arg2, Expression arg3, int function) {
        super(arg3 == null ? new Expression[] { arg1, arg2 } : new Expression[] { arg1, arg2, arg3 });
        this.function = function;
    }

    public StringFunction(Expression arg1, Expression arg2, Expression arg3, Expression arg4, int function) {
        super(new Expression[] { arg1, arg2, arg3, arg4 });
        this.function = function;
    }

    public StringFunction(Expression[] args, int function) {
        super(args);
        this.function = function;
    }

    @Override
    public Value getValue(SessionLocal session) {
        Value v1 = args[0].getValue(session), v2 = args[1].getValue(session);
        switch (function) {
        case LOCATE: {
            if (v1 == ValueNull.INSTANCE || v2 == ValueNull.INSTANCE) {
                return ValueNull.INSTANCE;
            }
            Value v3 = args.length >= 3 ? args[2].getValue(session) : null;
            if (v3 == ValueNull.INSTANCE) {
                return ValueNull.INSTANCE;
            }
            v1 = ValueInteger.get(locate(v1.getString(), v2.getString(), v3 == null ? 1 : v3.getInt()));
            break;
        }
        case INSERT: {
            Value v3 = args[2].getValue(session), v4 = args[3].getValue(session);
            if (v2 != ValueNull.INSTANCE && v3 != ValueNull.INSTANCE) {
                String s = insert(v1.getString(), v2.getInt(), v3.getInt(), v4.getString());
                v1 = s != null ? ValueVarchar.get(s, session) : ValueNull.INSTANCE;
            }
            break;
        }
        case REPLACE: {
            if (v1 == ValueNull.INSTANCE || v2 == ValueNull.INSTANCE) {
                return ValueNull.INSTANCE;
            }
            String after;
            if (args.length >= 3) {
                Value v3 = args[2].getValue(session);
                if (v3 == ValueNull.INSTANCE && session.getMode().getEnum() != ModeEnum.Oracle) {
                    return ValueNull.INSTANCE;
                }
                after = v3.getString();
                if (after == null) {
                    after = "";
                }
            } else {
                after = "";
            }
            v1 = ValueVarchar.get(StringUtils.replaceAll(v1.getString(), v2.getString(), after), session);
            break;
        }
        case LPAD:
        case RPAD:
            if (v1 == ValueNull.INSTANCE || v2 == ValueNull.INSTANCE) {
                return ValueNull.INSTANCE;
            }
            String padding;
            if (args.length >= 3) {
                Value v3 = args[2].getValue(session);
                if (v3 == ValueNull.INSTANCE) {
                    return ValueNull.INSTANCE;
                }
                padding = v3.getString();
            } else {
                padding = null;
            }
            v1 = ValueVarchar.get(StringUtils.pad(v1.getString(), v2.getInt(), padding, function == RPAD), session);
            break;
        case TRANSLATE: {
            if (v1 == ValueNull.INSTANCE || v2 == ValueNull.INSTANCE) {
                return ValueNull.INSTANCE;
            }
            Value v3 = args[2].getValue(session);
            if (v3 == ValueNull.INSTANCE) {
                return ValueNull.INSTANCE;
            }
            String matching = v2.getString();
            String replacement = v3.getString();
            if (session.getMode().getEnum() == ModeEnum.DB2) {
                String t = matching;
                matching = replacement;
                replacement = t;
            }
            v1 = ValueVarchar.get(translate(v1.getString(), matching, replacement), session);
            break;
        }
        default:
            throw DbException.getInternalError("function=" + function);
        }
        return v1;
    }

    private static int locate(String search, String s, int start) {
        if (start < 0) {
            return s.lastIndexOf(search, s.length() + start) + 1;
        }
        return s.indexOf(search, start == 0 ? 0 : start - 1) + 1;
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

    private static String translate(String original, String findChars, String replaceChars) {
        if (StringUtils.isNullOrEmpty(original) || StringUtils.isNullOrEmpty(findChars)) {
            return original;
        }
        // if it stays null, then no replacements have been made
        StringBuilder builder = null;
        // if shorter than findChars, then characters are removed
        // (if null, we don't access replaceChars at all)
        int replaceSize = replaceChars == null ? 0 : replaceChars.length();
        for (int i = 0, size = original.length(); i < size; i++) {
            char ch = original.charAt(i);
            int index = findChars.indexOf(ch);
            if (index >= 0) {
                if (builder == null) {
                    builder = new StringBuilder(size);
                    if (i > 0) {
                        builder.append(original, 0, i);
                    }
                }
                if (index < replaceSize) {
                    ch = replaceChars.charAt(index);
                }
            }
            if (builder != null) {
                builder.append(ch);
            }
        }
        return builder == null ? original : builder.toString();
    }

    @Override
    public Expression optimize(SessionLocal session) {
        boolean allConst = optimizeArguments(session, true);
        switch (function) {
        case LOCATE:
            type = TypeInfo.TYPE_INTEGER;
            break;
        case INSERT:
        case REPLACE:
        case LPAD:
        case RPAD:
        case TRANSLATE:
            type = TypeInfo.TYPE_VARCHAR;
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
