/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.h2.api.ErrorCode;
import org.h2.engine.Mode;
import org.h2.engine.Mode.ModeEnum;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.TypedValueExpression;
import org.h2.message.DbException;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueNull;
import org.h2.value.ValueVarchar;

/**
 * A regular expression function.
 */
public final class RegexpFunction extends FunctionN {

    /**
     * REGEXP_LIKE() (non-standard).
     */
    public static final int REGEXP_LIKE = 0;

    /**
     * REGEXP_REPLACE() (non-standard).
     */
    public static final int REGEXP_REPLACE = REGEXP_LIKE + 1;

    /**
     * REGEXP_SUBSTR() (non-standard).
     */
    public static final int REGEXP_SUBSTR = REGEXP_REPLACE + 1;

    private static final String[] NAMES = { //
            "REGEXP_LIKE", "REGEXP_REPLACE", "REGEXP_SUBSTR" //
    };

    private final int function;

    public RegexpFunction(int function) {
        super(new Expression[function == REGEXP_LIKE ? 3 : 6]);
        this.function = function;
    }

    @Override
    public Value getValue(SessionLocal session) {
        Value v1 = args[0].getValue(session);
        Value v2 = args[1].getValue(session);
        int length = args.length;
        switch (function) {
        case REGEXP_LIKE: {
            Value v3 = length >= 3 ? args[2].getValue(session) : null;
            if (v1 == ValueNull.INSTANCE || v2 == ValueNull.INSTANCE || v3 == ValueNull.INSTANCE) {
                return ValueNull.INSTANCE;
            }
            String regexp = v2.getString();
            String regexpMode = v3 != null ? v3.getString() : null;
            int flags = makeRegexpFlags(regexpMode, false);
            try {
                v1 = ValueBoolean.get(Pattern.compile(regexp, flags).matcher(v1.getString()).find());
            } catch (PatternSyntaxException e) {
                throw DbException.get(ErrorCode.LIKE_ESCAPE_ERROR_1, e, regexp);
            }
            break;
        }
        case REGEXP_REPLACE: {
            String input = v1.getString();
            if (session.getMode().getEnum() == ModeEnum.Oracle) {
                String replacement = args[2].getValue(session).getString();
                int position = length >= 4 ? args[3].getValue(session).getInt() : 1;
                int occurrence = length >= 5 ? args[4].getValue(session).getInt() : 0;
                String regexpMode = length >= 6 ? args[5].getValue(session).getString() : null;
                if (input == null) {
                    v1 = ValueNull.INSTANCE;
                } else {
                    String regexp = v2.getString();
                    v1 = regexpReplace(session, input, regexp != null ? regexp : "",
                            replacement != null ? replacement : "", position, occurrence, regexpMode);
                }
            } else {
                if (length > 4) {
                    throw DbException.get(ErrorCode.INVALID_PARAMETER_COUNT_2, getName(), "3..4");
                }
                Value v3 = args[2].getValue(session);
                Value v4 = length == 4 ? args[3].getValue(session) : null;
                if (v1 == ValueNull.INSTANCE || v2 == ValueNull.INSTANCE || v3 == ValueNull.INSTANCE
                        || v4 == ValueNull.INSTANCE) {
                    v1 = ValueNull.INSTANCE;
                } else {
                    v1 = regexpReplace(session, input, v2.getString(), v3.getString(), 1, 0,
                            v4 != null ? v4.getString() : null);
                }
            }
            break;
        }
        case REGEXP_SUBSTR: {
            Value v3 = length >= 3 ? args[2].getValue(session) : null;
            Value v4 = length >= 4 ? args[3].getValue(session) : null;
            Value v5 = length >= 5 ? args[4].getValue(session) : null;
            Value v6 = length >= 6 ? args[5].getValue(session) : null;
            v1 = regexpSubstr(v1, v2, v3, v4, v5, v6, session);
            break;
        }
        default:
            throw DbException.getInternalError("function=" + function);
        }
        return v1;
    }

    private static Value regexpReplace(SessionLocal session, String input, String regexp, String replacement,
            int position, int occurrence, String regexpMode) {
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
        boolean isInPostgreSqlMode = mode.getEnum() == ModeEnum.PostgreSQL;
        int flags = makeRegexpFlags(regexpMode, isInPostgreSqlMode);
        if (isInPostgreSqlMode && (regexpMode == null || regexpMode.isEmpty() || !regexpMode.contains("g"))) {
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

    private static Value regexpSubstr(Value inputString, Value regexpArg, Value positionArg, Value occurrenceArg,
            Value regexpModeArg, Value subexpressionArg, SessionLocal session) {
        if (inputString == ValueNull.INSTANCE || regexpArg == ValueNull.INSTANCE || positionArg == ValueNull.INSTANCE
                || occurrenceArg == ValueNull.INSTANCE || subexpressionArg == ValueNull.INSTANCE) {
            return ValueNull.INSTANCE;
        }
        String regexp = regexpArg.getString();

        int position = positionArg != null ? positionArg.getInt() - 1 : 0;
        int requestedOccurrence = occurrenceArg != null ? occurrenceArg.getInt() : 1;
        String regexpMode = regexpModeArg != null ? regexpModeArg.getString() : null;
        int subexpression = subexpressionArg != null ? subexpressionArg.getInt() : 0;
        int flags = makeRegexpFlags(regexpMode, false);
        try {
            Matcher m = Pattern.compile(regexp, flags).matcher(inputString.getString());

            boolean found = m.find(position);
            for (int occurrence = 1; occurrence < requestedOccurrence && found; occurrence++) {
                found = m.find();
            }

            if (!found) {
                return ValueNull.INSTANCE;
            } else {
                return ValueVarchar.get(m.group(subexpression), session);
            }
        } catch (PatternSyntaxException e) {
            throw DbException.get(ErrorCode.LIKE_ESCAPE_ERROR_1, e, regexp);
        } catch (IndexOutOfBoundsException e) {
            return ValueNull.INSTANCE;
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
    public Expression optimize(SessionLocal session) {
        boolean allConst = optimizeArguments(session, true);
        int min, max;
        switch (function) {
        case REGEXP_LIKE:
            min = 2;
            max = 3;
            type = TypeInfo.TYPE_BOOLEAN;
            break;
        case REGEXP_REPLACE:
            min = 3;
            max = 6;
            type = TypeInfo.TYPE_VARCHAR;
            break;
        case REGEXP_SUBSTR:
            min = 2;
            max = 6;
            type = TypeInfo.TYPE_VARCHAR;
            break;
        default:
            throw DbException.getInternalError("function=" + function);
        }
        int len = args.length;
        if (len < min || len > max) {
            throw DbException.get(ErrorCode.INVALID_PARAMETER_COUNT_2, getName(), min + ".." + max);
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
