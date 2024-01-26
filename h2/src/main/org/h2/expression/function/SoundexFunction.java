/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function;

import java.nio.charset.StandardCharsets;

import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.TypedValueExpression;
import org.h2.message.DbException;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueInteger;
import org.h2.value.ValueVarchar;

/**
 * A SOUNDEX or DIFFERENCE function.
 */
public final class SoundexFunction extends Function1_2 {

    /**
     * SOUNDEX() (non-standard).
     */
    public static final int SOUNDEX = 0;

    /**
     * DIFFERENCE() (non-standard).
     */
    public static final int DIFFERENCE = SOUNDEX + 1;

    private static final String[] NAMES = { //
            "SOUNDEX", "DIFFERENCE" //
    };

    private static final byte[] SOUNDEX_INDEX = //
            "71237128722455712623718272\000\000\000\000\000\00071237128722455712623718272"
                    .getBytes(StandardCharsets.ISO_8859_1);

    private final int function;

    public SoundexFunction(Expression arg1, Expression arg2, int function) {
        super(arg1, arg2);
        this.function = function;
    }

    @Override
    public Value getValue(SessionLocal session, Value v1, Value v2) {
        switch (function) {
        case SOUNDEX:
            v1 = ValueVarchar.get(new String(getSoundex(v1.getString()), StandardCharsets.ISO_8859_1), session);
            break;
        case DIFFERENCE: {
            v1 = ValueInteger.get(getDifference(v1.getString(), v2.getString()));
            break;
        }
        default:
            throw DbException.getInternalError("function=" + function);
        }
        return v1;
    }

    private static int getDifference(String s1, String s2) {
        // TODO function difference: compatibility with SQL Server and HSQLDB
        byte[] b1 = getSoundex(s1), b2 = getSoundex(s2);
        int e = 0;
        for (int i = 0; i < 4; i++) {
            if (b1[i] == b2[i]) {
                e++;
            }
        }
        return e;
    }

    private static byte[] getSoundex(String s) {
        byte[] chars = { '0', '0', '0', '0' };
        byte lastDigit = '0';
        for (int i = 0, j = 0, l = s.length(); i < l && j < 4; i++) {
            char c = s.charAt(i);
            if (c >= 'A' && c <= 'z') {
                byte newDigit = SOUNDEX_INDEX[c - 'A'];
                if (newDigit != 0) {
                    if (j == 0) {
                        chars[j++] = (byte) (c & 0xdf); // Converts a-z to A-Z
                        lastDigit = newDigit;
                    } else if (newDigit <= '6') {
                        if (newDigit != lastDigit) {
                            chars[j++] = lastDigit = newDigit;
                        }
                    } else if (newDigit == '7') {
                        lastDigit = newDigit;
                    }
                }
            }
        }
        return chars;
    }

    @Override
    public Expression optimize(SessionLocal session) {
        left = left.optimize(session);
        if (right != null) {
            right = right.optimize(session);
        }
        switch (function) {
        case SOUNDEX:
            type = TypeInfo.getTypeInfo(Value.VARCHAR, 4, 0, null);
            break;
        case DIFFERENCE:
            type = TypeInfo.TYPE_INTEGER;
            break;
        default:
            throw DbException.getInternalError("function=" + function);
        }
        if (left.isConstant() && (right == null || right.isConstant())) {
            return TypedValueExpression.getTypedIfNull(getValue(session), type);
        }
        return this;
    }

    @Override
    public String getName() {
        return NAMES[function];
    }

}
