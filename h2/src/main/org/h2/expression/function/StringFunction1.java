/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.h2.api.ErrorCode;
import org.h2.engine.Mode;
import org.h2.engine.Mode.ModeEnum;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.TypedValueExpression;
import org.h2.message.DbException;
import org.h2.util.StringUtils;
import org.h2.value.DataType;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueInteger;
import org.h2.value.ValueNull;
import org.h2.value.ValueVarbinary;
import org.h2.value.ValueVarchar;

/**
 * A string function with one argument.
 */
public final class StringFunction1 extends Function1 {

    // Fold functions

    /**
     * UPPER().
     */
    public static final int UPPER = 0;

    /**
     * LOWER().
     */
    public static final int LOWER = UPPER + 1;

    // Various non-standard functions

    /**
     * ASCII() (non-standard).
     */
    public static final int ASCII = LOWER + 1;

    /**
     * CHAR() (non-standard).
     */
    public static final int CHAR = ASCII + 1;

    /**
     * STRINGENCODE() (non-standard).
     */
    public static final int STRINGENCODE = CHAR + 1;

    /**
     * STRINGDECODE() (non-standard).
     */
    public static final int STRINGDECODE = STRINGENCODE + 1;

    /**
     * STRINGTOUTF8() (non-standard).
     */
    public static final int STRINGTOUTF8 = STRINGDECODE + 1;

    /**
     * UTF8TOSTRING() (non-standard).
     */
    public static final int UTF8TOSTRING = STRINGTOUTF8 + 1;

    /**
     * HEXTORAW() (non-standard).
     */
    public static final int HEXTORAW = UTF8TOSTRING + 1;

    /**
     * RAWTOHEX() (non-standard).
     */
    public static final int RAWTOHEX = HEXTORAW + 1;

    /**
     * SPACE() (non-standard).
     */
    public static final int SPACE = RAWTOHEX + 1;

    /**
     * QUOTE_IDENT() (non-standard).
     */
    public static final int QUOTE_IDENT = SPACE + 1;

    private static final String[] NAMES = { //
            "UPPER", "LOWER", "ASCII", "CHAR", "STRINGENCODE", "STRINGDECODE", "STRINGTOUTF8", "UTF8TOSTRING",
            "HEXTORAW", "RAWTOHEX", "SPACE", "QUOTE_IDENT" //
    };

    private final int function;

    public StringFunction1(Expression arg, int function) {
        super(arg);
        this.function = function;
    }

    @Override
    public Value getValue(SessionLocal session) {
        Value v = arg.getValue(session);
        if (v == ValueNull.INSTANCE) {
            return ValueNull.INSTANCE;
        }
        switch (function) {
        case UPPER:
            // TODO this is locale specific, need to document or provide a way
            // to set the locale
            v = ValueVarchar.get(v.getString().toUpperCase(), session);
            break;
        case LOWER:
            // TODO this is locale specific, need to document or provide a way
            // to set the locale
            v = ValueVarchar.get(v.getString().toLowerCase(), session);
            break;
        case ASCII: {
            String s = v.getString();
            v = s.isEmpty() ? ValueNull.INSTANCE : ValueInteger.get(s.charAt(0));
            break;
        }
        case CHAR:
            v = ValueVarchar.get(String.valueOf((char) v.getInt()), session);
            break;
        case STRINGENCODE:
            v = ValueVarchar.get(StringUtils.javaEncode(v.getString()), session);
            break;
        case STRINGDECODE:
            v = ValueVarchar.get(StringUtils.javaDecode(v.getString()), session);
            break;
        case STRINGTOUTF8:
            v = ValueVarbinary.getNoCopy(v.getString().getBytes(StandardCharsets.UTF_8));
            break;
        case UTF8TOSTRING:
            v = ValueVarchar.get(new String(v.getBytesNoCopy(), StandardCharsets.UTF_8), session);
            break;
        case HEXTORAW:
            v = hexToRaw(v.getString(), session);
            break;
        case RAWTOHEX:
            v = ValueVarchar.get(rawToHex(v, session.getMode()), session);
            break;
        case SPACE: {
            byte[] chars = new byte[Math.max(0, v.getInt())];
            Arrays.fill(chars, (byte) ' ');
            v = ValueVarchar.get(new String(chars, StandardCharsets.ISO_8859_1), session);
            break;
        }
        case QUOTE_IDENT:
            v = ValueVarchar.get(StringUtils.quoteIdentifier(v.getString()), session);
            break;
        default:
            throw DbException.getInternalError("function=" + function);
        }
        return v;
    }

    private static Value hexToRaw(String s, SessionLocal session) {
        if (session.getMode().getEnum() == ModeEnum.Oracle) {
            return ValueVarbinary.get(StringUtils.convertHexToBytes(s));
        }
        int len = s.length();
        if (len % 4 != 0) {
            throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, s);
        }
        StringBuilder builder = new StringBuilder(len / 4);
        for (int i = 0; i < len; i += 4) {
            try {
                builder.append((char) Integer.parseInt(s.substring(i, i + 4), 16));
            } catch (NumberFormatException e) {
                throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, s);
            }
        }
        return ValueVarchar.get(builder.toString(), session);
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

    @Override
    public Expression optimize(SessionLocal session) {
        arg = arg.optimize(session);
        switch (function) {
        /*
         * UPPER and LOWER may return string of different length for some
         * characters.
         */
        case UPPER:
        case LOWER:
        case STRINGENCODE:
        case SPACE:
        case QUOTE_IDENT:
            type = TypeInfo.TYPE_VARCHAR;
            break;
        case ASCII:
            type = TypeInfo.TYPE_INTEGER;
            break;
        case CHAR:
            type = TypeInfo.getTypeInfo(Value.VARCHAR, 1L, 0, null);
            break;
        case STRINGDECODE: {
            TypeInfo t = arg.getType();
            type = DataType.isCharacterStringType(t.getValueType())
                    ? TypeInfo.getTypeInfo(Value.VARCHAR, t.getPrecision(), 0, null)
                    : TypeInfo.TYPE_VARCHAR;
            break;
        }
        case STRINGTOUTF8:
            type = TypeInfo.TYPE_VARBINARY;
            break;
        case UTF8TOSTRING: {
            TypeInfo t = arg.getType();
            type = DataType.isBinaryStringType(t.getValueType())
                    ? TypeInfo.getTypeInfo(Value.VARCHAR, t.getPrecision(), 0, null)
                    : TypeInfo.TYPE_VARCHAR;
            break;
        }
        case HEXTORAW: {
            TypeInfo t = arg.getType();
            if (session.getMode().getEnum() == ModeEnum.Oracle) {
                if (DataType.isCharacterStringType(t.getValueType())) {
                    type = TypeInfo.getTypeInfo(Value.VARBINARY, t.getPrecision() / 2, 0, null);
                } else {
                    type = TypeInfo.TYPE_VARBINARY;
                }
            } else {
                if (DataType.isCharacterStringType(t.getValueType())) {
                    type = TypeInfo.getTypeInfo(Value.VARCHAR, t.getPrecision() / 4, 0, null);
                } else {
                    type = TypeInfo.TYPE_VARCHAR;
                }
            }
            break;
        }
        case RAWTOHEX: {
            TypeInfo t = arg.getType();
            long precision = t.getPrecision();
            int mul = DataType.isBinaryStringOrSpecialBinaryType(t.getValueType()) ? 2
                    : session.getMode().getEnum() == ModeEnum.Oracle ? 6 : 4;
            type = TypeInfo.getTypeInfo(Value.VARCHAR,
                    precision <= Long.MAX_VALUE / mul ? precision * mul : Long.MAX_VALUE, 0, null);
            break;
        }
        default:
            throw DbException.getInternalError("function=" + function);
        }
        if (arg.isConstant()) {
            return TypedValueExpression.getTypedIfNull(getValue(session), type);
        }
        return this;
    }

    @Override
    public String getName() {
        return NAMES[function];
    }

}
