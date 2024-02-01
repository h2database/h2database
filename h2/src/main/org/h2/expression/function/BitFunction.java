/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function;

import static org.h2.util.Bits.LONG_VH_BE;

import java.util.Arrays;

import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.TypedValueExpression;
import org.h2.expression.aggregate.Aggregate;
import org.h2.expression.aggregate.AggregateType;
import org.h2.message.DbException;
import org.h2.mvstore.db.Store;
import org.h2.value.DataType;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueBigint;
import org.h2.value.ValueBinary;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueInteger;
import org.h2.value.ValueSmallint;
import org.h2.value.ValueTinyint;
import org.h2.value.ValueVarbinary;

/**
 * A bitwise function.
 */
public final class BitFunction extends Function1_2 {

    /**
     * BITAND() (non-standard).
     */
    public static final int BITAND = 0;

    /**
     * BITOR() (non-standard).
     */
    public static final int BITOR = BITAND + 1;

    /**
     * BITXOR() (non-standard).
     */
    public static final int BITXOR = BITOR + 1;

    /**
     * BITNOT() (non-standard).
     */
    public static final int BITNOT = BITXOR + 1;

    /**
     * BITNAND() (non-standard).
     */
    public static final int BITNAND = BITNOT + 1;

    /**
     * BITNOR() (non-standard).
     */
    public static final int BITNOR = BITNAND + 1;

    /**
     * BITXNOR() (non-standard).
     */
    public static final int BITXNOR = BITNOR + 1;

    /**
     * BITGET() (non-standard).
     */
    public static final int BITGET = BITXNOR + 1;

    /**
     * BITCOUNT() (non-standard).
     */
    public static final int BITCOUNT = BITGET + 1;

    /**
     * LSHIFT() (non-standard).
     */
    public static final int LSHIFT = BITCOUNT + 1;

    /**
     * RSHIFT() (non-standard).
     */
    public static final int RSHIFT = LSHIFT + 1;

    /**
     * ULSHIFT() (non-standard).
     */
    public static final int ULSHIFT = RSHIFT + 1;

    /**
     * URSHIFT() (non-standard).
     */
    public static final int URSHIFT = ULSHIFT + 1;

    /**
     * ROTATELEFT() (non-standard).
     */
    public static final int ROTATELEFT = URSHIFT + 1;

    /**
     * ROTATERIGHT() (non-standard).
     */
    public static final int ROTATERIGHT = ROTATELEFT + 1;

    private static final String[] NAMES = { //
            "BITAND", "BITOR", "BITXOR", "BITNOT", "BITNAND", "BITNOR", "BITXNOR", "BITGET", "BITCOUNT", "LSHIFT",
            "RSHIFT", "ULSHIFT", "URSHIFT", "ROTATELEFT", "ROTATERIGHT" //
    };

    private final int function;

    public BitFunction(Expression arg1, Expression arg2, int function) {
        super(arg1, arg2);
        this.function = function;
    }

    @Override
    public Value getValue(SessionLocal session, Value v1, Value v2) {
        switch (function) {
        case BITGET:
            return bitGet(v1, v2);
        case BITCOUNT:
            return bitCount(v1);
        case LSHIFT:
            return shift(v1, v2.getLong(), false);
        case RSHIFT: {
            long offset = v2.getLong();
            return shift(v1, offset != Long.MIN_VALUE ? -offset : Long.MAX_VALUE, false);
        }
        case ULSHIFT:
            return shift(v1, v2.getLong(), true);
        case URSHIFT:
            return shift(v1, -v2.getLong(), true);
        case ROTATELEFT:
            return rotate(v1, v2.getLong(), false);
        case ROTATERIGHT:
            return rotate(v1, v2.getLong(), true);
        }
        return getBitwise(function, type, v1, v2);
    }

    private static ValueBoolean bitGet(Value v1, Value v2) {
        long offset = v2.getLong();
        boolean b;
        if (offset >= 0L) {
            switch (v1.getValueType()) {
            case Value.BINARY:
            case Value.VARBINARY: {
                byte[] bytes = v1.getBytesNoCopy();
                int bit = (int) (offset & 0x7);
                offset >>>= 3;
                b = offset < bytes.length && (bytes[(int) offset] & (1 << bit)) != 0;
                break;
            }
            case Value.TINYINT:
                b = offset < 8 && (v1.getByte() & (1 << offset)) != 0;
                break;
            case Value.SMALLINT:
                b = offset < 16 && (v1.getShort() & (1 << offset)) != 0;
                break;
            case Value.INTEGER:
                b = offset < 32 && (v1.getInt() & (1 << offset)) != 0;
                break;
            case Value.BIGINT:
                b = (v1.getLong() & (1L << offset)) != 0;
                break;
            default:
                throw DbException.getInvalidValueException("bit function parameter", v1.getTraceSQL());
            }
        } else {
            b = false;
        }
        return ValueBoolean.get(b);
    }

    private static ValueBigint bitCount(Value v1) {
        long c;
        switch (v1.getValueType()) {
        case Value.BINARY:
        case Value.VARBINARY: {
            byte[] bytes = v1.getBytesNoCopy();
            int l = bytes.length;
            c = 0L;
            int i = 0;
            for (int bound = l & 0xfffffff8; i < bound; i += 8) {
                c += Long.bitCount((long) LONG_VH_BE.get(bytes, i));
            }
            for (; i < l; i++) {
                c += Integer.bitCount(bytes[i] & 0xff);
            }
            break;
        }
        case Value.TINYINT:
            c = Integer.bitCount(v1.getByte() & 0xff);
            break;
        case Value.SMALLINT:
            c = Integer.bitCount(v1.getShort() & 0xffff);
            break;
        case Value.INTEGER:
            c = Integer.bitCount(v1.getInt());
            break;
        case Value.BIGINT:
            c = Long.bitCount(v1.getLong());
            break;
        default:
            throw DbException.getInvalidValueException("bit function parameter", v1.getTraceSQL());
        }
        return ValueBigint.get(c);
    }

    private static Value shift(Value v1, long offset, boolean unsigned) {
        if (offset == 0L) {
            return v1;
        }
        int vt = v1.getValueType();
        switch (vt) {
        case Value.BINARY:
        case Value.VARBINARY: {
            byte[] bytes = v1.getBytesNoCopy();
            int length = bytes.length;
            if (length == 0) {
                return v1;
            }
            byte[] newBytes = new byte[length];
            if (offset > -8L * length && offset < 8L * length) {
                if (offset > 0) {
                    int nBytes = (int) (offset >> 3);
                    int nBits = ((int) offset) & 0x7;
                    if (nBits == 0) {
                        System.arraycopy(bytes, nBytes, newBytes, 0, length - nBytes);
                    } else {
                        int nBits2 = 8 - nBits;
                        int dstIndex = 0, srcIndex = nBytes;
                        length--;
                        while (srcIndex < length) {
                            newBytes[dstIndex++] = (byte) (bytes[srcIndex++] << nBits
                                    | (bytes[srcIndex] & 0xff) >>> nBits2);
                        }
                        newBytes[dstIndex] = (byte) (bytes[srcIndex] << nBits);
                    }
                } else {
                    offset = -offset;
                    int nBytes = (int) (offset >> 3);
                    int nBits = ((int) offset) & 0x7;
                    if (nBits == 0) {
                        System.arraycopy(bytes, 0, newBytes, nBytes, length - nBytes);
                    } else {
                        int nBits2 = 8 - nBits;
                        int dstIndex = nBytes, srcIndex = 0;
                        newBytes[dstIndex++] = (byte) ((bytes[srcIndex] & 0xff) >>> nBits);
                        while (dstIndex < length) {
                            newBytes[dstIndex++] = (byte) (bytes[srcIndex++] << nBits2
                                    | (bytes[srcIndex] & 0xff) >>> nBits);
                        }
                    }
                }
            }
            return vt == Value.BINARY ? ValueBinary.getNoCopy(newBytes) : ValueVarbinary.getNoCopy(newBytes);
        }
        case Value.TINYINT: {
            byte v;
            if (offset < 8) {
                v = v1.getByte();
                if (offset > -8) {
                    if (offset > 0) {
                        v <<= (int) offset;
                    } else if (unsigned) {
                        v = (byte) ((v & 0xFF) >>> (int) -offset);
                    } else {
                        v >>= (int) -offset;
                    }
                } else if (unsigned) {
                    v = 0;
                } else {
                    v >>= 7;
                }
            } else {
                v = 0;
            }
            return ValueTinyint.get(v);
        }
        case Value.SMALLINT: {
            short v;
            if (offset < 16) {
                v = v1.getShort();
                if (offset > -16) {
                    if (offset > 0) {
                        v <<= (int) offset;
                    } else if (unsigned) {
                        v = (short) ((v & 0xFFFF) >>> (int) -offset);
                    } else {
                        v >>= (int) -offset;
                    }
                } else if (unsigned) {
                    v = 0;
                } else {
                    v >>= 15;
                }
            } else {
                v = 0;
            }
            return ValueSmallint.get(v);
        }
        case Value.INTEGER: {
            int v;
            if (offset < 32) {
                v = v1.getInt();
                if (offset > -32) {
                    if (offset > 0) {
                        v <<= (int) offset;
                    } else if (unsigned) {
                        v >>>= (int) -offset;
                    } else {
                        v >>= (int) -offset;
                    }
                } else if (unsigned) {
                    v = 0;
                } else {
                    v >>= 31;
                }
            } else {
                v = 0;
            }
            return ValueInteger.get(v);
        }
        case Value.BIGINT: {
            long v;
            if (offset < 64) {
                v = v1.getLong();
                if (offset > -64) {
                    if (offset > 0) {
                        v <<= offset;
                    } else if (unsigned) {
                        v >>>= -offset;
                    } else {
                        v >>= -offset;
                    }
                } else if (unsigned) {
                    v = 0;
                } else {
                    v >>= 63;
                }
            } else {
                v = 0;
            }
            return ValueBigint.get(v);
        }
        default:
            throw DbException.getInvalidValueException("bit function parameter", v1.getTraceSQL());
        }
    }

    private static Value rotate(Value v1, long offset, boolean right) {
        int vt = v1.getValueType();
        switch (vt) {
        case Value.BINARY:
        case Value.VARBINARY: {
            byte[] bytes = v1.getBytesNoCopy();
            int length = bytes.length;
            if (length == 0) {
                return v1;
            }
            long bitLength = length << 3L;
            offset %= bitLength;
            if (right) {
                offset = -offset;
            }
            if (offset == 0L) {
                return v1;
            } else if (offset < 0) {
                offset += bitLength;
            }
            byte[] newBytes = new byte[length];
            int nBytes = (int) (offset >> 3);
            int nBits = ((int) offset) & 0x7;
            if (nBits == 0) {
                System.arraycopy(bytes, nBytes, newBytes, 0, length - nBytes);
                System.arraycopy(bytes, 0, newBytes, length - nBytes, nBytes);
            } else {
                int nBits2 = 8 - nBits;
                for (int dstIndex = 0, srcIndex = nBytes; dstIndex < length;) {
                    newBytes[dstIndex++] = (byte) (bytes[srcIndex] << nBits
                            | (bytes[srcIndex = (srcIndex + 1) % length] & 0xFF) >>> nBits2);
                }
            }
            return vt == Value.BINARY ? ValueBinary.getNoCopy(newBytes) : ValueVarbinary.getNoCopy(newBytes);
        }
        case Value.TINYINT: {
            int o = (int) offset;
            if (right) {
                o = -o;
            }
            if ((o &= 0x7) == 0) {
                return v1;
            }
            int v = v1.getByte() & 0xFF;
            return ValueTinyint.get((byte) ((v << o) | (v >>> 8 - o)));
        }
        case Value.SMALLINT: {
            int o = (int) offset;
            if (right) {
                o = -o;
            }
            if ((o &= 0xF) == 0) {
                return v1;
            }
            int v = v1.getShort() & 0xFFFF;
            return ValueSmallint.get((short) ((v << o) | (v >>> 16 - o)));
        }
        case Value.INTEGER: {
            int o = (int) offset;
            if (right) {
                o = -o;
            }
            if ((o &= 0x1F) == 0) {
                return v1;
            }
            return ValueInteger.get(Integer.rotateLeft(v1.getInt(), o));
        }
        case Value.BIGINT: {
            int o = (int) offset;
            if (right) {
                o = -o;
            }
            if ((o &= 0x3F) == 0) {
                return v1;
            }
            return ValueBigint.get(Long.rotateLeft(v1.getLong(), o));
        }
        default:
            throw DbException.getInvalidValueException("bit function parameter", v1.getTraceSQL());
        }
    }

    /**
     * Computes the value of bitwise function.
     *
     * @param function
     *            one of {@link #BITAND}, {@link #BITOR}, {@link #BITXOR},
     *            {@link #BITNOT}, {@link #BITNAND}, {@link #BITNOR},
     *            {@link #BITXNOR}
     * @param type
     *            the type of result
     * @param v1
     *            the value of first argument
     * @param v2
     *            the value of second argument, or {@code null}
     * @return the resulting value
     */
    public static Value getBitwise(int function, TypeInfo type, Value v1, Value v2) {
        return type.getValueType() < Value.TINYINT ? getBinaryString(function, type, v1, v2)
                : getNumeric(function, type, v1, v2);
    }

    private static Value getBinaryString(int function, TypeInfo type, Value v1, Value v2) {
        byte[] bytes;
        if (function == BITNOT) {
            bytes = v1.getBytes();
            for (int i = 0, l = bytes.length; i < l; i++) {
                bytes[i] = (byte) ~bytes[i];
            }
        } else {
            byte[] bytes1 = v1.getBytesNoCopy(), bytes2 = v2.getBytesNoCopy();
            int length1 = bytes1.length, length2 = bytes2.length;
            int min, max;
            if (length1 <= length2) {
                min = length1;
                max = length2;
            } else {
                min = length2;
                max = length1;
                byte[] t = bytes1;
                bytes1 = bytes2;
                bytes2 = t;
            }
            int limit = (int) type.getPrecision();
            if (min > limit) {
                max = min = limit;
            } else if (max > limit) {
                max = limit;
            }
            bytes = new byte[max];
            int i = 0;
            switch (function) {
            case BITAND:
                for (; i < min; i++) {
                    bytes[i] = (byte) (bytes1[i] & bytes2[i]);
                }
                break;
            case BITOR:
                for (; i < min; i++) {
                    bytes[i] = (byte) (bytes1[i] | bytes2[i]);
                }
                System.arraycopy(bytes2, i, bytes, i, max - i);
                break;
            case BITXOR:
                for (; i < min; i++) {
                    bytes[i] = (byte) (bytes1[i] ^ bytes2[i]);
                }
                System.arraycopy(bytes2, i, bytes, i, max - i);
                break;
            case BITNAND:
                for (; i < min; i++) {
                    bytes[i] = (byte) ~(bytes1[i] & bytes2[i]);
                }
                Arrays.fill(bytes, i, max, (byte) -1);
                break;
            case BITNOR:
                for (; i < min; i++) {
                    bytes[i] = (byte) ~(bytes1[i] | bytes2[i]);
                }
                for (; i < max; i++) {
                    bytes[i] = (byte) ~bytes2[i];
                }
                break;
            case BITXNOR:
                for (; i < min; i++) {
                    bytes[i] = (byte) ~(bytes1[i] ^ bytes2[i]);
                }
                for (; i < max; i++) {
                    bytes[i] = (byte) ~bytes2[i];
                }
                break;
            default:
                throw DbException.getInternalError("function=" + function);
            }
        }
        return type.getValueType() == Value.BINARY ? ValueBinary.getNoCopy(bytes) : ValueVarbinary.getNoCopy(bytes);
    }

    private static Value getNumeric(int function, TypeInfo type, Value v1, Value v2) {
        long l1 = v1.getLong();
        switch (function) {
        case BITAND:
            l1 &= v2.getLong();
            break;
        case BITOR:
            l1 |= v2.getLong();
            break;
        case BITXOR:
            l1 ^= v2.getLong();
            break;
        case BITNOT:
            l1 = ~l1;
            break;
        case BITNAND:
            l1 = ~(l1 & v2.getLong());
            break;
        case BITNOR:
            l1 = ~(l1 | v2.getLong());
            break;
        case BITXNOR:
            l1 = ~(l1 ^ v2.getLong());
            break;
        default:
            throw DbException.getInternalError("function=" + function);
        }
        switch (type.getValueType()) {
        case Value.TINYINT:
            return ValueTinyint.get((byte) l1);
        case Value.SMALLINT:
            return ValueSmallint.get((short) l1);
        case Value.INTEGER:
            return ValueInteger.get((int) l1);
        case Value.BIGINT:
            return ValueBigint.get(l1);
        default:
            throw DbException.getInternalError();
        }
    }

    @Override
    public Expression optimize(SessionLocal session) {
        left = left.optimize(session);
        if (right != null) {
            right = right.optimize(session);
        }
        switch (function) {
        case BITNOT:
            return optimizeNot(session);
        case BITGET:
            type = TypeInfo.TYPE_BOOLEAN;
            break;
        case BITCOUNT:
            type = TypeInfo.TYPE_BIGINT;
            break;
        case LSHIFT:
        case RSHIFT:
        case ULSHIFT:
        case URSHIFT:
        case ROTATELEFT:
        case ROTATERIGHT:
            type = checkArgType(left);
            break;
        default:
            type = getCommonType(left, right);
            break;
        }
        if (left.isConstant() && (right == null || right.isConstant())) {
            return TypedValueExpression.getTypedIfNull(getValue(session), type);
        }
        return this;
    }

    private Expression optimizeNot(SessionLocal session) {
        type = checkArgType(left);
        if (left.isConstant()) {
            return TypedValueExpression.getTypedIfNull(getValue(session), type);
        } else if (left instanceof BitFunction) {
            BitFunction l = (BitFunction) left;
            int f = l.function;
            switch (f) {
            case BITAND:
            case BITOR:
            case BITXOR:
                f += BITNAND - BITAND;
                break;
            case BITNOT:
                return l.left;
            case BITNAND:
            case BITNOR:
            case BITXNOR:
                f -= BITNAND - BITAND;
                break;
            default:
                return this;
            }
            return new BitFunction(l.left, l.right, f).optimize(session);
        } else if (left instanceof Aggregate) {
            Aggregate l = (Aggregate) left;
            AggregateType t;
            switch (l.getAggregateType()) {
            case BIT_AND_AGG:
                t = AggregateType.BIT_NAND_AGG;
                break;
            case BIT_OR_AGG:
                t = AggregateType.BIT_NOR_AGG;
                break;
            case BIT_XOR_AGG:
                t = AggregateType.BIT_XNOR_AGG;
                break;
            case BIT_NAND_AGG:
                t = AggregateType.BIT_AND_AGG;
                break;
            case BIT_NOR_AGG:
                t = AggregateType.BIT_OR_AGG;
                break;
            case BIT_XNOR_AGG:
                t = AggregateType.BIT_XOR_AGG;
                break;
            default:
                return this;
            }
            Aggregate aggregate = new Aggregate(t, new Expression[] { l.getSubexpression(0) }, l.getSelect(),
                    l.isDistinct());
            aggregate.setFilterCondition(l.getFilterCondition());
            aggregate.setOverCondition(l.getOverCondition());
            return aggregate.optimize(session);
        }
        return this;
    }

    private static TypeInfo getCommonType(Expression arg1, Expression arg2) {
        TypeInfo t1 = checkArgType(arg1), t2 = checkArgType(arg2);
        int vt1 = t1.getValueType(), vt2 = t2.getValueType();
        boolean bs = DataType.isBinaryStringType(vt1);
        if (bs != DataType.isBinaryStringType(vt2)) {
            throw DbException.getInvalidValueException("bit function parameters",
                    t2.getSQL(t1.getSQL(new StringBuilder(), TRACE_SQL_FLAGS).append(" vs "), TRACE_SQL_FLAGS)
                            .toString());
        }
        if (bs) {
            long precision;
            if (vt1 == Value.BINARY) {
                precision = t1.getDeclaredPrecision();
                if (vt2 == Value.BINARY) {
                    precision = Math.max(precision, t2.getDeclaredPrecision());
                }
            } else {
                if (vt2 == Value.BINARY) {
                    vt1 = Value.BINARY;
                    precision = t2.getDeclaredPrecision();
                } else {
                    long precision1 = t1.getDeclaredPrecision(), precision2 = t2.getDeclaredPrecision();
                    precision = precision1 <= 0L || precision2 <= 0L ? -1L : Math.max(precision1, precision2);
                }
            }
            return TypeInfo.getTypeInfo(vt1, precision, 0, null);
        }
        return TypeInfo.getTypeInfo(Math.max(vt1, vt2));
    }

    /**
     * Checks the type of an argument of bitwise function (one of
     * {@link #BITAND}, {@link #BITOR}, {@link #BITXOR}, {@link #BITNOT},
     * {@link #BITNAND}, {@link #BITNOR}, {@link #BITXNOR}).
     *
     * @param arg
     *            the argument
     * @return the type of the specified argument
     * @throws DbException
     *             if argument type is not supported by bitwise functions
     */
    public static TypeInfo checkArgType(Expression arg) {
        TypeInfo t = arg.getType();
        switch (t.getValueType()) {
        case Value.NULL:
        case Value.BINARY:
        case Value.VARBINARY:
        case Value.TINYINT:
        case Value.SMALLINT:
        case Value.INTEGER:
        case Value.BIGINT:
            return t;
        }
        throw Store.getInvalidExpressionTypeException("bit function argument", arg);
    }

    @Override
    public String getName() {
        return NAMES[function];
    }

}
