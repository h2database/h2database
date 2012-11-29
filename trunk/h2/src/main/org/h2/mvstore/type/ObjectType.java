/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.type;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.h2.mvstore.DataUtils;
import org.h2.util.Utils;

/**
 * A data type implementation for the most common data types, including
 * serializable objects.
 */
public class ObjectType implements DataType {

    // TODO maybe support InputStream, Reader
    // TODO maybe support ResultSet, Date, Time, Timestamp
    // TODO maybe support boolean[], short[],...

    /**
     * The type constants are also used as tag values.
     */
    static final int TYPE_BOOLEAN = 1;
    static final int TYPE_BYTE = 2;
    static final int TYPE_SHORT = 3;
    static final int TYPE_INTEGER = 4;
    static final int TYPE_LONG = 5;
    static final int TYPE_BIG_INTEGER = 6;
    static final int TYPE_FLOAT = 7;
    static final int TYPE_DOUBLE = 8;
    static final int TYPE_BIG_DECIMAL = 9;
    static final int TYPE_CHARACTER = 10;
    static final int TYPE_STRING = 11;
    static final int TYPE_UUID = 12;
    static final int TYPE_BYTE_ARRAY = 13;
    static final int TYPE_INT_ARRAY = 14;
    static final int TYPE_LONG_ARRAY = 15;
    static final int TYPE_CHAR_ARRAY = 16;
    static final int TYPE_SERIALIZED_OBJECT = 17;

    /**
     * For very common values (e.g. 0 and 1) we save space by encoding the value
     * in the tag. e.g. TAG_BOOLEAN_TRUE and TAG_FLOAT_0.
     */
    static final int TAG_BOOLEAN_TRUE = 32;
    static final int TAG_INTEGER_NEGATIVE = 33;
    static final int TAG_INTEGER_FIXED = 34;
    static final int TAG_LONG_NEGATIVE = 35;
    static final int TAG_LONG_FIXED = 36;
    static final int TAG_BIG_INTEGER_0 = 37;
    static final int TAG_BIG_INTEGER_1 = 38;
    static final int TAG_BIG_INTEGER_SMALL = 39;
    static final int TAG_FLOAT_0 = 40;
    static final int TAG_FLOAT_1 = 41;
    static final int TAG_FLOAT_FIXED = 42;
    static final int TAG_DOUBLE_0 = 43;
    static final int TAG_DOUBLE_1 = 44;
    static final int TAG_DOUBLE_FIXED = 45;
    static final int TAG_BIG_DECIMAL_0 = 46;
    static final int TAG_BIG_DECIMAL_1 = 47;
    static final int TAG_BIG_DECIMAL_SMALL = 48;
    static final int TAG_BIG_DECIMAL_SMALL_SCALED = 49;

    /**
     * For small-values/small-arrays, we encode the value/array-length in the
     * tag.
     */
    static final int TAG_INTEGER_0_15 = 64;
    static final int TAG_LONG_0_7 = 80;
    static final int TAG_STRING_0_15 = 88;
    static final int TAG_BYTE_ARRAY_0_15 = 104;

    /**
     * Constants for floating point synchronization.
     */
    static final int FLOAT_ZERO_BITS = Float.floatToIntBits(0.0f);
    static final int FLOAT_ONE_BITS = Float.floatToIntBits(1.0f);
    static final long DOUBLE_ZERO_BITS = Double.doubleToLongBits(0.0d);
    static final long DOUBLE_ONE_BITS = Double.doubleToLongBits(1.0d);

    private AutoDetectDataType last = new StringType(this);

    @Override
    public int compare(Object a, Object b) {
        return last.compare(a, b);
    }

    @Override
    public int getMaxLength(Object obj) {
        return last.getMaxLength(obj);
    }

    @Override
    public int getMemory(Object obj) {
        return last.getMemory(obj);
    }

    @Override
    public void write(ByteBuffer buff, Object obj) {
        last.write(buff, obj);
    }

    private AutoDetectDataType newType(int typeId) {
        switch (typeId) {
        case TYPE_BOOLEAN:
            return new BooleanType(this);
        case TYPE_BYTE:
            return new ByteType(this);
        case TYPE_SHORT:
            return new ShortType(this);
        case TYPE_CHARACTER:
            return new CharacterType(this);
        case TYPE_INTEGER:
            return new IntegerType(this);
        case TYPE_LONG:
            return new LongType(this);
        case TYPE_FLOAT:
            return new FloatType(this);
        case TYPE_DOUBLE:
            return new DoubleType(this);
        case TYPE_BIG_INTEGER:
            return new BigIntegerType(this);
        case TYPE_BIG_DECIMAL:
            return new BigDecimalType(this);
        case TYPE_BYTE_ARRAY:
            return new ByteArrayType(this);
        case TYPE_CHAR_ARRAY:
            return new CharArrayType(this);
        case TYPE_INT_ARRAY:
            return new IntArrayType(this);
        case TYPE_LONG_ARRAY:
            return new LongArrayType(this);
        case TYPE_STRING:
            return new StringType(this);
        case TYPE_UUID:
            return new UUIDType(this);
        case TYPE_SERIALIZED_OBJECT:
            return new SerializedObjectType(this);
        }
        throw new RuntimeException("Unsupported type: " + typeId);
    }

    @Override
    public Object read(ByteBuffer buff) {
        int tag = buff.get();
        int typeId;
        if (tag <= TYPE_SERIALIZED_OBJECT) {
            typeId = tag;
        } else {
            switch(tag) {
            case TAG_BOOLEAN_TRUE:
                typeId = TYPE_BOOLEAN;
                break;
            case TAG_INTEGER_NEGATIVE:
            case TAG_INTEGER_FIXED:
                typeId = TYPE_INTEGER;
                break;
            case TAG_LONG_NEGATIVE:
            case TAG_LONG_FIXED:
                typeId = TYPE_LONG;
                break;
            case TAG_BIG_INTEGER_0:
            case TAG_BIG_INTEGER_1:
            case TAG_BIG_INTEGER_SMALL:
                typeId = TYPE_BIG_INTEGER;
                break;
            case TAG_FLOAT_0:
            case TAG_FLOAT_1:
            case TAG_FLOAT_FIXED:
                typeId = TYPE_FLOAT;
                break;
            case TAG_DOUBLE_0:
            case TAG_DOUBLE_1:
            case TAG_DOUBLE_FIXED:
                typeId = TYPE_DOUBLE;
                break;
            case TAG_BIG_DECIMAL_0:
            case TAG_BIG_DECIMAL_1:
            case TAG_BIG_DECIMAL_SMALL:
            case TAG_BIG_DECIMAL_SMALL_SCALED:
                typeId = TYPE_BIG_DECIMAL;
                break;
            default:
                if (tag >= TAG_INTEGER_0_15 && tag <= TAG_INTEGER_0_15 + 15) {
                    typeId = TYPE_INTEGER;
                } else if (tag >= TAG_STRING_0_15 && tag <= TAG_STRING_0_15 + 15) {
                    typeId = TYPE_STRING;
                } else if (tag >= TAG_LONG_0_7 && tag <= TAG_LONG_0_7 + 7) {
                    typeId = TYPE_LONG;
                } else if (tag >= TAG_BYTE_ARRAY_0_15 && tag <= TAG_BYTE_ARRAY_0_15 + 15) {
                    typeId = TYPE_BYTE_ARRAY;
                } else {
                    throw new RuntimeException("Unknown tag: " + tag);
                }
            }
        }
        if (typeId != last.typeId) {
            last = newType(typeId);
        }
        return last.read(buff, tag);
    }

    @Override
    public String asString() {
        return "o";
    }

    private static int getTypeId(Object obj) {
        if (obj instanceof Integer) {
            return TYPE_INTEGER;
        } else if (obj instanceof String) {
            return TYPE_STRING;
        } else if (obj instanceof Long) {
            return TYPE_LONG;
        } else if (obj instanceof BigDecimal) {
            if (obj.getClass() == BigDecimal.class) {
                return TYPE_BIG_DECIMAL;
            }
        } else if (obj instanceof byte[]) {
            return TYPE_BYTE_ARRAY;
        } else if (obj instanceof Double) {
            return TYPE_DOUBLE;
        } else if (obj instanceof Float) {
            return TYPE_FLOAT;
        } else if (obj instanceof Boolean) {
            return TYPE_BOOLEAN;
        } else if (obj instanceof UUID) {
            return TYPE_UUID;
        } else if (obj instanceof Byte) {
            return TYPE_BYTE;
        } else if (obj instanceof int[]) {
            return TYPE_INT_ARRAY;
        } else if (obj instanceof long[]) {
            return TYPE_LONG_ARRAY;
        } else if (obj instanceof char[]) {
            return TYPE_CHAR_ARRAY;
        } else if (obj instanceof Short) {
            return TYPE_SHORT;
        } else if (obj instanceof BigInteger) {
            if (obj.getClass() == BigInteger.class) {
                return TYPE_BIG_INTEGER;
            }
        } else if (obj instanceof Character) {
            return TYPE_CHARACTER;
        }
        if (obj == null) {
            throw new NullPointerException();
        }
        return TYPE_SERIALIZED_OBJECT;
    }

    /**
     * Switch the last remembered type to match the type of the given object.
     *
     * @param obj the object
     * @return the auto-detected type used
     */
    AutoDetectDataType switchType(Object obj) {
        int typeId = getTypeId(obj);
        AutoDetectDataType l = last;
        if (typeId != l.typeId) {
            l = last = newType(typeId);
        }
        return l;
    }

    /**
     * Compare the contents of two arrays.
     *
     * @param data1 the first array (must not be null)
     * @param data2 the second array (must not be null)
     * @return the result of the comparison (-1, 1 or 0)
     */
    public static int compareNotNull(char[] data1, char[] data2) {
        if (data1 == data2) {
            return 0;
        }
        int len = Math.min(data1.length, data2.length);
        for (int i = 0; i < len; i++) {
            char x = data1[i];
            char x2 = data2[i];
            if (x != x2) {
                return x > x2 ? 1 : -1;
            }
        }
        return Integer.signum(data1.length - data2.length);
    }

    /**
     * Compare the contents of two arrays.
     *
     * @param data1 the first array (must not be null)
     * @param data2 the second array (must not be null)
     * @return the result of the comparison (-1, 1 or 0)
     */
    public static int compareNotNull(int[] data1, int[] data2) {
        if (data1 == data2) {
            return 0;
        }
        int len = Math.min(data1.length, data2.length);
        for (int i = 0; i < len; i++) {
            int x = data1[i];
            int x2 = data2[i];
            if (x != x2) {
                return x > x2 ? 1 : -1;
            }
        }
        return Integer.signum(data1.length - data2.length);
    }

    /**
     * Compare the contents of two arrays.
     *
     * @param data1 the first array (must not be null)
     * @param data2 the second array (must not be null)
     * @return the result of the comparison (-1, 1 or 0)
     */
    public static int compareNotNull(long[] data1, long[] data2) {
        if (data1 == data2) {
            return 0;
        }
        int len = Math.min(data1.length, data2.length);
        for (int i = 0; i < len; i++) {
            long x = data1[i];
            long x2 = data2[i];
            if (x != x2) {
                return x > x2 ? 1 : -1;
            }
        }
        return Integer.signum(data1.length - data2.length);
    }

    /**
     * The base class for auto-detect data types.
     */
    abstract class AutoDetectDataType implements DataType {

        protected final ObjectType base;
        protected final int typeId;

        AutoDetectDataType(ObjectType base, int typeId) {
            this.base = base;
            this.typeId = typeId;
        }

        @Override
        public int getMemory(Object o) {
            return getType(o).getMemory(o);
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            AutoDetectDataType aType = getType(aObj);
            AutoDetectDataType bType = getType(bObj);
            if (aType == bType) {
                return aType.compare(aObj, bObj);
            }
            int typeDiff = aType.typeId - bType.typeId;
            return Integer.signum(typeDiff);
        }

        @Override
        public int getMaxLength(Object o) {
            return getType(o).getMaxLength(o);
        }

        @Override
        public void write(ByteBuffer buff, Object o) {
            getType(o).write(buff, o);
        }

        @Override
        public final Object read(ByteBuffer buff) {
            throw new RuntimeException();
        }

        /**
         * Get the type for the given object.
         *
         * @param o the object
         * @return the type
         */
        AutoDetectDataType getType(Object o) {
            return base.switchType(o);
        }

        /**
         * Read an object from the buffer.
         *
         * @param buff the buffer
         * @param tag the first byte of the object (usually the type)
         * @return the read object
         */
        abstract Object read(ByteBuffer buff, int tag);

        @Override
        public String asString() {
            return "o" + typeId;
        }

    }

    /**
     * The type for boolean true and false.
     */
    class BooleanType extends AutoDetectDataType {

        BooleanType(ObjectType base) {
            super(base, TYPE_BOOLEAN);
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            if (aObj instanceof Boolean && bObj instanceof Boolean) {
                Boolean a = (Boolean) aObj;
                Boolean b = (Boolean) bObj;
                return a.compareTo(b);
            }
            return super.compare(aObj, bObj);
        }

        @Override
        public int getMemory(Object obj) {
            return obj instanceof Boolean ? 0 : super.getMemory(obj);
        }

        @Override
        public int getMaxLength(Object obj) {
            return obj instanceof Boolean ? 1 : super.getMaxLength(obj);
        }

        @Override
        public void write(ByteBuffer buff, Object obj) {
            if (obj instanceof Boolean) {
                int tag = ((Boolean) obj) ? TAG_BOOLEAN_TRUE : TYPE_BOOLEAN;
                buff.put((byte) tag);
            } else {
                super.write(buff, obj);
            }
        }

        @Override
        public Object read(ByteBuffer buff, int tag) {
            return tag == TYPE_BOOLEAN ? Boolean.FALSE : Boolean.TRUE;
        }

    }

    /**
     * The type for byte objects.
     */
    class ByteType extends AutoDetectDataType {

        ByteType(ObjectType base) {
            super(base, TYPE_BYTE);
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            if (aObj instanceof Byte && bObj instanceof Byte) {
                Byte a = (Byte) aObj;
                Byte b = (Byte) bObj;
                return a.compareTo(b);
            }
            return super.compare(aObj, bObj);
        }

        @Override
        public int getMemory(Object obj) {
            return obj instanceof Byte ? 0 : super.getMemory(obj);
        }

        @Override
        public int getMaxLength(Object obj) {
            return obj instanceof Byte ? 2 : super.getMaxLength(obj);
        }

        @Override
        public void write(ByteBuffer buff, Object obj) {
            if (obj instanceof Byte) {
                buff.put((byte) TYPE_BYTE);
                buff.put(((Byte) obj).byteValue());
            } else {
                super.write(buff, obj);
            }
        }

        public Object read(ByteBuffer buff, int tag) {
            return Byte.valueOf(buff.get());
        }

    }

    /**
     * The type for character objects.
     */
    class CharacterType extends AutoDetectDataType {

        CharacterType(ObjectType base) {
            super(base, TYPE_CHARACTER);
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            if (aObj instanceof Character && bObj instanceof Character) {
                Character a = (Character) aObj;
                Character b = (Character) bObj;
                return a.compareTo(b);
            }
            return super.compare(aObj, bObj);
        }

        @Override
        public int getMemory(Object obj) {
            return obj instanceof Character ? 24 : super.getMemory(obj);
        }

        @Override
        public int getMaxLength(Object obj) {
            return obj instanceof Character ? 3 : super.getMaxLength(obj);
        }

        @Override
        public void write(ByteBuffer buff, Object obj) {
            if (obj instanceof Character) {
                buff.put((byte) TYPE_CHARACTER);
                buff.putChar(((Character) obj).charValue());
            } else {
                super.write(buff, obj);
            }
        }

        @Override
        public Object read(ByteBuffer buff, int tag) {
            return Character.valueOf(buff.getChar());
        }

    }

    /**
     * The type for short objects.
     */
    class ShortType extends AutoDetectDataType {

        ShortType(ObjectType base) {
            super(base, TYPE_SHORT);
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            if (aObj instanceof Short && bObj instanceof Short) {
                Short a = (Short) aObj;
                Short b = (Short) bObj;
                return a.compareTo(b);
            }
            return super.compare(aObj, bObj);
        }

        @Override
        public int getMemory(Object obj) {
            return obj instanceof Short ? 24 : super.getMemory(obj);
        }

        @Override
        public int getMaxLength(Object obj) {
            return obj instanceof Short ? 3 : super.getMaxLength(obj);
        }

        @Override
        public void write(ByteBuffer buff, Object obj) {
            if (obj instanceof Short) {
                buff.put((byte) TYPE_SHORT);
                buff.putShort(((Short) obj).shortValue());
            } else {
                super.write(buff, obj);
            }
        }

        @Override
        public Object read(ByteBuffer buff, int tag) {
            return Short.valueOf(buff.getShort());
        }

    }

    /**
     * The type for integer objects.
     */
    class IntegerType extends AutoDetectDataType {

        IntegerType(ObjectType base) {
            super(base, TYPE_INTEGER);
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            if (aObj instanceof Integer && bObj instanceof Integer) {
                Integer a = (Integer) aObj;
                Integer b = (Integer) bObj;
                return a.compareTo(b);
            }
            return super.compare(aObj, bObj);
        }

        @Override
        public int getMemory(Object obj) {
            return obj instanceof Integer ? 24 : super.getMemory(obj);
        }

        @Override
        public int getMaxLength(Object obj) {
            return obj instanceof Integer ?
                    1 + DataUtils.MAX_VAR_INT_LEN :
                    super.getMaxLength(obj);
        }

        @Override
        public void write(ByteBuffer buff, Object obj) {
            if (obj instanceof Integer) {
                int x = (Integer) obj;
                if (x < 0) {
                    // -Integer.MIN_VALUE is smaller than 0
                    if (-x < 0 || -x > DataUtils.COMPRESSED_VAR_INT_MAX) {
                        buff.put((byte) TAG_INTEGER_FIXED);
                        buff.putInt(x);
                    } else {
                        buff.put((byte) TAG_INTEGER_NEGATIVE);
                        DataUtils.writeVarInt(buff, -x);
                    }
                } else if (x <= 15) {
                    buff.put((byte) (TAG_INTEGER_0_15 + x));
                } else if (x <= DataUtils.COMPRESSED_VAR_INT_MAX) {
                    buff.put((byte) TYPE_INTEGER);
                    DataUtils.writeVarInt(buff, x);
                } else {
                    buff.put((byte) TAG_INTEGER_FIXED);
                    buff.putInt(x);
                }
            } else {
                super.write(buff, obj);
            }
        }

        @Override
        public Object read(ByteBuffer buff, int tag) {
            switch (tag) {
            case TYPE_INTEGER:
                return DataUtils.readVarInt(buff);
            case TAG_INTEGER_NEGATIVE:
                return -DataUtils.readVarInt(buff);
            case TAG_INTEGER_FIXED:
                return buff.getInt();
            }
            return tag - TAG_INTEGER_0_15;
        }

    }

    /**
     * The type for long objects.
     */
    public class LongType extends AutoDetectDataType {

        LongType(ObjectType base) {
            super(base, TYPE_LONG);
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            if (aObj instanceof Long && bObj instanceof Long) {
                Long a = (Long) aObj;
                Long b = (Long) bObj;
                return a.compareTo(b);
            }
            return super.compare(aObj, bObj);
        }

        @Override
        public int getMemory(Object obj) {
            return obj instanceof Long ? 30 : super.getMemory(obj);
        }

        @Override
        public int getMaxLength(Object obj) {
            return obj instanceof Long ?
                    1 + DataUtils.MAX_VAR_LONG_LEN :
                    super.getMaxLength(obj);
        }

        @Override
        public void write(ByteBuffer buff, Object obj) {
            if (obj instanceof Long) {
                long x = (Long) obj;
                if (x < 0) {
                    // -Long.MIN_VALUE is smaller than 0
                    if (-x < 0 || -x > DataUtils.COMPRESSED_VAR_LONG_MAX) {
                        buff.put((byte) TAG_LONG_FIXED);
                        buff.putLong(x);
                    } else {
                        buff.put((byte) TAG_LONG_NEGATIVE);
                        DataUtils.writeVarLong(buff, -x);
                    }
                } else if (x <= 7) {
                    buff.put((byte) (TAG_LONG_0_7 + x));
                } else if (x <= DataUtils.COMPRESSED_VAR_LONG_MAX) {
                    buff.put((byte) TYPE_LONG);
                    DataUtils.writeVarLong(buff, x);
                } else {
                    buff.put((byte) TAG_LONG_FIXED);
                    buff.putLong(x);
                }
            } else {
                super.write(buff, obj);
            }
        }

        @Override
        public Object read(ByteBuffer buff, int tag) {
            switch (tag) {
            case TYPE_LONG:
                return DataUtils.readVarLong(buff);
            case TAG_LONG_NEGATIVE:
                return -DataUtils.readVarLong(buff);
            case TAG_LONG_FIXED:
                return buff.getLong();
            }
            return Long.valueOf(tag - TAG_LONG_0_7);
        }

    }

    /**
     * The type for float objects.
     */
    class FloatType extends AutoDetectDataType {

        FloatType(ObjectType base) {
            super(base, TYPE_FLOAT);
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            if (aObj instanceof Float && bObj instanceof Float) {
                Float a = (Float) aObj;
                Float b = (Float) bObj;
                return a.compareTo(b);
            }
            return super.compare(aObj, bObj);
        }

        @Override
        public int getMemory(Object obj) {
            return obj instanceof Float ? 24 : super.getMemory(obj);
        }

        @Override
        public int getMaxLength(Object obj) {
            return obj instanceof Float ?
                1 + DataUtils.MAX_VAR_INT_LEN :
                super.getMaxLength(obj);
        }

        @Override
        public void write(ByteBuffer buff, Object obj) {
            if (obj instanceof Float) {
                float x = (Float) obj;
                int f = Float.floatToIntBits(x);
                if (f == ObjectType.FLOAT_ZERO_BITS) {
                    buff.put((byte) TAG_FLOAT_0);
                } else if (f == ObjectType.FLOAT_ONE_BITS) {
                        buff.put((byte) TAG_FLOAT_1);
                } else {
                    int value = Integer.reverse(f);
                    if (value >= 0 && value <= DataUtils.COMPRESSED_VAR_INT_MAX) {
                        buff.put((byte) TYPE_FLOAT);
                        DataUtils.writeVarInt(buff, value);
                    } else {
                        buff.put((byte) TAG_FLOAT_FIXED);
                        buff.putFloat(x);
                    }
                }
            } else {
                super.write(buff, obj);
            }
        }

        @Override
        public Object read(ByteBuffer buff, int tag) {
            switch (tag) {
            case TAG_FLOAT_0:
                return 0f;
            case TAG_FLOAT_1:
                return 1f;
            case TAG_FLOAT_FIXED:
                return buff.getFloat();
            }
            return Float.intBitsToFloat(Integer.reverse(DataUtils.readVarInt(buff)));
        }

    }

    /**
     * The type for double objects.
     */
    class DoubleType extends AutoDetectDataType {

        DoubleType(ObjectType base) {
            super(base, TYPE_DOUBLE);
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            if (aObj instanceof Double && bObj instanceof Double) {
                Double a = (Double) aObj;
                Double b = (Double) bObj;
                return a.compareTo(b);
            }
            return super.compare(aObj, bObj);
        }

        @Override
        public int getMemory(Object obj) {
            return obj instanceof Double ? 30 : super.getMemory(obj);
        }

        @Override
        public int getMaxLength(Object obj) {
            return obj instanceof Double ?
                1 + DataUtils.MAX_VAR_LONG_LEN :
                super.getMaxLength(obj);
        }

        @Override
        public void write(ByteBuffer buff, Object obj) {
            if (obj instanceof Double) {
                double x = (Double) obj;
                long d = Double.doubleToLongBits(x);
                if (d == ObjectType.DOUBLE_ZERO_BITS) {
                    buff.put((byte) TAG_DOUBLE_0);
                } else if (d == ObjectType.DOUBLE_ONE_BITS) {
                        buff.put((byte) TAG_DOUBLE_1);
                } else {
                    long value = Long.reverse(d);
                    if (value >= 0 && value <= DataUtils.COMPRESSED_VAR_LONG_MAX) {
                        buff.put((byte) TYPE_DOUBLE);
                        DataUtils.writeVarLong(buff, value);
                    } else {
                        buff.put((byte) TAG_DOUBLE_FIXED);
                        buff.putDouble(x);
                    }
                }
            } else {
                super.write(buff, obj);
            }
        }

        @Override
        public Object read(ByteBuffer buff, int tag) {
            switch (tag) {
            case TAG_DOUBLE_0:
                return 0d;
            case TAG_DOUBLE_1:
                return 1d;
            case TAG_DOUBLE_FIXED:
                return buff.getDouble();
            }
            return Double.longBitsToDouble(Long.reverse(DataUtils.readVarLong(buff)));
        }

    }

    /**
     * The type for BigInteger objects.
     */
    class BigIntegerType extends AutoDetectDataType {

        BigIntegerType(ObjectType base) {
            super(base, TYPE_BIG_INTEGER);
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            if (aObj instanceof BigInteger && bObj instanceof BigInteger) {
                BigInteger a = (BigInteger) aObj;
                BigInteger b = (BigInteger) bObj;
                return a.compareTo(b);
            }
            return super.compare(aObj, bObj);
        }

        @Override
        public int getMemory(Object obj) {
            return obj instanceof BigInteger ? 100 : super.getMemory(obj);
        }

        @Override
        public int getMaxLength(Object obj) {
            if (!(obj instanceof BigInteger)) {
                return super.getMaxLength(obj);
            }
            BigInteger x = (BigInteger) obj;
            if (BigInteger.ZERO.equals(x) || BigInteger.ONE.equals(x)) {
                return 1;
            }
            int bits = x.bitLength();
            if (bits <= 63) {
                return 1 + DataUtils.MAX_VAR_LONG_LEN;
            }
            byte[] bytes = x.toByteArray();
            return 1 + DataUtils.MAX_VAR_INT_LEN + bytes.length;
        }

        @Override
        public void write(ByteBuffer buff, Object obj) {
            if (obj instanceof BigInteger) {
                BigInteger x = (BigInteger) obj;
                if (BigInteger.ZERO.equals(x)) {
                    buff.put((byte) TAG_BIG_INTEGER_0);
                } else if (BigInteger.ONE.equals(x)) {
                    buff.put((byte) TAG_BIG_INTEGER_1);
                } else {
                    int bits = x.bitLength();
                    if (bits <= 63) {
                        buff.put((byte) TAG_BIG_INTEGER_SMALL);
                        DataUtils.writeVarLong(buff, x.longValue());
                    } else {
                        buff.put((byte) TYPE_BIG_INTEGER);
                        byte[] bytes = x.toByteArray();
                        DataUtils.writeVarInt(buff, bytes.length);
                        buff.put(bytes);
                    }
                }
            } else {
                super.write(buff, obj);
            }
        }

        @Override
        public Object read(ByteBuffer buff, int tag) {
            switch (tag) {
            case TAG_BIG_INTEGER_0:
                return BigInteger.ZERO;
            case TAG_BIG_INTEGER_1:
                return BigInteger.ONE;
            case TAG_BIG_INTEGER_SMALL:
                return BigInteger.valueOf(DataUtils.readVarLong(buff));
            }
            int len = DataUtils.readVarInt(buff);
            byte[] bytes = Utils.newBytes(len);
            buff.get(bytes);
            return new BigInteger(bytes);
        }

    }

    /**
     * The type for BigDecimal objects.
     */
    class BigDecimalType extends AutoDetectDataType {

        BigDecimalType(ObjectType base) {
            super(base, TYPE_BIG_DECIMAL);
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            if (aObj instanceof BigDecimal && bObj instanceof BigDecimal) {
                BigDecimal a = (BigDecimal) aObj;
                BigDecimal b = (BigDecimal) bObj;
                return a.compareTo(b);
            }
            return super.compare(aObj, bObj);
        }

        @Override
        public int getMemory(Object obj) {
            return obj instanceof BigDecimal ? 150 : super.getMemory(obj);
        }

        @Override
        public int getMaxLength(Object obj) {
            if (!(obj instanceof BigDecimal)) {
                return super.getMaxLength(obj);
            }
            BigDecimal x = (BigDecimal) obj;
            if (BigDecimal.ZERO.equals(x) || BigDecimal.ONE.equals(x)) {
                return 1;
            }
            int scale = x.scale();
            BigInteger b = x.unscaledValue();
            int bits = b.bitLength();
            if (bits <= 63) {
                if (scale == 0) {
                    return 1 + DataUtils.MAX_VAR_LONG_LEN;
                }
                return 1 + DataUtils.MAX_VAR_INT_LEN +
                        DataUtils.MAX_VAR_LONG_LEN;
            }
            byte[] bytes = b.toByteArray();
            return 1 + DataUtils.MAX_VAR_INT_LEN +
                    DataUtils.MAX_VAR_INT_LEN + bytes.length;
        }

        @Override
        public void write(ByteBuffer buff, Object obj) {
            if (obj instanceof BigDecimal) {
                BigDecimal x = (BigDecimal) obj;
                if (BigDecimal.ZERO.equals(x)) {
                    buff.put((byte) TAG_BIG_DECIMAL_0);
                } else if (BigDecimal.ONE.equals(x)) {
                    buff.put((byte) TAG_BIG_DECIMAL_1);
                } else {
                    int scale = x.scale();
                    BigInteger b = x.unscaledValue();
                    int bits = b.bitLength();
                    if (bits < 64) {
                        if (scale == 0) {
                            buff.put((byte) TAG_BIG_DECIMAL_SMALL);
                        } else {
                            buff.put((byte) TAG_BIG_DECIMAL_SMALL_SCALED);
                            DataUtils.writeVarInt(buff, scale);
                        }
                        DataUtils.writeVarLong(buff, b.longValue());
                    } else {
                        buff.put((byte) TYPE_BIG_DECIMAL);
                        DataUtils.writeVarInt(buff, scale);
                        byte[] bytes = b.toByteArray();
                        DataUtils.writeVarInt(buff, bytes.length);
                        buff.put(bytes);
                    }
                }
            } else {
                super.write(buff, obj);
            }
        }

        @Override
        public Object read(ByteBuffer buff, int tag) {
            switch (tag) {
            case TAG_BIG_DECIMAL_0:
                return BigDecimal.ZERO;
            case TAG_BIG_DECIMAL_1:
                return BigDecimal.ONE;
            case TAG_BIG_DECIMAL_SMALL:
                return BigDecimal.valueOf(DataUtils.readVarLong(buff));
            case TAG_BIG_DECIMAL_SMALL_SCALED:
                int scale = DataUtils.readVarInt(buff);
                return BigDecimal.valueOf(DataUtils.readVarLong(buff), scale);
            }
            int scale = DataUtils.readVarInt(buff);
            int len = DataUtils.readVarInt(buff);
            byte[] bytes = Utils.newBytes(len);
            buff.get(bytes);
            BigInteger b = new BigInteger(bytes);
            return new BigDecimal(b, scale);
        }

    }

    /**
     * The type for string objects.
     */
    class StringType extends AutoDetectDataType {

        StringType(ObjectType base) {
            super(base, TYPE_STRING);
        }

        @Override
        public int getMemory(Object obj) {
            if (!(obj instanceof String)) {
                return super.getMemory(obj);
            }
            return 24 + 2 * obj.toString().length();
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            if (aObj instanceof String && bObj instanceof String) {
                return aObj.toString().compareTo(bObj.toString());
            }
            return super.compare(aObj, bObj);
        }

        @Override
        public int getMaxLength(Object obj) {
            if (!(obj instanceof String)) {
                return super.getMaxLength(obj);
            }
            return 1 + DataUtils.MAX_VAR_INT_LEN + 3 * obj.toString().length();
        }

        @Override
        public void write(ByteBuffer buff, Object obj) {
            if (!(obj instanceof String)) {
                super.write(buff, obj);
                return;
            }
            String s = (String) obj;
            int len = s.length();
            if (len <= 15) {
                buff.put((byte) (TAG_STRING_0_15 + len));
            } else {
                buff.put((byte) TYPE_STRING);
                DataUtils.writeVarInt(buff, len);
            }
            DataUtils.writeStringData(buff, s, len);
        }

        @Override
        public Object read(ByteBuffer buff, int tag) {
            int len;
            if (tag == TYPE_STRING) {
                len = DataUtils.readVarInt(buff);
            } else {
                len = tag - TAG_STRING_0_15;
            }
            return DataUtils.readString(buff, len);
        }

    }

    /**
     * The type for UUID objects.
     */
    class UUIDType extends AutoDetectDataType {

        UUIDType(ObjectType base) {
            super(base, TYPE_UUID);
        }

        @Override
        public int getMemory(Object obj) {
            return obj instanceof UUID ? 40 : super.getMemory(obj);
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            if (aObj instanceof UUID && bObj instanceof UUID) {
                UUID a = (UUID) aObj;
                UUID b = (UUID) bObj;
                return a.compareTo(b);
            }
            return super.compare(aObj, bObj);
        }

        @Override
        public int getMaxLength(Object obj) {
            if (!(obj instanceof UUID)) {
                return super.getMaxLength(obj);
            }
            return 17;
        }

        @Override
        public void write(ByteBuffer buff, Object obj) {
            if (!(obj instanceof UUID)) {
                super.write(buff, obj);
                return;
            }
            buff.put((byte) TYPE_UUID);
            UUID a = (UUID) obj;
            buff.putLong(a.getMostSignificantBits());
            buff.putLong(a.getLeastSignificantBits());
        }

        @Override
        public Object read(ByteBuffer buff, int tag) {
            long a = buff.getLong(), b = buff.getLong();
            return new UUID(a, b);
        }

    }

    /**
     * The type for byte arrays.
     */
    class ByteArrayType extends AutoDetectDataType {

        ByteArrayType(ObjectType base) {
            super(base, TYPE_BYTE_ARRAY);
        }

        @Override
        public int getMemory(Object obj) {
            if (!(obj instanceof byte[])) {
                return super.getMemory(obj);
            }
            return 24 + ((byte[]) obj).length;
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            if (aObj instanceof byte[] && bObj instanceof byte[]) {
                byte[] a = (byte[]) aObj;
                byte[] b = (byte[]) bObj;
                return Utils.compareNotNull(a, b);
            }
            return super.compare(aObj, bObj);
        }

        @Override
        public int getMaxLength(Object obj) {
            if (!(obj instanceof byte[])) {
                return super.getMaxLength(obj);
            }
            return 1 + DataUtils.MAX_VAR_INT_LEN + ((byte[]) obj).length;
        }

        @Override
        public void write(ByteBuffer buff, Object obj) {
            if (obj instanceof byte[]) {
                byte[] data = (byte[]) obj;
                int len = data.length;
                if (len <= 15) {
                    buff.put((byte) (TAG_BYTE_ARRAY_0_15 + len));
                } else {
                    buff.put((byte) TYPE_BYTE_ARRAY);
                    DataUtils.writeVarInt(buff,  data.length);
                }
                buff.put(data);
            } else {
                super.write(buff, obj);
            }
        }

        @Override
        public Object read(ByteBuffer buff, int tag) {
            int len;
            if (tag == TYPE_BYTE_ARRAY) {
                len = DataUtils.readVarInt(buff);
            } else {
                len = tag - TAG_BYTE_ARRAY_0_15;
            }
            byte[] data = new byte[len];
            buff.get(data);
            return data;
        }

    }

    /**
     * The type for char arrays.
     */
    class CharArrayType extends AutoDetectDataType {

        CharArrayType(ObjectType base) {
            super(base, TYPE_CHAR_ARRAY);
        }

        @Override
        public int getMemory(Object obj) {
            if (!(obj instanceof char[])) {
                return super.getMemory(obj);
            }
            return 24 + 2 * ((char[]) obj).length;
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            if (aObj instanceof char[] && bObj instanceof char[]) {
                char[] a = (char[]) aObj;
                char[] b = (char[]) bObj;
                return compareNotNull(a, b);
            }
            return super.compare(aObj, bObj);
        }

        @Override
        public int getMaxLength(Object obj) {
            if (!(obj instanceof char[])) {
                return super.getMaxLength(obj);
            }
            return 1 + DataUtils.MAX_VAR_INT_LEN + 2 * ((char[]) obj).length;
        }

        @Override
        public void write(ByteBuffer buff, Object obj) {
            if (obj instanceof char[]) {
                buff.put((byte) TYPE_CHAR_ARRAY);
                char[] data = (char[]) obj;
                int len = data.length;
                DataUtils.writeVarInt(buff,  len);
                buff.asCharBuffer().put(data);
                buff.position(buff.position() + len + len);
            } else {
                super.write(buff, obj);
            }
        }

        @Override
        public Object read(ByteBuffer buff, int tag) {
            int len = DataUtils.readVarInt(buff);
            char[] data = new char[len];
            buff.asCharBuffer().get(data);
            buff.position(buff.position() + len + len);
            return data;
        }

    }

    /**
     * The type for char arrays.
     */
    class IntArrayType extends AutoDetectDataType {

        IntArrayType(ObjectType base) {
            super(base, TYPE_INT_ARRAY);
        }

        @Override
        public int getMemory(Object obj) {
            if (!(obj instanceof int[])) {
                return super.getMemory(obj);
            }
            return 24 + 4 * ((int[]) obj).length;
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            if (aObj instanceof int[] && bObj instanceof int[]) {
                int[] a = (int[]) aObj;
                int[] b = (int[]) bObj;
                return compareNotNull(a, b);
            }
            return super.compare(aObj, bObj);
        }

        @Override
        public int getMaxLength(Object obj) {
            if (!(obj instanceof int[])) {
                return super.getMaxLength(obj);
            }
            return 1 + DataUtils.MAX_VAR_INT_LEN + 4 * ((int[]) obj).length;
        }

        @Override
        public void write(ByteBuffer buff, Object obj) {
            if (obj instanceof int[]) {
                buff.put((byte) TYPE_INT_ARRAY);
                int[] data = (int[]) obj;
                int len = data.length;
                DataUtils.writeVarInt(buff,  len);
                buff.asIntBuffer().put(data);
                buff.position(buff.position() + 4 * len);
            } else {
                super.write(buff, obj);
            }
        }

        @Override
        public Object read(ByteBuffer buff, int tag) {
            int len = DataUtils.readVarInt(buff);
            int[] data = new int[len];
            buff.asIntBuffer().get(data);
            buff.position(buff.position() + 4 * len);
            return data;
        }

    }

    /**
     * The type for char arrays.
     */
    class LongArrayType extends AutoDetectDataType {

        LongArrayType(ObjectType base) {
            super(base, TYPE_LONG_ARRAY);
        }

        @Override
        public int getMemory(Object obj) {
            if (!(obj instanceof long[])) {
                return super.getMemory(obj);
            }
            return 24 + 8 * ((long[]) obj).length;
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            if (aObj instanceof long[] && bObj instanceof long[]) {
                long[] a = (long[]) aObj;
                long[] b = (long[]) bObj;
                return compareNotNull(a, b);
            }
            return super.compare(aObj, bObj);
        }

        @Override
        public int getMaxLength(Object obj) {
            if (!(obj instanceof long[])) {
                return super.getMaxLength(obj);
            }
            return 1 + DataUtils.MAX_VAR_INT_LEN + 8 * ((long[]) obj).length;
        }

        @Override
        public void write(ByteBuffer buff, Object obj) {
            if (obj instanceof long[]) {
                buff.put((byte) TYPE_LONG_ARRAY);
                long[] data = (long[]) obj;
                int len = data.length;
                DataUtils.writeVarInt(buff,  len);
                buff.asLongBuffer().put(data);
                buff.position(buff.position() + 8 * len);
            } else {
                super.write(buff, obj);
            }
        }

        @Override
        public Object read(ByteBuffer buff, int tag) {
            int len = DataUtils.readVarInt(buff);
            long[] data = new long[len];
            buff.asLongBuffer().get(data);
            buff.position(buff.position() + 8 * len);
            return data;
        }

    }

    /**
     * The type for serialized objects.
     */
    class SerializedObjectType extends AutoDetectDataType {

        SerializedObjectType(ObjectType base) {
            super(base, TYPE_SERIALIZED_OBJECT);
        }

        @SuppressWarnings("unchecked")
        @Override
        public int compare(Object aObj, Object bObj) {
            if (aObj == bObj) {
                return 0;
            }
            DataType ta = getType(aObj);
            DataType tb = getType(bObj);
            if (ta != this && ta == tb) {
                return ta.compare(aObj, bObj);
            }
            // TODO ensure comparable type (both may be comparable but not
            // with each other)
            if (aObj instanceof Comparable) {
                if (aObj.getClass().isAssignableFrom(bObj.getClass())) {
                    return ((Comparable<Object>) aObj).compareTo(bObj);
                }
            }
            if (bObj instanceof Comparable) {
                if (bObj.getClass().isAssignableFrom(aObj.getClass())) {
                    return -((Comparable<Object>) bObj).compareTo(aObj);
                }
            }
            byte[] a = Utils.serialize(aObj);
            byte[] b = Utils.serialize(bObj);
            return Utils.compareNotNull(a, b);
        }

        @Override
        public int getMemory(Object obj) {
            DataType t = getType(obj);
            if (t == this) {
                return 1000;
            }
            return t.getMemory(obj);
        }

        @Override
        public int getMaxLength(Object obj) {
            DataType t = getType(obj);
            if (t == this) {
                byte[] data = Utils.serialize(obj);
                return 1 + DataUtils.MAX_VAR_INT_LEN + data.length;
            }
            return t.getMaxLength(obj);
        }

        @Override
        public void write(ByteBuffer buff, Object obj) {
            DataType t = getType(obj);
            if (t == this) {
                buff.put((byte) TYPE_SERIALIZED_OBJECT);
                byte[] data = Utils.serialize(obj);
                DataUtils.writeVarInt(buff, data.length);
                buff.put(data);
            } else {
                t.write(buff, obj);
            }
        }

        @Override
        public Object read(ByteBuffer buff, int tag) {
            int len = DataUtils.readVarInt(buff);
            byte[] data = new byte[len];
            buff.get(data);
            return Utils.deserialize(data);
        }

    }

}
