/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;

import org.h2.engine.CastDataProvider;
import org.h2.util.MathUtils;

/**
 * Implementation of the ARRAY data type.
 */
public class ValueArray extends ValueCollectionBase {

    /**
     * Empty array.
     */
    public static final ValueArray EMPTY = get(Value.EMPTY_VALUES);

    private TypeInfo type;

    private TypeInfo componentType;

    private ValueArray(TypeInfo componentType, Value[] list) {
        super(list);
        this.componentType = componentType;
    }

    /**
     * Get or create a array value for the given value array.
     * Do not clone the data.
     *
     * @param list the value array
     * @return the value
     */
    public static ValueArray get(Value[] list) {
        return new ValueArray(null, list);
    }

    /**
     * Get or create a array value for the given value array.
     * Do not clone the data.
     *
     * @param componentType the type of elements, or {@code null}
     * @param list the value array
     * @return the value
     */
    public static ValueArray get(TypeInfo componentType, Value[] list) {
        return new ValueArray(componentType, list);
    }

    @Override
    public TypeInfo getType() {
        TypeInfo type = this.type;
        if (type == null) {
            TypeInfo componentType = getComponentType();
            this.type = type = TypeInfo.getTypeInfo(getValueType(), values.length, 0,
                    componentType.getValueType() != NULL ? new ExtTypeInfoArray(componentType) : null);
        }
        return type;
    }

    @Override
    public int getValueType() {
        return ARRAY;
    }

    public TypeInfo getComponentType() {
        TypeInfo type = componentType;
        if (type == null) {
            int length = values.length;
            if (length == 0) {
                type = TypeInfo.TYPE_NULL;
            } else {
                int t = values[0].getValueType();
                if (length > 1) {
                    for (int i = 1; i < length; i++) {
                        int t2 = values[i].getValueType();
                        if (t2 != Value.NULL) {
                            if (t == Value.NULL) {
                                t = t2;
                            } else if (t != t2) {
                                t = Value.NULL;
                                break;
                            }
                        }
                    }
                }
                type = TypeInfo.getTypeInfo(t);
            }
            componentType = type;
        }
        return type;
    }

    @Override
    public String getString() {
        StringBuilder builder = new StringBuilder().append('[');
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                builder.append(", ");
            }
            builder.append(values[i].getString());
        }
        return builder.append(']').toString();
    }

    @Override
    public int compareTypeSafe(Value o, CompareMode mode, CastDataProvider provider) {
        ValueArray v = (ValueArray) o;
        if (values == v.values) {
            return 0;
        }
        int l = values.length;
        int ol = v.values.length;
        int len = Math.min(l, ol);
        for (int i = 0; i < len; i++) {
            Value v1 = values[i];
            Value v2 = v.values[i];
            int comp = v1.compareTo(v2, provider, mode);
            if (comp != 0) {
                return comp;
            }
        }
        return Integer.compare(l, ol);
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        prep.setArray(parameterIndex, prep.getConnection().createArrayOf("NULL", (Object[]) getObject()));
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder) {
        builder.append("ARRAY [");
        int length = values.length;
        for (int i = 0; i < length; i++) {
            if (i > 0) {
                builder.append(", ");
            }
            values[i].getSQL(builder);
        }
        return builder.append(']');
    }

    @Override
    public String getTraceSQL() {
        StringBuilder builder = new StringBuilder("[");
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                builder.append(", ");
            }
            Value v = values[i];
            builder.append(v == null ? "null" : v.getTraceSQL());
        }
        return builder.append(']').toString();
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ValueArray)) {
            return false;
        }
        ValueArray v = (ValueArray) other;
        if (values == v.values) {
            return true;
        }
        int len = values.length;
        if (len != v.values.length) {
            return false;
        }
        for (int i = 0; i < len; i++) {
            if (!values[i].equals(v.values[i])) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Value convertPrecision(long precision) {
        int p = MathUtils.convertLongToInt(precision);
        if (values.length <= p) {
            return this;
        }
        return get(getComponentType(), Arrays.copyOf(values, p));
    }

}
