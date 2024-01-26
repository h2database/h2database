/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import org.h2.engine.CastDataProvider;
import org.h2.engine.Constants;
import org.h2.message.DbException;

/**
 * Implementation of the ARRAY data type.
 */
public final class ValueArray extends ValueCollectionBase {

    /**
     * Empty array.
     */
    public static final ValueArray EMPTY = get(TypeInfo.TYPE_NULL, Value.EMPTY_VALUES, null);

    private TypeInfo type;

    private final TypeInfo componentType;

    private ValueArray(TypeInfo componentType, Value[] list, CastDataProvider provider) {
        super(list);
        int length = list.length;
        if (length > Constants.MAX_ARRAY_CARDINALITY) {
            String typeName = getTypeName(getValueType());
            throw DbException.getValueTooLongException(typeName, typeName, length);
        }
        for (int i = 0; i < length; i++) {
            list[i] = list[i].castTo(componentType, provider);
        }
        this.componentType = componentType;
    }

    /**
     * Get or create a array value for the given value array.
     * Do not clone the data.
     *
     * @param list the value array
     * @param provider the cast information provider
     * @return the value
     */
    public static ValueArray get(Value[] list, CastDataProvider provider) {
        return new ValueArray(TypeInfo.getHigherType(list), list, provider);
    }

    /**
     * Get or create a array value for the given value array.
     * Do not clone the data.
     *
     * @param componentType the type of elements, or {@code null}
     * @param list the value array
     * @param provider the cast information provider
     * @return the value
     */
    public static ValueArray get(TypeInfo componentType, Value[] list, CastDataProvider provider) {
        return new ValueArray(componentType, list, provider);
    }

    @Override
    public TypeInfo getType() {
        TypeInfo type = this.type;
        if (type == null) {
            TypeInfo componentType = getComponentType();
            this.type = type = TypeInfo.getTypeInfo(getValueType(), values.length, 0, componentType);
        }
        return type;
    }

    @Override
    public int getValueType() {
        return ARRAY;
    }

    public TypeInfo getComponentType() {
        return componentType;
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
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        builder.append("ARRAY [");
        int length = values.length;
        for (int i = 0; i < length; i++) {
            if (i > 0) {
                builder.append(", ");
            }
            values[i].getSQL(builder, sqlFlags);
        }
        return builder.append(']');
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

}
