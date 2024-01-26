/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import org.h2.api.ErrorCode;
import org.h2.engine.CastDataProvider;
import org.h2.engine.Constants;
import org.h2.message.DbException;
import org.h2.result.SimpleResult;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Row value.
 */
public final class ValueRow extends ValueCollectionBase {

    /**
     * Empty row.
     */
    public static final ValueRow EMPTY = get(Value.EMPTY_VALUES);

    private TypeInfo type;

    private ValueRow(TypeInfo type, Value[] list) {
        super(list);
        int degree = list.length;
        if (degree > Constants.MAX_COLUMNS) {
            throw DbException.get(ErrorCode.TOO_MANY_COLUMNS_1, "" + Constants.MAX_COLUMNS);
        }
        if (type != null) {
            if (type.getValueType() != ROW || ((ExtTypeInfoRow) type.getExtTypeInfo()).getFields().size() != degree) {
                throw DbException.getInternalError();
            }
            this.type = type;
        }
    }

    /**
     * Get or create a row value for the given value array.
     * Do not clone the data.
     *
     * @param list the value array
     * @return the value
     */
    public static ValueRow get(Value[] list) {
        return new ValueRow(null, list);
    }

    /**
     * Get or create a typed row value for the given value array.
     * Do not clone the data.
     *
     * @param extTypeInfo the extended data type information
     * @param list the value array
     * @return the value
     */
    public static ValueRow get(ExtTypeInfoRow extTypeInfo, Value[] list) {
        return new ValueRow(new TypeInfo(ROW, -1, -1, extTypeInfo), list);
    }

    /**
     * Get or create a typed row value for the given value array.
     * Do not clone the data.
     *
     * @param typeInfo the data type information
     * @param list the value array
     * @return the value
     */
    public static ValueRow get(TypeInfo typeInfo, Value[] list) {
        return new ValueRow(typeInfo, list);
    }

    @Override
    public TypeInfo getType() {
        TypeInfo type = this.type;
        if (type == null) {
            this.type = type = TypeInfo.getTypeInfo(Value.ROW, 0, 0, new ExtTypeInfoRow(values));
        }
        return type;
    }

    @Override
    public int getValueType() {
        return ROW;
    }

    @Override
    public String getString() {
        StringBuilder builder = new StringBuilder("ROW (");
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                builder.append(", ");
            }
            builder.append(values[i].getString());
        }
        return builder.append(')').toString();
    }

    public SimpleResult getResult() {
        SimpleResult result = new SimpleResult();
        for (int i = 0, l = values.length; i < l;) {
            Value v = values[i++];
            result.addColumn("C" + i, v.getType());
        }
        result.addRow(values);
        return result;
    }

    @Override
    public int compareTypeSafe(Value o, CompareMode mode, CastDataProvider provider) {
        ValueRow v = (ValueRow) o;
        if (values == v.values) {
            return 0;
        }
        int len = values.length;
        if (len != v.values.length) {
            throw DbException.get(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
        }
        for (int i = 0; i < len; i++) {
            Value v1 = values[i];
            Value v2 = v.values[i];
            int comp = v1.compareTo(v2, provider, mode);
            if (comp != 0) {
                return comp;
            }
        }
        return 0;
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        builder.append("ROW (");
        int length = values.length;
        for (int i = 0; i < length; i++) {
            if (i > 0) {
                builder.append(", ");
            }
            values[i].getSQL(builder, sqlFlags);
        }
        return builder.append(')');
    }

    /**
     * Creates a copy of this row but the new instance will contain the {@link #values} according to
     * {@code newOrder}.<br />
     * E.g.: ROW('a', 'b').cloneWithOrder([1, 0]) returns ROW('b', 'a')
     * @param newOrder array of indexes to create the new values array
     */
    public ValueRow cloneWithOrder(int[] newOrder) {
        int length = values.length;
        if (newOrder.length != values.length) {
            throw DbException.getInternalError("Length of the new orders is different than values count.");
        }

        Value[] newValues = new Value[length];
        for (int i = 0; i < length; i++) {
            newValues[i] = values[newOrder[i]];
        }

        ExtTypeInfoRow typeInfoRow = (ExtTypeInfoRow) type.getExtTypeInfo();
        Map.Entry<String, TypeInfo>[] fields = typeInfoRow.getFields().toArray(createEntriesArray(length));
        LinkedHashMap<String, TypeInfo> newFields = new LinkedHashMap<>(length);
        for (int i = 0; i < length; i++) {
            Map.Entry<String, TypeInfo> field = fields[newOrder[i]];
            newFields.put(field.getKey(), field.getValue());
        }
        ExtTypeInfoRow newTypeInfoRow = new ExtTypeInfoRow(newFields);
        TypeInfo newType = new TypeInfo(type.getValueType(), type.getDeclaredPrecision(),
                type.getDeclaredScale(), newTypeInfoRow);

        return new ValueRow(newType, newValues);
    }

    @SuppressWarnings("unchecked")
    private static <K,V> Map.Entry<K,V>[] createEntriesArray(int length) {
        return (Map.Entry<K,V>[])new Map.Entry[length];
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ValueRow)) {
            return false;
        }
        ValueRow v = (ValueRow) other;
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
