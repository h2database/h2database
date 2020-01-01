/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.util.Objects;

import org.h2.engine.CastDataProvider;

/**
 * Extended parameters of the ARRAY data type.
 */
public final class ExtTypeInfoArray extends ExtTypeInfo {

    private final TypeInfo componentType;

    /**
     * Creates new instance of extended parameters of the ARRAY data type.
     *
     * @param componentType
     *            the type of elements
     */
    public ExtTypeInfoArray(TypeInfo componentType) {
        this.componentType = componentType;
    }

    @Override
    public Value cast(Value value, CastDataProvider provider) {
        if (value.getValueType() != Value.ARRAY) {
            value = value.convertTo(Value.ARRAY);
        }
        ValueArray a = (ValueArray) value;
        Value[] values = a.getList();
        int length = values.length;
        for (int i = 0; i < length; i++) {
            Value v = values[i];
            Value v2 = v.convertTo(componentType, provider, null);
            if (v != v2) {
                Value[] newValues = new Value[length];
                System.arraycopy(values, 0, newValues, 0, i);
                newValues[i] = v2;
                while (++i < length) {
                    newValues[i] = values[i].convertTo(componentType, provider, null);
                }
                return ValueArray.get(newValues);
            }
        }
        return a;
    }

    @Override
    public int hashCode() {
        return (componentType == null) ? 0 : componentType.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || obj.getClass() != ExtTypeInfoArray.class) {
            return false;
        }
        return Objects.equals(componentType, ((ExtTypeInfoArray) obj).componentType);
    }

    @Override
    public String getCreateSQL() {
        return componentType.toString();
    }

    /**
     * Returns the type of elements.
     *
     * @return the type of elements
     */
    public TypeInfo getComponentType() {
        return componentType;
    }

}
