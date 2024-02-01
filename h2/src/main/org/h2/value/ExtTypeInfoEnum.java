/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.util.Arrays;
import java.util.Locale;

import org.h2.api.ErrorCode;
import org.h2.engine.CastDataProvider;
import org.h2.engine.Constants;
import org.h2.message.DbException;

/**
 * Extended parameters of the ENUM data type.
 */
public final class ExtTypeInfoEnum extends ExtTypeInfo {

    private final String[] enumerators, cleaned;

    private TypeInfo type;

    /**
     * Returns enumerators for the two specified values for a binary operation.
     *
     * @param left
     *            left (first) operand
     * @param right
     *            right (second) operand
     * @return enumerators from the left or the right value, or an empty array
     *         if both values do not have enumerators
     */
    public static ExtTypeInfoEnum getEnumeratorsForBinaryOperation(Value left, Value right) {
        if (left.getValueType() == Value.ENUM) {
            return ((ValueEnum) left).getEnumerators();
        } else if (right.getValueType() == Value.ENUM) {
            return ((ValueEnum) right).getEnumerators();
        } else {
            throw DbException.get(ErrorCode.UNKNOWN_DATA_TYPE_1,
                    "type1=" + left.getValueType() + ", type2=" + right.getValueType());
        }
    }

    private static String sanitize(String label) {
        if (label == null) {
            return null;
        }
        int length = label.length();
        if (length > Constants.MAX_STRING_LENGTH) {
            throw DbException.getValueTooLongException("ENUM", label, length);
        }
        return label.trim().toUpperCase(Locale.ENGLISH);
    }

    private static StringBuilder toSQL(StringBuilder builder, String[] enumerators) {
        builder.append('(');
        for (int i = 0; i < enumerators.length; i++) {
            if (i != 0) {
                builder.append(", ");
            }
            builder.append('\'');
            String s = enumerators[i];
            for (int j = 0, length = s.length(); j < length; j++) {
                char c = s.charAt(j);
                if (c == '\'') {
                    builder.append('\'');
                }
                builder.append(c);
            }
            builder.append('\'');
        }
        return builder.append(')');
    }

    /**
     * Creates new instance of extended parameters of the ENUM data type.
     *
     * @param enumerators
     *            the enumerators. May not be modified by caller or this class.
     */
    public ExtTypeInfoEnum(String[] enumerators) {
        int length;
        if (enumerators == null || (length = enumerators.length) == 0) {
            throw DbException.get(ErrorCode.ENUM_EMPTY);
        }
        if (length > Constants.MAX_ARRAY_CARDINALITY) {
            throw DbException.getValueTooLongException("ENUM", "(" + length + " elements)", length);
        }
        final String[] cleaned = new String[length];
        for (int i = 0; i < length; i++) {
            String l = sanitize(enumerators[i]);
            if (l == null || l.isEmpty()) {
                throw DbException.get(ErrorCode.ENUM_EMPTY);
            }
            for (int j = 0; j < i; j++) {
                if (l.equals(cleaned[j])) {
                    throw DbException.get(ErrorCode.ENUM_DUPLICATE, //
                            toSQL(new StringBuilder(), enumerators).toString());
                }
            }
            cleaned[i] = l;
        }
        this.enumerators = enumerators;
        this.cleaned = Arrays.equals(cleaned, enumerators) ? enumerators : cleaned;
    }

    TypeInfo getType() {
        TypeInfo type = this.type;
        if (type == null) {
            int p = 0;
            for (String s : enumerators) {
                int l = s.length();
                if (l > p) {
                    p = l;
                }
            }
            this.type = type = new TypeInfo(Value.ENUM, p, 0, this);
        }
        return type;
    }

    /**
     * Get count of elements in enumeration.
     *
     * @return count of elements in enumeration
     */
    public int getCount() {
        return enumerators.length;
    }

    /**
     * Returns an enumerator with specified 0-based ordinal value.
     *
     * @param ordinal
     *            ordinal value of an enumerator
     * @return the enumerator with specified ordinal value
     */
    public String getEnumerator(int ordinal) {
        return enumerators[ordinal];
    }

    /**
     * Get ValueEnum instance for an ordinal.
     * @param ordinal ordinal value of an enum
     * @param provider the cast information provider
     * @return ValueEnum instance
     */
    public ValueEnum getValue(int ordinal, CastDataProvider provider) {
        String label;
        if (provider == null || !provider.zeroBasedEnums()) {
            if (ordinal < 1 || ordinal > enumerators.length) {
                throw DbException.get(ErrorCode.ENUM_VALUE_NOT_PERMITTED, getTraceSQL(), Integer.toString(ordinal));
            }
            label = enumerators[ordinal - 1];
        } else {
            if (ordinal < 0 || ordinal >= enumerators.length) {
                throw DbException.get(ErrorCode.ENUM_VALUE_NOT_PERMITTED, getTraceSQL(), Integer.toString(ordinal));
            }
            label = enumerators[ordinal];
        }
        return new ValueEnum(this, label, ordinal);
    }

    /**
     * Get ValueEnum instance for a label string.
     * @param label label string
     * @param provider the cast information provider
     * @return ValueEnum instance
     */
    public ValueEnum getValue(String label, CastDataProvider provider) {
        ValueEnum value = getValueOrNull(label, provider);
        if (value == null) {
            throw DbException.get(ErrorCode.ENUM_VALUE_NOT_PERMITTED, toString(), label);
        }
        return value;
    }

    private ValueEnum getValueOrNull(String label, CastDataProvider provider) {
        String l = sanitize(label);
        if (l != null) {
            for (int i = 0, ordinal = provider == null || !provider.zeroBasedEnums() ? 1
                    : 0; i < cleaned.length; i++, ordinal++) {
                if (l.equals(cleaned[i])) {
                    return new ValueEnum(this, enumerators[i], ordinal);
                }
            }
        }
        return null;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(enumerators) + 203_117;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || obj.getClass() != ExtTypeInfoEnum.class) {
            return false;
        }
        return Arrays.equals(enumerators, ((ExtTypeInfoEnum) obj).enumerators);
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        return toSQL(builder, enumerators);
    }

}
