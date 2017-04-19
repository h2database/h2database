/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.util.Locale;
import org.h2.api.ErrorCode;
import org.h2.message.DbException;
import org.h2.util.MathUtils;

public class ValueEnum extends ValueEnumBase {
    private static enum Validation {
        DUPLICATE,
        EMPTY,
        INVALID,
        VALID
    }

    private final String[] enumerators;

    private ValueEnum(final String[] enumerators, final int ordinal) {
        super(enumerators[ordinal], ordinal);
        this.enumerators = enumerators;
    }

    /**
     * Check for any violations, such as empty
     * values, duplicate values.
     *
     * @param enumerators the enumerators
     */
    public static final void check(final String[] enumerators) {
        switch (validate(enumerators)) {
            case VALID:
                return;
            case EMPTY:
                throw DbException.get(ErrorCode.ENUM_EMPTY);
            case DUPLICATE:
                throw DbException.get(ErrorCode.ENUM_DUPLICATE,
                        toString(enumerators));
            default:
                throw DbException.get(ErrorCode.INVALID_VALUE_2,
                        toString(enumerators));
        }
    }

    private static final void check(final String[] enumerators, final Value value) {
        check(enumerators);

        switch (validate(enumerators, value)) {
            case VALID:
                return;
            default:
                throw DbException.get(ErrorCode.ENUM_VALUE_NOT_PERMITTED_2,
                        toString(enumerators), value.toString());
        }
    }

    @Override
    protected int compareSecure(final Value v, final CompareMode mode) {
        final ValueEnum ev = ValueEnum.get(enumerators, v);
        return MathUtils.compareInt(getInt(), ev.getInt());
    }

    /**
     * Create an ENUM value from the provided enumerators
     * and value.
     *
     * @param enumerators the enumerators
     * @param value a value
     * @return the ENUM value
     */
    public static ValueEnum get(final String[] enumerators, final Value value) {
        check(enumerators, value);

        if (DataType.isStringType(value.getType())) {
            final String cleanLabel = sanitize(value.getString());

            for (int i = 0; i < enumerators.length; i++) {
                if (cleanLabel.equals(sanitize(enumerators[i])))
                    return new ValueEnum(enumerators, i);
            }

            throw DbException.get(ErrorCode.GENERAL_ERROR_1, "Unexpected error");
        } else {
            return new ValueEnum(enumerators, value.getInt());
        }
    }

    public String[] getEnumerators() {
        return enumerators;
    }

    /**
     * Evaluates whether a valid ENUM can be constructed
     * from the provided enumerators and value.
     *
     * @param enumerators the enumerators
     * @param value the value
     * @return whether a valid ENUM can be constructed from the provided values
     */
    public static boolean isValid(final String enumerators[], final Value value) {
        return validate(enumerators, value).equals(Validation.VALID);
    }

    private static String sanitize(final String label) {
        return label == null ? null : label.trim().toUpperCase(Locale.ENGLISH);
    }

    private static String[] sanitize(final String[] enumerators) {
        if (enumerators == null || enumerators.length == 0) return null;

        final String[] clean = new String[enumerators.length];

        for (int i = 0; i < enumerators.length; i++) {
            clean[i] = sanitize(enumerators[i]);
        }

        return clean;
    }

    private static String toString(final String[] enumerators) {
        String result = "(";
        for (int i = 0; i < enumerators.length; i++) {
            result += "'" + enumerators[i] + "'";
            if (i < enumerators.length - 1) {
                result += ", ";
            }
        }
        result += ")";
        return result;
    }

    private static Validation validate(final String[] enumerators) {
        final String[] cleaned = sanitize(enumerators);

        if (cleaned == null || cleaned.length == 0) {
            return Validation.EMPTY;
        }

        for (int i = 0; i < cleaned.length; i++) {
            if (cleaned[i] == null || cleaned[i].equals("")) {
                return Validation.EMPTY;
            }

            if (i < cleaned.length - 1) {
                for (int j = i + 1; j < cleaned.length; j++) {
                    if (cleaned[i].equals(cleaned[j])) {
                        return Validation.DUPLICATE;
                    }
                }
            }
        }

        return Validation.VALID;
    }

    private static Validation validate(final String[] enumerators, final Value value) {
        final Validation validation = validate(enumerators);
        if (!validation.equals(Validation.VALID)) {
            return validation;
        }

        if (DataType.isStringType(value.getType())) {
            final String cleanLabel = sanitize(value.getString());

            for (int i = 0; i < enumerators.length; i++) {
                if (cleanLabel.equals(sanitize(enumerators[i]))) {
                    return Validation.VALID;
                }
            }

            return Validation.INVALID;
        } else {
            final int ordinal = value.getInt();

            if (ordinal < 0 || ordinal >= enumerators.length) {
                return Validation.INVALID;
            }

            return Validation.VALID;
        }
    }
}
