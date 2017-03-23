package org.h2.value;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.h2.api.ErrorCode;
import org.h2.message.DbException;
import org.h2.util.MathUtils;
import org.h2.value.DataType;

public class ValueEnum extends Value {
    public static final int PRECISION = 10;
    public static final int DISPLAY_SIZE = 11;

    private static enum Validation {
        DUPLICATE,
        EMPTY,
        INVALID,
        VALID
    }

    private final String[] enumerators;
    private final String label;
    private final int ordinal;

    private ValueEnum(final String[] enumerators, final int ordinal) {
        this.label = enumerators[ordinal];
        this.enumerators = enumerators;
        this.ordinal = ordinal;
    }

    @Override
    public Value add(final Value v) {
        final Value iv = v.convertTo(Value.INT);
        return convertTo(Value.INT).add(iv);
    }

    private static final void check(final String[] enumerators) {
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

    private static final void check(final String[] enumerators, final String label) {
        check(enumerators);

        switch (validate(enumerators, label)) {
            case VALID:
                return;
            default:
                throw DbException.get(ErrorCode.ENUM_VALUE_NOT_PERMITTED_2,
                        toString(enumerators), "'" + label + "'");
        }
    }

    private static final void check(final String[] enumerators, final int ordinal) {
        check(enumerators);

        switch (validate(enumerators, ordinal)) {
            case VALID:
                return;
            default:
                throw DbException.get(ErrorCode.ENUM_VALUE_NOT_PERMITTED_2,
                        toString(enumerators), Integer.toString(ordinal));
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
        return MathUtils.compareInt(ordinal(), ev.ordinal());
    }

    @Override
    public Value divide(final Value v) {
        final Value iv = v.convertTo(Value.INT);
        return convertTo(Value.INT).divide(iv);
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ValueEnum &&
            ordinal() == ((ValueEnum) other).ordinal();
    }

    public static ValueEnum get(final String[] enumerators, final String label) {
        check(enumerators, label);

        for (int i = 0; i < enumerators.length; i++) {
            if (label.equals(enumerators[i]))
                return new ValueEnum(enumerators, i);
        }

        throw DbException.get(ErrorCode.GENERAL_ERROR_1, "Unexpected error");
    }

    public static ValueEnum get(final String[] enumerators, final int ordinal) {
        check(enumerators, ordinal);
        return new ValueEnum(enumerators, ordinal);
    }

    public static ValueEnum get(final String[] enumerators, final Value value) {
        check(enumerators, value);

        if (DataType.isStringType(value.getType())) {
            return get(enumerators, value.getString());
        } else {
            return get(enumerators, value.getInt());
        }
    }

    @Override
    public int getDisplaySize() {
        return DISPLAY_SIZE;
    }

    public String[] getEnumerators() {
        return enumerators;
    }

    @Override
    public int getInt() {
        return ordinal;
    }

    @Override
    public long getLong() {
        return ordinal;
    }

    @Override
    public Object getObject() {
        return ordinal;
    }

    @Override
    public long getPrecision() {
        return PRECISION;
    }

    @Override
    public int getSignum() {
        return Integer.signum(ordinal);
    }

    @Override
    public String getSQL() {
        return getString();
    }

    @Override
    public String getString() {
        return label;
    }

    @Override
    public int getType() {
        return Value.ENUM;
    }

    @Override
    public int hashCode() {
        return enumerators.hashCode() + ordinal;
    }

    public static boolean isValid(final String enumerators[], final String label) {
        return validate(enumerators, label).equals(Validation.VALID);
    }

    public static boolean isValid(final String enumerators[], final int ordinal) {
        return validate(enumerators, ordinal).equals(Validation.VALID);
    }

    public static boolean isValid(final String enumerators[], final Value value) {
        return validate(enumerators, value).equals(Validation.VALID);
    }

    protected int ordinal() {
        return ordinal;
    }

    @Override
    public Value modulus(final Value v) {
        final Value iv = v.convertTo(Value.INT);
        return convertTo(Value.INT).modulus(iv);
    }

    @Override
    public Value multiply(final Value v) {
        final Value iv = v.convertTo(Value.INT);
        return convertTo(Value.INT).multiply(iv);
    }

    @Override
    public void set(final PreparedStatement prep, final int parameterIndex)
            throws SQLException {
         prep.setInt(parameterIndex, ordinal);
    }

    @Override
    public Value subtract(final Value v) {
        final Value iv = v.convertTo(Value.INT);
        return convertTo(Value.INT).subtract(iv);
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

    private static Validation validate(final String[] enumerators, final String label) {
        check(enumerators);

        final String cleanLabel = label.trim().toLowerCase();

        for (int i = 0; i < enumerators.length; i++) {
            if (cleanLabel.equals(enumerators[i])) {
                return Validation.VALID;
            }
        }

        return Validation.INVALID;
    }

    private static Validation validate(final String[] enumerators) {
        for (int i = 0; i < enumerators.length; i++) {
            if (enumerators[i] == null || enumerators[i].trim().equals("")) {
                return Validation.EMPTY;
            }

            if (i < enumerators.length - 1) {
                for (int j = i + 1; j < enumerators.length; j++) {
                    if (enumerators[i].equals(enumerators[j])) {
                        return Validation.DUPLICATE;
                    }
                }
            }
        }

        return Validation.VALID;
    }

    private static Validation validate(final String[] enumerators, final int ordinal) {
        check(enumerators);

        if (ordinal < 0 || ordinal >= enumerators.length) {
            return Validation.INVALID;
        }

        return Validation.VALID;
    }

    private static Validation validate(final String[] enumerators, final Value value) {
        if (DataType.isStringType(value.getType())) {
            return validate(enumerators, value.getString());
        } else {
            return validate(enumerators, value.getInt());
        }
    }
}
