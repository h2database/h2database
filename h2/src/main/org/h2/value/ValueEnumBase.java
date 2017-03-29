package org.h2.value;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.h2.message.DbException;
import org.h2.util.MathUtils;

public class ValueEnumBase extends Value {
    public static final int PRECISION = 10;
    public static final int DISPLAY_SIZE = 11;

    private final String label;
    private final int ordinal;

    protected ValueEnumBase(final String label, final int ordinal) {
        this.label = label;
        this.ordinal = ordinal;
    }

    @Override
    public Value add(final Value v) {
        final Value iv = v.convertTo(Value.INT);
        return convertTo(Value.INT).add(iv);
    }

    @Override
    protected int compareSecure(final Value v, final CompareMode mode) {
        return MathUtils.compareInt(ordinal(), v.getInt());
    }

    @Override
    public Value divide(final Value v) {
        final Value iv = v.convertTo(Value.INT);
        return convertTo(Value.INT).divide(iv);
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ValueEnumBase &&
            ordinal() == ((ValueEnumBase) other).ordinal() &&
            getString() == ((ValueEnumBase) other).getString();
    }

    public static ValueEnumBase get(final String label, final int ordinal) {
        return new ValueEnumBase(label, ordinal);
    }

    @Override
    public int getDisplaySize() {
        return DISPLAY_SIZE;
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
        int results = 31;
        results += getString().hashCode();
        results += ordinal();
        return results;
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


    protected int ordinal() {
        return ordinal;
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
}
