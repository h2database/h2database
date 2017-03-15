package org.h2.value;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.h2.util.MathUtils;

public class ValueEnum extends Value {
    public static final int PRECISION = 10;
    public static final int DISPLAY_SIZE = 11;

    private final String[] labels;
    private final String label;
    private final int ordinal;

    public ValueEnum(final String[] labels, final int ordinal) {
        this.label = labels[ordinal];
        this.labels = labels;
        this.ordinal = ordinal;
    }

    @Override 
    public Value add(final Value v) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    protected int compareSecure(final Value o, final CompareMode mode) {
        final ValueEnum v = (ValueEnum) o;
        return MathUtils.compareInt(ordinal(), v.ordinal());
    }

    @Override
    public Value divide(final Value v) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ValueEnum &&
            ordinal() == ((ValueEnum) other).ordinal();
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
    public int hashCode() {
        return ordinal;
    }

    @Override
    public int getType() {
        return Value.INT;
    }

    @Override
    public Value modulus(final Value v) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public Value multiply(final Value v) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public Value negate() {
        throw new UnsupportedOperationException("Not yet implemented");
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
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
