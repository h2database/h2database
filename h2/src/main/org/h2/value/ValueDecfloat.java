/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.math.BigDecimal;
import java.math.RoundingMode;

import org.h2.api.ErrorCode;
import org.h2.engine.CastDataProvider;
import org.h2.message.DbException;

/**
 * Implementation of the DECFLOAT data type.
 */
public final class ValueDecfloat extends ValueBigDecimalBase {

    /**
     * The value 'zero'.
     */
    public static final ValueDecfloat ZERO = new ValueDecfloat(BigDecimal.ZERO);

    /**
     * The value 'one'.
     */
    public static final ValueDecfloat ONE = new ValueDecfloat(BigDecimal.ONE);

    /**
     * The positive infinity value.
     */
    public static final ValueDecfloat POSITIVE_INFINITY = new ValueDecfloat(null);

    /**
     * The negative infinity value.
     */
    public static final ValueDecfloat NEGATIVE_INFINITY = new ValueDecfloat(null);

    /**
     * The not a number value.
     */
    public static final ValueDecfloat NAN = new ValueDecfloat(null);

    private ValueDecfloat(BigDecimal value) {
        super(value);
    }

    @Override
    public String getString() {
        if (value == null) {
            if (this == POSITIVE_INFINITY) {
                return "Infinity";
            } else if (this == NEGATIVE_INFINITY) {
                return "-Infinity";
            } else {
                return "NaN";
            }
        }
        return value.toString();
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        if ((sqlFlags & NO_CASTS) == 0) {
            return getSQL(builder.append("CAST(")).append(" AS DECFLOAT)");
        }
        return getSQL(builder);
    }

    private StringBuilder getSQL(StringBuilder builder) {
        if (value != null) {
            return builder.append(value);
        } else if (this == POSITIVE_INFINITY) {
            return builder.append("'Infinity'");
        } else if (this == NEGATIVE_INFINITY) {
            return builder.append("'-Infinity'");
        } else {
            return builder.append("'NaN'");
        }
    }

    @Override
    public TypeInfo getType() {
        TypeInfo type = this.type;
        if (type == null) {
            this.type = type = new TypeInfo(DECFLOAT, value != null ? value.precision() : 1, 0, null);
        }
        return type;
    }

    @Override
    public int getValueType() {
        return DECFLOAT;
    }

    @Override
    public Value add(Value v) {
        BigDecimal value2 = ((ValueDecfloat) v).value;
        if (value != null) {
            if (value2 != null) {
                return get(value.add(value2));
            }
            return v;
        } else if (value2 != null || this == v) {
            return this;
        }
        return NAN;
    }

    @Override
    public Value subtract(Value v) {
        BigDecimal value2 = ((ValueDecfloat) v).value;
        if (value != null) {
            if (value2 != null) {
                return get(value.subtract(value2));
            }
            return v == POSITIVE_INFINITY ? NEGATIVE_INFINITY : v == NEGATIVE_INFINITY ? POSITIVE_INFINITY : NAN;
        } else if (value2 != null) {
            return this;
        } else if (this == POSITIVE_INFINITY) {
            if (v == NEGATIVE_INFINITY) {
                return POSITIVE_INFINITY;
            }
        } else if (this == NEGATIVE_INFINITY && v == POSITIVE_INFINITY) {
            return NEGATIVE_INFINITY;
        }
        return NAN;
    }

    @Override
    public Value negate() {
        if (value != null) {
            return get(value.negate());
        }
        return this == POSITIVE_INFINITY ? NEGATIVE_INFINITY : this == NEGATIVE_INFINITY ? POSITIVE_INFINITY : NAN;
    }

    @Override
    public Value multiply(Value v) {
        BigDecimal value2 = ((ValueDecfloat) v).value;
        if (value != null) {
            if (value2 != null) {
                return get(value.multiply(value2));
            }
            if (v == POSITIVE_INFINITY) {
                int s = value.signum();
                if (s > 0) {
                    return POSITIVE_INFINITY;
                } else if (s < 0) {
                    return NEGATIVE_INFINITY;
                }
            } else if (v == NEGATIVE_INFINITY) {
                int s = value.signum();
                if (s > 0) {
                    return NEGATIVE_INFINITY;
                } else if (s < 0) {
                    return POSITIVE_INFINITY;
                }
            }
        } else if (value2 != null) {
            if (this == POSITIVE_INFINITY) {
                int s = value2.signum();
                if (s > 0) {
                    return POSITIVE_INFINITY;
                } else if (s < 0) {
                    return NEGATIVE_INFINITY;
                }
            } else if (this == NEGATIVE_INFINITY) {
                int s = value2.signum();
                if (s > 0) {
                    return NEGATIVE_INFINITY;
                } else if (s < 0) {
                    return POSITIVE_INFINITY;
                }
            }
        } else if (this == POSITIVE_INFINITY) {
            if (v == POSITIVE_INFINITY) {
                return POSITIVE_INFINITY;
            } else if (v == NEGATIVE_INFINITY) {
                return NEGATIVE_INFINITY;
            }
        } else if (this == NEGATIVE_INFINITY) {
            if (v == POSITIVE_INFINITY) {
                return NEGATIVE_INFINITY;
            } else if (v == NEGATIVE_INFINITY) {
                return POSITIVE_INFINITY;
            }
        }
        return NAN;
    }

    @Override
    public Value divide(Value v, TypeInfo quotientType) {
        BigDecimal value2 = ((ValueDecfloat) v).value;
        if (value2 != null && value2.signum() == 0) {
            throw DbException.get(ErrorCode.DIVISION_BY_ZERO_1, getTraceSQL());
        }
        if (value != null) {
            if (value2 != null) {
                return divide(value, value2, quotientType);
            } else {
                if (v != NAN) {
                    return ZERO;
                }
            }
        } else if (value2 != null && this != NAN) {
            return (this == POSITIVE_INFINITY) == (value2.signum() > 0) ? POSITIVE_INFINITY : NEGATIVE_INFINITY;
        }
        return NAN;
    }

    /**
     * Divides to {@link BigDecimal} values and returns a {@code DECFLOAT}
     * result of the specified data type.
     *
     * @param dividend the dividend
     * @param divisor the divisor
     * @param quotientType the type of quotient
     * @return the quotient
     */
    public static ValueDecfloat divide(BigDecimal dividend, BigDecimal divisor, TypeInfo quotientType) {
        int quotientPrecision = (int) quotientType.getPrecision();
        BigDecimal quotient = dividend.divide(divisor,
                dividend.scale() - dividend.precision() + divisor.precision() - divisor.scale() + quotientPrecision,
                RoundingMode.HALF_DOWN);
        int precision = quotient.precision();
        if (precision > quotientPrecision) {
            quotient = quotient.setScale(quotient.scale() - precision + quotientPrecision, RoundingMode.HALF_UP);
        }
        return get(quotient);
    }

    @Override
    public Value modulus(Value v) {
        BigDecimal value2 = ((ValueDecfloat) v).value;
        if (value2 != null && value2.signum() == 0) {
            throw DbException.get(ErrorCode.DIVISION_BY_ZERO_1, getTraceSQL());
        }
        if (value != null) {
            if (value2 != null) {
                return get(value.remainder(value2));
            } else if (v != NAN) {
                return this;
            }
        }
        return NAN;
    }

    @Override
    public int compareTypeSafe(Value o, CompareMode mode, CastDataProvider provider) {
        BigDecimal value2 = ((ValueDecfloat) o).value;
        if (value != null) {
            if (value2 != null) {
                return value.compareTo(value2);
            }
            return o == NEGATIVE_INFINITY ? 1 : -1;
        } else if (value2 != null) {
            return this == NEGATIVE_INFINITY ? -1 : 1;
        } else if (this == o) {
            return 0;
        } else if (this == NEGATIVE_INFINITY) {
            return -1;
        } else if (o == NEGATIVE_INFINITY) {
            return 1;
        } else {
            return this == POSITIVE_INFINITY ? -1 : 1;
        }
    }

    @Override
    public int getSignum() {
        if (value != null) {
            return value.signum();
        }
        return this == POSITIVE_INFINITY ? 1 : this == NEGATIVE_INFINITY ? -1 : 0;
    }

    @Override
    public BigDecimal getBigDecimal() {
        if (value != null) {
            return value;
        }
        throw getDataConversionError(NUMERIC);
    }

    @Override
    public float getFloat() {
        if (value != null) {
            return value.floatValue();
        } else if (this == POSITIVE_INFINITY) {
            return Float.POSITIVE_INFINITY;
        } else if (this == NEGATIVE_INFINITY) {
            return Float.NEGATIVE_INFINITY;
        } else {
            return Float.NaN;
        }
    }

    @Override
    public double getDouble() {
        if (value != null) {
            return value.doubleValue();
        } else if (this == POSITIVE_INFINITY) {
            return Double.POSITIVE_INFINITY;
        } else if (this == NEGATIVE_INFINITY) {
            return Double.NEGATIVE_INFINITY;
        } else {
            return Double.NaN;
        }
    }

    @Override
    public int hashCode() {
        return value != null ? getClass().hashCode() * 31 + value.hashCode() : System.identityHashCode(this);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof ValueDecfloat) {
            BigDecimal value2 = ((ValueDecfloat) other).value;
            if (value != null) {
                return value.equals(value2);
            } else if (value2 == null && this == other) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int getMemory() {
        return value != null ? value.precision() + 120 : 32;
    }

    /**
     * Returns {@code true}, if this value is finite.
     *
     * @return {@code true}, if this value is finite, {@code false} otherwise
     */
    public boolean isFinite() {
        return value != null;
    }

    /**
     * Get or create a DECFLOAT value for the given big decimal.
     *
     * @param dec the big decimal
     * @return the value
     */
    public static ValueDecfloat get(BigDecimal dec) {
        dec = dec.stripTrailingZeros();
        if (BigDecimal.ZERO.equals(dec)) {
            return ZERO;
        } else if (BigDecimal.ONE.equals(dec)) {
            return ONE;
        }
        return (ValueDecfloat) Value.cache(new ValueDecfloat(dec));
    }

}
