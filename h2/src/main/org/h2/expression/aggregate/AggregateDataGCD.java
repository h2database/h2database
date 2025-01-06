/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.aggregate;

import java.math.BigInteger;

import org.h2.engine.SessionLocal;
import org.h2.expression.function.GCDFunction;
import org.h2.message.DbException;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueNumeric;

/**
 * Data stored while calculating GCD_AGG or LCM_AGG aggregate.
 */
final class AggregateDataGCD extends AggregateData {

    private final boolean lcm;

    private boolean skipRemaining, overflow;

    private BigInteger bi;

    AggregateDataGCD(boolean lcm) {
        this.lcm = lcm;
    }

    @Override
    void add(SessionLocal session, Value v) {
        if (v == ValueNull.INSTANCE || skipRemaining) {
            return;
        }
        BigInteger n = v.getBigInteger();
        if (lcm) {
            if (n.signum() == 0) {
                bi = BigInteger.ZERO;
                skipRemaining = true;
                overflow = false;
            } else {
                if (bi == null) {
                    bi = n.abs();
                } else if (!overflow) {
                    bi = bi.multiply(n).abs().divide(bi.gcd(n));
                    overflow = bi.bitLength() > GCDFunction.MAX_BIT_LENGTH;
                }
            }
        } else {
            if (bi == null) {
                bi = n.abs();
            } else if (n.signum() != 0) {
                bi = bi.gcd(n);
            } else {
                return;
            }
            skipRemaining = bi.equals(BigInteger.ONE);
        }
    }

    @Override
    Value getValue(SessionLocal session) {
        if (overflow) {
            throw DbException.getValueTooLongException("NUMERIC", "unknown least common multiple", -1);
        }
        return bi != null ? ValueNumeric.get(bi) : ValueNull.INSTANCE;
    }

}
