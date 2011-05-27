/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import org.h2.test.TestBase;
import org.h2.value.Value;
import org.h2.value.ValueDouble;
import org.h2.value.ValueFloat;
import org.h2.value.ValueUuid;

/**
 * Tests features of values.
 */
public class TestValue extends TestBase {

    public void test() throws Exception {
        testUUID();
        testDouble(false);
        testDouble(true);
    }
    
    private void testDouble(boolean useFloat) throws Exception {
        double[] d = new double[]{
                Double.NEGATIVE_INFINITY,
                -1,
                0,
                1,
                Double.POSITIVE_INFINITY,
                Double.NaN
        };
        Value[] values = new Value[d.length];
        for (int i = 0; i < d.length; i++) {
            Value v = useFloat ? (Value) ValueFloat.get((float) d[i]) : (Value) ValueDouble.get(d[i]);
            values[i] = v;
            check(values[i].compareTypeSave(values[i], null) == 0);
            check(v.equals(v));
            check(i < 2 ? -1 : i > 2 ? 1 : 0, v.getSignum());
        }
        for (int i = 0; i < d.length - 1; i++) {
            check(values[i].compareTypeSave(values[i+1], null) < 0);
            check(values[i + 1].compareTypeSave(values[i], null) > 0);
            check(!values[i].equals(values[i+1]));
        }        
    }

    private void testUUID() throws Exception {
        long maxHigh = 0, maxLow = 0, minHigh = -1L, minLow = -1L;
        for (int i = 0; i < 100; i++) {
            ValueUuid uuid = ValueUuid.getNewRandom();
            maxHigh |= uuid.getHigh();
            maxLow |= uuid.getLow();
            minHigh &= uuid.getHigh();
            minLow &= uuid.getLow();
        }
        ValueUuid max = ValueUuid.get(maxHigh, maxLow);
        check(max.getString(), "ffffffff-ffff-4fff-bfff-ffffffffffff");
        ValueUuid min = ValueUuid.get(minHigh, minLow);
        check(min.getString(), "00000000-0000-4000-8000-000000000000");
    }

}
