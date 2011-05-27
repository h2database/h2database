/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import org.h2.test.TestBase;
import org.h2.value.ValueUuid;

/**
 * Tests features of values.
 */
public class TestValue extends TestBase {

    public void test() throws Exception {
        testUUID();
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
