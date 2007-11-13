package org.h2.test.unit;

import org.h2.test.TestBase;
import org.h2.value.ValueUuid;

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
