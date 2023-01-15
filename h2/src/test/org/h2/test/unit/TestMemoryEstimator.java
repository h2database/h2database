/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.BasicDataType;
import org.h2.test.TestBase;
import org.h2.util.MemoryEstimator;

/**
 * Class TestMemoryEstimator.
 * <UL>
 * <LI> 12/7/19 10:38 PM initial creation
 * </UL>
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
public class TestMemoryEstimator extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().testFromMain();
    }

    @Override
    public void test() {
        testEstimator();
        testPageEstimator();
    }

    private void testEstimator() {
        Random random = new Random();
        AtomicLong stat = new AtomicLong();
        TestDataType dataType = new TestDataType();
        int sum = 0;
        int sum2 = 0;
        int err2 = 0;
        int size = 10000;
        for (int i = 0; i < size; i++) {
            int x = (int)Math.abs(100 + random.nextGaussian() * 30);
            int y = MemoryEstimator.estimateMemory(stat, dataType, x);
            sum += x;
            sum2 += x * x;
            err2 += (x - y) * (x - y);
        }
        int avg = sum / size;
        double err = Math.sqrt(1.0 * err2 / sum2);
        int pct = MemoryEstimator.samplingPct(stat);
        String msg = "Avg=" + avg + ", err=" + err + ", pct=" + pct + " " + (dataType.getCount() * 100 / size);
        assertTrue(msg, err < 0.3);
        assertTrue(msg, pct <= 7);
    }

    private void testPageEstimator() {
        Random random = new Random();
        AtomicLong stat = new AtomicLong();
        TestDataType dataType = new TestDataType();
        long sum = 0;
        long sum2 = 0;
        long err2 = 0;
        int size = 10000;
        int pageSz;
        for (int i = 0; i < size; i+=pageSz) {
            pageSz = random.nextInt(48) + 1;
            Integer[] storage = dataType.createStorage(pageSz);
            int x = 0;
            for (int k = 0; k < pageSz; k++) {
                storage[k] = (int)Math.abs(100 + random.nextGaussian() * 30);
                x += storage[k];
            }
            int y = MemoryEstimator.estimateMemory(stat, dataType, storage, pageSz);
            sum += x;
            sum2 += x * x;
            err2 += (x - y) * (x - y);
        }
        long avg = sum / size;
        double err = Math.sqrt(1.0 * err2 / sum2);
        int pct = MemoryEstimator.samplingPct(stat);
        String msg = "Avg=" + avg + ", err=" + err + ", pct=" + pct + " " + (dataType.getCount() * 100 / size);
        assertTrue(msg, err < 0.12);
        assertTrue(msg, pct <= 4);
    }

    private static class TestDataType extends BasicDataType<Integer> {
        private int count;

        TestDataType() {
        }

        public int getCount() {
            return count;
        }

        @Override
        public int getMemory(Integer obj) {
            ++count;
            return obj;
        }

        @Override
        public void write(WriteBuffer buff, Integer obj) {}

        @Override
        public Integer read(ByteBuffer buff) { return null; }

        @Override
        public Integer[] createStorage(int size) { return new Integer[size]; }
    }

}
