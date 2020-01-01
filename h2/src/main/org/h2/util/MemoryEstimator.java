/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import static org.h2.engine.Constants.MEMORY_POINTER;
import org.h2.mvstore.type.DataType;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Class MemoryEstimator.
 * <UL>
 * <LI> 12/7/19 10:45 PM initial creation
 * </UL>
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
public final class MemoryEstimator
{
    private MemoryEstimator() {}

    public static <T> int estimateMemory(AtomicLong stats, DataType<T> dataType, T data) {
        long statsData = stats.get();
        int counter = (int)(statsData & 0xFF);
        int skipSum = (int)((statsData >> 8) & 0xFFFF);
        long initialized = statsData & 0x1000000;
        long sum = statsData >> 32;
        int mem = 0;
        int cnt = 0;
        if (initialized == 0 || counter-- == 0) {
            cnt = 1;
            mem = data == null ? 0 : dataType.getMemory(data);
            long delta = (mem << 8) - sum;
            if (initialized == 0) {
                if (++counter == 256) {
                    initialized = 0x1000000;
                }
                sum = (sum * counter + delta + (counter >> 1)) / counter;
            } else {
                long absDelta = delta >= 0 ? delta : -delta;
                int magnitude = calculateMagnitude(sum, absDelta);
                sum += ((delta >> (7 - magnitude)) + 1) >> 1;
                counter = ((1 << magnitude) - 1) & 0xFF;

                delta = (counter << 8) - skipSum;
                skipSum += (delta + 128) >> 8;
            }
        }
        long updatedStatsData = sum << 32 | initialized | (skipSum << 8) | counter;
        updatedStatsData = updateStatsData(stats, statsData, updatedStatsData, mem, cnt);
        return (int)(updatedStatsData >> 40);
    }

    public static <T> int estimateMemory(AtomicLong stats, DataType<T> dataType, T[] storage, int count) {
        long statsData = stats.get();
        int counter = (int)(statsData & 0xFF);
        int skipSum = (int)((statsData >> 8) & 0xFFFF);
        long initialized = statsData & 0x1000000;
        long sum = statsData >> 32;
        int indx = 0;
        int memSum = 0;
        if (initialized != 0 && counter >= count) {
            counter -= count;
        } else {
            int cnt = count;
            while (cnt-- > 0) {
                T data = storage[indx++];
                int mem = data == null ? 0 : dataType.getMemory(data);
                memSum += mem;
                long delta = (mem << 8) - sum;
                if (initialized == 0) {
                    if (++counter == 256) {
                        initialized = 0x1000000;
                    }
                    sum = (sum * counter + delta + (counter >> 1)) / counter;
                } else {
                    cnt -= counter;
                    long absDelta = delta >= 0 ? delta : -delta;
                    int magnitude = calculateMagnitude(sum, absDelta);
                    sum += ((delta >> (7 - magnitude)) + 1) >> 1;
                    counter += ((1 << magnitude) - 1) & 0xFF;

                    delta = (counter << 8) - skipSum;
                    skipSum += (delta + 128) >> 8;
                }
            }
        }
        long updatedStatsData = sum << 32 | initialized | (skipSum << 8) | counter;
        updatedStatsData = updateStatsData(stats, statsData, updatedStatsData, memSum, indx);
        return ((int)(updatedStatsData >> 40) + MEMORY_POINTER) * count;
    }

    private static int calculateMagnitude(long sum, long absDelta) {
        int magnitude = 0;
        while (absDelta < sum && magnitude < 7) {
            ++magnitude;
            absDelta <<= 1;
        }
        return magnitude;
    }

    private static long updateStatsData(AtomicLong stats, long statsData, long updatedStatsData, int itemsMem, int itemsCount) {
        while (!stats.compareAndSet(statsData, updatedStatsData)) {
            statsData = stats.get();
            long sum = statsData >> 32;
            if (itemsCount > 0) {
                sum += itemsMem - ((sum * itemsCount + 128) >> 8);
            }
            updatedStatsData = (sum << 32) | (statsData & 0x1FFFFFF);
        }
        return updatedStatsData;
    }

    public static int samplingPct(AtomicLong stats) {
        long statsData = stats.get();
        int count = (statsData & 0x1000000) == 0 ? (int)(statsData & 0xFF) : 256;
        int total = (int)((statsData >> 8) & 0xFFFF) + count;
        return (count * 100 + (total >> 1)) / total;
    }
}
