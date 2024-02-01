/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import static org.h2.engine.Constants.MEMORY_POINTER;

import java.util.concurrent.atomic.AtomicLong;

import org.h2.mvstore.type.DataType;

/**
 * Class MemoryEstimator.
 *
 * Calculation of the amount of memory occupied by keys, values and pages of the MVTable
 * may become expensive operation for complex data types like Row.
 * On the other hand, result of the calculation is used by page cache to limit it's size
 * and determine when eviction is needed. Another usage is to trigger auto commit,
 * based on amount of unsaved changes. In both cases reasonable (lets say ~30%) approximation
 * would be good enough and will do the job.
 * This class replaces exact calculation with an estimate based on
 * a sliding window average of last 256 values.
 * If estimation gets close to the exact value, then next N calculations are skipped
 * and replaced with the estimate, where N depends on the estimation error.
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
public final class MemoryEstimator {

    // Structure of statsData long value:
    // 0 - 7   skip counter (how many more requests will skip calculation and use an estimate instead)
    // 8 - 23  total number of skips between last 256 calculations
    //         (used for sampling percentage calculation only)
    // 24      bit is 0 when window is not completely filled yet, 1 once it become full
    // 25 - 31 unused
    // 32 - 63 sliding window sum of estimated values

    private static final int SKIP_SUM_SHIFT = 8;
    private static final int COUNTER_MASK = (1 << SKIP_SUM_SHIFT) - 1;
    private static final int SKIP_SUM_MASK = 0xFFFF;
    private static final int INIT_BIT_SHIFT = 24;
    private static final int INIT_BIT = 1 << INIT_BIT_SHIFT;
    private static final int WINDOW_SHIFT = 8;
    private static final int MAGNITUDE_LIMIT = WINDOW_SHIFT - 1;
    private static final int WINDOW_SIZE = 1 << WINDOW_SHIFT;
    private static final int WINDOW_HALF_SIZE = WINDOW_SIZE >> 1;
    private static final int SUM_SHIFT = 32;

    private MemoryEstimator() {}

    /**
     * Estimates memory size of the data based on previous values.
     * @param stats AtomicLong holding statistical data about the estimated sequence
     * @param dataType used for calculation of the next sequence value, if necessary
     * @param data which size is to be calculated as the next sequence value, if necessary
     * @param <T> type of the data
     * @return next estimated or calculated value of the sequence
     */
    public static <T> int estimateMemory(AtomicLong stats, DataType<T> dataType, T data) {
        long statsData = stats.get();
        int counter = getCounter(statsData);
        int skipSum = getSkipSum(statsData);
        long initialized = statsData & INIT_BIT;
        long sum = statsData >>> SUM_SHIFT;
        int mem = 0;
        int cnt = 0;
        if (initialized == 0 || counter-- == 0) {
            cnt = 1;
            mem = data == null ? 0 : dataType.getMemory(data);
            long delta = ((long) mem << WINDOW_SHIFT) - sum;
            if (initialized == 0) {
                if (++counter == WINDOW_SIZE) {
                    initialized = INIT_BIT;
                }
                sum = (sum * counter + delta + (counter >> 1)) / counter;
            } else {
                long absDelta = delta >= 0 ? delta : -delta;
                int magnitude = calculateMagnitude(sum, absDelta);
                sum += ((delta >> (MAGNITUDE_LIMIT - magnitude)) + 1) >> 1;
                counter = ((1 << magnitude) - 1) & COUNTER_MASK;

                delta = (counter << WINDOW_SHIFT) - skipSum;
                skipSum += (delta + WINDOW_HALF_SIZE) >> WINDOW_SHIFT;
            }
        }
        long updatedStatsData = updateStatsData(stats, statsData, counter, skipSum, initialized, sum, cnt, mem);
        return getAverage(updatedStatsData);
    }

    /**
     * Estimates memory size of the data set based on previous values.
     * @param stats AtomicLong holding statistical data about the estimated sequence
     * @param dataType used for calculation of the next sequence value, if necessary
     * @param storage of the data set, which size is to be calculated
     * @param count number of data items in the storage
     * @param <T> type of the data in the storage
     * @return next estimated or calculated size of the storage
     */
    public static <T> int estimateMemory(AtomicLong stats, DataType<T> dataType, T[] storage, int count) {
        long statsData = stats.get();
        int counter = getCounter(statsData);
        int skipSum = getSkipSum(statsData);
        long initialized = statsData & INIT_BIT;
        long sum = statsData >>> SUM_SHIFT;
        int index = 0;
        int memSum = 0;
        if (initialized != 0 && counter >= count) {
            counter -= count;
        } else {
            int cnt = count;
            while (cnt-- > 0) {
                T data = storage[index++];
                int mem = data == null ? 0 : dataType.getMemory(data);
                memSum += mem;
                long delta = ((long) mem << WINDOW_SHIFT) - sum;
                if (initialized == 0) {
                    if (++counter == WINDOW_SIZE) {
                        initialized = INIT_BIT;
                    }
                    sum = (sum * counter + delta + (counter >> 1)) / counter;
                } else {
                    cnt -= counter;
                    long absDelta = delta >= 0 ? delta : -delta;
                    int magnitude = calculateMagnitude(sum, absDelta);
                    sum += ((delta >> (MAGNITUDE_LIMIT - magnitude)) + 1) >> 1;
                    counter += ((1 << magnitude) - 1) & COUNTER_MASK;

                    delta = ((long) counter << WINDOW_SHIFT) - skipSum;
                    skipSum += (delta + WINDOW_HALF_SIZE) >> WINDOW_SHIFT;
                }
            }
        }
        long updatedStatsData = updateStatsData(stats, statsData, counter, skipSum, initialized, sum, index, memSum);
        return (getAverage(updatedStatsData) + MEMORY_POINTER) * count;
    }

    /**
     * Calculates percentage of how many times actual calculation happened (vs. estimation)
     * @param stats AtomicLong holding statistical data about the estimated sequence
     * @return sampling percentage in range 0 - 100
     */
    public static int samplingPct(AtomicLong stats) {
        long statsData = stats.get();
        int count = (statsData & INIT_BIT) == 0 ? getCounter(statsData) : WINDOW_SIZE;
        int total = getSkipSum(statsData) + count;
        return (count * 100 + (total >> 1)) / total;
    }

    private static int calculateMagnitude(long sum, long absDelta) {
        int magnitude = 0;
        while (absDelta < sum && magnitude < MAGNITUDE_LIMIT) {
            ++magnitude;
            absDelta <<= 1;
        }
        return magnitude;
    }

    private static long updateStatsData(AtomicLong stats, long statsData,
                                        int counter, int skipSum, long initialized, long sum,
                                        int itemsCount, int itemsMem) {
        return updateStatsData(stats, statsData,
                                constructStatsData(sum, initialized, skipSum, counter), itemsCount, itemsMem);
    }

    private static long constructStatsData(long sum, long initialized, int skipSum, int counter) {
        return (sum << SUM_SHIFT) | initialized | ((long) skipSum << SKIP_SUM_SHIFT) | counter;
    }

    private static long updateStatsData(AtomicLong stats, long statsData, long updatedStatsData,
                                        int itemsCount, int itemsMem) {
        while (!stats.compareAndSet(statsData, updatedStatsData)) {
            statsData = stats.get();
            long sum = statsData >>> SUM_SHIFT;
            if (itemsCount > 0) {
                sum += itemsMem - ((sum * itemsCount + WINDOW_HALF_SIZE) >> WINDOW_SHIFT);
            }
            updatedStatsData = (sum << SUM_SHIFT) | (statsData & (INIT_BIT | SKIP_SUM_MASK | COUNTER_MASK));
        }
        return updatedStatsData;
    }

    private static int getCounter(long statsData) {
        return (int)(statsData & COUNTER_MASK);
    }

    private static int getSkipSum(long statsData) {
        return (int)((statsData >> SKIP_SUM_SHIFT) & SKIP_SUM_MASK);
    }

    private static int getAverage(long updatedStatsData) {
        return (int)(updatedStatsData >>> (SUM_SHIFT + WINDOW_SHIFT));
    }
}
