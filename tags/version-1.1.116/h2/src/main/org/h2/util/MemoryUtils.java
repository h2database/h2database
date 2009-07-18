/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import org.h2.constant.SysProperties;

/**
 * This is a utility class with functions to measure the free and used memory.
 */
public class MemoryUtils {

    /**
     * An 0-size byte array.
     */
    public static final byte[] EMPTY_BYTES = new byte[0];

    /**
     * An 0-size int array.
     */
    public static final int[] EMPTY_INTS = new int[0];

    private static long lastGC;
    private static final int GC_DELAY = 50;
    private static final int MAX_GC = 8;
    private static volatile byte[] reserveMemory;

    private MemoryUtils() {
        // utility class
    }

    /**
     * Get the used memory in KB.
     *
     * @return the used memory
     */
    public static int getMemoryUsed() {
        collectGarbage();
        Runtime rt = Runtime.getRuntime();
        long mem = rt.totalMemory() - rt.freeMemory();
        return (int) (mem >> 10);
    }

    /**
     * Get the free memory in KB.
     *
     * @return the used memory
     */
    public static int getMemoryFree() {
        collectGarbage();
        Runtime rt = Runtime.getRuntime();
        long mem = rt.freeMemory();
        return (int) (mem >> 10);
    }

    private static synchronized void collectGarbage() {
        Runtime runtime = Runtime.getRuntime();
        long total = runtime.totalMemory();
        long time = System.currentTimeMillis();
        if (lastGC + GC_DELAY < time) {
            for (int i = 0; i < MAX_GC; i++) {
                runtime.gc();
                long now = runtime.totalMemory();
                if (now == total) {
                    lastGC = System.currentTimeMillis();
                    break;
                }
                total = now;
            }
        }
    }

    /**
     * Allocate a little main memory that is freed up when if no memory is
     * available, so that rolling back a large transaction is easier.
     */
    public static void allocateReserveMemory() {
        if (reserveMemory == null) {
            reserveMemory = new byte[SysProperties.RESERVE_MEMORY];
        }
    }

    /**
     * Free up the reserve memory.
     */
    public static void freeReserveMemory() {
        reserveMemory = null;
    }

    /**
     * Create an array of bytes with the given size. If this is not possible
     * because not enough memory is available, an OutOfMemoryError with the
     * requested size in the message is thrown.
     *
     * @param len the number of bytes requested
     * @return the byte array
     * @throws OutOfMemoryError
     */
    public static byte[] newBytes(int len) {
        try {
            if (len == 0) {
                return EMPTY_BYTES;
            }
            return new byte[len];
        } catch (OutOfMemoryError e) {
            Error e2 = new OutOfMemoryError("Requested memory: " + len);
            e2.initCause(e);
            throw e2;
        }
    }

    /**
     * Create an array of ints with the given size.
     *
     * @param len the number of bytes requested
     * @return the int array
     */
    public static int[] newInts(int len) {
        if (len == 0) {
            return EMPTY_INTS;
        }
        return new int[len];
    }

}
