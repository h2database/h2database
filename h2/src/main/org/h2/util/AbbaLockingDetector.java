/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License, Version
 * 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.util;

import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A simple CPU profiling tool similar to java -Xrunhprof. It can be used
 * in-process (to profile the current application) or as a standalone program
 * (to profile a different process, or files containing full thread dumps).
 */
public class AbbaLockingDetector implements Runnable {

    private int tickInterval_ms = 2;
    private volatile boolean stop;
    private final Set<String> lockObjectNames = new HashSet<String>();

    /**
     * Necessary because String[] does not implement a deep equals and hashcode.
     */
    private static final class LockOrder {
        public final String[] names;

        public LockOrder(String[] names) {
            this.names = names;
        }

        @Override
        public boolean equals(Object obj) {
            return Arrays.deepEquals(names, ((LockOrder) obj).names);
        }

        @Override
        public int hashCode() {
            return Arrays.deepHashCode(names);
        }
    }

    private final Set<LockOrder> lockObjectOrders = new HashSet<LockOrder>();
    private final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    private Thread thread;

    /**
     * Start collecting locking data.
     * 
     * @return this
     */
    public AbbaLockingDetector startCollecting() {
        thread = new Thread(this, "AbbaLockingDetector");
        thread.setDaemon(true);
        thread.start();
        return this;
    }

    /**
     * Stop collecting.
     * 
     * @return this
     */
    public AbbaLockingDetector stopCollecting() {
        stop = true;
        if (thread != null) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                // ignore
            }
            thread = null;
        }
        return this;
    }

    @Override
    public void run() {
        while (!stop) {
            try {
                tick();
            } catch (Throwable t) {
                break;
            }
        }
    }

    private void tick() {
        if (tickInterval_ms > 0) {
            try {
                Thread.sleep(tickInterval_ms);
            } catch (InterruptedException ex) {
                // ignore
            }
        }

        ThreadInfo[] list = threadMXBean.dumpAllThreads(true/* lockedMonitors */, true/* lockedSynchronizers */);
        processList(list);
    }

    private void processList(ThreadInfo[] threadInfoList) {
        List<String> lockOrder = new ArrayList<String>();
        for (ThreadInfo threadInfo : threadInfoList) {
            MonitorInfo[] monitorInfoList = threadInfo.getLockedMonitors();
            lockOrder.clear();
            for (MonitorInfo monitorInfo : monitorInfoList) {
                String lockName = monitorInfo.getClassName();
                if (lockName.startsWith("org.h2")) {
                    lockObjectNames.add(lockName);
                    // ignore locks which are locked multiple times in succession - Java locks are recursive
                    if (lockOrder.isEmpty() || !lockOrder.get(lockOrder.size() - 1).equals(lockName)) {
                        lockOrder.add(lockName);
                    }
                }
            }
            if (lockOrder.size()>1) {
                lockObjectOrders.add(new LockOrder(lockOrder.toArray(new String[lockOrder.size()])));
            }
        }
    }

    public void dump() {
        stopCollecting();
        System.out.println("Locked Objects");
        System.out.println("--------------------------------");
        for (String s : lockObjectNames) {
            System.out.println(s);
        }
        System.out.println();
        System.out.println("Locking Orders");
        System.out.println("--------------------------------");
        for (LockOrder s : lockObjectOrders) {
            System.out.println(Arrays.toString(s.names));
        }
    }

}
