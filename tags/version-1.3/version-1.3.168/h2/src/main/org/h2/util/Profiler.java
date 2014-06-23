/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.lang.instrument.Instrumentation;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.h2.constant.SysProperties;
import org.h2.engine.Constants;

/**
 * A simple CPU profiling tool similar to java -Xrunhprof.
 */
public class Profiler implements Runnable {

    private static Instrumentation instrumentation;
    private static final int MAX_ELEMENTS = 1000;

    public int interval = 2;
    public int depth = 32;
    public boolean paused;

    private String[] ignoreLines = StringUtils.arraySplit("", ',', true);
    private String[] ignorePackages = StringUtils.arraySplit(
            "java," +
            "sun," +
            "com.sun.,"
            , ',', true);
    private String[] ignoreThreads = StringUtils.arraySplit(
            "java.lang.Object.wait," +
            "java.lang.Thread.dumpThreads," +
            "java.lang.Thread.getThreads," +
            "java.lang.Thread.sleep," +
            "java.lang.UNIXProcess.waitForProcessExit," +
            "java.net.PlainSocketImpl.accept," +
            "java.net.PlainSocketImpl.socketAccept," +
            "java.net.SocketInputStream.socketRead," +
            "java.net.SocketOutputStream.socketWrite," +
            "sun.awt.windows.WToolkit.eventLoop," +
            "sun.misc.Unsafe.park," +
            "dalvik.system.VMStack.getThreadStackTrace," +
            "dalvik.system.NativeStart.run"
            , ',', true);
    private volatile boolean stop;
    private HashMap<String, Integer> counts = new HashMap<String, Integer>();
    private HashMap<String, Integer> packages = new HashMap<String, Integer>();
    private int minCount = 1;
    private int total;
    private Thread thread;
    private long time;

    /**
     * This method is called when the agent is installed.
     *
     * @param agentArgs the agent arguments
     * @param inst the instrumentation object
     */
    public static void premain(String agentArgs, Instrumentation inst) {
        instrumentation = inst;
    }

    /**
     * Get the instrumentation object if started as an agent.
     *
     * @return the instrumentation, or null
     */
    public static Instrumentation getInstrumentation() {
        return instrumentation;
    }

    /**
     * Start collecting profiling data.
     */
    public void startCollecting() {
        thread = new Thread(this, "Profiler");
        thread.setDaemon(true);
        thread.start();
    }

    /**
     * Stop collecting.
     */
    public void stopCollecting() {
        stop = true;
        if (thread != null) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                // ignore
            }
            thread = null;
        }
    }

    public void run() {
        time = System.currentTimeMillis();
        while (!stop) {
            try {
                tick();
            } catch (Throwable t) {
                break;
            }
        }
        time = System.currentTimeMillis() - time;
    }

    private void tick() {
        if (interval > 0) {
            if (paused) {
                return;
            }
            try {
                Thread.sleep(interval);
            } catch (Exception e) {
                // ignore
            }
        }
        Map<Thread, StackTraceElement[]> map = Thread.getAllStackTraces();
        for (Map.Entry<Thread, StackTraceElement[]> entry : map.entrySet()) {
            Thread t = entry.getKey();
            if (t.getState() != Thread.State.RUNNABLE) {
                continue;
            }
            StackTraceElement[] dump = entry.getValue();
            if (dump == null || dump.length == 0) {
                continue;
            }
            if (startsWithAny(dump[0].toString(), ignoreThreads)) {
                continue;
            }
            StringBuilder buff = new StringBuilder();
            // simple recursive calls are ignored
            String last = null;
            boolean packageCounts = false;
            for (int j = 0, i = 0; i < dump.length && j < depth; i++) {
                String el = dump[i].toString();
                if (!el.equals(last) && !startsWithAny(el, ignoreLines)) {
                    last = el;
                    buff.append("at ").append(el).append(SysProperties.LINE_SEPARATOR);
                    if (!packageCounts && !startsWithAny(el, ignorePackages)) {
                        packageCounts = true;
                        int index = 0;
                        for (; index < el.length(); index++) {
                            char c = el.charAt(index);
                            if (Character.isUpperCase(c) || c == '(') {
                                break;
                            }
                        }
                        if (index > 0 && el.charAt(index - 1) == '.') {
                            index--;
                        }
                        String packageName = el.substring(0, index);
                        increment(packages, packageName, 0);
                    }
                    j++;
                }
            }
            if (buff.length() > 0) {
                minCount = increment(counts, buff.toString().trim(), minCount);
                total++;
            }
        }
    }

    private static boolean startsWithAny(String s, String[] prefixes) {
        for (String p : prefixes) {
            if (p.length() > 0 && s.startsWith(p)) {
                return true;
            }
        }
        return false;
    }

    private static int increment(HashMap<String, Integer> map, String trace, int minCount) {
        Integer oldCount = map.get(trace);
        if (oldCount == null) {
            map.put(trace, 1);
        } else {
            map.put(trace, oldCount + 1);
        }
        while (map.size() > MAX_ELEMENTS) {
            for (Iterator<Map.Entry<String, Integer>> ei = map.entrySet().iterator(); ei.hasNext();) {
                Map.Entry<String, Integer> e = ei.next();
                if (e.getValue() <= minCount) {
                    ei.remove();
                }
            }
            if (map.size() > MAX_ELEMENTS) {
                minCount++;
            }
        }
        return minCount;
    }

    /**
     * Get the top stack traces.
     *
     * @param count the maximum number of stack traces
     * @return the stack traces.
     */
    public String getTop(int count) {
        stopCollecting();
        StringBuilder buff = new StringBuilder();
        buff.append("Profiler: top ").append(count).append(" stack trace(s) of ").append(time).
            append(" ms [build-").append(Constants.BUILD_ID).append("]:").append(SysProperties.LINE_SEPARATOR);
        if (counts.size() == 0) {
            buff.append("(none)").append(SysProperties.LINE_SEPARATOR);
        }
        appendTop(buff, counts, count, total, false);
        buff.append("packages:").append(SysProperties.LINE_SEPARATOR);
        appendTop(buff, packages, count, total, true);
        buff.append('.');
        return buff.toString();
    }

    private static void appendTop(StringBuilder buff, HashMap<String, Integer> map, int count, int total, boolean table) {
        for (int x = 0, min = 0;;) {
            int highest = 0;
            Map.Entry<String, Integer> best = null;
            for (Map.Entry<String, Integer> el : map.entrySet()) {
                if (el.getValue() > highest) {
                    best = el;
                    highest = el.getValue();
                }
            }
            if (best == null) {
                break;
            }
            map.remove(best.getKey());
            if (++x >= count) {
                if (best.getValue() < min) {
                    break;
                }
                min = best.getValue();
            }
            int c = best.getValue();
            int percent = 100 * c / Math.max(total, 1);
            if (table) {
                if (percent > 1) {
                    buff.append(percent).
                        append("%: ").append(best.getKey()).
                        append(SysProperties.LINE_SEPARATOR);
                }
            } else {
                buff.append(c).append('/').append(total).append(" (").
                    append(percent).
                    append("%):").append(SysProperties.LINE_SEPARATOR).
                    append(best.getKey()).
                    append(SysProperties.LINE_SEPARATOR);
            }
        }
    }

}
