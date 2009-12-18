/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.h2.engine.Constants;

/**
 * A simple CPU profiling tool similar to java -Xrunhprof.
 */
public class Profiler implements Runnable {
    private static final int MAX_ELEMENTS = 1000;
    private int interval = 50;
    private int depth = 16;
    private String[] ignoreLines = StringUtils.arraySplit("", ',', true);
    private String[] ignoreThreads = StringUtils.arraySplit(
            "java.net.,java.lang.Thread.,sun.awt.", ',', true);
    private volatile boolean stop;
    private HashMap<String, Integer> counts = new HashMap<String, Integer>();
    private int minCount = 1;
    private int total;
    private Thread thread;

    /**
     * Start collecting profiling data.
     */
    public void startCollecting() {
        thread = new Thread(this);
        thread.setName("Profiler");
        thread.setDaemon(true);
        thread.start();
    }

    /**
     * Stop collecting.
     */
    public void stopCollecting() {
        stop = true;
    }

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
        if (interval > 0) {
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
            if (dump.length == 0) {
                continue;
            }
            boolean ignoreThis = false;
            for (String ig : ignoreThreads) {
                if (ig.length() > 0 && dump[0].toString().startsWith(ig)) {
                    ignoreThis = true;
                    break;
                }
            }
            if (ignoreThis) {
                continue;
            }
            StringBuilder buff = new StringBuilder();
            // simple recursive calls are ignored
            String last = null;
            for (int j = 0, i = 0; i < dump.length && j < depth; i++) {
                String el = dump[i].toString();
                ignoreThis = false;
                for (String ig : ignoreLines) {
                    if (ig.length() > 0 && el.startsWith(ig)) {
                        ignoreThis = true;
                        break;
                    }
                }
                if (!ignoreThis && !el.equals(last)) {
                    last = el;
                    buff.append("at ").append(el).append('\n');
                    j++;
                }
            }
            if (buff.length() > 0) {
                increment(buff.toString());
            }
        }
    }

    private void increment(String trace) {
        total++;
        Integer oldCount = counts.get(trace);
        if (oldCount == null) {
            counts.put(trace, 1);
        } else {
            counts.put(trace, oldCount + 1);
        }
        if (counts.size() > MAX_ELEMENTS) {
            for (Iterator<Map.Entry<String, Integer>> ei = counts.entrySet().iterator(); ei.hasNext();) {
                Map.Entry<String, Integer> e = ei.next();
                if (e.getValue() <= minCount) {
                    ei.remove();
                }
            }
            if (counts.size() > MAX_ELEMENTS) {
                minCount++;
            }
        }
    }

    /**
     * Get the top stack traces.
     *
     * @param max the maximum number of results
     * @return the stack traces.
     */
    public String getTop(int max) {
        StringBuilder buff = new StringBuilder();
        buff.append("Profiler: top ").append(max).append(" stack trace(s) [build-").append(Constants.BUILD_ID).append("]\n");
        @SuppressWarnings("unchecked")
        Map.Entry<String, Integer>[] array = new Map.Entry[counts.size()];
        counts.entrySet().toArray(array);
        Arrays.sort(array, new Comparator<Map.Entry<String, Integer>>() {
            public int compare(Map.Entry<String, Integer> a, Map.Entry<String, Integer> b) {
                return b.getValue() - a.getValue();
            }
        });
        int x = 0, min = 0;
        for (Map.Entry<String, Integer> el : array) {
            if (++x >= max) {
                if (el.getValue() < min) {
                    break;
                }
                min = el.getValue();
            }
            buff.append(el.getValue()).append('/').append(total).
                append('\n').append(el.getKey());
        }
        return buff.toString();
    }

}
