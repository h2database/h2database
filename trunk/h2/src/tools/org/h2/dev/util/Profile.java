/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.LineNumberReader;
import java.io.OutputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A simple CPU profiling tool similar to java -Xrunhprof.
 */
public class Profile {

    private static final String LINE_SEPARATOR = System.getProperty("line.separator", "\n");

    private static final int MAX_ELEMENTS = 1000;

    public int interval = 2;
    public int depth = 48;
    public boolean paused;
    public boolean sumClasses;

    private int pid;

    private String[] ignoreLines = {};
    private String[] ignorePackages = (
            "java," +
            "sun," +
            "com.sun."
            ).split(",");
    private String[] ignoreThreads = (
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
            ).split(",");

    private volatile boolean stop;
    private HashMap<String, Integer> counts = new HashMap<String, Integer>();

    /**
     * The summary (usually one entry per package, unless sumClasses is enabled,
     * in which case it's one entry per class).
     */
    private HashMap<String, Integer> summary = new HashMap<String, Integer>();
    private int minCount = 1;
    private int total;
    private Thread thread;
    private long time;
    private long start;

    public static void main(String... args) {
        new Profile().run(args);
    }

    void run(String... args) {
        if (args.length == 0) {
            System.out.println("Show profiling data");
            System.out.println("Usage: " + getClass().getName() + " <pid>");
            System.out.println("Processes:");
            String processes = exec("jps", "-l");
            System.out.println(processes);
            return;
        }
        pid = Integer.parseInt(args[0]);
        start = System.currentTimeMillis();
        long last = 0;
        while (true) {
            tick();
            long t = System.currentTimeMillis();
            if (t - last > 5000) {
                paused = true;
                System.out.println(getTop(3));
                paused = false;
                last = t;
            }
        }
    }

    List<String[]> readAllStackTraces(int pid) {
        try {
            ArrayList<String[]> list = new ArrayList<String[]>();
            String jstack = exec("jstack", "" + pid);
            LineNumberReader r = new LineNumberReader(new StringReader(jstack));
            while (true) {
                String line = r.readLine();
                if (line == null) {
                    break;
                }
                if (!line.startsWith("\"")) {
                    // not a thread
                    continue;
                }
                line = r.readLine();
                if (line == null) {
                    break;
                }
                line = line.trim();
                if (!line.startsWith("java.lang.Thread.State: RUNNABLE")) {
                    continue;
                }
                ArrayList<String> stack = new ArrayList<String>();
                while (true) {
                    line = r.readLine();
                    if (line == null) {
                        break;
                    }
                    line = line.trim();
                    if (line.startsWith("- ")) {
                        continue;
                    }
                    if (!line.startsWith("at ")) {
                        break;
                    }
                    line = line.substring(3).trim();
                    stack.add(line);
                }
                if (stack.size() > 0) {
                    String[] s = stack.toArray(new String[stack.size()]);
                    list.add(s);
                }
            }
            return list;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String exec(String... args) {
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            Process p = Runtime.getRuntime().exec(args);
            copyInThread(p.getInputStream(), out);
            copyInThread(p.getErrorStream(), err);
            p.waitFor();
            String e = new String(err.toByteArray(), "UTF-8");
            if (e.length() > 0) {
                throw new RuntimeException(e);
            }
            String output = new String(out.toByteArray(), "UTF-8");
            return output;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void copyInThread(final InputStream in, final OutputStream out) {
        new Thread() {
            public void run() {
                copy(in, out);
            }
        }.run();
    }

    public static long copy(InputStream in, OutputStream out) {
        try {
            long copied = 0;
            int len = 4096;
            byte[] buffer = new byte[len];
            while (true) {
                len = in.read(buffer, 0, buffer.length);
                if (len < 0) {
                    break;
                }
                if (out != null) {
                    out.write(buffer, 0, len);
                }
                copied += len;
            }
            return copied;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
        List<String[]> list = readAllStackTraces(pid);
        for (String[] dump : list) {
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
                    buff.append("at ").append(el).append(LINE_SEPARATOR);
                    if (!packageCounts && !startsWithAny(el, ignorePackages)) {
                        packageCounts = true;
                        int index = 0;
                        for (; index < el.length(); index++) {
                            char c = el.charAt(index);
                            if (c == '(' || Character.isUpperCase(c)) {
                                break;
                            }
                        }
                        if (index > 0 && el.charAt(index - 1) == '.') {
                            index--;
                        }
                        if (sumClasses) {
                            int m = el.indexOf('.', index + 1);
                            index = m >= 0 ? m : index;
                        }
                        String groupName = el.substring(0, index);
                        increment(summary, groupName, 0);
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
        // stopCollecting();
        long time = System.currentTimeMillis() - start;
        StringBuilder buff = new StringBuilder();
        buff.append("Profiler: top ").append(count).append(" stack trace(s) of ").append(time).
            append(" ms:").append(LINE_SEPARATOR);
        if (counts.size() == 0) {
            buff.append("(none)").append(LINE_SEPARATOR);
        }
        HashMap<String, Integer> copy = new HashMap<String, Integer>(counts);
        appendTop(buff, copy, count, total, false);
        buff.append("summary:").append(LINE_SEPARATOR);
        copy = new HashMap<String, Integer>(summary);
        appendTop(buff, copy, count, total, true);
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
                        append(LINE_SEPARATOR);
                }
            } else {
                buff.append(c).append('/').append(total).append(" (").
                    append(percent).
                    append("%):").append(LINE_SEPARATOR).
                    append(best.getKey()).
                    append(LINE_SEPARATOR);
            }
        }
    }

}
