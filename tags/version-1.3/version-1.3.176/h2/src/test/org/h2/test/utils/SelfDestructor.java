/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.utils;

import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.util.Map;

/**
 * This is a self-destructor class to kill a long running process automatically
 * after a pre-defined time. The class reads the number of minutes from the
 * system property 'h2.selfDestruct' and starts a countdown thread to kill the
 * virtual machine if it still runs then.
 */
public class SelfDestructor extends Thread {
    private static final String PROPERTY_NAME = "h2.selfDestruct";

    /**
     * Start the countdown. If the self-destruct system property is set, this
     * value is used, otherwise the given default value is used.
     *
     * @param defaultMinutes the default number of minutes after which the
     *            current process is killed.
     */
    public static void startCountdown(int defaultMinutes) {
        final int minutes = Integer.parseInt(
                System.getProperty(PROPERTY_NAME, "" + defaultMinutes));
        if (minutes == 0) {
            return;
        }
        Thread thread = new Thread() {
            @Override
            public void run() {
                for (int i = minutes; i >= 0; i--) {
                    while (true) {
                        try {
                            String name = "SelfDestructor " + i + " min";
                            setName(name);
                            break;
                        } catch (OutOfMemoryError e) {
                            // ignore
                        }
                    }
                    try {
                        Thread.sleep(60 * 1000);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
                try {
                    String time = new Timestamp(
                            System.currentTimeMillis()).toString();
                    System.out.println(time + " Killing the process after " +
                            minutes + " minute(s)");
                    try {
                        Map<Thread, StackTraceElement[]> map =
                                Thread.getAllStackTraces();
                        for (Map.Entry<Thread, StackTraceElement[]> en :
                                map.entrySet()) {
                            System.out.println(en.getKey());
                            for (StackTraceElement el : en.getValue()) {
                                System.out.println("  " + el);
                            }
                        }
                        System.out.println();
                        System.out.flush();
                        try {
                            Thread.sleep(1000);
                        } catch (Exception e) {
                            // ignore
                        }
                        int activeCount = Thread.activeCount();
                        Thread[] threads = new Thread[activeCount + 100];
                        int len = Thread.enumerate(threads);
                        Method stop = Thread.class.getMethod("stop", Throwable.class);
                        for (int i = 0; i < len; i++) {
                            Thread t = threads[i];
                            String threadName = "Thread #" + i + ": " + t.getName();
                            Error e = new Error(threadName);
                            if (t != Thread.currentThread()) {
                                stop.invoke(t, e);
                                t.interrupt();
                            }
                        }
                    } catch (Throwable t) {
                        t.printStackTrace();
                        // ignore
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (Exception e) {
                        // ignore
                    }
                    System.out.println("Killing the process now");
                } catch (Throwable t) {
                    try {
                        t.printStackTrace(System.out);
                    } catch (Throwable t2) {
                        // ignore (out of memory)
                    }
                }
                Runtime.getRuntime().halt(1);
            }
        };
        thread.setDaemon(true);
        thread.start();
    }

    /**
     * Get the string to be added when starting the Java process.
     *
     * @param minutes the countdown time in minutes
     * @return the setting
     */
    public static String getPropertyString(int minutes) {
        return "-D" + PROPERTY_NAME + "=" + minutes;
    }

}
