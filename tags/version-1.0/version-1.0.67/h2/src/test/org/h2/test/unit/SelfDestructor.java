/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.sql.Timestamp;

/**
 * This is a self-destructor class to kill a long running process automatically after
 * a pre-defined time. The class reads the number of minutes
 * from the system property 'h2.selfDestruct' and starts a countdown thread
 * to kill the virtual machine if it still runs then.
 */
public class SelfDestructor extends Thread {
    private static final String PROPERTY_NAME = "h2.selfDestruct";

    /**
     * Start the countdown. If the self-destruct system property is set, this value is used,
     * otherwise the given default value is used.
     *
     * @param defaultMinutes the default number of minutes after which the current process is killed.
     */
    public static void startCountdown(int defaultMinutes) {
        final int minutes = Integer.parseInt(System.getProperty(PROPERTY_NAME, "" + defaultMinutes));
        if (minutes != 0) {
            Thread thread = new Thread() {
                public void run() {
                    for (int i = minutes; i >= 0; i--) {
                        setName("SelfDestructor " + i + " min");
                        try {
                            Thread.sleep(60 * 1000);
                        } catch (InterruptedException e) {
                            // ignore
                        }
                    }
                    String time = new Timestamp(System.currentTimeMillis()).toString();
                    System.out.println(time + " Killing the process after " + minutes + " minutes");
                    Runtime.getRuntime().halt(1);
                }
            };
            thread.setDaemon(true);
            thread.start();
        }
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
