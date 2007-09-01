/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.coverage;

import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.LineNumberReader;

import org.h2.util.IOUtils;

/**
 * The class used at runtime to measure the code usage and performance.
 */
public class Profile extends Thread {
    public static final boolean LIST_UNVISITED = true;
    public static final boolean FAST = false;
    public static final boolean TRACE = false;
    public static final Profile MAIN = new Profile();
    public static int current;
    private BufferedWriter trace;
    public int[] count;
    public int[] time;
    boolean stop;
    int maxIndex;
    int lastIndex;
    long lastTime;
    static int top = 15;

    static {
        try {
            String s = System.getProperty("profile.top");
            if (s != null) {
                top = Integer.parseInt(s);
            }
        } catch (Throwable e) {
            // ignore SecurityExceptions
        }
    }

    public static void visit(int i) {
        if (FAST) {
            current = i;
        } else {
            MAIN.addVisit(i);
        }
    }

    public void run() {
        list();
    }

    public static void startCollecting() {
        MAIN.stop = false;
        MAIN.lastTime = System.currentTimeMillis();
    }

    public static void stopCollecting() {
        MAIN.stop = true;
    }

    public static void list() {
        if (MAIN.lastIndex == 0) {
            // don't list anything if no statistics collected
            return;
        }
        try {
            MAIN.listUnvisited();
            MAIN.listTop("MOST CALLED", MAIN.count, top);
            MAIN.listTop("MOST TIME USED", MAIN.time, top);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    Profile() {
        FileReader reader = null;
        try {
            reader = new FileReader("profile.txt");
            LineNumberReader r = new LineNumberReader(reader);
            while (r.readLine() != null) {
                // nothing - just count lines
            }
            maxIndex = r.getLineNumber();
            count = new int[maxIndex];
            time = new int[maxIndex];
            lastTime = System.currentTimeMillis();
            Runtime.getRuntime().addShutdownHook(this);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            IOUtils.closeSilently(reader);
        }
    }

    void addVisit(int i) {
        if (stop) {
            return;
        }
        long now = System.currentTimeMillis();
        if (TRACE && trace != null) {
            int duration = (int) (now - lastTime);
            try {
                trace.write(i + "\t" + duration + "\r\n");
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
        count[i]++;
        time[lastIndex] += (int) (now - lastTime);
        lastTime = now;
        lastIndex = i;
    }

    void listUnvisited() throws Exception {
        printLine('=');
        print("NOT COVERED");
        printLine('-');
        FileReader reader = null;
        FileWriter fileWriter = null;
        try {
            reader = new FileReader("profile.txt");
            LineNumberReader r = new LineNumberReader(reader);
            fileWriter = new FileWriter("notCovered.txt");
            BufferedWriter writer = new BufferedWriter(fileWriter);
            int unvisited = 0;
            int unvisitedThrow = 0;
            for (int i = 0; i < maxIndex; i++) {
                String line = r.readLine();
                if (count[i] == 0) {
                    if (!line.endsWith("throw")) {
                        writer.write(line + "\r\n");
                        if (LIST_UNVISITED) {
                            print(line + "\r\n");
                        }
                        unvisited++;
                    } else {
                        unvisitedThrow++;
                    }
                }
            }
            int percent = (100 * unvisited / maxIndex);
            print("Not covered: " + percent + " % " + " (" + unvisited + " of " + maxIndex + "; throw="
                    + unvisitedThrow + ")");
        } finally {
            IOUtils.closeSilently(fileWriter);
            IOUtils.closeSilently(reader);
        }
    }

    void listTop(String title, int[] list, int max) throws Exception {
        printLine('-');
        int total = 0;
        int totalLines = 0;
        for (int j = 0; j < maxIndex; j++) {
            int l = list[j];
            if (l > 0) {
                total += list[j];
                totalLines++;
            }
        }
        if (max == 0) {
            max = totalLines;
        }
        print(title);
        print("Total: " + total);
        printLine('-');
        String[] text = new String[max];
        int[] index = new int[max];
        for (int i = 0; i < max; i++) {
            int big = list[0];
            int bigIndex = 0;
            for (int j = 1; j < maxIndex; j++) {
                int l = list[j];
                if (l > big) {
                    big = l;
                    bigIndex = j;
                }
            }
            list[bigIndex] = -(big + 1);
            index[i] = bigIndex;
        }
        FileReader reader = null;
        try {
            reader = new FileReader("profile.txt");
            LineNumberReader r = new LineNumberReader(reader);
            for (int i = 0; i < maxIndex; i++) {
                String line = r.readLine();
                int k = list[i];
                if (k < 0) {
                    k = -(k + 1);
                    list[i] = k;
                    for (int j = 0; j < max; j++) {
                        if (index[j] == i) {
                            int percent = (100 * k / total);
                            text[j] = k + " " + percent + "%: " + line;
                        }
                    }
                }
            }
            for (int i = 0; i < max; i++) {
                print(text[i]);
            }
        } finally {
            IOUtils.closeSilently(reader);
        }
    }

    void print(String s) {
        System.out.println(s);
    }

    void printLine(char c) {
        for (int i = 0; i < 60; i++) {
            System.out.print(c);
        }
        print("");
    }
}
