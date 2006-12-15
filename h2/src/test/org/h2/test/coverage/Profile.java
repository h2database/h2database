/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.coverage;

import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.LineNumberReader;

/**
 * The class used at runtime to measure the code usage and performance.
 */
public class Profile extends Thread {
    public static final boolean LIST_UNVISITED = true;
    public static final boolean FAST = false;
    public static final boolean TRACE = false;
    public static int current;
    public static Profile main = new Profile();
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
            main.addVisit(i);
        }
    }

    public void run() {
        list();
    }

    public static void startCollecting() {
        main.stop = false;
        main.lastTime = System.currentTimeMillis();
    }

    public static void stopCollecting() {
        main.stop = true;
    }

    public static void list() {
        if (main.lastIndex == 0) {
            // don't list anything if no statistics collected
            return;
        }
        try {
            main.listUnvisited();
            main.listTop("MOST CALLED", main.count, top);
            main.listTop("MOST TIME USED", main.time, top);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    Profile() {
        try {
            LineNumberReader r = new LineNumberReader(new FileReader(
                    "profile.txt"));
            while (r.readLine() != null) {
                // nothing
            }
            maxIndex = r.getLineNumber();
            count = new int[maxIndex];
            time = new int[maxIndex];
            lastTime = System.currentTimeMillis();
            Runtime.getRuntime().addShutdownHook(this);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
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
        LineNumberReader r = new LineNumberReader(new FileReader("profile.txt"));
        BufferedWriter writer = new BufferedWriter(new FileWriter("notcovered.txt"));
        int unvisited = 0;
        int unvisitedthrow = 0;
        for (int i = 0; i < maxIndex; i++) {
            String line = r.readLine();
            if (count[i] == 0) {
                if (!line.endsWith("throw")) {
                    writer.write(line + "\r\n");
                    if(LIST_UNVISITED) {
                        print(line+"\r\n");
                    }
                    unvisited++;
                } else {
                    unvisitedthrow++;
                }
            }
        }
        writer.close();
        int percent = (100 * unvisited / maxIndex);
        print("Not covered: " + percent + " % " + " (" + unvisited + " of "
                + maxIndex + "; throw=" + unvisitedthrow + ")");
    }

    void listTop(String title, int[] list, int max) throws Exception {
        printLine('-');
        int total = 0;
        int totallines = 0;
        for (int j = 0; j < maxIndex; j++) {
            int l = list[j];
            if (l > 0) {
                total += list[j];
                totallines++;
            }
        }
        if (max == 0) {
            max = totallines;
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
        LineNumberReader r = new LineNumberReader(new FileReader("profile.txt"));
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

