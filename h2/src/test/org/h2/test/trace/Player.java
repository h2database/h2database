/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 */
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.h2.test.trace;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.HashMap;

/**
 * The command line tool to re-run the log file. This is done using reflection.
 *
 * @author Thomas Mueller
 *
 */
public class Player {

    private boolean log;

    private static final String[] IMPORTED_PACKAGES = { "", "java.lang.",
            "java.sql." };

    private HashMap objects = new HashMap();
    private String lastReturn;
    private boolean checkResults;

    /**
     * Execute a trace file using the command line. The log file name to execute (replayed) must be specified
     * as the last parameter. The following optional command line parameters are supported:
     * <ul>
     * <li><code>-log</code> to enable logging the executed statement to System.out
     * <li><code>-checkResults</code> if this is set, the values returned at runtime are compared with the values in the log file
     * </ul>
     *
     * @param args the arguments of the application
     */
    public static void main(String[] args) throws Exception {
        int todoTest;
        new Player().run(args);
    }

    /**
     * Execute a trace file.
     *
     * @param fileName
     * @param log print debug information
     * @param checkResult if the result of each method should be compared against the result in the file
     */
    public static void execute(String fileName, boolean log, boolean checkResult) throws IOException {
        new Player().runFile(fileName, log);
    }

    private void run(String[] args) throws IOException {
        String fileName;
        try {
            fileName = args[args.length - 1];
            for (int i = 0; i < args.length - 1; i++) {
                if ("-log".equals(args[i])) {
                    log = true;
                } else if ("-checkResults".equals(args[i])) {
                    checkResults = true;
                } else {
                    throw new Error("Unknown setting: " + args[i]);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Usage: java " + getClass().getName()
                    + " [-log] [-checkResult] <fileName>");
            return;
        }
        runFile(fileName, log);
    }

    private void runFile(String fileName, boolean log) throws IOException {
        this.log = log;
        LineNumberReader reader = new LineNumberReader(new BufferedReader(
                new FileReader(fileName)));
        while (true) {
            String line = reader.readLine();
            if (line == null) {
                break;
            }
            runLine(line.trim());
        }
    }

    void log(String s) {
        if (log) {
            System.out.println(s);
        }
    }

    void logError(String s) {
        System.out.println("ERROR: " + s);
    }

    private void runLine(String line) {
        if (line.startsWith("//return")) {
            if (checkResults && lastReturn != null) {
                int start = line.indexOf(' ');
                int end = line.lastIndexOf(';');
                if (start >= 0 && end > start) {
                    String expected = line.substring(start, end).trim();
                    if (lastReturn.equals(expected)) {
                        logError("expected: " + expected + " got: " + lastReturn);
                    }
                }
            }
        }
        if (!line.startsWith("/**/")) {
            return;
        }
        line = line.substring("/**/".length()) + ";";
        Statement s = Parser.parseStatement(this, line);
        log("> " + s.toString());
        try {
            s.execute();
            Object result = s.getReturnObject();
            lastReturn = null;
            if (result == null) {
                lastReturn = "null";
            } else if (result instanceof String) {
                String r = (String) result;
                lastReturn = r;
            } else {
                lastReturn = quoteSimple(result);
            }
            if (lastReturn != null) {
                log(">     return " + lastReturn + ";");
            }
        } catch (Exception e) {
            e.printStackTrace();
            log("error: " + e.toString());
        }
    }

    static Class getClass(String className) {
        for (int i = 0; i < IMPORTED_PACKAGES.length; i++) {
            try {
                return Class.forName(IMPORTED_PACKAGES[i] + className);
            } catch (ClassNotFoundException e) {
            }
        }
        throw new Error("Class not found: " + className);
    }

    void assign(String objectName, Object obj) {
        objects.put(objectName, obj);
    }

    Object getObject(String name) {
        return objects.get(name);
    }
    
    private String quoteSimple(Object result) {
        // TODO Auto-generated method stub
        return null;
    }

}
