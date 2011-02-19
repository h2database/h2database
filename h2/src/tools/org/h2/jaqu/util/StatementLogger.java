/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: James Moger
 */
package org.h2.jaqu.util;

import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Utility class to optionally log generated statements to an output stream.<br>
 * Default output stream is System.out.<br>
 * Statement logging is disabled by default.
 * <p>
 * This class also tracks the counts for generated statements by major type.
 * 
 */
public class StatementLogger {
    
    public static boolean logStatements = false;
    
    public static PrintWriter out = new PrintWriter(System.out);

    public final static AtomicLong selectCount = new AtomicLong(0);
    
    public final static AtomicLong createCount = new AtomicLong(0);
    
    public final static AtomicLong insertCount = new AtomicLong(0);
    
    public final static AtomicLong updateCount = new AtomicLong(0);
    
    public final static AtomicLong mergeCount = new AtomicLong(0);
    
    public final static AtomicLong deleteCount = new AtomicLong(0);

    public static void create(String statement) {
        createCount.incrementAndGet();        
        log(statement);
    }

    public static void insert(String statement) {
        insertCount.incrementAndGet();
        log(statement);
    }

    public static void update(String statement) {
        updateCount.incrementAndGet();
        log(statement);
    }
    
    public static void merge(String statement) {
        mergeCount.incrementAndGet();
        log(statement);
    }

    public static void delete(String statement) {
        deleteCount.incrementAndGet();
        log(statement);
    }
    
    public static void select(String statement) {
        selectCount.incrementAndGet();
        log(statement);
    }
    
    private static void log(String statement) {
        if (logStatements)
            out.println(statement);
    }
    
    public static void printStats() {
        out.println("JaQu Runtime Stats");
        out.println("=======================");
        printStat("CREATE", createCount);
        printStat("INSERT", insertCount);
        printStat("UPDATE", updateCount);
        printStat("MERGE", mergeCount);
        printStat("DELETE", deleteCount);
        printStat("SELECT", selectCount);
    }
    
    private static void printStat(String name, AtomicLong value) {
        if (value.get() > 0) {
            DecimalFormat df = new DecimalFormat("###,###,###,###");
            out.println(name + "=" + df.format(createCount.get()));
        }
    }
}