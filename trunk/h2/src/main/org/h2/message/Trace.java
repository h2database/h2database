/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.message;

import org.h2.constant.SysProperties;

/**
 * This class represents a trace module.
 */
public class Trace {

    // TODO trace: java code generation does not always work

    private TraceSystem traceSystem;
    private String module;
    private String lineSeparator;

    public static final String LOCK = "lock";
    public static final String SETTING = "setting";
    public static final String COMMAND = "command";
    public static final String INDEX = "index";
    public static final String SEQUENCE = "sequence";
    public static final String CONSTRAINT = "constraint";
    public static final String USER = "user";
    public static final String TRIGGER = "trigger";
    public static final String FUNCTION = "function";
    public static final String JDBC = "jdbc";
    public static final String FILE_LOCK = "fileLock";
    public static final String TABLE = "table";
    public static final String LOG = "log";
    public static final String SCHEMA = "schema";
    public static final String DATABASE = "database";
    public static final String SESSION = "session";
    public static final String AGGREGATE = "aggregate";

    public Trace(TraceSystem traceSystem, String module) {
        this.traceSystem = traceSystem;
        this.module = module;
        this.lineSeparator = SysProperties.LINE_SEPARATOR;
    }

    public boolean info() {
        return traceSystem.isEnabled(TraceSystem.INFO);
    }

    public boolean debug() {
        return traceSystem.isEnabled(TraceSystem.DEBUG);
    }

    public void error(String s) {
        traceSystem.write(TraceSystem.ERROR, module, s, null);
    }

    public void error(String s, Throwable t) {
        traceSystem.write(TraceSystem.ERROR, module, s, t);
    }

    public void info(String s) {
        traceSystem.write(TraceSystem.INFO, module, s, null);
    }

    public void debugCode(String java) {
        traceSystem.write(TraceSystem.DEBUG, module, lineSeparator + "/**/" + java, null);
    }

    public void infoCode(String java) {
        traceSystem.write(TraceSystem.INFO, module, lineSeparator + "/**/" + java, null);
    }

    public void infoSQL(String sql) {
        sql = replaceNewline(sql);
        if (sql.startsWith("/*")) {
            sql = sql.substring(sql.indexOf("*/") + 2).trim();
        }
        traceSystem.write(TraceSystem.INFO, module, lineSeparator + "/*SQL*/" + sql, null);
    }

    private String replaceNewline(String s) {
        if (s.indexOf('\r') < 0 && s.indexOf('\n') < 0) {
            return s;
        }
        StringBuffer buff = new StringBuffer(s.length());
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            if (ch == '\r' || ch == '\n') {
                ch = ' ';
            }
            buff.append(ch);
        }
        return buff.toString();
    }

    public void debug(String s) {
        traceSystem.write(TraceSystem.DEBUG, module, s, null);
    }

    public void debug(String s, Throwable t) {
        traceSystem.write(TraceSystem.DEBUG, module, s, t);
    }

}
