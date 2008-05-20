/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.message;

import org.h2.constant.SysProperties;
import org.h2.util.StringUtils;

/**
 * This class represents a trace module.
 */
public class Trace {

    private TraceWriter traceWriter;
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

    Trace(TraceWriter traceWriter, String module) {
        this.traceWriter = traceWriter;
        this.module = module;
        this.lineSeparator = SysProperties.LINE_SEPARATOR;
    }

    public boolean isInfoEnabled() {
        return traceWriter.isEnabled(TraceSystem.INFO);
    }

    public boolean isDebugEnabled() {
        return traceWriter.isEnabled(TraceSystem.DEBUG);
    }

    public void error(String s) {
        traceWriter.write(TraceSystem.ERROR, module, s, null);
    }

    public void error(String s, Throwable t) {
        traceWriter.write(TraceSystem.ERROR, module, s, t);
    }

    public void info(String s) {
        traceWriter.write(TraceSystem.INFO, module, s, null);
    }

    public void debugCode(String java) {
        traceWriter.write(TraceSystem.DEBUG, module, lineSeparator + "/**/" + java, null);
    }

    public void infoCode(String java) {
        traceWriter.write(TraceSystem.INFO, module, lineSeparator + "/**/" + java, null);
    }

    public void infoSQL(String sql, String params, int count, long time) {
        StringBuffer buff = new StringBuffer(sql.length() + 20);
        buff.append(lineSeparator);
        buff.append("/*SQL");
        boolean space = false;
        if (params.length() > 0) {
            // This looks like a bug, but it is intentional:
            // If there are no parameters, the SQL statement is 
            // the rest of the line. If there are parameters, they
            // are appended at the end of the line. Knowing the size 
            // of the statement simplifies separating the SQL statement
            // from the parameters (no need to parse).
            space = true;
            buff.append(" l:");
            buff.append(sql.length());
        }
        if (count > 0) {
            space = true;
            buff.append(" #:");
            buff.append(count);
        }
        if (time > 0) {
            space = true;
            buff.append(" t:");
            buff.append(time);
        }
        if (!space) {
            buff.append(' ');
        }
        buff.append("*/");
        buff.append(StringUtils.javaEncode(sql));
        buff.append(params);
        buff.append(';');
        sql = buff.toString();
        traceWriter.write(TraceSystem.INFO, module, sql, null);
    }

    public void debug(String s) {
        traceWriter.write(TraceSystem.DEBUG, module, s, null);
    }

    public void debug(String s, Throwable t) {
        traceWriter.write(TraceSystem.DEBUG, module, s, t);
    }

}
