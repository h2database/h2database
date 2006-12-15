/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.message;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.sql.SQLException;

import org.h2.engine.Constants;
import org.h2.util.StringUtils;

/**
 * 
 * @author tgdmuth6
 *
 */
public class TraceObject {
    public static final int CALLABLE_STATEMENT = 0, CONNECTION = 1, DATABASE_META_DATA = 2,
        PREPARED_STATEMENT = 3, RESULT_SET = 4, RESULT_SET_META_DATA = 5,
        SAVEPOINT = 6, SQL_EXCEPTION = 7, STATEMENT = 8, BLOB = 9, CLOB = 10,
        PARAMETER_META_DATA = 11;
    public static final int DATASOURCE = 12, XA_DATASOURCE = 13, XID = 14;
    
    private static int LAST = XID + 1;  
    private Trace trace;
    private static final int[] ID = new int[LAST];
    private static final String[] PREFIX = {
        "call", "conn", "dbMeta", "prep", "rs", "rsMeta", "sp", "ex", "stat", "blob", "clob", "pMeta",
        "ds", "xads", "xid"
    };
    private int type, id;
    
    protected void setTrace(Trace trace, int type, int id) {
        this.trace = trace;
        this.type = type;
        this.id = id;
    }
    
    protected int getTraceId() {
        return id;
    }
    
    /**
     * INTERNAL
     */
    public String toString() {
        return PREFIX[type] + id ;
    }
    
    protected int getNextId(int type) {
        return ID[type]++;
    }
    
    protected boolean debug() {
        return trace.debug();
    }

    protected Trace getTrace() {
        return trace;
    }
    
    protected void debugCodeAssign(String className, int type, int id) {
        if(!trace.debug()) {
            return;
        }
        trace.debugCode(className + " " + toString() + " = ");
    }
    
    protected void infoCodeAssign(String className, int type, int id) {
        if(!trace.info()) {
            return;
        }
        trace.infoCode(className + " " + toString() + " = ");
    }    
    
    protected void debugCodeCall(String text) {
        if(!trace.debug()) {
            return;
        }
        trace.debugCode(toString() + "." + text + "();");
    }
    
    protected void debugCodeCall(String text, long param) {
        if(!trace.debug()) {
            return;
        }
        trace.debugCode(toString() + "." + text + "("+param+");");
    }
    
    protected void debugCodeCall(String text, String param) {
        if(!trace.debug()) {
            return;
        }
        trace.debugCode(toString() + "." + text + "("+quote(param)+");");
    }    
    
    protected void debugCode(String text) {
        if(!trace.debug()) {
            return;
        }
        trace.debugCode(toString() + "." + text);
    }
    
    protected String quote(String s) {
        return StringUtils.quoteJavaString(s);
    }
    
    protected String quoteTime(java.sql.Time x) {
        if(x == null) {
            return "null";
        }
        return "Time.valueOf(\"" + x.toString() + "\")";
    }

    protected String quoteTimestamp(java.sql.Timestamp x) {
        if(x == null) {
            return "null";
        }
        return "Timestamp.valueOf(\"" + x.toString() + "\")";
    }
    
    protected String quoteDate(java.sql.Date x) {
        if(x == null) {
            return "null";
        }
        return "Date.valueOf(\"" + x.toString() + "\")";
    }    
    
    protected String quoteBigDecimal(BigDecimal x) {
        if(x == null) {
            return "null";
        }
        return "new BigDecimal(\"" + x.toString() + "\")";
    }
    
    protected String quoteBytes(byte[] x) {
        if(x == null) {
            return "null";
        }
        return "new byte[" + x.length + "]";
    }
    
    protected String quoteArray(String[] s) {
        return StringUtils.quoteJavaStringArray(s);
    }
    
    protected String quoteIntArray(int[] s) {
        return StringUtils.quoteJavaIntArray(s);
    }
    
    protected SQLException logAndConvert(Throwable e) {
        if(Constants.LOG_ALL_ERRORS)  {
            synchronized(this.getClass()) {
                // e.printStackTrace();
                try {
                    FileWriter writer = new FileWriter("c:\\temp\\h2error.txt",  true);
                    PrintWriter p = new PrintWriter(writer);
                    e.printStackTrace(p);
                    p.close();
                    writer.close();
                } catch(IOException e2) {
                    e2.printStackTrace();
                }
            }
        }
        if(trace == null) {
            TraceSystem.traceThrowable(e);
        } else {
            if(e instanceof SQLException) {
                trace.error("SQLException", e);
                return (SQLException)e;
            } else {
                trace.error("Uncaught Exception", e);
            }
        }
        return Message.convert(e);
    }
    
}
