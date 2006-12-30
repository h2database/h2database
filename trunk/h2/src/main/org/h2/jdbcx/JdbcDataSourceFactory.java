/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbcx;

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;

import org.h2.engine.Constants;
import org.h2.message.Trace;
import org.h2.message.TraceSystem;

public class JdbcDataSourceFactory implements ObjectFactory {
    
    private static TraceSystem traceSystem;
    private Trace trace;
    
    static {
        traceSystem = new TraceSystem(Constants.CLIENT_TRACE_DIRECTORY + "h2datasource" + Constants.SUFFIX_TRACE_FILE);
        traceSystem.setLevelFile(TraceSystem.DEBUG);
    }
    
    public JdbcDataSourceFactory() {
        trace = traceSystem.getTrace("JDBCX");
    }
    
    public synchronized Object getObjectInstance(Object obj, Name name, Context nameCtx, Hashtable environment) throws Exception {
        trace.debug("getObjectInstance obj=" + obj + " name=" + name + " nameCtx=" + nameCtx + " environment=" + environment);
        Reference ref     = (Reference) obj;
        if (ref.getClassName().equals(JdbcDataSource.class.getName())) {
            JdbcDataSource dataSource = new JdbcDataSource();
            dataSource.setURL((String) ref.get("url").getContent());
            dataSource.setUser((String) ref.get("user").getContent());
            dataSource.setPassword((String) ref.get("password").getContent());
            return dataSource;
        }
        return null;
    }
    
    TraceSystem getTraceSystem() {
        return traceSystem;
    }
    
    Trace getTrace() {
        return trace;
    }

}
