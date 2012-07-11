/*
 * Copyright 2012 H2 Group. Multiple-Licensed under the H2 License, Version 1.0,
 * and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.ext;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.Collections;
import org.h2.command.ddl.CreateAggregate;
import org.h2.command.ddl.CreateFunctionAlias;
import org.h2.engine.Session;
import org.h2.message.Trace;
import org.h2.schema.Schema;

/**
 * Implements auto-registration of FUNCTION ALIAS's at database open time from
 * specially prepared jars.
 */
public final class AutoRegisterFunctionAliases {

    /** Not meant to be instantiated */
    private AutoRegisterFunctionAliases() {
    }

    public static final String META_INF_NAME = "/META-INF/org.h2.ext.FunctionRegistry";
    public static final String SYS_PROP_AUTOREG = "h2.ext.fnAutoreg";
    public static final String JDBC_PROP_AUTOREG = "extFnAutoreg";

    private static final void registerFunction(Trace trace, Session session, Schema schema, String clazz,
            String method, String alias) {
        CreateFunctionAlias cfn = new CreateFunctionAlias(session, schema);
        cfn.setIfNotExists(true);
        cfn.setJavaClassMethod(clazz + "." + method);
        cfn.setAliasName(alias.toUpperCase());
        cfn.setDeterministic(true);
        try {
            cfn.update();
            trace.info("registered function alias {0} of {1}/{2}", alias.toUpperCase(), clazz, method);
        } catch (Exception ex) {
            trace.error(ex, "registration error: function {0} of {1}/{2}", alias.toUpperCase(), clazz, method);
        }
    }

    private static final void registerAggregate(Trace trace, Session session, Schema schema, String clazz, String alias) {
        CreateAggregate ca = new CreateAggregate(session);
        ca.setIfNotExists(true);
        ca.setJavaClassMethod(clazz);
        ca.setName(alias.toUpperCase());
        ca.setSchema(schema);
        try {
            ca.update();
            trace.info("registered aggregate {0} of {1}", alias.toUpperCase(), clazz);
        } catch (Exception ex) {
            trace.error(ex, "registration error: aggregate {0} of {1}", alias.toUpperCase(), clazz);
        }
    }

    private static final void register(Trace trace, ClassLoader cl, Session session, Schema schema, String className)
            throws ClassNotFoundException {
        String alias = null;
        className = className.trim();

        if (className.lastIndexOf(' ') > 0) {
            alias = className.substring(className.lastIndexOf(' ') + 1).toUpperCase();
            className = className.substring(0, className.lastIndexOf(' ')).trim();
        } else {
            alias = className.substring(className.lastIndexOf('.') + 1).toUpperCase();
        }

        Class clazz = cl.loadClass(className);

        for (Class x : clazz.getInterfaces()) {
            if ("org.h2.api.AggregateFunction".equalsIgnoreCase(x.getCanonicalName())) {
                registerAggregate(trace, session, schema, className, alias);
                return;
            }
        }

        // not an aggregate implementation
        for (Method m : clazz.getDeclaredMethods()) {
            if (m.getName().toUpperCase().startsWith("FN_") && m.isAccessible()
                    && (m.getModifiers() & Modifier.STATIC) == Modifier.STATIC) {
                registerFunction(trace, session, schema, className, m.getName(), m.getName().substring(3));
            }
        }
    }

    public static final void registerFromClasspath(Trace trace, ClassLoader cl, Session session, Schema schema,
            String tag) throws IOException {
        String metaName = META_INF_NAME;

        if (tag != null && !"".equals(tag)) {
            metaName += "." + (tag.toUpperCase());
        }

        for (URL url : Collections.list(cl.getResources(metaName))) {
            trace.info("found function alias registration at {0}", url.toString());
            BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()));
            try {
                String line = null;
                while ((line = in.readLine()) != null) {
                    line = line.trim();
                    if (!"".equals(line)) {
                        trace.info("registration of {0}", line);
                        register(trace, cl, session, schema, line);
                    }
                }
            } catch (Exception ex) {
                trace.error(ex, "error reading from function auto-register file: {0}", url.toString());
            } finally {
                in.close();
            }
        }
    }
}
