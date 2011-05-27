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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;

/**
 * Represents a statement (a single line in the log file).
 *
 * @author Thomas Mueller
 *
 */
class Statement {
    private Player player;

    private boolean assignment;

    private String assignClass;

    private String assignVariable;

    private boolean staticCall;

    private String staticCallClass;

    private String objectName;

    private Object object;

    private String methodName;

    private Arg[] args;

    private Class[] parameterTypes;

    private Object[] parameters;

    private Method method;

    private Class returnClass;

    private Object returnObject;

    Statement(Player player) {
        this.player = player;
    }

    private Method findMethod(Class clazz) throws Exception {
        if ((clazz.getModifiers() & Modifier.PUBLIC) == 0) {
            // http://forum.java.sun.com/thread.jspa?threadID=704100&messageID=4084720
            // bug 4071957
            Class[] interfaces = clazz.getInterfaces();
            for (int i = 0; i < interfaces.length; i++) {
                Class c = interfaces[i];
                if (c.getName().startsWith("javax.")) {
                    try {
                        return c.getMethod(methodName, parameterTypes);
                    } catch (Exception e) {
                        // TODO this is slow, but a workaround for a JVM bug
                    }
                }
            }
        }
        try {
            return clazz.getMethod(methodName, parameterTypes);
        } catch (NoSuchMethodException e) {
            Method[] methods = clazz.getMethods();
            methods:
            for (int i = 0; i < methods.length; i++) {
                Method m = methods[i];
                if (methodName.equals(m.getName())) {
                    Class[] argClasses = m.getParameterTypes();
                    for (int j = 0; j < args.length; j++) {
                        if (!argClasses[j].isAssignableFrom(args[j].getValueClass())) {
                            continue methods;
                        }
                    }
                    return m;
                }
            }
        }
        throw new Error("Method with args not found: " + clazz.getName() + "."
                + methodName + " args: " + args.length);
    }

    void execute() throws Exception {
        if (object == player) {
            // there was an exception previously
            player.log("> " + assignVariable + " not set");
            if (assignment) {
                player.assign(assignVariable, player);
            }
            return;
        }
        Class clazz;
        if (staticCall) {
            if (staticCallClass == null || staticCallClass.length() == 0) {
                player.log("?class? " + staticCallClass);
            }
            clazz = Player.getClass(staticCallClass);
        } else {
            clazz = object.getClass();
        }
        parameterTypes = new Class[args.length];
        parameters = new Object[args.length];
        for (int i = 0; i < args.length; i++) {
            Arg arg = args[i];
            arg.execute();
            parameterTypes[i] = arg.getValueClass();
            parameters[i] = arg.getValue();
        }
        method = findMethod(clazz);
        returnClass = method.getReturnType();
        try {
            Object obj = method.invoke(object, parameters);
            if (assignment) {
                player.assign(assignVariable, obj);
            }
            returnObject = obj;
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            Throwable t = e.getTargetException();
            player.log("> " + t.toString());
            if (assignment) {
                player.assign(assignVariable, player);
            }
        }
    }

    public String toString() {
        StringBuffer buff = new StringBuffer();
        if (assignment) {
            buff.append(assignClass);
            buff.append(' ');
            buff.append(assignVariable);
            buff.append('=');
        }
        if (staticCall) {
            buff.append(staticCallClass);
        } else {
            buff.append(objectName);
        }
        buff.append('.');
        buff.append(methodName);
        buff.append('(');
        for (int i = 0; args != null && i < args.length; i++) {
            if (i > 0) {
                buff.append(", ");
            }
            buff.append(args[i].toString());
        }
        buff.append(");");
        return buff.toString();
    }

    Class getReturnClass() {
        return returnClass;
    }

    Object getReturnObject() {
        return returnObject;
    }

    void setAssign(String className, String variableName) {
        this.assignment = true;
        this.assignClass = className;
        this.assignVariable = variableName;
    }

    void setStaticCall(String className) {
        this.staticCall = true;
        this.staticCallClass = className;
    }

    void setMethodCall(String objectName, Object object, String methodName) {
        this.objectName = objectName;
        this.object = object;
        this.methodName = methodName;
    }

    public void setArgs(ArrayList list) {
        args = new Arg[list.size()];
        list.toArray(args);
    }
}
