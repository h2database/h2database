/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;

import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.message.Message;

/**
 * This utility class contains functions related to class loading.
 * There is a mechanism to restrict class loading.
 */
public class ClassUtils {

    private static final boolean ALLOW_ALL;
    private static final HashSet<String> ALLOWED_CLASS_NAMES = New.hashSet();
    private static final String[] ALLOWED_CLASS_NAME_PREFIXES;

    static {
        String s = SysProperties.ALLOWED_CLASSES;
        ArrayList<String> prefixes = New.arrayList();
        boolean allowAll = false;
        for (String p : StringUtils.arraySplit(s, ',', true)) {
            if (p.equals("*")) {
                allowAll = true;
            } else if (p.endsWith("*")) {
                prefixes.add(p.substring(0, p.length() - 1));
            } else {
                ALLOWED_CLASS_NAMES.add(p);
            }
        }
        ALLOW_ALL = allowAll;
        ALLOWED_CLASS_NAME_PREFIXES = new String[prefixes.size()];
        prefixes.toArray(ALLOWED_CLASS_NAME_PREFIXES);
    }

    private ClassUtils() {
        // utility class
    }

    /**
     * Load a class without performing access rights checking.
     *
     * @param className the name of the class
     * @return the class object
     */
    public static Class< ? > loadSystemClass(String className) throws ClassNotFoundException {
        return Class.forName(className);
    }

    /**
     * Load a class, but check if it is allowed to load this class first. To
     * perform access rights checking, the system property h2.allowedClasses
     * needs to be set to a list of class file name prefixes.
     *
     * @param className the name of the class
     * @return the class object
     */
    public static Class< ? > loadUserClass(String className) throws SQLException {
        if (!ALLOW_ALL && !ALLOWED_CLASS_NAMES.contains(className)) {
            boolean allowed = false;
            for (String s : ALLOWED_CLASS_NAME_PREFIXES) {
                if (className.startsWith(s)) {
                    allowed = true;
                }
            }
            if (!allowed) {
                throw Message.getSQLException(ErrorCode.ACCESS_DENIED_TO_CLASS_1, className);
            }
        }
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw Message.getSQLException(ErrorCode.CLASS_NOT_FOUND_1, e, className);
        } catch (NoClassDefFoundError e) {
            throw Message.getSQLException(ErrorCode.CLASS_NOT_FOUND_1, e, className);
        }
    }

    /**
     * Checks if the given method takes a variable number of arguments. For Java
     * 1.4 and older, false is returned. Example:
     * <pre>
     * public static double mean(double... values)
     * </pre>
     *
     * @param m the method to test
     * @return true if the method takes a variable number of arguments.
     */
    public static boolean isVarArgs(Method m) {
        if ("1.5".compareTo(SysProperties.JAVA_SPECIFICATION_VERSION) > 0) {
            return false;
        }
        try {
            Method isVarArgs = m.getClass().getMethod("isVarArgs");
            Boolean result = (Boolean) isVarArgs.invoke(m);
            return result.booleanValue();
        } catch (Exception e) {
            return false;
        }
    }

}
