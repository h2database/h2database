/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

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
    private static final HashSet ALLOWED_CLASS_NAMES = new HashSet();
    private static final String[] ALLOWED_CLASS_NAME_PREFIXES;

    static {
        String s = SysProperties.ALLOWED_CLASSES;
        String[] list = StringUtils.arraySplit(s, ',', true);
        ArrayList prefixes = new ArrayList();
        boolean allowAll = false;
        for (int i = 0; i < list.length; i++) {
            String p = list[i];
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

    public static Class loadSystemClass(String className) throws ClassNotFoundException {
        return Class.forName(className);
    }

    public static Class loadUserClass(String className) throws ClassNotFoundException, SQLException {
        if (!ALLOW_ALL && !ALLOWED_CLASS_NAMES.contains(className)) {
            boolean allowed = false;
            for (int i = 0; i < ALLOWED_CLASS_NAME_PREFIXES.length; i++) {
                String s = ALLOWED_CLASS_NAME_PREFIXES[i];
                if (className.startsWith(s)) {
                    allowed = true;
                }
            }
            if (!allowed) {
                throw Message.getSQLException(ErrorCode.ACCESS_DENIED_TO_CLASS_1, className);
            }
        }
        return Class.forName(className);
    }

}
