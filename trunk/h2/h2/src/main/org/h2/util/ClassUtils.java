/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

public class ClassUtils {
    
    public static Class loadClass(String className) throws ClassNotFoundException {
        // TODO support special syntax to load classes using another classloader
        return Class.forName(className);
    }

}
