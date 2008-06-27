/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jaqu;

/**
 * This class provides static methods that represents common SQL functions.
 */
public class Function {
    
    private static final Long COUNT_STAR = new Long(0);

    public static Long countStar() {
        return COUNT_STAR;
    }

}
