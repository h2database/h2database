/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.java;

/**
 * This annotation marks fields that are not shared and therefore don't need to
 * be garbage collected separately.
 */
public @interface Local {
    // empty
}
