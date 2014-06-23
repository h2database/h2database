/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.android;

/**
 * This exception is thrown when the database file is full and can't grow.
 */
public class H2FullException extends H2Exception {
    private static final long serialVersionUID = 1L;

    H2FullException() {
        super();
    }

    H2FullException(String error) {
        super(error);
    }
}
