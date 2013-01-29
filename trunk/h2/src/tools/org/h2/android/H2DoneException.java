/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.android;

/**
 * This exception is thrown the requested data is not available, for example
 * when calling simpleQueryForString() or simpleQueryForLong() for a statement
 * that doesn't return a value.
 */
public class H2DoneException extends H2Exception {
    private static final long serialVersionUID = 1L;

    H2DoneException() {
        super();
    }

    H2DoneException(String error) {
        super(error);
    }
}
