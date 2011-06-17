/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.message;

/**
 * This exception wraps a checked exception.
 * It is used in methods where checked exceptions are not supported,
 * for example in a Comparator.
 */
public class InternalException extends RuntimeException {

    private static final long serialVersionUID = -5369631382082604330L;
    private Exception cause;

    public InternalException(Exception e) {
        super(e.getMessage());
        cause = e;
    }

    public Exception getOriginalCause() {
        return cause;
    }
}
