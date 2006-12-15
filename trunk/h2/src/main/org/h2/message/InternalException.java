/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.message;

public class InternalException extends RuntimeException {

    private static final long serialVersionUID = -5369631382082604330L;
    private Exception cause;

    public InternalException(Exception e) {
        cause = e;
    }
    
    public Exception getOriginalCause() {
        return cause;
    }
}
