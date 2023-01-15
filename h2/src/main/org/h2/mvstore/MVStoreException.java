/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

/**
 * Various kinds of MVStore problems, along with associated error code.
 */
public class MVStoreException extends RuntimeException {

    private static final long serialVersionUID = 2847042930249663807L;

    private final int errorCode;

    public MVStoreException(int errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public int getErrorCode() {
        return errorCode;
    }
}
