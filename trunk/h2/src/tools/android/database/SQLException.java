/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package android.database;

/**
 * A database exception.
 */
public class SQLException extends Exception {

    private static final long serialVersionUID = 1L;

    public SQLException() {
        super();
    }

    public SQLException(String error) {
        super(error);
    }

}
