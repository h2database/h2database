/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.android;

/**
 * A class that implements this interface can listen to transaction begin and
 * end events.
 */
public interface H2TransactionListener {

    /**
     * The transaction has been started.
     */
    void onBegin();

    /**
     * The transaction will be committed.
     */
    void onCommit();

    /**
     * The transaction will be rolled back.
     */
    void onRollback();
}
