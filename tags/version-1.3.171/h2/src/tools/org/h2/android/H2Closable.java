/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.android;

import android.database.sqlite.SQLiteClosable;

/**
 * An object that can be closed.
 */
public abstract class H2Closable extends SQLiteClosable {

    /**
     * TODO
     */
    public void acquireReference() {
        // TODO
    }

    /**
     * TODO
     */
    public void releaseReference() {
        // TODO
    }

    /**
     * TODO
     */
    public void releaseReferenceFromContainer() {
        // TODO
    }

    /**
     * TODO
     */
    protected abstract void onAllReferencesReleased();

    /**
     * TODO
     */
    protected void onAllReferencesReleasedFromContainer() {
        // TODO
    }

}
