/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;


/**
 * A page store index.
 */
public abstract class PageIndex extends BaseIndex {

    /**
     * The root page of this index.
     */
    protected int rootPageId;

    private boolean sortedInsertMode;

    public int getRootPageId() {
        return rootPageId;
    }

    /**
     * Write back the row count if it has changed.
     */
    public abstract void writeRowCount();

    public void setSortedInsertMode(boolean sortedInsertMode) {
        this.sortedInsertMode = sortedInsertMode;
    }

    boolean isSortedInsertMode() {
        return sortedInsertMode;
    }

}
