/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.store.btree;

/**
 * A position in a cursor
 */
class CursorPos {

    /**
     * The current page.
     */
    Page page;

    /**
     * The current index.
     */
    int index;
}

