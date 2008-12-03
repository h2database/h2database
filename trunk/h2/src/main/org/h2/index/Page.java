/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;


/**
 * A page.
 */
class Page {

    /**
     * An empty page.
     */
    static final int TYPE_EMPTY = 0;

    /**
     * A data leaf page without overflow.
     */
    static final int TYPE_DATA_LEAF = 2;

    /**
     * A data leaf page with overflow.
     */
    static final int TYPE_DATA_LEAF_WITH_OVERFLOW = 3;

    /**
     * A data node page without overflow.
     */
    static final int TYPE_DATA_NODE = 4;

    /**
     * The last overflow page.
     */
    static final int TYPE_DATA_OVERFLOW_LAST = 6;

    /**
     * An overflow pages (more to come).
     */
    static final int TYPE_DATA_OVERFLOW_WITH_MORE = 7;

    /**
     * This is a root page.
     */
    static final int ROOT = 0;

}
