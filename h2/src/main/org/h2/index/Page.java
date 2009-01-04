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
public class Page {

    /**
     * This is the last page of a chain.
     */
    public static final int FLAG_LAST = 16;

    /**
     * An empty page.
     */
    public static final int TYPE_EMPTY = 0;

    /**
     * A data leaf page (without overflow: + FLAG_LAST).
     */
    public static final int TYPE_DATA_LEAF = 1;

    /**
     * A data node page (never has overflow pages).
     */
    public static final int TYPE_DATA_NODE = 2;

    /**
     * An overflow page (the last page: + FLAG_LAST).
     */
    public static final int TYPE_DATA_OVERFLOW = 3;

    /**
     * A page containing a list of free pages (the last page: + FLAG_LAST).
     */
    public static final int TYPE_FREE_LIST = 4;

    /**
     * A log page.
     */
    public static final int TYPE_LOG = 5;

    /**
     * This is a root page.
     */
    static final int ROOT = 0;

}
