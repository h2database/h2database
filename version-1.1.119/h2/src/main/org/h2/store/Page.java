/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.sql.SQLException;
import org.h2.engine.Session;


/**
 * A page. Format:
 * <ul><li>0-3: parent page id (0 for root)
 * </li><li>4-4: page type
 * </li><li>page-type specific data
 * </li></ul>
 */
public abstract class Page extends Record {

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
     * A data overflow page (the last page: + FLAG_LAST).
     */
    public static final int TYPE_DATA_OVERFLOW = 3;

    /**
     * A b-tree leaf page (without overflow: + FLAG_LAST).
     */
    public static final int TYPE_BTREE_LEAF = 4;

    /**
     * A b-tree node page (never has overflow pages).
     */
    public static final int TYPE_BTREE_NODE = 5;

    /**
     * A page containing a list of free pages (the last page: + FLAG_LAST).
     */
    public static final int TYPE_FREE_LIST = 6;

    /**
     * A stream trunk page.
     */
    public static final int TYPE_STREAM_TRUNK = 7;

    /**
     * A stream data page.
     */
    public static final int TYPE_STREAM_DATA = 8;

    /**
     * Copy the data to a new location, change the parent to point to the new
     * location, and free up the current page.
     *
     * @param session the session
     * @param newPos the new position
     */
    public abstract void moveTo(Session session, int newPos) throws SQLException;

}
