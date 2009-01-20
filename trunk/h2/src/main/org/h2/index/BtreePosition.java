/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

/**
 * Represents a position of a b-tree index.
 */
class BtreePosition {

    /**
     * The index in the row list.
     */
    int position;

    /**
     * The b-tree page.
     */
    BtreePage page;

    /**
     * The next upper b-tree position.
     */
    BtreePosition next;

    BtreePosition(BtreePage page, int position, BtreePosition next) {
        this.page = page;
        this.position = position;
        this.next = next;
    }
}
