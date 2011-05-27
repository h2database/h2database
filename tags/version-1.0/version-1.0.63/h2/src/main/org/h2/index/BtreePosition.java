/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

/**
 * @author Thomas
 */
class BtreePosition {
    int position;
    BtreePage page;
    BtreePosition next;

    BtreePosition(BtreePage page, int position, BtreePosition next) {
        this.page = page;
        this.position = position;
        this.next = next;
    }
}
