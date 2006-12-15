/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;

import org.h2.engine.Constants;
import org.h2.message.Message;
import org.h2.result.Row;
import org.h2.result.SearchRow;

/**
 * @author Thomas
 */
public class BtreeCursor implements Cursor {
    private BtreeIndex index;
    private BtreePosition top;
    private Row current;
    private boolean first;
    private SearchRow last;

    BtreeCursor(BtreeIndex index, SearchRow last) {
        this.index = index;
        this.last = last;
        first = true;
    }

    void setStackPosition(int position) {
        top.position = position;
    }

    void push(BtreePage page, int position) {
        if (Constants.CHECK && (top != null && top.page == page)) {
            throw Message.getInternalError();
        }
        top = new BtreePosition(page, position, top);
    }

    BtreePosition pop() {
        BtreePosition t = top;
        if (t == null) {
            return null;
        }
        top = top.next;
        return t;
    }

    void setCurrentRow(int pos) throws SQLException {
        current = pos == POS_NO_ROW ? null : index.getRow(pos);
    }
    
    public Row get() throws SQLException {
        return current;
    }

    public int getPos() {
        return current.getPos();
    }

    public boolean next() throws SQLException {
        if (first) {
            first = false;
            return current != null;
        }
        top.page.next(this, top.position);
        if(current != null && last != null) {
            if (index.compareRows(current, last) > 0) {
                current = null;
            }
        }
        return current != null;
    }

}
