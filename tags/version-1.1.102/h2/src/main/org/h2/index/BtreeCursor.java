/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;

import org.h2.constant.SysProperties;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.result.Row;
import org.h2.result.SearchRow;

/**
 * The cursor implementation for the b tree index.
 */
public class BtreeCursor implements Cursor {
    private BtreeIndex index;
    private BtreePosition top;
    private SearchRow currentSearchRow;
    private Row currentRow;
    private boolean first;
    private SearchRow last;
    private Session session;

    BtreeCursor(Session session, BtreeIndex index, SearchRow last) {
        this.session = session;
        this.index = index;
        this.last = last;
        first = true;
    }

    Session getSession() {
        return session;
    }

    void setStackPosition(int position) {
        top.position = position;
    }

    /**
     * Put this b-tree position on top of the stack.
     * 
     * @param page the b-tree page to add
     * @param position the index in the row list in this page
     */
    void push(BtreePage page, int position) {
        if (SysProperties.CHECK && (top != null && top.page == page)) {
            throw Message.getInternalError();
        }
        top = new BtreePosition(page, position, top);
    }

    /**
     * Get the next b-tree position from the stack.
     * 
     * @return the next position or null
     */
    BtreePosition pop() {
        BtreePosition t = top;
        if (t == null) {
            return null;
        }
        top = top.next;
        return t;
    }

    void setCurrentRow(SearchRow searchRow) {
        this.currentSearchRow = searchRow;
        currentRow = null;
    }

    public Row get() throws SQLException {
        if (currentRow == null && currentSearchRow != null) {
            currentRow = index.getRow(session, currentSearchRow.getPos());
        }
        return currentRow;
    }

    public SearchRow getSearchRow() {
        return currentSearchRow;
    }

    public int getPos() {
        return currentSearchRow.getPos();
    }

    public boolean next() throws SQLException {
        if (first) {
            first = false;
        } else {
            top.page.next(this, top.position);
            if (currentSearchRow != null && last != null) {
                if (index.compareRows(currentSearchRow, last) > 0) {
                    currentSearchRow = null;
                    currentRow = null;
                }
            }
        }
        return currentSearchRow != null;
    }
    
    public boolean previous() throws SQLException {
        if (currentSearchRow != null) {
            top.page.previous(this, top.position);
        }
        return currentSearchRow != null;
    }
    
}
