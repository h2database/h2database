/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;

import org.h2.result.Row;
import org.h2.result.SearchRow;

/**
 * The cursor implementation for a tree index.
 */
public class TreeCursor implements Cursor {
    private TreeIndex tree;
    private TreeNode node;
    private boolean beforeFirst;
    private SearchRow first, last;

    TreeCursor(TreeIndex tree, TreeNode node, SearchRow first, SearchRow last) {
        this.tree = tree;
        this.node = node;
        this.first = first;
        this.last = last;
        beforeFirst = true;
    }

    public Row get() {
        return node == null ? null : node.row;
    }

    public SearchRow getSearchRow() {
        return get();
    }

    public int getPos() {
        return node.row.getPos();
    }

    public boolean next() throws SQLException {
        if (beforeFirst) {
            beforeFirst = false;
            if (node == null) {
                return false;
            }
            if (first != null && tree.compareRows(node.row, first) < 0) {
                node = tree.next(node);
            }
        } else {
            node = tree.next(node);
        }
        if (node != null && last != null) {
            if (tree.compareRows(node.row, last) > 0) {
                node = null;
            }
        }
        return node != null;
    }

    public boolean previous() {
        node = tree.previous(node);
        return node != null;
    }


}
