/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import org.h2.result.Row;

/**
 * Represents a index node of a tree index.
 */
public class TreeNode {
    int balance;
    TreeNode left, right, parent;
    Row row;

    TreeNode(Row row) {
        this.row = row;
    }

    boolean isFromLeft() {
        return parent == null || parent.left == this;
    }

}
