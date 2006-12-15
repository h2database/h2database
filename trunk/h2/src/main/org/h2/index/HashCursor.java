/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import org.h2.result.Row;

/**
 * @author Thomas
 */
public class HashCursor implements Cursor {
    private Row row;
    private boolean end;

    HashCursor(Row row) {
        this.row = row;
    }

    public Row get() {
        return row;
    }
    
    public int getPos() {
        return row == null ? -1 : row.getPos();
    }

    public boolean next() {
        if(row==null || end) {
            row = null;
            return false;
        }
        end = true;
        return true;
    }
}
