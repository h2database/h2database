/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;

import org.h2.message.Message;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;
import org.h2.value.ValueLong;

public class RangeCursor implements Cursor {

    private boolean beforeFirst;
    private long current;
    private Row currentRow;
    private long min, max;
    
    RangeCursor(long min, long max) {
        this.min = min;
        this.max = max;
        beforeFirst = true;
    }
    
    public Row get() {
        return currentRow;
    }
    
    public SearchRow getSearchRow() throws SQLException {
        return currentRow;
    }

    public int getPos() {
        throw Message.getInternalError();
    }

    public boolean next() throws SQLException {
        if (beforeFirst) {
            beforeFirst = false;
            current = min;
        } else {
            current++;
        }
        currentRow = new Row(new Value[]{ValueLong.get(current)}, 0);
        return current <= max;
    }

}
