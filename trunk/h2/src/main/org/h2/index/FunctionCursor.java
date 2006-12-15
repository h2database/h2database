/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;

import org.h2.message.Message;
import org.h2.result.LocalResult;
import org.h2.result.Row;

public class FunctionCursor implements Cursor {
    
    private LocalResult result;
    private Row row;    
    
    FunctionCursor(LocalResult result) {
        this.result = result;
    }

    public Row get() {
        return row;
    }
    
    public int getPos() {
        throw Message.getInternalError();
    }

    public boolean next() throws SQLException {
        if(result.next()) {
            row = new Row(result.currentRow());
        } else {
            row = null;
        }
        return row != null;
    }

}
