/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;

import org.h2.message.Message;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.util.ObjectArray;


/**
 * @author Thomas
 */

public class MetaCursor implements Cursor {

    private Row current;
    private ObjectArray rows;
    private int index;
    
    MetaCursor(ObjectArray rows) {
        this.rows = rows;
    }
    
    public Row get() {
        return current;
    }
    
	public SearchRow getSearchRow() throws SQLException {
		return current;
	}
    
    public int getPos() {
        throw Message.getInternalError();
    }

    public boolean next() throws SQLException {
        current =  (Row) (index >= rows.size() ? null : rows.get(index++));
        return current != null;
    }

}
