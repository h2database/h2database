/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;

import org.h2.result.Row;
import org.h2.result.SearchRow;


public interface Cursor {
    
    int POS_NO_ROW = -1;

    Row get() throws SQLException;
    SearchRow getSearchRow() throws SQLException;
    int getPos();
    boolean next() throws SQLException;

}
