/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;

import org.h2.result.Row;
import org.h2.store.DataPageBinary;

/**
 * A leaf page that contains data of one or multiple rows.
 * Format:
 * <ul><li>0-3: parent page id
 * </li><li>4-4: page type
 * </li><li>5-5: entry count
 * </li><li>6- entries: 4 bytes leaf page id, 4 bytes key
 * </li></ul>
 */
class PageDataNode extends PageData {

    // optimization
    // int childrenEntryCount;
    
    PageDataNode(PageScanIndex index, int pageId, int parentPageId, DataPageBinary data) {
        super(index, pageId, parentPageId, data);
    }

    void read() {
        int todo;
    }

    int addRow(Row row) throws SQLException {
        int todo;
        return 0;
    }

    Cursor find() {
        int todo;
        return null;
    }

    void write() throws SQLException {
        int todo;
    }

}
