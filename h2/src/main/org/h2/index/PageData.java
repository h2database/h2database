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
 * A page that contains data rows.
 */
abstract class PageData {

    /**
     * The index.
     */
    protected final PageScanIndex index; 
    
    /**
     * The data page.
     */
    protected final DataPageBinary data;
    
    /**
     * the page number.
     */
    protected final int pageId;
    
    /**
     * The page number of the parent.
     */
    protected final int parentPageId;
    
    /**
     * The number of entries.
     */
    protected int entryCount;
    
    /**
     * If the page has unwritten changes.
     */
    protected boolean changed;
    
    PageData(PageScanIndex index, int pageId, int parentPageId, DataPageBinary data) {
        this.index = index;
        this.pageId = pageId;
        this.parentPageId = parentPageId;
        this.data = data;
    }
    
    /**
     * Read the data.
     */
    abstract void read() throws SQLException;

    /**
     * Add a row.
     * 
     * @param row the row
     * @return 0 if successful, or the split position if the page needs to be
     *         split
     */
    abstract int addRow(Row row) throws SQLException;

    /**
     * Get a cursor.
     * 
     * @return the cursor
     */
    abstract Cursor find();

    /**
     * Write the page.
     */
    abstract void write() throws SQLException;
    
}
