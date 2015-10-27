/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.util.List;
import java.util.concurrent.Future;
import org.h2.result.SearchRow;

/**
 * Support for asynchronous batched lookups in indexes. The flow is the following:
 * H2 engine will be calling {@link #addSearchRows(SearchRow, SearchRow)} until
 * method {@link #isBatchFull()}} will return {@code true} or there are no more
 * search rows to add. Then method {@link #find()} will be called to execute batched lookup.
 * Note that a single instance of {@link IndexLookupBatch} can be reused for multiple 
 * sequential batched lookups.
 * 
 * @see Index#createLookupBatch(TableFilter)
 * @author Sergi Vladykin
 */
public interface IndexLookupBatch {
    /**
     * Add search row pair to the batch.
     * 
     * @param first the first row, or null for no limit
     * @param last the last row, or null for no limit
     * @see Index#find(TableFilter, SearchRow, SearchRow)
     */
    void addSearchRows(SearchRow first, SearchRow last);

    /**
     * Check if this batch is full.
     * 
     * @return {@code true} If batch is full, will not accept any 
     *          more rows and {@link #find()} can be executed.
     */
    boolean isBatchFull();
    
    /**
     * Execute batched lookup and return future cursor for each provided
     * search row pair. Note that this method must return exactly the same number
     * of future cursors in result list as number of {@link #addSearchRows(SearchRow, SearchRow)} 
     * calls has been done before {@link #find()} call exactly in the same order.  
     * 
     * @return List of future cursors for collected search rows.
     */
    List<Future<Cursor>> find();
}
