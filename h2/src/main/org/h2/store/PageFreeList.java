/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.sql.SQLException;
import org.h2.constant.ErrorCode;
import org.h2.index.Page;
import org.h2.message.Message;
import org.h2.util.IntArray;

/**
 * The list of free pages of a page store.
 * The format of a free list trunk page is:
 * <ul><li>0-3: parent page id (0 for head)
 * </li><li>4-4: page type
 * </li><li>5-8: the next page (if there are more) or number of entries
 * </li><li>9-remainder: data (4 bytes each entry)
 * </li></ul>
 */
public class PageFreeList {

    private final PageStore store;
    private DataPage page;
    private int pageId;
    private int nextPage;
    private IntArray array = new IntArray();

    PageFreeList(PageStore store, int pageId) throws SQLException {
        this.store = store;
        readTrunk(pageId);
        int maybeWorkLikeAStack;
        int alsoReturnTrunkPagesOnceTheyAreEmpty;
    }

    int allocate() throws SQLException {
        while (true) {
            int size = array.size();
            if (size > 0) {
                int x = array.get(size - 1);
                array.remove(size - 1);
                return x;
            }
            if (nextPage == 0) {
                return -1;
            }
            readTrunk(nextPage);
        }
    }

    private void readTrunk(int pageId) throws SQLException {
        if (nextPage == 0) {
            return;
        }
        int parentPage = pageId;
        pageId = nextPage;
        store.readPage(pageId, page);
        int p = page.readInt();
        int t = page.readByte();
        boolean last = (t & Page.FLAG_LAST) != 0;
        t &= ~Page.FLAG_LAST;
        if (t != Page.TYPE_FREE_LIST || p != parentPage) {
            throw Message.getSQLException(
                    ErrorCode.FILE_CORRUPTED_1,
                    "type:" + t + " parent:" + p +
                    " expected type:" + Page.TYPE_FREE_LIST + " expected parent:" + parentPage);
        }
        int size;
        if (last) {
            nextPage = 0;
            size = page.readInt();
        } else {
            nextPage = page.readInt();
            size = (store.getPageSize() - page.length()) / DataPage.LENGTH_INT;
        }
        for (int i = 0; i < size; i++) {
            array.add(page.readInt());
        }
    }

    void free(int pageId) {

    }

}
