/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.engine.Constants;
import org.h2.message.Message;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.store.DataPageBinary;

/**
 * A leaf page that contains data of one or multiple rows.
 * Format:
 * <ul><li>0-3: parent page id
 * </li><li>4-4: page type
 * </li><li>5-6: entry count
 * </li><li>7-10: rightmost child page id
 * </li><li>11- entries: 4 bytes leaf page id, 4 bytes key
 * </li></ul>
 */
class PageDataNode extends PageData {

    /**
     * The page ids of the children.
     */
    int[] childPageIds;
    
    PageDataNode(PageScanIndex index, int pageId, int parentPageId, DataPageBinary data) {
        super(index, pageId, parentPageId, data);
        int todoOptimizationChildrenEntryCount;
    }

    void read() {
        data.setPos(5);
        entryCount = data.readShortInt();
        childPageIds = new int[entryCount + 1];
        childPageIds[entryCount] = data.readInt();
        keys = new int[entryCount];
        for (int i = 0; i < entryCount; i++) {
            childPageIds[i] = data.readInt();
            keys[i] = data.readInt();
        }
    }

    void write() throws SQLException {
        data.reset();
        data.writeInt(parentPageId);
        data.writeByte((byte) Page.TYPE_DATA_NODE);
        data.writeShortInt(entryCount);
        data.writeInt(childPageIds[entryCount]);
        for (int i = 0; i < entryCount; i++) {
            data.writeInt(childPageIds[i]);
            data.writeInt(keys[i]);
        }
        index.getPageStore().writePage(pageId, data);
    }

    int addRow(Row row) throws SQLException {
        int x = find(row.getPos());
        PageData page = index.getPage(childPageIds[x]);
        int splitPoint = page.addRow(row);
        if (splitPoint == 0) {
            return 0;
        }
        int pivot = page.getKey(splitPoint);
        PageData page2 = page.split(splitPoint);
        int[] newKeys = new int[entryCount + 1];
        int[] newChildPageIds = new int[entryCount + 2];
        System.arraycopy(keys, 0, newKeys, 0, x);
        System.arraycopy(childPageIds, 0, newChildPageIds, 0, x);
        if (x < entryCount) {
            System.arraycopy(keys, x, newKeys, x + 1, entryCount - x);
            System.arraycopy(childPageIds, x, newChildPageIds, x + 1, entryCount - x + 1);
        }
        newKeys[x] = pivot;
        newChildPageIds[x] = page2.getPageId();
        keys = newKeys;
        childPageIds = newChildPageIds;
        entryCount++;
        int maxEntries = (index.getPageStore().getPageSize() - 11) / 8;
        if (entryCount >= maxEntries) {
            int todoSplitAtLastInsertionPoint;
            return entryCount / 2;
        }
        write();
        return 0;
    }

    Cursor find() throws SQLException {
        int child = childPageIds[0];
        return index.getPage(child).find();
    }

    PageData split(int splitPoint) throws SQLException {
        int todo;
        return null;
    }

    /**
     * Initialize the page.
     * 
     * @param page1 the first child page
     * @param pivot the pivot key
     * @param page2 the last child page
     */
    void init(PageData page1, int pivot, PageData page2) {
        entryCount = 1;
        childPageIds = new int[] { page1.getPageId(), page2.getPageId() };
        keys = new int[] { pivot };
    }
    
    int getLastKey() throws SQLException {
        int todoRemove;
        return index.getPage(childPageIds[entryCount]).getLastKey();
    }

    /**
     * Get the next leaf page.
     * 
     * @param key the last key of the current page
     * @return the next leaf page
     */
    public PageDataLeaf getNextPage(int key) throws SQLException {
        int i = find(key) + 1;
        if (i > entryCount) {
            if (parentPageId == Page.ROOT) {
                return null;
            }
            PageDataNode next = (PageDataNode) index.getPage(parentPageId);
            return next.getNextPage(keys[entryCount - 1]);
        }
        PageData page = index.getPage(childPageIds[i]);
        return page.getFirstLeaf();
    }
    
    PageDataLeaf getFirstLeaf() throws SQLException {
        int child = childPageIds[0];
        return index.getPage(child).getFirstLeaf();
    }
    
    private void removeRow(int i) throws SQLException {
        entryCount--;
        if (entryCount <= 0) {
            Message.getInternalError();
        }
        int[] newKeys = new int[entryCount];
        int[] newChildPageIds = new int[entryCount + 1];
        System.arraycopy(keys, 0, newKeys, 0, i);
        System.arraycopy(childPageIds, 0, newChildPageIds, 0, i);
        System.arraycopy(keys, i + 1, newKeys, i, entryCount - i);
        System.arraycopy(childPageIds, i + 1, newChildPageIds, i, entryCount - i + 1);
        keys = newKeys;
        childPageIds = newChildPageIds;
    }
    
    boolean remove(int key) throws SQLException {
        int todo;
        int at = find(key);
        // merge is not implemented to allow concurrent usage of btrees
        // TODO maybe implement merge
        PageData page = index.getPage(childPageIds[at]);
        boolean empty = page.remove(key);
        if (!empty) {
            // the first row didn't change - nothing to do
            return false;
        }
        // this child is now empty
        if (entryCount == 1) {
            // no more children - this page is empty as well
            // it can't be the root otherwise the index would have been
            // truncated
            return true;
        }
        if (at == 0) {
            // the first child is empty - then the first row of this subtree
            // has changed
            removeRow(at);
        } else {
            // otherwise the first row didn't change
            removeRow(at - 1);
        }
        return false;
    }

}
