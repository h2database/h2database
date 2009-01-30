/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;

import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.result.Row;
import org.h2.store.DataPage;

/**
 * A leaf page that contains data of one or multiple rows.
 * Format:
 * <ul><li>0-3: parent page id
 * </li><li>4-4: page type
 * </li><li>5-6: entry count
 * </li><li>7-10: row count of all children (-1 if not known)
 * </li><li>11-14: rightmost child page id
 * </li><li>15- entries: 4 bytes leaf page id, 4 bytes key
 * </li></ul>
 */
class PageDataNode extends PageData {

    /**
     * The page ids of the children.
     */
    private int[] childPageIds;

    private int rowCountStored = UNKNOWN_ROWCOUNT;

    private int rowCount = UNKNOWN_ROWCOUNT;

    PageDataNode(PageScanIndex index, int pageId, int parentPageId, DataPage data) {
        super(index, pageId, parentPageId, data);
    }

    void read() {
        data.setPos(5);
        entryCount = data.readShortInt();
        rowCount = rowCountStored = data.readInt();
        childPageIds = new int[entryCount + 1];
        childPageIds[entryCount] = data.readInt();
        keys = new int[entryCount];
        for (int i = 0; i < entryCount; i++) {
            childPageIds[i] = data.readInt();
            keys[i] = data.readInt();
        }
        check();
    }

    private void addChild(int x, int childPageId, int key) {
        int[] newKeys = new int[entryCount + 1];
        int[] newChildPageIds = new int[entryCount + 2];
        if (childPageIds != null) {
            System.arraycopy(childPageIds, 0, newChildPageIds, 0, x + 1);
        }
        if (entryCount > 0) {
            System.arraycopy(keys, 0, newKeys, 0, x);
            if (x < entryCount) {
                System.arraycopy(keys, x, newKeys, x + 1, entryCount - x);
                System.arraycopy(childPageIds, x, newChildPageIds, x + 1, entryCount - x + 1);
            }
        }
        newKeys[x] = key;
        newChildPageIds[x + 1] = childPageId;
        keys = newKeys;
        childPageIds = newChildPageIds;
        entryCount++;
    }

    int addRow(Row row) throws SQLException {
        while (true) {
            int x = find(row.getPos());
            PageData page = index.getPage(childPageIds[x]);
            int splitPoint = page.addRow(row);
            if (splitPoint == 0) {
                break;
            }
            int pivot = page.getKey(splitPoint - 1);
            PageData page2 = page.split(splitPoint);
            index.getPageStore().updateRecord(page, true, page.data);
            index.getPageStore().updateRecord(page2, true, page2.data);
            addChild(x, page2.getPageId(), pivot);
            int maxEntries = (index.getPageStore().getPageSize() - 15) / 8;
            if (entryCount >= maxEntries) {
                int todoSplitAtLastInsertionPoint;
                return entryCount / 2;
            }
            index.getPageStore().updateRecord(this, true, data);
        }
        updateRowCount(1);
        return 0;
    }

    private void updateRowCount(int offset) throws SQLException {
        if (rowCount != UNKNOWN_ROWCOUNT) {
            rowCount += offset;
        }
        if (rowCountStored != UNKNOWN_ROWCOUNT) {
            rowCountStored = UNKNOWN_ROWCOUNT;
            index.getPageStore().updateRecord(this, true, data);
        }
    }

    Cursor find() throws SQLException {
        int child = childPageIds[0];
        return index.getPage(child).find();
    }

    PageData split(int splitPoint) throws SQLException {
        int newPageId = index.getPageStore().allocatePage();
        PageDataNode p2 = new PageDataNode(index, newPageId, parentPageId, index.getPageStore().createDataPage());
        int firstChild = childPageIds[splitPoint];
        for (int i = splitPoint; i < entryCount;) {
            p2.addChild(p2.entryCount, childPageIds[splitPoint + 1], keys[splitPoint]);
            removeChild(splitPoint);
        }
        int lastChild = childPageIds[splitPoint - 1];
        removeChild(splitPoint - 1);
        childPageIds[splitPoint - 1] = lastChild;
        p2.childPageIds[0] = firstChild;
        p2.remapChildren();
        return p2;
    }

    protected void remapChildren() throws SQLException {
        for (int i = 0; i < childPageIds.length; i++) {
            int child = childPageIds[i];
            PageData p = index.getPage(child);
            p.setParentPageId(getPos());
            index.getPageStore().updateRecord(p, true, p.data);
        }
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
        check();
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
            return next.getNextPage(key);
        }
        PageData page = index.getPage(childPageIds[i]);
        return page.getFirstLeaf();
    }

    PageDataLeaf getFirstLeaf() throws SQLException {
        int child = childPageIds[0];
        return index.getPage(child).getFirstLeaf();
    }

    boolean remove(int key) throws SQLException {
        int at = find(key);
        // merge is not implemented to allow concurrent usage of btrees
        // TODO maybe implement merge
        PageData page = index.getPage(childPageIds[at]);
        boolean empty = page.remove(key);
        updateRowCount(-1);
        if (!empty) {
            // the first row didn't change - nothing to do
            return false;
        }
        // this child is now empty
        index.getPageStore().freePage(page.getPageId());
        if (entryCount < 1) {
            // no more children - this page is empty as well
            return true;
        }
        removeChild(at);
        index.getPageStore().updateRecord(this, true, data);
        return false;
    }

    Row getRow(Session session, int key) throws SQLException {
        int at = find(key);
        PageData page = index.getPage(childPageIds[at]);
        return page.getRow(session, key);
    }

    int getRowCount() throws SQLException {
        if (rowCount == UNKNOWN_ROWCOUNT) {
            int count = 0;
            for (int i = 0; i < childPageIds.length; i++) {
                PageData page = index.getPage(childPageIds[i]);
                count += page.getRowCount();
            }
            rowCount = count;
        }
        return rowCount;
    }

    void setRowCountStored(int rowCount) throws SQLException {
        this.rowCount = rowCount;
        if (rowCountStored != rowCount) {
            rowCountStored = rowCount;
            index.getPageStore().updateRecord(this, true, data);
        }
    }

    private void check() {
        for (int i = 0; i < childPageIds.length; i++) {
            if (childPageIds[i] == 0) {
                Message.throwInternalError();
            }
        }
    }

    public int getByteCount(DataPage dummy) throws SQLException {
        return index.getPageStore().getPageSize();
    }

    public void write(DataPage buff) throws SQLException {
        check();
        data.reset();
        data.writeInt(parentPageId);
        data.writeByte((byte) Page.TYPE_DATA_NODE);
        data.writeShortInt(entryCount);
        data.writeInt(rowCountStored);
        data.writeInt(childPageIds[entryCount]);
        for (int i = 0; i < entryCount; i++) {
            data.writeInt(childPageIds[i]);
            data.writeInt(keys[i]);
        }
        index.getPageStore().writePage(getPos(), data);
    }

    private void removeChild(int i) throws SQLException {
        entryCount--;
        if (entryCount < 0) {
            Message.throwInternalError();
        }
        int[] newKeys = new int[entryCount];
        int[] newChildPageIds = new int[entryCount + 1];
        System.arraycopy(keys, 0, newKeys, 0, Math.min(entryCount, i));
        System.arraycopy(childPageIds, 0, newChildPageIds, 0, i);
        if (entryCount > i) {
            System.arraycopy(keys, i + 1, newKeys, i, entryCount - i);
        }
        System.arraycopy(childPageIds, i + 1, newChildPageIds, i, entryCount - i + 1);
        keys = newKeys;
        childPageIds = newChildPageIds;
    }

}
