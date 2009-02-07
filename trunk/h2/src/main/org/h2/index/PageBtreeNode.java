/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;
import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.store.DataPage;

/**
 * A leaf page that contains index data.
 * Format:
 * <ul><li>0-3: parent page id
 * </li><li>4-4: page type
 * </li><li>5-6: entry count
 * </li><li>7-10: row count of all children (-1 if not known)
 * </li><li>11-14: rightmost child page id
 * </li><li>15- entries: 4 bytes leaf page id, 4 bytes key
 * </li></ul>
 */
class PageBtreeNode extends PageBtree {

    /**
     * The page ids of the children.
     */
    private int[] childPageIds;

    private int rowCountStored = UNKNOWN_ROWCOUNT;

    private int rowCount = UNKNOWN_ROWCOUNT;

    PageBtreeNode(PageBtreeIndex index, int pageId, int parentPageId, DataPage data) {
        super(index, pageId, parentPageId, data);
    }

    void read() {
        data.setPos(5);
        entryCount = data.readShortInt();
        rowCount = rowCountStored = data.readInt();
        childPageIds = new int[entryCount + 1];
        childPageIds[entryCount] = data.readInt();
        rows = new SearchRow[entryCount];
        offsets = new int[entryCount];
        for (int i = 0; i < entryCount; i++) {
            childPageIds[i] = data.readInt();
            offsets[i] = data.readInt();
        }
        check();
    }

    /**
     * Add a row if possible. If it is possible this method returns 0, otherwise
     * the split point. It is always possible to add one row.
     *
     * @param row the now to add
     * @return the split point of this page, or 0 if no split is required
     */
    private int addChild(int x, int childPageId, SearchRow row) {
//        int rowLength = index.getRowSize(data, row);
//
//        int last = entryCount == 0 ? pageSize : offsets[entryCount - 1];
//        if (entryCount > 0 && last - rowLength < start + KEY_OFFSET_PAIR_LENGTH) {
//            int todoSplitAtLastInsertionPoint;
//            return (entryCount / 2) + 1;
//        }
//
//        int[] newOffsets = new int[entryCount + 1];
//        SearchRow[] newRows = new SearchRow[entryCount + 1];
//        int[] newChildPageIds = new int[entryCount + 2];
//        if (childPageIds != null) {
//            System.arraycopy(childPageIds, 0, newChildPageIds, 0, x + 1);
//        }
//        if (entryCount > 0) {
//            System.arraycopy(offsets, 0, newOffsets, 0, x);
//            System.arraycopy(rows, 0, newRows, 0, x);
//            if (x < entryCount) {
//                System.arraycopy(offsets, x, newOffsets, x + 1, entryCount - x);
//                System.arraycopy(rows, x, newRows, x + 1, entryCount - x);
//                System.arraycopy(childPageIds, x, newChildPageIds, x + 1, entryCount - x + 1);
//            }
//        }
//        newOffsets[x] = offset;
//        newRows[x] = row;
//        newChildPageIds[x + 1] = childPageId;
//        offsets = newOffsets;
//        childPageIds = newChildPageIds;
//        entryCount++;
        return 0;
    }

    int addRow(Session session, SearchRow row) throws SQLException {
//        while (true) {
//            int x = find(session, row, false);
//            PageBtree page = index.getPage(childPageIds[x]);
//            int splitPoint = page.addRow(session, row);
//            if (splitPoint == 0) {
//                break;
//            }
//            SearchRow pivot = page.getRow(session, splitPoint - 1);
//            PageBtree page2 = page.split(session, splitPoint);
//            index.getPageStore().updateRecord(page, true, page.data);
//            index.getPageStore().updateRecord(page2, true, page2.data);
//            int splitPoint = addChild(x, page2.getPageId(), pivot);
//            if (splitPoint != 0) {
//                int todoSplitAtLastInsertionPoint;
//                return splitPoint / 2;
//            }
//            index.getPageStore().updateRecord(this, true, data);
//        }
//        updateRowCount(1);
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

    PageBtree split(Session session, int splitPoint) throws SQLException {
        int newPageId = index.getPageStore().allocatePage();
        PageBtreeNode p2 = new PageBtreeNode(index, newPageId, parentPageId, index.getPageStore().createDataPage());
        int firstChild = childPageIds[splitPoint];
        for (int i = splitPoint; i < entryCount;) {
            p2.addChild(p2.entryCount, childPageIds[splitPoint + 1], rows[splitPoint]);
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
            PageBtree p = index.getPage(child);
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
    void init(PageBtree page1, SearchRow pivot, PageBtree page2) {
        entryCount = 1;
        childPageIds = new int[] { page1.getPageId(), page2.getPageId() };
        rows = new SearchRow[] { pivot };
        check();
    }

    void find(PageBtreeCursor cursor, SearchRow first, boolean bigger) throws SQLException {
        int i = find(cursor.getSession(), first, bigger) + 1;
        if (i > entryCount) {
            if (parentPageId == Page.ROOT) {
                return;
            }
            PageBtreeNode next = (PageBtreeNode) index.getPage(parentPageId);
            next.find(cursor, first, bigger);
            return;
        }
        PageBtree page = index.getPage(childPageIds[i]);
        page.find(cursor, first, bigger);
    }

    PageBtreeLeaf getFirstLeaf() throws SQLException {
        int child = childPageIds[0];
        return index.getPage(child).getFirstLeaf();
    }

    boolean remove(Session session, SearchRow row) throws SQLException {
        int at = find(session, row, false);
        // merge is not implemented to allow concurrent usage of btrees
        // TODO maybe implement merge
        PageBtree page = index.getPage(childPageIds[at]);
        boolean empty = page.remove(session, row);
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

    int getRowCount() throws SQLException {
        if (rowCount == UNKNOWN_ROWCOUNT) {
            int count = 0;
            for (int i = 0; i < childPageIds.length; i++) {
                PageBtree page = index.getPage(childPageIds[i]);
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
        data.writeByte((byte) Page.TYPE_BTREE_NODE);
        data.writeShortInt(entryCount);
        data.writeInt(rowCountStored);
        data.writeInt(childPageIds[entryCount]);
        for (int i = 0; i < entryCount; i++) {
            data.writeInt(childPageIds[i]);
            data.writeInt(offsets[i]);
        }
        index.getPageStore().writePage(getPos(), data);
    }

    private void removeChild(int i) throws SQLException {
        entryCount--;
        if (entryCount < 0) {
            Message.throwInternalError();
        }
        int[] newOffsets = new int[entryCount];
        SearchRow[] newRows = new SearchRow[entryCount + 1];
        int[] newChildPageIds = new int[entryCount + 1];
        System.arraycopy(offsets, 0, newOffsets, 0, Math.min(entryCount, i));
        System.arraycopy(rows, 0, newRows, 0, Math.min(entryCount, i));
        System.arraycopy(childPageIds, 0, newChildPageIds, 0, i);
        if (entryCount > i) {
            System.arraycopy(offsets, i + 1, newOffsets, i, entryCount - i);
            System.arraycopy(rows, i + 1, newRows, i, entryCount - i);
        }
        System.arraycopy(childPageIds, i + 1, newChildPageIds, i, entryCount - i + 1);
        offsets = newOffsets;
        rows = newRows;
        childPageIds = newChildPageIds;
    }

}