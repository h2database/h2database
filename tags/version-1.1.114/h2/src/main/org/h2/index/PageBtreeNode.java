/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;
import org.h2.message.Message;
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
 * </li><li>15- entries: 4 bytes leaf page id, 4 bytes offset to data
 * </li></ul>
 */
class PageBtreeNode extends PageBtree {

    private static final int CHILD_OFFSET_PAIR_LENGTH = 8;
    private static final int CHILD_OFFSET_PAIR_START = 15;

    /**
     * The page ids of the children.
     */
    private int[] childPageIds;

    private int rowCountStored = UNKNOWN_ROWCOUNT;

    private int rowCount = UNKNOWN_ROWCOUNT;

    PageBtreeNode(PageBtreeIndex index, int pageId, int parentPageId, DataPage data) {
        super(index, pageId, parentPageId, data);
        start = CHILD_OFFSET_PAIR_START;
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
        start = data.length();
    }

    /**
     * Add a row if possible. If it is possible this method returns 0, otherwise
     * the split point. It is always possible to add one row.
     *
     * @param row the now to add
     * @return the split point of this page, or 0 if no split is required
     */
    private int addChild(int x, int childPageId, SearchRow row) throws SQLException {
        int rowLength = index.getRowSize(data, row);
        int pageSize = index.getPageStore().getPageSize();
        int last = entryCount == 0 ? pageSize : offsets[entryCount - 1];
        if (entryCount > 0 && last - rowLength < start + CHILD_OFFSET_PAIR_LENGTH) {
            int todoSplitAtLastInsertionPoint;
            return (entryCount / 2) + 1;
        }
        int offset = last - rowLength;
        int[] newOffsets = new int[entryCount + 1];
        SearchRow[] newRows = new SearchRow[entryCount + 1];
        int[] newChildPageIds = new int[entryCount + 2];
        if (childPageIds != null) {
            System.arraycopy(childPageIds, 0, newChildPageIds, 0, x + 1);
        }
        if (entryCount > 0) {
            readAllRows();
            System.arraycopy(offsets, 0, newOffsets, 0, x);
            System.arraycopy(rows, 0, newRows, 0, x);
            if (x < entryCount) {
                for (int j = x; j < entryCount; j++) {
                    newOffsets[j + 1] = offsets[j] - rowLength;
                }
                offset = (x == 0 ? pageSize : offsets[x - 1]) - rowLength;
                System.arraycopy(rows, x, newRows, x + 1, entryCount - x);
                System.arraycopy(childPageIds, x + 1, newChildPageIds, x + 2, entryCount - x);
            }
        }
        newOffsets[x] = offset;
        newRows[x] = row;
        newChildPageIds[x + 1] = childPageId;
        start += CHILD_OFFSET_PAIR_LENGTH;
        offsets = newOffsets;
        rows = newRows;
        childPageIds = newChildPageIds;
        entryCount++;
        return 0;
    }

    int addRow(SearchRow row) throws SQLException {
        while (true) {
            int x = find(row, false, true);
            PageBtree page = index.getPage(childPageIds[x]);
            int splitPoint = page.addRow(row);
            if (splitPoint == 0) {
                break;
            }
            SearchRow pivot = page.getRow(splitPoint - 1);
            PageBtree page2 = page.split(splitPoint);
            index.getPageStore().updateRecord(page, true, page.data);
            index.getPageStore().updateRecord(page2, true, page2.data);
            splitPoint = addChild(x, page2.getPageId(), pivot);
            if (splitPoint != 0) {
                int todoSplitAtLastInsertionPoint;
                return splitPoint / 2;
            }
            index.getPageStore().updateRecord(this, true, data);
        }
        updateRowCount(1);
        written = false;
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

    PageBtree split(int splitPoint) throws SQLException {
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
        for (int child : childPageIds) {
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
    void init(PageBtree page1, SearchRow pivot, PageBtree page2) throws SQLException {
        entryCount = 0;
        childPageIds = new int[] { page1.getPageId() };
        rows = new SearchRow[0];
        offsets = new int[0];
        addChild(0, page2.getPageId(), pivot);
        check();
    }

    void find(PageBtreeCursor cursor, SearchRow first, boolean bigger) throws SQLException {
        int i = find(first, bigger, false);
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

    boolean remove(SearchRow row) throws SQLException {
        int at = find(row, false, false);
        // merge is not implemented to allow concurrent usage of btrees
        // TODO maybe implement merge
        PageBtree page = index.getPage(childPageIds[at]);
        boolean empty = page.remove(row);
        updateRowCount(-1);
        if (!empty) {
            // the first row didn't change - nothing to do
            return false;
        }
        // this child is now empty
        index.getPageStore().freePage(page.getPageId(), true, page.data);
        if (entryCount < 1) {
            // no more children - this page is empty as well
            return true;
        }
        written = false;
        removeChild(at);
        index.getPageStore().updateRecord(this, true, data);
        return false;
    }

    int getRowCount() throws SQLException {
        if (rowCount == UNKNOWN_ROWCOUNT) {
            int count = 0;
            for (int child : childPageIds) {
                PageBtree page = index.getPage(child);
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
        for (int child : childPageIds) {
            if (child == 0) {
                Message.throwInternalError();
            }
        }
    }

    public int getByteCount(DataPage dummy) {
        return index.getPageStore().getPageSize();
    }

    public void write(DataPage buff) throws SQLException {
        check();
        write();
        index.getPageStore().writePage(getPos(), data);
    }

    private void write() throws SQLException {
        if (written) {
            return;
        }
        // make sure rows are read
        for (int i = 0; i < entryCount; i++) {
            getRow(i);
        }
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
        for (int i = 0; i < entryCount; i++) {
            index.writeRow(data, offsets[i], rows[i]);
        }
        written = true;
    }

    private void removeChild(int i) throws SQLException {
        readAllRows();
        entryCount--;
        written = false;
        if (entryCount < 0) {
            Message.throwInternalError();
        }
        SearchRow[] newRows = new SearchRow[entryCount];
        int[] newOffsets = new int[entryCount];
        int[] newChildPageIds = new int[entryCount + 1];
        System.arraycopy(offsets, 0, newOffsets, 0, Math.min(entryCount, i));
        System.arraycopy(rows, 0, newRows, 0, Math.min(entryCount, i));
        System.arraycopy(childPageIds, 0, newChildPageIds, 0, i);
        if (entryCount > i) {
            System.arraycopy(rows, i + 1, newRows, i, entryCount - i);
            int startNext = i > 0 ? offsets[i - 1] : index.getPageStore().getPageSize();
            int rowLength = startNext - offsets[i];
            for (int j = i; j < entryCount; j++) {
                newOffsets[j] = offsets[j + 1] + rowLength;
            }
        }
        System.arraycopy(childPageIds, i + 1, newChildPageIds, i, entryCount - i + 1);
        offsets = newOffsets;
        rows = newRows;
        childPageIds = newChildPageIds;
        start -= CHILD_OFFSET_PAIR_LENGTH;
    }

    /**
     * Set the cursor to the first row of the next page.
     *
     * @param cursor the cursor
     * @param row the current row
     */
    void nextPage(PageBtreeCursor cursor, SearchRow row) throws SQLException {
        int i = find(row, false, false) + 1;
        if (i > entryCount) {
            if (parentPageId == Page.ROOT) {
                cursor.setCurrent(null, 0);
                return;
            }
            PageBtreeNode next = (PageBtreeNode) index.getPage(parentPageId);
            next.nextPage(cursor, getRow(entryCount - 1));
            return;
        }
        PageBtree page = index.getPage(childPageIds[i]);
        PageBtreeLeaf leaf = page.getFirstLeaf();
        cursor.setCurrent(leaf, 0);
    }

    public String toString() {
        return "page[" + getPos() + "] btree node table:" + index.getId() + " entries:" + entryCount;
    }


}