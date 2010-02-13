/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.result.SearchRow;
import org.h2.store.Data;
import org.h2.store.Page;
import org.h2.store.PageStore;

/**
 * A b-tree leaf page that contains index data. Format:
 * <ul>
 * <li>page type: byte</li>
 * <li>checksum: short</li>
 * <li>parent page id (0 for root): int</li>
 * <li>index id: varInt</li>
 * <li>entry count: short</li>
 * <li>list of offsets: short</li>
 * <li>data (key: varLong, value,...)</li>
 * </ul>
 */
public class PageBtreeLeaf extends PageBtree {

    private static final int OFFSET_LENGTH = 2;

    private PageBtreeLeaf(PageBtreeIndex index, int pageId, Data data) {
        super(index, pageId, data);
    }

    /**
     * Read a b-tree leaf page.
     *
     * @param index the index
     * @param data the data
     * @param pageId the page id
     * @return the page
     */
    public static Page read(PageBtreeIndex index, Data data, int pageId) {
        PageBtreeLeaf p = new PageBtreeLeaf(index, pageId, data);
        p.read();
        return p;
    }

    /**
     * Create a new page.
     *
     * @param index the index
     * @param pageId the page id
     * @param parentPageId the parent
     * @return the page
     */
    static PageBtreeLeaf create(PageBtreeIndex index, int pageId, int parentPageId) {
        PageBtreeLeaf p = new PageBtreeLeaf(index, pageId, index.getPageStore().createData());
        index.getPageStore().logUndo(p, null);
        p.parentPageId = parentPageId;
        p.writeHead();
        p.start = p.data.length();
        return p;
    }

    private void read() {
        data.reset();
        int type = data.readByte();
        data.readShortInt();
        this.parentPageId = data.readInt();
        onlyPosition = (type & Page.FLAG_LAST) == 0;
        int indexId = data.readVarInt();
        if (indexId != index.getId()) {
            throw DbException.get(ErrorCode.FILE_CORRUPTED_1,
                    "page:" + getPos() + " expected index:" + index.getId() +
                    "got:" + indexId);
        }
        entryCount = data.readShortInt();
        offsets = new int[entryCount];
        rows = new SearchRow[entryCount];
        for (int i = 0; i < entryCount; i++) {
            offsets[i] = data.readShortInt();
        }
        start = data.length();
        written = true;
    }

    int addRowTry(SearchRow row) {
        return addRow(row, true);
    }

    private int addRow(SearchRow row, boolean tryOnly) {
        int rowLength = index.getRowSize(data, row, onlyPosition);
        int pageSize = index.getPageStore().getPageSize();
        int last = entryCount == 0 ? pageSize : offsets[entryCount - 1];
        if (last - rowLength < start + OFFSET_LENGTH) {
            if (tryOnly && entryCount > 1) {
                int x = find(row, false, true, true);
                if (entryCount < 5) {
                    // required, otherwise the index doesn't work correctly
                    return entryCount / 2;
                }
                // split near the insertion point to better fill pages
                // split in half would be:
                // return entryCount / 2;
                int third = entryCount / 3;
                return x < third ? third : x >= 2 * third ? 2 * third : x;
            }
            readAllRows();
            onlyPosition = true;
            // change the offsets (now storing only positions)
            int o = pageSize;
            for (int i = 0; i < entryCount; i++) {
                o -= index.getRowSize(data, getRow(i), true);
                offsets[i] = o;
            }
            last = entryCount == 0 ? pageSize : offsets[entryCount - 1];
            rowLength = index.getRowSize(data, row, true);
            if (SysProperties.CHECK && last - rowLength < start + OFFSET_LENGTH) {
                throw DbException.throwInternalError();
            }
        }
        index.getPageStore().logUndo(this, data);
        readAllRows();
        changeCount = index.getPageStore().getChangeCount();
        written = false;
        int offset = last - rowLength;
        int[] newOffsets = new int[entryCount + 1];
        SearchRow[] newRows = new SearchRow[entryCount + 1];
        int x;
        if (entryCount == 0) {
            x = 0;
        } else {
            x = find(row, false, true, true);
            System.arraycopy(offsets, 0, newOffsets, 0, x);
            System.arraycopy(rows, 0, newRows, 0, x);
            if (x < entryCount) {
                for (int j = x; j < entryCount; j++) {
                    newOffsets[j + 1] = offsets[j] - rowLength;
                }
                offset = (x == 0 ? pageSize : offsets[x - 1]) - rowLength;
                System.arraycopy(rows, x, newRows, x + 1, entryCount - x);
            }
        }
        entryCount++;
        start += OFFSET_LENGTH;
        newOffsets[x] = offset;
        newRows[x] = row;
        offsets = newOffsets;
        rows = newRows;
        index.getPageStore().update(this);
        return -1;
    }

    private void removeRow(int at) {
        readAllRows();
        index.getPageStore().logUndo(this, data);
        entryCount--;
        written = false;
        changeCount = index.getPageStore().getChangeCount();
        if (entryCount <= 0) {
            DbException.throwInternalError();
        }
        int[] newOffsets = new int[entryCount];
        SearchRow[] newRows = new SearchRow[entryCount];
        System.arraycopy(offsets, 0, newOffsets, 0, at);
        System.arraycopy(rows, 0, newRows, 0, at);
        int startNext = at > 0 ? offsets[at - 1] : index.getPageStore().getPageSize();
        int rowLength = startNext - offsets[at];
        for (int j = at; j < entryCount; j++) {
            newOffsets[j] = offsets[j + 1] + rowLength;
        }
        System.arraycopy(rows, at + 1, newRows, at, entryCount - at);
        start -= OFFSET_LENGTH;
        offsets = newOffsets;
        rows = newRows;
    }

    int getEntryCount() {
        return entryCount;
    }

    PageBtree split(int splitPoint) {
        int newPageId = index.getPageStore().allocatePage();
        PageBtreeLeaf p2 = PageBtreeLeaf.create(index, newPageId, parentPageId);
        for (int i = splitPoint; i < entryCount;) {
            p2.addRow(getRow(splitPoint), false);
            removeRow(splitPoint);
        }
        return p2;
    }

    PageBtreeLeaf getFirstLeaf() {
        return this;
    }

    PageBtreeLeaf getLastLeaf() {
        return this;
    }

    SearchRow remove(SearchRow row) {
        int at = find(row, false, false, true);
        SearchRow delete = getRow(at);
        if (index.compareRows(row, delete) != 0 || delete.getKey() != row.getKey()) {
            throw DbException.get(ErrorCode.ROW_NOT_FOUND_WHEN_DELETING_1, index.getSQL() + ": " + row);
        }
        index.getPageStore().logUndo(this, data);
        if (entryCount == 1) {
            // the page is now empty
            return row;
        }
        removeRow(at);
        index.getPageStore().update(this);
        if (at == entryCount) {
            // the last row changed
            return getRow(at - 1);
        }
        // the last row didn't change
        return null;
    }

    void freeRecursive() {
        index.getPageStore().logUndo(this, data);
        index.getPageStore().free(getPos());
    }

    int getRowCount() {
        return entryCount;
    }

    void setRowCountStored(int rowCount) {
        // ignore
    }

    public void write() {
        writeData();
        index.getPageStore().writePage(getPos(), data);
    }

    private void writeHead() {
        data.reset();
        data.writeByte((byte) (Page.TYPE_BTREE_LEAF | (onlyPosition ? 0 : Page.FLAG_LAST)));
        data.writeShortInt(0);
        data.writeInt(parentPageId);
        data.writeVarInt(index.getId());
        data.writeShortInt(entryCount);
    }

    private void writeData() {
        if (written) {
            return;
        }
        readAllRows();
        writeHead();
        for (int i = 0; i < entryCount; i++) {
            data.writeShortInt(offsets[i]);
        }
        for (int i = 0; i < entryCount; i++) {
            index.writeRow(data, offsets[i], rows[i], onlyPosition);
        }
        written = true;
    }

    void find(PageBtreeCursor cursor, SearchRow first, boolean bigger) {
        int i = find(first, bigger, false, false);
        if (i > entryCount) {
            if (parentPageId == PageBtree.ROOT) {
                return;
            }
            PageBtreeNode next = (PageBtreeNode) index.getPage(parentPageId);
            next.find(cursor, first, bigger);
            return;
        }
        cursor.setCurrent(this, i);
    }

    void last(PageBtreeCursor cursor) {
        cursor.setCurrent(this, entryCount - 1);
    }

    void remapChildren() {
        // nothing to do
    }

    /**
     * Set the cursor to the first row of the next page.
     *
     * @param cursor the cursor
     */
    void nextPage(PageBtreeCursor cursor) {
        if (parentPageId == PageBtree.ROOT) {
            cursor.setCurrent(null, 0);
            return;
        }
        PageBtreeNode next = (PageBtreeNode) index.getPage(parentPageId);
        next.nextPage(cursor, getPos());
    }

    /**
     * Set the cursor to the last row of the previous page.
     *
     * @param cursor the cursor
     */
    void previousPage(PageBtreeCursor cursor) {
        if (parentPageId == PageBtree.ROOT) {
            cursor.setCurrent(null, 0);
            return;
        }
        PageBtreeNode next = (PageBtreeNode) index.getPage(parentPageId);
        next.previousPage(cursor, getPos());
    }

    public String toString() {
        return "page[" + getPos() + "] b-tree leaf table:" + index.getId() + " entries:" + entryCount;
    }

    public void moveTo(Session session, int newPos) {
        PageStore store = index.getPageStore();
        readAllRows();
        PageBtreeLeaf p2 = PageBtreeLeaf.create(index, newPos, parentPageId);
        store.logUndo(this, data);
        store.logUndo(p2, null);
        p2.rows = rows;
        p2.entryCount = entryCount;
        p2.offsets = offsets;
        p2.onlyPosition = onlyPosition;
        p2.parentPageId = parentPageId;
        p2.start = start;
        store.update(p2);
        if (parentPageId == ROOT) {
            index.setRootPageId(session, newPos);
        } else {
            PageBtreeNode p = (PageBtreeNode) store.getPage(parentPageId);
            p.moveChild(getPos(), newPos);
        }
        store.free(getPos());
    }

}
