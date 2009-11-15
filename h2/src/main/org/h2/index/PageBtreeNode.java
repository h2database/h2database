/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;
import org.h2.api.DatabaseEventListener;
import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.result.SearchRow;
import org.h2.store.Data;
import org.h2.store.DataPage;
import org.h2.store.Page;
import org.h2.store.PageStore;
import org.h2.util.MemoryUtils;

/**
 * A b-tree node page that contains index data. Format:
 * <ul>
 * <li>page type: byte</li>
 * <li>checksum: short</li>
 * <li>parent page id (0 for root): int</li>
 * <li>index id: varInt</li>
 * <li>count of all children (-1 if not known): int</li>
 * <li>entry count: short</li>
 * <li>rightmost child page id: int</li>
 * <li>entries (child page id: int, offset: short) The row contains the largest
 * key of the respective child, meaning row[0] contains the largest key of
 * child[0].
 */
public class PageBtreeNode extends PageBtree {

    private static final int CHILD_OFFSET_PAIR_LENGTH = 6;

    /**
     * The page ids of the children.
     */
    private int[] childPageIds;

    private int rowCountStored = UNKNOWN_ROWCOUNT;

    private int rowCount = UNKNOWN_ROWCOUNT;

    private PageBtreeNode(PageBtreeIndex index, int pageId, Data data) {
        super(index, pageId, data);
    }

    /**
     * Read a b-tree node page.
     *
     * @param index the index
     * @param data the data
     * @param pageId the page id
     * @return the page
     */
    public static Page read(PageBtreeIndex index, Data data, int pageId) throws SQLException {
        PageBtreeNode p = new PageBtreeNode(index, pageId, data);
        p.read();
        return p;
    }

    /**
     * Create a new b-tree node page.
     *
     * @param index the index
     * @param pageId the page id
     * @param parentPageId the parent page id
     * @return the page
     */
    static PageBtreeNode create(PageBtreeIndex index, int pageId, int parentPageId) throws SQLException {
        PageBtreeNode p = new PageBtreeNode(index, pageId, index.getPageStore().createData());
        index.getPageStore().logUndo(p, null);
        p.parentPageId = parentPageId;
        p.writeHead();
        // 4 bytes for the rightmost child page id
        p.start = p.data.length() + 4;
        return p;
    }

    private void read() throws SQLException {
        data.reset();
        int type = data.readByte();
        data.readShortInt();
        this.parentPageId = data.readInt();
        onlyPosition = (type & Page.FLAG_LAST) == 0;
        int indexId = data.readVarInt();
        if (indexId != index.getId()) {
            throw Message.getSQLException(ErrorCode.FILE_CORRUPTED_1,
                    "page:" + getPos() + " expected index:" + index.getId() +
                    "got:" + indexId);
        }
        rowCount = rowCountStored = data.readInt();
        entryCount = data.readShortInt();
        childPageIds = new int[entryCount + 1];
        childPageIds[entryCount] = data.readInt();
        rows = PageStore.newSearchRows(entryCount);
        offsets = MemoryUtils.newIntArray(entryCount);
        for (int i = 0; i < entryCount; i++) {
            childPageIds[i] = data.readInt();
            offsets[i] = data.readShortInt();
        }
        check();
        start = data.length();
        written = true;
    }

    /**
     * Add a row. If it is possible this method returns -1, otherwise
     * the split point. It is always possible to two rows.
     *
     * @param row the now to add
     * @return the split point of this page, or -1 if no split is required
     */
    private int addChildTry(SearchRow row) throws SQLException {
        if (entryCount < 2) {
            return -1;
        }
        int rowLength = index.getRowSize(data, row, onlyPosition);
        int pageSize = index.getPageStore().getPageSize();
        int last = entryCount == 0 ? pageSize : offsets[entryCount - 1];
        if (last - rowLength < start + CHILD_OFFSET_PAIR_LENGTH) {
            return entryCount / 2;
        }
        return -1;
    }

    /**
     * Add a child at the given position.
     *
     * @param x the position
     * @param childPageId the child
     * @param row the row smaller than the first row of the child and its children
     */
    private void addChild(int x, int childPageId, SearchRow row) throws SQLException {
        int rowLength = index.getRowSize(data, row, onlyPosition);
        int pageSize = index.getPageStore().getPageSize();
        int last = entryCount == 0 ? pageSize : offsets[entryCount - 1];
        if (last - rowLength < start + CHILD_OFFSET_PAIR_LENGTH) {
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
            if (SysProperties.CHECK && last - rowLength < start + CHILD_OFFSET_PAIR_LENGTH) {
                throw Message.throwInternalError();
            }
        }
        int offset = last - rowLength;
        int[] newOffsets = new int[entryCount + 1];
        SearchRow[] newRows = new SearchRow[entryCount + 1];
        int[] newChildPageIds = new int[entryCount + 2];
        if (childPageIds != null) {
            System.arraycopy(childPageIds, 0, newChildPageIds, 0, x + 1);
        }
        if (entryCount > 0) {
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
        written = false;
    }

    int addRowTry(SearchRow row) throws SQLException {
        while (true) {
            int x = find(row, false, true, true);
            PageBtree page = index.getPage(childPageIds[x]);
            int splitPoint = page.addRowTry(row);
            if (splitPoint == -1) {
                break;
            }
            SearchRow pivot = page.getRow(splitPoint - 1);
            index.getPageStore().logUndo(this, data);
            int splitPoint2 = addChildTry(pivot);
            if (splitPoint2 != -1) {
                return splitPoint2;
            }
            PageBtree page2 = page.split(splitPoint);
            readAllRows();
            addChild(x, page2.getPos(), pivot);
            index.getPageStore().update(page);
            index.getPageStore().update(page2);
            index.getPageStore().update(this);
        }
        updateRowCount(1);
        written = false;
        return -1;
    }

    private void updateRowCount(int offset) throws SQLException {
        if (rowCount != UNKNOWN_ROWCOUNT) {
            rowCount += offset;
        }
        if (rowCountStored != UNKNOWN_ROWCOUNT) {
            rowCountStored = UNKNOWN_ROWCOUNT;
            index.getPageStore().logUndo(this, data);
            if (written) {
                writeHead();
            }
            index.getPageStore().update(this);
        }
    }

    PageBtree split(int splitPoint) throws SQLException {
        int newPageId = index.getPageStore().allocatePage();
        PageBtreeNode p2 = PageBtreeNode.create(index, newPageId, parentPageId);
        index.getPageStore().logUndo(this, data);
        if (onlyPosition) {
            // TODO optimize: maybe not required
            p2.onlyPosition = true;
        }
        int firstChild = childPageIds[splitPoint];
        readAllRows();
        for (int i = splitPoint; i < entryCount;) {
            p2.addChild(p2.entryCount, childPageIds[splitPoint + 1], rows[splitPoint]);
            removeChild(splitPoint);
        }
        int lastChild = childPageIds[splitPoint - 1];
        removeChild(splitPoint - 1);
        childPageIds[splitPoint - 1] = lastChild;
        if (p2.childPageIds == null) {
            p2.childPageIds = new int[1];
        }
        p2.childPageIds[0] = firstChild;
        p2.remapChildren();
        return p2;
    }

    protected void remapChildren() throws SQLException {
        for (int child : childPageIds) {
            PageBtree p = index.getPage(child);
            p.setParentPageId(getPos());
            index.getPageStore().update(p);
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
        childPageIds = new int[] { page1.getPos() };
        rows = new SearchRow[0];
        offsets = MemoryUtils.EMPTY_INT_ARRAY;
        addChild(0, page2.getPos(), pivot);
        rowCount = page1.getRowCount() + page2.getRowCount();
        check();
    }

    void find(PageBtreeCursor cursor, SearchRow first, boolean bigger) throws SQLException {
        int i = find(first, bigger, false, false);
        if (i > entryCount) {
            if (parentPageId == PageBtree.ROOT) {
                return;
            }
            PageBtreeNode next = (PageBtreeNode) index.getPage(parentPageId);
            next.find(cursor, first, bigger);
            return;
        }
        PageBtree page = index.getPage(childPageIds[i]);
        page.find(cursor, first, bigger);
    }

    void last(PageBtreeCursor cursor) throws SQLException {
        int child = childPageIds[entryCount];
        index.getPage(child).last(cursor);
    }

    PageBtreeLeaf getFirstLeaf() throws SQLException {
        int child = childPageIds[0];
        return index.getPage(child).getFirstLeaf();
    }

    PageBtreeLeaf getLastLeaf() throws SQLException {
        int child = childPageIds[entryCount];
        return index.getPage(child).getLastLeaf();
    }

    SearchRow remove(SearchRow row) throws SQLException {
        int at = find(row, false, false, true);
        // merge is not implemented to allow concurrent usage
        // TODO maybe implement merge
        PageBtree page = index.getPage(childPageIds[at]);
        SearchRow last = page.remove(row);
        index.getPageStore().logUndo(this, data);
        updateRowCount(-1);
        written = false;
        if (last == null) {
            // the last row didn't change - nothing to do
            return null;
        } else if (last == row) {
            // this child is now empty
            index.getPageStore().free(page.getPos(), true);
            if (entryCount < 1) {
                // no more children - this page is empty as well
                return row;
            }
            if (at == entryCount) {
                // removing the last child
                last = getRow(at - 1);
            } else {
                last = null;
            }
            removeChild(at);
            index.getPageStore().update(this);
            return last;
        }
        // the last row is in the last child
        if (at == entryCount) {
            return last;
        }
        int child = childPageIds[at];
        removeChild(at);
        // TODO this can mean only the position is now stored
        // should split at the next possible moment
        addChild(at, child, last);
        // remove and add swapped two children, fix that
        int temp = childPageIds[at];
        childPageIds[at] = childPageIds[at + 1];
        childPageIds[at + 1] = temp;
        index.getPageStore().update(this);
        return null;
    }

    int getRowCount() throws SQLException {
        if (rowCount == UNKNOWN_ROWCOUNT) {
            int count = 0;
            for (int child : childPageIds) {
                PageBtree page = index.getPage(child);
                count += page.getRowCount();
                index.getDatabase().setProgress(DatabaseEventListener.STATE_SCAN_FILE, index.getName(), count, Integer.MAX_VALUE);
            }
            rowCount = count;
        }
        return rowCount;
    }

    void setRowCountStored(int rowCount) throws SQLException {
        this.rowCount = rowCount;
        if (rowCountStored != rowCount) {
            rowCountStored = rowCount;
            index.getPageStore().logUndo(this, data);
            if (written) {
                writeHead();
            }
            index.getPageStore().update(this);
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
        index.getPageStore().checkUndo(getPos());
        index.getPageStore().writePage(getPos(), data);
    }

    private void writeHead() {
        data.reset();
        data.writeByte((byte) (Page.TYPE_BTREE_NODE | (onlyPosition ? 0 : Page.FLAG_LAST)));
        data.writeShortInt(0);
        data.writeInt(parentPageId);
        data.writeVarInt(index.getId());
        data.writeInt(rowCountStored);
        data.writeShortInt(entryCount);
    }

    private void write() throws SQLException {
        if (written) {
            return;
        }
        readAllRows();
        writeHead();
        data.writeInt(childPageIds[entryCount]);
        for (int i = 0; i < entryCount; i++) {
            data.writeInt(childPageIds[i]);
            data.writeShortInt(offsets[i]);
        }
        for (int i = 0; i < entryCount; i++) {
            index.writeRow(data, offsets[i], rows[i], onlyPosition);
        }
        written = true;
    }

    void freeChildren() throws SQLException {
        for (int i = 0; i <= entryCount; i++) {
            int childPageId = childPageIds[i];
            PageBtree child = index.getPage(childPageId);
            index.getPageStore().free(childPageId, false);
            child.freeChildren();
        }
    }

    private void removeChild(int i) throws SQLException {
        readAllRows();
        entryCount--;
        written = false;
        if (entryCount < 0) {
            Message.throwInternalError();
        }
        SearchRow[] newRows = PageStore.newSearchRows(entryCount);
        int[] newOffsets = MemoryUtils.newIntArray(entryCount);
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
     * @param ROW the current row
     */
    void nextPage(PageBtreeCursor cursor, int pageId) throws SQLException {
        int i;
        // TODO maybe keep the index in the child page (transiently)
        for (i = 0; i < childPageIds.length; i++) {
            if (childPageIds[i] == pageId) {
                i++;
                break;
            }
        }
        if (i > entryCount) {
            if (parentPageId == PageBtree.ROOT) {
                cursor.setCurrent(null, 0);
                return;
            }
            PageBtreeNode next = (PageBtreeNode) index.getPage(parentPageId);
            next.nextPage(cursor, getPos());
            return;
        }
        PageBtree page = index.getPage(childPageIds[i]);
        PageBtreeLeaf leaf = page.getFirstLeaf();
        cursor.setCurrent(leaf, 0);
    }

    /**
     * Set the cursor to the last row of the previous page.
     *
     * @param cursor the cursor
     * @param ROW the current row
     */
    void previousPage(PageBtreeCursor cursor, int pageId) throws SQLException {
        int i;
        // TODO maybe keep the index in the child page (transiently)
        for (i = childPageIds.length - 1; i >= 0; i--) {
            if (childPageIds[i] == pageId) {
                i--;
                break;
            }
        }
        if (i < 0) {
            if (parentPageId == PageBtree.ROOT) {
                cursor.setCurrent(null, 0);
                return;
            }
            PageBtreeNode previous = (PageBtreeNode) index.getPage(parentPageId);
            previous.previousPage(cursor, getPos());
            return;
        }
        PageBtree page = index.getPage(childPageIds[i]);
        PageBtreeLeaf leaf = page.getLastLeaf();
        cursor.setCurrent(leaf, leaf.entryCount - 1);
    }


    public String toString() {
        return "page[" + getPos() + "] b-tree node table:" + index.getId() + " entries:" + entryCount;
    }

    public void moveTo(Session session, int newPos) throws SQLException {
        PageStore store = index.getPageStore();
        store.logUndo(this, data);
        PageBtreeNode p2 = PageBtreeNode.create(index, newPos, parentPageId);
        readAllRows();
        p2.rowCountStored = rowCountStored;
        p2.rowCount = rowCount;
        p2.childPageIds = childPageIds;
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
        for (int i = 0; i < childPageIds.length; i++) {
            PageBtree p = (PageBtree) store.getPage(childPageIds[i]);
            p.setParentPageId(newPos);
            store.update(p);
        }
        store.free(getPos(), true);
    }

    /**
     * One of the children has moved to a new page.
     *
     * @param oldPos the old position
     * @param newPos the new position
     */
    void moveChild(int oldPos, int newPos) throws SQLException {
        for (int i = 0; i < childPageIds.length; i++) {
            if (childPageIds[i] == oldPos) {
                index.getPageStore().logUndo(this, data);
                written = false;
                childPageIds[i] = newPos;
                index.getPageStore().update(this);
                return;
            }
        }
        throw Message.throwInternalError();
    }

}