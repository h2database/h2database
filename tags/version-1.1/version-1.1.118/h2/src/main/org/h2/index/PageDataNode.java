/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;
import java.util.Arrays;
import org.h2.constant.ErrorCode;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.result.Row;
import org.h2.store.Data;
import org.h2.store.DataPage;
import org.h2.store.Page;
import org.h2.store.PageStore;
import org.h2.util.MemoryUtils;

/**
 * A leaf page that contains data of one or multiple rows.
 * Format:
 * <ul><li>0-3: parent page id
 * </li><li>4-4: page type
 * </li><li>5-8: index id
 * </li><li>9-10: entry count
 * </li><li>11-14: row count of all children (-1 if not known)
 * </li><li>15-18: rightmost child page id
 * </li><li>19- entries: 4 bytes leaf page id, 4 bytes key
 * </li></ul>
 * The key is the largest key of the respective child, meaning
 * key[0] is the largest key of child[0].
 */
public class PageDataNode extends PageData {

    private static final int ENTRY_START = 19;

    private static final int ENTRY_LENGTH = 8;

    /**
     * The page ids of the children.
     */
    private int[] childPageIds;

    private int rowCountStored = UNKNOWN_ROWCOUNT;

    private int rowCount = UNKNOWN_ROWCOUNT;

    PageDataNode(PageScanIndex index, int pageId, Data data) {
        super(index, pageId, data);
    }

    /**
     * Read a data node page.
     *
     * @param index the index
     * @param data the data
     * @param pageId the page id
     * @return the page
     */
    public static Page read(PageScanIndex index, Data data, int pageId) throws SQLException {
        PageDataNode p = new PageDataNode(index, pageId, data);
        p.read();
        return p;
    }

    private void read() throws SQLException {
        data.reset();
        this.parentPageId = data.readInt();
        data.readByte();
        int indexId = data.readInt();
        if (indexId != index.getId()) {
            throw Message.getSQLException(ErrorCode.FILE_CORRUPTED_1,
                    "page:" + getPos() + " expected index:" + index.getId() +
                    "got:" + indexId);
        }
        entryCount = data.readShortInt();
        rowCount = rowCountStored = data.readInt();
        childPageIds = new int[entryCount + 1];
        childPageIds[entryCount] = data.readInt();
        keys = MemoryUtils.newInts(entryCount);
        for (int i = 0; i < entryCount; i++) {
            childPageIds[i] = data.readInt();
            keys[i] = data.readInt();
        }
        check();
    }

    private void addChild(int x, int childPageId, int key) {
        written = false;
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

    int addRowTry(Row row) throws SQLException {
        while (true) {
            int x = find(row.getPos());
            PageData page = index.getPage(childPageIds[x], getPos());
            int splitPoint = page.addRowTry(row);
            if (splitPoint == -1) {
                break;
            }
            int maxEntries = (index.getPageStore().getPageSize() - ENTRY_START) / ENTRY_LENGTH;
            if (entryCount >= maxEntries) {
                return entryCount / 2;
            }
            int pivot = splitPoint == 0 ? row.getPos() : page.getKey(splitPoint - 1);
            PageData page2 = page.split(splitPoint);
            index.getPageStore().updateRecord(page, true, page.data);
            index.getPageStore().updateRecord(page2, true, page2.data);
            addChild(x, page2.getPos(), pivot);
            index.getPageStore().updateRecord(this, true, data);
        }
        updateRowCount(1);
        return -1;
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

    Cursor find(Session session) throws SQLException {
        int child = childPageIds[0];
        return index.getPage(child, getPos()).find(session);
    }

    PageData split(int splitPoint) throws SQLException {
        int newPageId = index.getPageStore().allocatePage();
        PageDataNode p2 = new PageDataNode(index, newPageId, index.getPageStore().createData());
        p2.parentPageId = parentPageId;
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
        for (int child : childPageIds) {
            PageData p = index.getPage(child, -1);
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
        childPageIds = new int[] { page1.getPos(), page2.getPos() };
        keys = new int[] { pivot };
        check();
    }

    int getLastKey() throws SQLException {
        return index.getPage(childPageIds[entryCount], getPos()).getLastKey();
    }

    /**
     * Get the next leaf page.
     *
     * @param key the last key of the current page
     * @return the next leaf page
     */
    PageDataLeaf getNextPage(int key) throws SQLException {
        int i = find(key) + 1;
        if (i > entryCount) {
            if (parentPageId == PageData.ROOT) {
                return null;
            }
            PageDataNode next = (PageDataNode) index.getPage(parentPageId, -1);
            return next.getNextPage(key);
        }
        PageData page = index.getPage(childPageIds[i], getPos());
        return page.getFirstLeaf();
    }

    PageDataLeaf getFirstLeaf() throws SQLException {
        int child = childPageIds[0];
        return index.getPage(child, getPos()).getFirstLeaf();
    }

    boolean remove(int key) throws SQLException {
        int at = find(key);
        // merge is not implemented to allow concurrent usage
        // TODO maybe implement merge
        PageData page = index.getPage(childPageIds[at], getPos());
        boolean empty = page.remove(key);
        updateRowCount(-1);
        if (!empty) {
            // the first row didn't change - nothing to do
            return false;
        }
        // this child is now empty
        index.getPageStore().freePage(page.getPos(), true, page.data);
        if (entryCount < 1) {
            // no more children - this page is empty as well
            return true;
        }
        removeChild(at);
        index.getPageStore().updateRecord(this, true, data);
        return false;
    }

    void freeChildren() throws SQLException {
        for (int i = 0; i <= entryCount; i++) {
            int childPageId = childPageIds[i];
            PageData child = index.getPage(childPageId, getPos());
            index.getPageStore().freePage(childPageId, false, null);
            child.freeChildren();
        }
    }

    Row getRow(int key) throws SQLException {
        int at = find(key);
        PageData page = index.getPage(childPageIds[at], getPos());
        return page.getRow(key);
    }

    int getRowCount() throws SQLException {
        if (rowCount == UNKNOWN_ROWCOUNT) {
            int count = 0;
            for (int child : childPageIds) {
                PageData page = index.getPage(child, getPos());
                if (getPos() == page.getPos()) {
                    throw Message.throwInternalError("Page it its own child: " + getPos());
                }
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
        write();
        index.getPageStore().writePage(getPos(), data);
    }

    private void write() {
        if (written) {
            return;
        }
        check();
        data.reset();
        data.writeInt(parentPageId);
        data.writeByte((byte) Page.TYPE_DATA_NODE);
        data.writeInt(index.getId());
        data.writeShortInt(entryCount);
        data.writeInt(rowCountStored);
        data.writeInt(childPageIds[entryCount]);
        for (int i = 0; i < entryCount; i++) {
            data.writeInt(childPageIds[i]);
            data.writeInt(keys[i]);
        }
        written = true;
    }

    private void removeChild(int i) {
        written = false;
        entryCount--;
        if (entryCount < 0) {
            Message.throwInternalError();
        }
        int[] newKeys = MemoryUtils.newInts(entryCount);
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

    public String toString() {
        return "page[" + getPos() + "] data node table:" + index.getId() + " entries:" + entryCount + " " + Arrays.toString(childPageIds);
    }

    public void moveTo(Session session, int newPos) throws SQLException {
        PageStore store = index.getPageStore();
        PageDataNode p2 = new PageDataNode(index, newPos, store.createData());
        p2.rowCountStored = rowCountStored;
        p2.rowCount = rowCount;
        p2.childPageIds = childPageIds;
        p2.keys = keys;
        p2.entryCount = entryCount;
        p2.parentPageId = parentPageId;
        store.updateRecord(p2, false, null);
        if (parentPageId == ROOT) {
            index.setRootPageId(session, newPos);
        } else {
            PageDataNode p = (PageDataNode) store.getPage(parentPageId);
            p.moveChild(getPos(), newPos);
        }
        for (int i = 0; i < childPageIds.length; i++) {
            PageData p = (PageData) store.getPage(childPageIds[i]);
            p.setParentPageId(newPos);
            store.updateRecord(p, true, p.data);
        }
        store.freePage(getPos(), true, data);
    }

    /**
     * One of the children has moved to another page.
     *
     * @param oldPos the old position
     * @param newPos the new position
     */
    void moveChild(int oldPos, int newPos) throws SQLException {
        for (int i = 0; i < childPageIds.length; i++) {
            if (childPageIds[i] == oldPos) {
                written = false;
                childPageIds[i] = newPos;
                index.getPageStore().updateRecord(this, true, data);
                return;
            }
        }
        throw Message.throwInternalError();
    }

}
