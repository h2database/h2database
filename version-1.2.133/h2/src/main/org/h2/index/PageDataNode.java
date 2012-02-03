/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.util.Arrays;
import org.h2.api.DatabaseEventListener;
import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.store.Data;
import org.h2.store.Page;
import org.h2.store.PageStore;
import org.h2.util.Utils;

/**
 * A leaf page that contains data of one or multiple rows. Format:
 * <ul>
 * <li>page type: byte (0)</li>
 * <li>checksum: short (1-2)</li>
 * <li>parent page id (0 for root): int (3-6)</li>
 * <li>table id: varInt</li>
 * <li>count of all children (-1 if not known): int</li>
 * <li>entry count: short</li>
 * <li>rightmost child page id: int</li>
 * <li>entries (child page id: int, key: varLong)</li>
 * </ul>
 * The key is the largest key of the respective child, meaning key[0] is the
 * largest key of child[0].
 */
public class PageDataNode extends PageData {

    /**
     * The page ids of the children.
     */
    private int[] childPageIds;

    private int rowCountStored = UNKNOWN_ROWCOUNT;

    private int rowCount = UNKNOWN_ROWCOUNT;

    /**
     * The number of bytes used in the page
     */
    private int length;

    private PageDataNode(PageDataIndex index, int pageId, Data data) {
        super(index, pageId, data);
    }

    /**
     * Create a new page.
     *
     * @param index the index
     * @param pageId the page id
     * @param parentPageId the parent
     * @return the page
     */
    static PageDataNode create(PageDataIndex index, int pageId, int parentPageId) {
        PageDataNode p = new PageDataNode(index, pageId, index.getPageStore().createData());
        index.getPageStore().logUndo(p, null);
        p.parentPageId = parentPageId;
        p.writeHead();
        // 4 bytes for the rightmost child page id
        p.length = p.data.length() + 4;
        return p;
    }

    /**
     * Read a data node page.
     *
     * @param index the index
     * @param data the data
     * @param pageId the page id
     * @return the page
     */
    public static Page read(PageDataIndex index, Data data, int pageId) {
        PageDataNode p = new PageDataNode(index, pageId, data);
        p.read();
        return p;
    }

    private void read() {
        data.reset();
        data.readByte();
        data.readShortInt();
        this.parentPageId = data.readInt();
        int indexId = data.readVarInt();
        if (indexId != index.getId()) {
            throw DbException.get(ErrorCode.FILE_CORRUPTED_1,
                    "page:" + getPos() + " expected index:" + index.getId() +
                    "got:" + indexId);
        }
        rowCount = rowCountStored = data.readInt();
        entryCount = data.readShortInt();
        childPageIds = new int[entryCount + 1];
        childPageIds[entryCount] = data.readInt();
        keys = Utils.newLongArray(entryCount);
        for (int i = 0; i < entryCount; i++) {
            childPageIds[i] = data.readInt();
            keys[i] = data.readVarLong();
        }
        length = data.length();
        check();
        written = true;
    }

    private void addChild(int x, int childPageId, long key) {
        index.getPageStore().logUndo(this, data);
        written = false;
        changeCount = index.getPageStore().getChangeCount();
        long[] newKeys = new long[entryCount + 1];
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
        length += 4 + Data.getVarLongLen(key);
    }

    int addRowTry(Row row) {
        index.getPageStore().logUndo(this, data);
        int keyOffsetPairLen = 4 + Data.getVarLongLen(row.getKey());
        while (true) {
            int x = find(row.getKey());
            PageData page = index.getPage(childPageIds[x], getPos());
            int splitPoint = page.addRowTry(row);
            if (splitPoint == -1) {
                break;
            }
            if (length + keyOffsetPairLen > index.getPageStore().getPageSize()) {
                return entryCount / 2;
            }
            long pivot = splitPoint == 0 ? row.getKey() : page.getKey(splitPoint - 1);
            PageData page2 = page.split(splitPoint);
            index.getPageStore().update(page);
            index.getPageStore().update(page2);
            addChild(x, page2.getPos(), pivot);
            index.getPageStore().update(this);
        }
        updateRowCount(1);
        return -1;
    }

    private void updateRowCount(int offset) {
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

    Cursor find(Session session, long min, long max, boolean multiVersion) {
        int x = find(min);
        int child = childPageIds[x];
        return index.getPage(child, getPos()).find(session, min, max, multiVersion);
    }

    PageData split(int splitPoint) {
        int newPageId = index.getPageStore().allocatePage();
        PageDataNode p2 = PageDataNode.create(index, newPageId, parentPageId);
        int firstChild = childPageIds[splitPoint];
        for (int i = splitPoint; i < entryCount;) {
            p2.addChild(p2.entryCount, childPageIds[splitPoint + 1], keys[splitPoint]);
            removeChild(splitPoint);
        }
        int lastChild = childPageIds[splitPoint - 1];
        removeChild(splitPoint - 1);
        childPageIds[splitPoint - 1] = lastChild;
        p2.childPageIds[0] = firstChild;
        p2.remapChildren(getPos());
        return p2;
    }

    protected void remapChildren(int old) {
        for (int child : childPageIds) {
            PageData p = index.getPage(child, old);
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
    void init(PageData page1, long pivot, PageData page2) {
        entryCount = 1;
        childPageIds = new int[] { page1.getPos(), page2.getPos() };
        keys = new long[] { pivot };
        length += 4 + Data.getVarLongLen(pivot);
        check();
    }

    long getLastKey() {
        return index.getPage(childPageIds[entryCount], getPos()).getLastKey();
    }

    /**
     * Get the next leaf page.
     *
     * @param key the last key of the current page
     * @return the next leaf page
     */
    PageDataLeaf getNextPage(long key) {
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

    PageDataLeaf getFirstLeaf() {
        int child = childPageIds[0];
        return index.getPage(child, getPos()).getFirstLeaf();
    }

    boolean remove(long key) {
        int at = find(key);
        // merge is not implemented to allow concurrent usage
        // TODO maybe implement merge
        PageData page = index.getPage(childPageIds[at], getPos());
        boolean empty = page.remove(key);
        index.getPageStore().logUndo(this, data);
        updateRowCount(-1);
        if (!empty) {
            // the first row didn't change - nothing to do
            return false;
        }
        // this child is now empty
        index.getPageStore().free(page.getPos());
        if (entryCount < 1) {
            // no more children - this page is empty as well
            return true;
        }
        removeChild(at);
        index.getPageStore().update(this);
        return false;
    }

    void freeRecursive() {
        index.getPageStore().logUndo(this, data);
        index.getPageStore().free(getPos());
        for (int childPageId : childPageIds) {
            index.getPage(childPageId, getPos()).freeRecursive();
        }
    }

    Row getRow(long key) {
        int at = find(key);
        PageData page = index.getPage(childPageIds[at], getPos());
        return page.getRow(key);
    }

    int getRowCount() {
        if (rowCount == UNKNOWN_ROWCOUNT) {
            int count = 0;
            for (int child : childPageIds) {
                PageData page = index.getPage(child, getPos());
                if (getPos() == page.getPos()) {
                    throw DbException.throwInternalError("Page it its own child: " + getPos());
                }
                count += page.getRowCount();
                index.getDatabase().setProgress(DatabaseEventListener.STATE_SCAN_FILE, index.getTable() + "." + index.getName(), count, Integer.MAX_VALUE);
            }
            rowCount = count;
        }
        return rowCount;
    }

    void setRowCountStored(int rowCount) {
        this.rowCount = rowCount;
        if (rowCountStored != rowCount) {
            rowCountStored = rowCount;
            index.getPageStore().logUndo(this, data);
            if (written) {
                changeCount = index.getPageStore().getChangeCount();
                writeHead();
            }
            index.getPageStore().update(this);
        }
    }

    private void check() {
        for (int child : childPageIds) {
            if (child == 0) {
                DbException.throwInternalError();
            }
        }
    }

    public void write() {
        writeData();
        index.getPageStore().writePage(getPos(), data);
    }

    private void writeHead() {
        data.reset();
        data.writeByte((byte) Page.TYPE_DATA_NODE);
        data.writeShortInt(0);
        if (SysProperties.CHECK2) {
            if (data.length() != START_PARENT) {
                DbException.throwInternalError();
            }
        }
        data.writeInt(parentPageId);
        data.writeVarInt(index.getId());
        data.writeInt(rowCountStored);
        data.writeShortInt(entryCount);
    }

    private void writeData() {
        if (written) {
            return;
        }
        check();
        writeHead();
        data.writeInt(childPageIds[entryCount]);
        for (int i = 0; i < entryCount; i++) {
            data.writeInt(childPageIds[i]);
            data.writeVarLong(keys[i]);
        }
        if (length != data.length()) {
            DbException.throwInternalError("expected pos: " + length + " got: " + data.length());
        }
        written = true;
    }

    private void removeChild(int i) {
        index.getPageStore().logUndo(this, data);
        written = false;
        changeCount = index.getPageStore().getChangeCount();
        entryCount--;
        int removedKeyIndex = i < keys.length ? i : i - 1;
        length -= 4 + Data.getVarLongLen(keys[removedKeyIndex]);
        if (entryCount < 0) {
            DbException.throwInternalError();
        }
        long[] newKeys = Utils.newLongArray(entryCount);
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

    public void moveTo(Session session, int newPos) {
        PageStore store = index.getPageStore();
        // load the pages into the cache, to ensure old pages
        // are written
        for (int child : childPageIds) {
            store.getPage(child);
        }
        if (parentPageId != ROOT) {
            store.getPage(parentPageId);
        }
        store.logUndo(this, data);
        PageDataNode p2 = PageDataNode.create(index, newPos, parentPageId);
        p2.rowCountStored = rowCountStored;
        p2.rowCount = rowCount;
        p2.childPageIds = childPageIds;
        p2.keys = keys;
        p2.entryCount = entryCount;
        p2.length = length;
        store.update(p2);
        if (parentPageId == ROOT) {
            index.setRootPageId(session, newPos);
        } else {
            PageDataNode p = (PageDataNode) store.getPage(parentPageId);
            p.moveChild(getPos(), newPos);
        }
        for (int child : childPageIds) {
            PageData p = (PageData) store.getPage(child);
            p.setParentPageId(newPos);
            store.update(p);
        }
        store.free(getPos());
    }

    /**
     * One of the children has moved to another page.
     *
     * @param oldPos the old position
     * @param newPos the new position
     */
    void moveChild(int oldPos, int newPos) {
        for (int i = 0; i < childPageIds.length; i++) {
            if (childPageIds[i] == oldPos) {
                index.getPageStore().logUndo(this, data);
                written = false;
                changeCount = index.getPageStore().getChangeCount();
                childPageIds[i] = newPos;
                index.getPageStore().update(this);
                return;
            }
        }
        throw DbException.throwInternalError();
    }

}
