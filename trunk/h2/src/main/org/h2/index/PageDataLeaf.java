/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.lang.ref.SoftReference;
import java.util.Arrays;
import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.store.Data;
import org.h2.store.Page;
import org.h2.store.PageStore;

/**
 * A leaf page that contains data of one or multiple rows. Format:
 * <ul>
 * <li>page type: byte (0)</li>
 * <li>checksum: short (1-2)</li>
 * <li>parent page id (0 for root): int (3-6)</li>
 * <li>table id: varInt</li>
 * <li>column count: varInt</li>
 * <li>entry count: short</li>
 * <li>with overflow: the first overflow page id: int</li>
 * <li>list of key / offset pairs (key: varLong, offset: shortInt)</li>
 * <li>data</li>
 * </ul>
 */
public class PageDataLeaf extends PageData {

    /**
     * The row offsets.
     */
    private int[] offsets;

    /**
     * The rows.
     */
    private Row[] rows;

    /**
     * For pages with overflow: the soft reference to the row
     */
    private SoftReference<Row> rowRef;

    /**
     * The page id of the first overflow page (0 if no overflow).
     */
    private int firstOverflowPageId;

    /**
     * The start of the data area.
     */
    private int start;

    /**
     * The size of the row in bytes for large rows.
     */
    private int overflowRowSize;

    private int columnCount;

    private int memorySize;

    private PageDataLeaf(PageDataIndex index, int pageId, Data data) {
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
    static PageDataLeaf create(PageDataIndex index, int pageId, int parentPageId) {
        PageDataLeaf p = new PageDataLeaf(index, pageId, index.getPageStore().createData());
        index.getPageStore().logUndo(p, null);
        p.parentPageId = parentPageId;
        p.columnCount = index.getTable().getColumns().length;
        p.writeHead();
        p.start = p.data.length();
        return p;
    }

    /**
     * Read a data leaf page.
     *
     * @param index the index
     * @param data the data
     * @param pageId the page id
     * @return the page
     */
    public static Page read(PageDataIndex index, Data data, int pageId) {
        PageDataLeaf p = new PageDataLeaf(index, pageId, data);
        p.read();
        return p;
    }

    private void read() {
        data.reset();
        int type = data.readByte();
        data.readShortInt();
        this.parentPageId = data.readInt();
        int tableId = data.readVarInt();
        if (tableId != index.getId()) {
            throw DbException.get(ErrorCode.FILE_CORRUPTED_1,
                    "page:" + getPos() + " expected table:" + index.getId() +
                    " got:" + tableId + " type:" + type);
        }
        columnCount = data.readVarInt();
        entryCount = data.readShortInt();
        offsets = new int[entryCount];
        keys = new long[entryCount];
        rows = new Row[entryCount];
        if (type == Page.TYPE_DATA_LEAF) {
            if (entryCount != 1) {
                DbException.throwInternalError("entries: " + entryCount);
            }
            firstOverflowPageId = data.readInt();
        }
        for (int i = 0; i < entryCount; i++) {
            keys[i] = data.readVarLong();
            offsets[i] = data.readShortInt();
        }
        start = data.length();
        written = true;
    }

    private int getRowLength(Row row) {
        int size = 0;
        for (int i = 0; i < columnCount; i++) {
            size += data.getValueLen(row.getValue(i));
        }
        return size;
    }

    private int findInsertionPoint(long key) {
        int x = find(key);
        if (x < keys.length && keys[x] == key) {
            throw index.getDuplicateKeyException();
        }
        return x;
    }

    int addRowTry(Row row) {
        index.getPageStore().logUndo(this, data);
        int rowLength = getRowLength(row);
        int pageSize = index.getPageStore().getPageSize();
        int last = entryCount == 0 ? pageSize : offsets[entryCount - 1];
        int keyOffsetPairLen = 2 + Data.getVarLongLen(row.getKey());
        if (entryCount > 0 && last - rowLength < start + keyOffsetPairLen) {
            int x = findInsertionPoint(row.getKey());
            if (entryCount > 1) {
                if (entryCount < 5) {
                    // required, otherwise the index doesn't work correctly
                    return entryCount / 2;
                }
                if (index.isSortedInsertMode()) {
                    return x < 2 ? 1 : x > entryCount - 1 ? entryCount - 1 : x;
                }
                // split near the insertion point to better fill pages
                // split in half would be:
                // return entryCount / 2;
                int third = entryCount / 3;
                return x < third ? third : x >= 2 * third ? 2 * third : x;
            }
            return x;
        }
        index.getPageStore().logUndo(this, data);
        int offset = last - rowLength;
        int[] newOffsets = new int[entryCount + 1];
        long[] newKeys = new long[entryCount + 1];
        Row[] newRows = new Row[entryCount + 1];
        int x;
        if (entryCount == 0) {
            x = 0;
        } else {
            readAllRows();
            x = findInsertionPoint(row.getKey());
            System.arraycopy(offsets, 0, newOffsets, 0, x);
            System.arraycopy(keys, 0, newKeys, 0, x);
            System.arraycopy(rows, 0, newRows, 0, x);
            if (x < entryCount) {
                for (int j = x; j < entryCount; j++) {
                    newOffsets[j + 1] = offsets[j] - rowLength;
                }
                System.arraycopy(keys, x, newKeys, x + 1, entryCount - x);
                System.arraycopy(rows, x, newRows, x + 1, entryCount - x);
            }
        }
        written = false;
        changeCount = index.getPageStore().getChangeCount();
        last = x == 0 ? pageSize : offsets[x - 1];
        offset = last - rowLength;
        entryCount++;
        start += keyOffsetPairLen;
        newOffsets[x] = offset;
        newKeys[x] = row.getKey();
        newRows[x] = row;
        memorySize += row.getMemorySize();
        offsets = newOffsets;
        keys = newKeys;
        rows = newRows;
        index.getPageStore().update(this);
        if (offset < start) {
            if (entryCount > 1) {
                DbException.throwInternalError();
            }
            // need to write the overflow page id
            start += 4;
            int remaining = rowLength - (pageSize - start);
            // fix offset
            offset = start;
            offsets[x] = offset;
            int previous = getPos();
            int dataOffset = pageSize;
            int page = index.getPageStore().allocatePage();
            firstOverflowPageId = page;
            this.overflowRowSize = pageSize + rowLength;
            writeData();
            // free up the space used by the row
            rowRef = new SoftReference<Row>(rows[0]);
            rows[0] = null;
            Data all = index.getPageStore().createData();
            all.checkCapacity(data.length());
            all.write(data.getBytes(), 0, data.length());
            data.truncate(index.getPageStore().getPageSize());
            do {
                int type, size, next;
                if (remaining <= pageSize - PageDataOverflow.START_LAST) {
                    type = Page.TYPE_DATA_OVERFLOW | Page.FLAG_LAST;
                    size = remaining;
                    next = 0;
                } else {
                    type = Page.TYPE_DATA_OVERFLOW;
                    size = pageSize - PageDataOverflow.START_MORE;
                    next = index.getPageStore().allocatePage();
                }
                PageDataOverflow overflow = PageDataOverflow.create(index.getPageStore(), page, type, previous, next, all, dataOffset, size);
                index.getPageStore().update(overflow);
                dataOffset += size;
                remaining -= size;
                previous = page;
                page = next;
            } while (remaining > 0);
        }
        return -1;
    }

    private void removeRow(int i) {
        index.getPageStore().logUndo(this, data);
        written = false;
        changeCount = index.getPageStore().getChangeCount();
        readAllRows();
        Row r = rows[i];
        if (r != null) {
            memorySize += r.getMemorySize();
        }
        entryCount--;
        if (entryCount < 0) {
            DbException.throwInternalError();
        }
        freeOverflow();
        firstOverflowPageId = 0;
        overflowRowSize = 0;
        rowRef = null;
        int keyOffsetPairLen = 2 + Data.getVarLongLen(keys[i]);
        int[] newOffsets = new int[entryCount];
        long[] newKeys = new long[entryCount];
        Row[] newRows = new Row[entryCount];
        System.arraycopy(offsets, 0, newOffsets, 0, i);
        System.arraycopy(keys, 0, newKeys, 0, i);
        System.arraycopy(rows, 0, newRows, 0, i);
        int startNext = i > 0 ? offsets[i - 1] : index.getPageStore().getPageSize();
        int rowLength = startNext - offsets[i];
        int clearStart = offsets[entryCount];
        Arrays.fill(data.getBytes(), clearStart, clearStart + rowLength, (byte) 0);
        for (int j = i; j < entryCount; j++) {
            newOffsets[j] = offsets[j + 1] + rowLength;
        }
        System.arraycopy(keys, i + 1, newKeys, i, entryCount - i);
        System.arraycopy(rows, i + 1, newRows, i, entryCount - i);
        start -= keyOffsetPairLen;
        offsets = newOffsets;
        keys = newKeys;
        rows = newRows;
    }

    Cursor find(Session session, long min, long max, boolean multiVersion) {
        int x = find(min);
        return new PageDataCursor(session, this, x, max, multiVersion);
    }

    /**
     * Get the row at the given index.
     *
     * @param at the index
     * @return the row
     */
    Row getRowAt(int at) {
        Row r = rows[at];
        if (r == null) {
            if (firstOverflowPageId == 0) {
                data.setPos(offsets[at]);
                r = index.readRow(data, columnCount);
            } else {
                if (rowRef != null) {
                    r = rowRef.get();
                    if (r != null) {
                        return r;
                    }
                }
                PageStore store = index.getPageStore();
                Data buff = store.createData();
                int pageSize = store.getPageSize();
                int offset = offsets[at];
                buff.write(data.getBytes(), offset, pageSize - offset);
                int next = firstOverflowPageId;
                do {
                    PageDataOverflow page = index.getPageOverflow(next);
                    next = page.readInto(buff);
                } while (next != 0);
                overflowRowSize = pageSize + buff.length();
                buff.setPos(0);
                r = index.readRow(buff, columnCount);
            }
            r.setKey(keys[at]);
            if (firstOverflowPageId != 0) {
                rowRef = new SoftReference<Row>(r);
            } else {
                rows[at] = r;
                memorySize += r.getMemorySize();
            }
        }
        return r;
    }

    int getEntryCount() {
        return entryCount;
    }

    PageData split(int splitPoint) {
        int newPageId = index.getPageStore().allocatePage();
        PageDataLeaf p2 = PageDataLeaf.create(index, newPageId, parentPageId);
        for (int i = splitPoint; i < entryCount;) {
            int split = p2.addRowTry(getRowAt(splitPoint));
            if (split != -1) {
                DbException.throwInternalError("split " + split);
            }
            removeRow(splitPoint);
        }
        return p2;
    }

    long getLastKey() {
        // TODO re-use keys, but remove this mechanism
        if (entryCount == 0) {
            return 0;
        }
        return getRowAt(entryCount - 1).getKey();
    }

    PageDataLeaf getNextPage() {
        if (parentPageId == PageData.ROOT) {
            return null;
        }
        PageDataNode next = (PageDataNode) index.getPage(parentPageId, -1);
        return next.getNextPage(keys[entryCount - 1]);
    }

    PageDataLeaf getFirstLeaf() {
        return this;
    }

    protected void remapChildren(int old) {
        if (firstOverflowPageId == 0) {
            return;
        }
        PageDataOverflow overflow = index.getPageOverflow(firstOverflowPageId);
        overflow.setParentPageId(getPos());
        index.getPageStore().update(overflow);
    }

    boolean remove(long key) {
        int i = find(key);
        if (keys[i] != key) {
            throw DbException.get(ErrorCode.ROW_NOT_FOUND_WHEN_DELETING_1, index.getSQL() + ": " + key + " " + keys[i]);
        }
        index.getPageStore().logUndo(this, data);
        if (entryCount == 1) {
            freeRecursive();
            return true;
        }
        removeRow(i);
        index.getPageStore().update(this);
        return false;
    }

    void freeRecursive() {
        index.getPageStore().logUndo(this, data);
        index.getPageStore().free(getPos());
        freeOverflow();
    }

    private void freeOverflow() {
        if (firstOverflowPageId != 0) {
            int next = firstOverflowPageId;
            do {
                PageDataOverflow page = index.getPageOverflow(next);
                page.free();
                next = page.getNextOverflow();
            } while (next != 0);
        }
    }

    Row getRow(long key) {
        int at = find(key);
        return getRowAt(at);
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
        data.truncate(index.getPageStore().getPageSize());
    }

    private void readAllRows() {
        for (int i = 0; i < entryCount; i++) {
            getRowAt(i);
        }
    }

    private void writeHead() {
        data.reset();
        int type;
        if (firstOverflowPageId == 0) {
            type = Page.TYPE_DATA_LEAF | Page.FLAG_LAST;
        } else {
            type = Page.TYPE_DATA_LEAF;
        }
        data.writeByte((byte) type);
        data.writeShortInt(0);
        if (SysProperties.CHECK2) {
            if (data.length() != START_PARENT) {
                DbException.throwInternalError();
            }
        }
        data.writeInt(parentPageId);
        data.writeVarInt(index.getId());
        data.writeVarInt(columnCount);
        data.writeShortInt(entryCount);
    }

    private void writeData() {
        if (written) {
            return;
        }
        readAllRows();
        writeHead();
        if (firstOverflowPageId != 0) {
            data.writeInt(firstOverflowPageId);
            data.checkCapacity(overflowRowSize);
        }
        for (int i = 0; i < entryCount; i++) {
            data.writeVarLong(keys[i]);
            data.writeShortInt(offsets[i]);
        }
        for (int i = 0; i < entryCount; i++) {
            data.setPos(offsets[i]);
            Row r = getRowAt(i);
            for (int j = 0; j < columnCount; j++) {
                data.writeValue(r.getValue(j));
            }
        }
        written = true;
    }

    public String toString() {
        return "page[" + getPos() + "] data leaf table:" + index.getId() +
            " entries:" + entryCount + " parent:" + parentPageId +
            (firstOverflowPageId == 0 ? "" : " overflow:" + firstOverflowPageId) +
            " keys:" + Arrays.toString(keys) + " offsets:" + Arrays.toString(offsets);
    }

    public void moveTo(Session session, int newPos) {
        PageStore store = index.getPageStore();
        // load the pages into the cache, to ensure old pages
        // are written
        if (parentPageId != ROOT) {
            store.getPage(parentPageId);
        }
        store.logUndo(this, data);
        PageDataLeaf p2 = PageDataLeaf.create(index, newPos, parentPageId);
        readAllRows();
        p2.keys = keys;
        p2.overflowRowSize = overflowRowSize;
        p2.firstOverflowPageId = firstOverflowPageId;
        p2.rowRef = rowRef;
        p2.rows = rows;
        if (firstOverflowPageId != 0) {
            p2.rows[0] = getRowAt(0);
        }
        p2.entryCount = entryCount;
        p2.offsets = offsets;
        p2.start = start;
        p2.remapChildren(getPos());
        p2.writeData();
        p2.data.truncate(index.getPageStore().getPageSize());
        store.update(p2);
        store.free(getPos());
        if (parentPageId == ROOT) {
            index.setRootPageId(session, newPos);
        } else {
            PageDataNode p = (PageDataNode) store.getPage(parentPageId);
            p.moveChild(getPos(), newPos);
        }
    }

    /**
     * Set the overflow page id.
     *
     * @param old the old overflow page id
     * @param overflow the new overflow page id
     */
    void setOverflow(int old, int overflow) {
        if (SysProperties.CHECK && old != firstOverflowPageId) {
            DbException.throwInternalError("move " + this + " " + firstOverflowPageId);
        }
        index.getPageStore().logUndo(this, data);
        firstOverflowPageId = overflow;
        if (written) {
            changeCount = index.getPageStore().getChangeCount();
            writeHead();
            data.writeInt(firstOverflowPageId);
        }
        index.getPageStore().update(this);
    }

    public int getMemorySize() {
        return index.getMemorySizePerPage();
    }

}
