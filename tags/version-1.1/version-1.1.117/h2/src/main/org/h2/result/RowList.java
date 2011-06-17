/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import java.sql.SQLException;

import org.h2.constant.SysProperties;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.store.DataPage;
import org.h2.store.FileStore;
import org.h2.util.Cache;
import org.h2.util.CacheObject;
import org.h2.util.ObjectArray;
import org.h2.value.Value;
import org.h2.value.ValueLob;

/**
 * A list of rows. If the list grows too large, it is buffered to disk
 * automatically.
 */
public class RowList {

    private final Session session;
    private final ObjectArray<Row> list = ObjectArray.newInstance();
    private int size;
    private int index, listIndex;
    private FileStore file;
    private DataPage rowBuff;
    private Cache cache;
    private ObjectArray<ValueLob> lobs;
    private int memory, maxMemory;
    private boolean written;
    private boolean readUncached;

    /**
     * Construct a new row list for this session.
     *
     * @param session the session
     */
    public RowList(Session session) {
        this.session = session;
        if (SysProperties.DEFAULT_MAX_OPERATION_MEMORY > 0 && session.getDatabase().isPersistent()) {
            maxMemory = session.getDatabase().getMaxOperationMemory();
        }
    }

    private void writeRow(DataPage buff, Row r) throws SQLException {
        buff.checkCapacity(1 + DataPage.LENGTH_INT * 7);
        buff.writeByte((byte) 1);
        buff.writeInt(r.getMemorySize());
        buff.writeInt(r.getColumnCount());
        buff.writeInt(r.getPos());
        buff.writeInt(r.getVersion());
        buff.writeInt(r.isDeleted() ? 1 : 0);
        buff.writeInt(r.getSessionId());
        buff.writeInt(r.getStorageId());
        for (int i = 0; i < r.getColumnCount(); i++) {
            Value v = r.getValue(i);
            if (v.getType() == Value.CLOB || v.getType() == Value.BLOB) {
                // need to keep a reference to temporary lobs,
                // otherwise the temp file is deleted
                ValueLob lob = (ValueLob) v;
                if (lob.getSmall() == null && lob.getTableId() == 0) {
                    if (lobs == null) {
                        lobs = ObjectArray.newInstance();
                    }
                    lobs.add(lob);
                }
            }
            buff.checkCapacity(buff.getValueLen(v));
            buff.writeValue(v);
        }
    }

    private void writeAllRows() throws SQLException {
        if (file == null) {
            Database db = session.getDatabase();
            if (!db.isPageStoreEnabled()) {
                cache = db.getDataFile().getCache();
            }
            String fileName = db.createTempFile();
            file = db.openFile(fileName, "rw", false);
            file.seek(FileStore.HEADER_LENGTH);
            rowBuff = DataPage.create(db, Constants.DEFAULT_DATA_PAGE_SIZE);
            file.seek(FileStore.HEADER_LENGTH);
        }
        DataPage buff = rowBuff;
        initBuffer(buff);
        for (int i = 0; i < list.size(); i++) {
            if (i > 0 && buff.length() > Constants.IO_BUFFER_SIZE) {
                flushBuffer(buff);
                initBuffer(buff);
            }
            Row r = list.get(i);
            writeRow(buff, r);
        }
        flushBuffer(buff);
        file.autoDelete();
        list.clear();
        memory = 0;
    }

    private void initBuffer(DataPage buff) {
        buff.reset();
        buff.writeInt(0);
    }

    private void flushBuffer(DataPage buff) throws SQLException {
        buff.checkCapacity(1);
        buff.writeByte((byte) 0);
        buff.fillAligned();
        buff.setInt(0, buff.length() / Constants.FILE_BLOCK_SIZE);
        buff.updateChecksum();
        file.write(buff.getBytes(), 0, buff.length());
    }

    /**
     * Add a row to the list.
     *
     * @param r the row to add
     */
    public void add(Row r) throws SQLException {
        list.add(r);
        memory += r.getMemorySize();
        if (maxMemory > 0 && memory > maxMemory) {
            writeAllRows();
        }
        size++;
    }

    /**
     * Remove all rows from the list.
     */
    public void reset() throws SQLException {
        index = 0;
        if (file != null) {
            listIndex = 0;
            if (!written) {
                writeAllRows();
                written = true;
            }
            list.clear();
            file.seek(FileStore.HEADER_LENGTH);
        }
    }

    /**
     * Check if there are more rows in this list.
     *
     * @return true it there are more rows
     */
    public boolean hasNext() {
        return index < size;
    }

    private Row readRow(DataPage buff) throws SQLException {
        if (buff.readByte() == 0) {
            return null;
        }
        int memory = buff.readInt();
        int columnCount = buff.readInt();
        int pos = buff.readInt();
        int version = buff.readInt();
        if (readUncached) {
            pos = 0;
        }
        boolean deleted = buff.readInt() == 1;
        int sessionId = buff.readInt();
        int storageId = buff.readInt();
        Value[] values = new Value[columnCount];
        for (int i = 0; i < columnCount; i++) {
            Value v = buff.readValue();
            if (v.isLinked()) {
                ValueLob lob = (ValueLob) v;
                // the table id is 0 if it was linked when writing
                // a temporary entry
                if (lob.getTableId() == 0) {
                    session.unlinkAtCommit(lob);
                }
            }
            values[i] = v;
        }
        if (pos != 0 && cache != null) {
            CacheObject found = cache.find(pos);
            if (found != null) {
                return (Row) found;
            }
        }
        Row row = new Row(values, memory);
        row.setPos(pos);
        row.setVersion(version);
        row.setDeleted(deleted);
        row.setSessionId(sessionId);
        row.setStorageId(storageId);
        return row;
    }

    /**
     * Get the next row from the list.
     *
     * @return the next row
     */
    public Row next() throws SQLException {
        Row r;
        if (file == null) {
            r = list.get(index++);
        } else {
            if (listIndex >= list.size()) {
                list.clear();
                listIndex = 0;
                DataPage buff = rowBuff;
                buff.reset();
                int min = Constants.FILE_BLOCK_SIZE;
                file.readFully(buff.getBytes(), 0, min);
                int len = buff.readInt() * Constants.FILE_BLOCK_SIZE;
                buff.checkCapacity(len);
                if (len - min > 0) {
                    file.readFully(buff.getBytes(), min, len - min);
                }
                buff.check(len);
                for (int i = 0;; i++) {
                    r = readRow(buff);
                    if (r == null) {
                        break;
                    }
                    list.add(r);
                }
            }
            index++;
            r = list.get(listIndex++);
        }
        return r;
    }

    /**
     * Get the number of rows in this list.
     *
     * @return the number of rows
     */
    public int size() {
        return size;
    }

    /**
     * Do not use the cache.
     */
    public void invalidateCache() {
        readUncached = true;
    }

    /**
     * Close the result list and delete the temporary file.
     */
    public void close() {
        if (file != null) {
            file.autoDelete();
            file.closeAndDeleteSilently();
            file = null;
            rowBuff = null;
        }
    }

}
