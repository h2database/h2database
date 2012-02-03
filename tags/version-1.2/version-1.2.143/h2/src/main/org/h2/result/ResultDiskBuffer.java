/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import org.h2.constant.SysProperties;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.store.Data;
import org.h2.store.FileStore;
import org.h2.util.New;
import org.h2.value.Value;

/**
 * This class implements the disk buffer for the LocalResult class.
 */
class ResultDiskBuffer implements ResultExternal {

    private static final int READ_AHEAD = 128;

    private Data rowBuff;
    private FileStore file;
    private ArrayList<ResultDiskTape> tapes;
    private ResultDiskTape mainTape;
    private SortOrder sort;
    private int columnCount;

    /**
     * Represents a virtual disk tape for the merge sort algorithm.
     * Each virtual disk tape is a region of the temp file.
     */
    static class ResultDiskTape {

        /**
         * The start position of this tape in the file.
         */
        long start;

        /**
         * The end position of this tape in the file.
         */
        long end;

        /**
         * The current read position.
         */
        long pos;

        /**
         * A list of rows in the buffer.
         */
        ArrayList<Value[]> buffer = New.arrayList();
    }

    ResultDiskBuffer(Session session, SortOrder sort, int columnCount) {
        this.sort = sort;
        this.columnCount = columnCount;
        Database db = session.getDatabase();
        rowBuff = Data.create(db, SysProperties.PAGE_SIZE);
        String fileName = session.getDatabase().createTempFile();
        file = session.getDatabase().openFile(fileName, "rw", false);
        file.setCheckedWriting(false);
        file.seek(FileStore.HEADER_LENGTH);
        if (sort != null) {
            tapes = New.arrayList();
        } else {
            mainTape = new ResultDiskTape();
            mainTape.pos = FileStore.HEADER_LENGTH;
        }
    }

    public void addRows(ArrayList<Value[]> rows) {
        if (sort != null) {
            sort.sort(rows);
        }
        Data buff = rowBuff;
        long start = file.getFilePointer();
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int bufferLen = 0;
        int maxBufferSize = SysProperties.LARGE_RESULT_BUFFER_SIZE;
        for (Value[] row : rows) {
            buff.reset();
            buff.writeInt(0);
            for (int j = 0; j < columnCount; j++) {
                Value v = row[j];
                buff.checkCapacity(buff.getValueLen(v));
                buff.writeValue(v);
            }
            buff.fillAligned();
            int len = buff.length();
            buff.setInt(0, len);
            if (maxBufferSize > 0) {
                buffer.write(buff.getBytes(), 0, len);
                bufferLen += len;
                if (bufferLen > maxBufferSize) {
                    byte[] data = buffer.toByteArray();
                    buffer.reset();
                    file.write(data, 0, data.length);
                    bufferLen = 0;
                }
            } else {
                file.write(buff.getBytes(), 0, len);
            }
        }
        if (bufferLen > 0) {
            byte[] data = buffer.toByteArray();
            file.write(data, 0, data.length);
        }
        if (sort != null) {
            ResultDiskTape tape = new ResultDiskTape();
            tape.start = start;
            tape.end = file.getFilePointer();
            tapes.add(tape);
        } else {
            mainTape.end = file.getFilePointer();
        }
    }

    public void done() {
        file.seek(FileStore.HEADER_LENGTH);
        file.autoDelete();
    }

    public void reset() {
        if (sort != null) {
            for (ResultDiskTape tape : tapes) {
                tape.pos = tape.start;
                tape.buffer = New.arrayList();
            }
        } else {
            mainTape.pos = FileStore.HEADER_LENGTH;
            mainTape.buffer = New.arrayList();
        }
    }

    private void readRow(ResultDiskTape tape) {
        int min = Constants.FILE_BLOCK_SIZE;
        Data buff = rowBuff;
        buff.reset();
        file.readFully(buff.getBytes(), 0, min);
        int len = buff.readInt();
        buff.checkCapacity(len);
        if (len - min > 0) {
            file.readFully(buff.getBytes(), min, len - min);
        }
        tape.pos += len;
        Value[] row = new Value[columnCount];
        for (int k = 0; k < columnCount; k++) {
            row[k] = buff.readValue();
        }
        tape.buffer.add(row);
    }

    public Value[] next() {
        return sort != null ? nextSorted() : nextUnsorted();
    }

    private Value[] nextUnsorted() {
        file.seek(mainTape.pos);
        if (mainTape.buffer.size() == 0) {
            for (int j = 0; mainTape.pos < mainTape.end && j < READ_AHEAD; j++) {
                readRow(mainTape);
            }
        }
        Value[] row = mainTape.buffer.get(0);
        mainTape.buffer.remove(0);
        return row;
    }

    private Value[] nextSorted() {
        int next = -1;
        for (int i = 0; i < tapes.size(); i++) {
            ResultDiskTape tape = tapes.get(i);
            if (tape.buffer.size() == 0 && tape.pos < tape.end) {
                file.seek(tape.pos);
                for (int j = 0; tape.pos < tape.end && j < READ_AHEAD; j++) {
                    readRow(tape);
                }
            }
            if (tape.buffer.size() > 0) {
                if (next == -1) {
                    next = i;
                } else if (compareTapes(tape, tapes.get(next)) < 0) {
                    next = i;
                }
            }
        }
        ResultDiskTape t = tapes.get(next);
        Value[] row = t.buffer.get(0);
        t.buffer.remove(0);
        return row;
    }

    private int compareTapes(ResultDiskTape a, ResultDiskTape b) {
        Value[] va = a.buffer.get(0);
        Value[] vb = b.buffer.get(0);
        return sort.compare(va, vb);
    }

    protected void finalize() {
        if (!SysProperties.runFinalize) {
            return;
        }
        close();
    }

    public void close() {
        if (file != null) {
            file.closeAndDeleteSilently();
            file = null;
        }
    }

    public int removeRow(Value[] values) {
        throw DbException.throwInternalError();
    }

    public boolean contains(Value[] values) {
        throw DbException.throwInternalError();
    }

    public int addRow(Value[] values) {
        throw DbException.throwInternalError();
    }

}
