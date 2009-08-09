/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.log;

import java.io.IOException;
import java.sql.SQLException;

import org.h2.api.DatabaseEventListener;
import org.h2.constant.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.store.DataPage;
import org.h2.store.DiskFile;
import org.h2.store.FileStore;
import org.h2.store.Record;
import org.h2.store.Storage;
import org.h2.util.FileUtils;
import org.h2.util.MathUtils;
import org.h2.util.MemoryUtils;
import org.h2.util.ObjectArray;

/**
 * Each transaction log file contains a number of log records.
 *
 * Header format:
 * <pre>
 * int logId (<0 means ignore: rolled back already)
 * int firstUncommittedLogRecordId (-1 if none)
 * int firstUnwrittenLogRecordId (-1 if none)
 * </pre>
 *
 * Record format:
 * <pre>
 * int block size
 * byte 'D' (delete) / 'I' (insert) / 'C' (commit) /
 *      'R' (rollback) / 'P' (prepare commit) / 'T' (truncate)
 * int session
 * [delete/insert only:]
 * int storage
 * int record.pos
 * int record.blockCount
 * [prepare commit only:]
 * string transaction
 * </pre>
 */
public class LogFile {

    /**
     * The size of the smallest possible transaction log entry in bytes.
     */
    public static final int BLOCK_SIZE = 16;

    private static final int BUFFER_SIZE = 8 * 1024;

    private LogSystem logSystem;
    private Database database;
    private int id;
    private String fileNamePrefix;
    private String fileName;
    private FileStore file;
    private int bufferPos;
    private byte[] buffer;
    private ObjectArray<Record> unwritten;
    private DataPage rowBuff;
    private int pos = LogSystem.LOG_WRITTEN;
    private int firstUncommittedPos = LogSystem.LOG_WRITTEN;
    private int firstUnwrittenPos = LogSystem.LOG_WRITTEN;

    LogFile(LogSystem log, int id, String fileNamePrefix) throws SQLException {
        this.logSystem = log;
        this.database = log.getDatabase();
        this.id = id;
        this.fileNamePrefix = fileNamePrefix;
        fileName = getFileName();
        file = log.getDatabase().openFile(fileName, log.getAccessMode(), false);
        rowBuff = log.getRowBuffer();
        buffer = new byte[BUFFER_SIZE];
        unwritten = ObjectArray.newInstance();
        try {
            readHeader();
            if (!log.getDatabase().isReadOnly()) {
                writeHeader();
            }
            pos = getBlock();
            firstUncommittedPos = pos;
        } catch (SQLException e) {
            close(false);
            throw e;
        }
    }

    /**
     * Open the file if it is in fact a log file for this database.
     *
     * @param log the log system
     * @param fileNamePrefix the expected file name prefix
     * @param fileName the file name
     * @return null or the log file
     */
    static LogFile openIfLogFile(LogSystem log, String fileNamePrefix, String fileName) throws SQLException {
        if (!fileName.endsWith(Constants.SUFFIX_LOG_FILE)) {
            return null;
        }
        if (!FileUtils.fileStartsWith(fileName, fileNamePrefix + ".")) {
            return null;
        }
        String s = fileName.substring(fileNamePrefix.length() + 1, fileName.length()
                - Constants.SUFFIX_LOG_FILE.length());
        for (int i = 0; i < s.length(); i++) {
            if (!Character.isDigit(s.charAt(i))) {
                return null;
            }
        }
        int id = Integer.parseInt(s);
        if (!FileUtils.exists(fileName)) {
            // the file could have been deleted by now (by the DelayedFileDeleter)
            return null;
        }
        return new LogFile(log, id, fileNamePrefix);
    }

    /**
     * Get the name of this transaction log file.
     *
     * @return the file name
     */
    public String getFileName() {
        return fileNamePrefix + "." + id + Constants.SUFFIX_LOG_FILE;
    }

    int getId() {
        return id;
    }

    private int getBlock() throws SQLException {
        if (file == null) {
            throw Message.getSQLException(ErrorCode.SIMULATED_POWER_OFF);
        }
        return (int) (file.getFilePointer() / BLOCK_SIZE);
    }

    private void writeBuffer(DataPage buff, Record rec) throws SQLException {
        if (file == null) {
            throw Message.getSQLException(ErrorCode.SIMULATED_POWER_OFF);
        }
        int size = MathUtils.roundUp(buff.length() + DataPage.LENGTH_FILLER, BLOCK_SIZE);
        int blockCount = size / BLOCK_SIZE;
        buff.fill(size);
        buff.setInt(0, blockCount);
        buff.updateChecksum();
        // IOLogger.getInstance().logWrite(this.fileName,
        //     file.getFilePointer(), buff.length());
        if (rec != null) {
            unwritten.add(rec);
        }
        if (buff.length() + bufferPos > buffer.length) {
            // the buffer is full
            flush();
        }
        if (buff.length() >= buffer.length) {
            // special case really long write request: write it without buffering
            file.write(buff.getBytes(), 0, buff.length());
            pos = getBlock();
            return;
        }
        System.arraycopy(buff.getBytes(), 0, buffer, bufferPos, buff.length());
        bufferPos += buff.length();
        pos = getBlock() + (bufferPos / BLOCK_SIZE);
    }

    /**
     * Write a commit entry for this session.
     *
     * @param session the session
     */
    void commit(Session session) throws SQLException {
        DataPage buff = rowBuff;
        buff.reset();
        buff.writeInt(0);
        buff.writeByte((byte) 'C');
        buff.writeInt(session.getId());
        writeBuffer(buff, null);
        if (logSystem.getFlushOnEachCommit()) {
            flush();
        }
    }

    /**
     * Write a prepare commit entry for this session.
     *
     * @param session the session
     * @param transaction the transaction name
     */
    void prepareCommit(Session session, String transaction) throws SQLException {
        DataPage buff = rowBuff;
        buff.reset();
        buff.writeInt(0);
        buff.writeByte((byte) 'P');
        buff.writeInt(session.getId());
        buff.writeString(transaction);
        writeBuffer(buff, null);
        if (logSystem.getFlushOnEachCommit()) {
            flush();
        }
    }

    private DataPage readPage() throws SQLException {
        byte[] buff = new byte[BLOCK_SIZE];
        file.readFully(buff, 0, BLOCK_SIZE);
        DataPage s = DataPage.create(database, buff);
        int blocks = Math.abs(s.readInt());
        if (blocks <= 0) {
            // Math.abs(Integer.MIN_VALUE) == Integer.MIN_VALUE
            s.reset();
        } else {
            byte[] b2 = MemoryUtils.newBytes(blocks * BLOCK_SIZE);
            System.arraycopy(buff, 0, b2, 0, BLOCK_SIZE);
            buff = b2;
            file.readFully(buff, BLOCK_SIZE, blocks * BLOCK_SIZE - BLOCK_SIZE);
            s = DataPage.create(database, buff);
            s.check(blocks * BLOCK_SIZE);
        }
        return s;
    }

    /**
     * Redo or undo one item in the log file.
     *
     * @param undo true if the operation should be undone
     * @param readOnly if the file is read only
     * @return true if there are potentially more operations
     */
    private boolean redoOrUndo(boolean undo, boolean readOnly) throws SQLException {
        int pos = getBlock();
        DataPage in = readPage();
        int blocks = in.readInt();
        if (blocks < 0) {
            return true;
        } else if (blocks == 0 && !database.isReadOnly()) {
            truncate(pos);
            return false;
        }
        char type = (char) in.readByte();
        int sessionId = in.readInt();
        if (type == 'P') {
            if (undo) {
                Message.throwInternalError("can't undo prepare commit");
            }
            String transaction = in.readString();
            logSystem.setPreparedCommitForSession(this, sessionId, pos, transaction, blocks);
            return true;
        } else if (type == 'C') {
            if (undo) {
                Message.throwInternalError("can't undo commit");
            }
            logSystem.setLastCommitForSession(sessionId, id, pos);
            return true;
        } else if (type == 'R') {
            if (undo) {
                Message.throwInternalError("can't undo rollback");
            }
            return true;
        } else if (type == 'S') {
            if (undo) {
                Message.throwInternalError("can't undo summary");
            }
        }
        if (readOnly && type != 'S') {
            return true;
        }
        if (undo) {
            if (logSystem.isSessionCommitted(sessionId, id, pos)) {
                logSystem.removeSession(sessionId);
                return true;
            }
        } else {
            if (type != 'S') {
                if (!readOnly) {
                    logSystem.addUndoLogRecord(this, pos, sessionId);
                }
            }
        }
        int storageId = in.readInt();
        Storage storage = logSystem.getStorageForRecovery(storageId);
        DataPage rec = null;
        int recordId = in.readInt();
        int blockCount = in.readInt();
        if (type != 'T') {
            rec = in.readDataPageNoSize();
        }
        switch(type) {
        case 'S': {
            int fileType = in.readByte();
            boolean diskFile;
            if (fileType == 'D') {
                diskFile = true;
            } else if (fileType == 'I') {
                diskFile = false;
            } else {
                // unknown type, maybe linear index file (future)
                break;
            }
            int sumLength = in.readInt();
            byte[] summary = MemoryUtils.newBytes(sumLength);
            if (sumLength > 0) {
                in.read(summary, 0, sumLength);
            }
            if (diskFile) {
                database.getDataFile().initFromSummary(summary);
            } else {
                database.getIndexFile().initFromSummary(summary);
            }
            break;
        }
        case 'T':
            if (undo) {
                Message.throwInternalError("cannot undo truncate");
            }
            logSystem.addRedoLog(storage, recordId, blockCount, null);
            storage.setRecordCount(0);
            storage.getDiskFile().setPageOwner(recordId / DiskFile.BLOCKS_PER_PAGE, -1);
            logSystem.setLastCommitForSession(sessionId, id, pos);
            break;
        case 'I':
            if (undo) {
                logSystem.addRedoLog(storage, recordId, blockCount, null);
                storage.setRecordCount(storage.getRecordCount() - 1);
            } else {
                logSystem.getOrAddSessionState(sessionId);
                logSystem.addRedoLog(storage, recordId, blockCount, rec);
                storage.setRecordCount(storage.getRecordCount() + 1);
            }
            break;
        case 'D':
            if (undo) {
                logSystem.addRedoLog(storage, recordId, blockCount, rec);
                storage.setRecordCount(storage.getRecordCount() + 1);
            } else {
                logSystem.getOrAddSessionState(sessionId);
                logSystem.addRedoLog(storage, recordId, blockCount, null);
                storage.setRecordCount(storage.getRecordCount() - 1);
            }
            break;
        default:
            Message.throwInternalError("type=" + type);
        }
        return true;
    }

    /**
     * Re-apply all changes of this transaction log file and seek to the end of
     * the file.
     */
    void redoAllGoEnd() throws SQLException {
        boolean readOnly = logSystem.getDatabase().isReadOnly();
        long length = file.length();
        if (length <= FileStore.HEADER_LENGTH) {
            return;
        }
        try {
            int max = (int) (length / BLOCK_SIZE);
            while (true) {
                pos = getBlock();
                database.setProgress(DatabaseEventListener.STATE_RECOVER, fileName, pos, max);
                if ((long) pos * BLOCK_SIZE >= length) {
                    break;
                }
                boolean more = redoOrUndo(false, readOnly);
                if (!more) {
                    break;
                }
            }
            database.setProgress(DatabaseEventListener.STATE_RECOVER, fileName, max, max);
        } catch (SQLException e) {
            database.getTrace(Trace.LOG).debug("Stop reading log file: " + e.getMessage(), e);
            // wrong checksum (at the end of the log file)
        } catch (OutOfMemoryError e) {
            // OutOfMemoryError means not enough memory is allocated to the VM.
            // this is not necessarily at the end of the log file
            // set the log system to read only so the current log file stays when closing
            logSystem.setReadOnly(true);
            throw Message.convert(e);
        } catch (Throwable e) {
            database.getTrace(Trace.LOG).error("Error reading log file (non-fatal)", e);
            // on power loss, sometime there is garbage at the end of the file
            // we stop recovering in this case (checksum mismatch)
        }
        go(pos);
    }

    /**
     * Go to the specified location in the file.
     *
     * @param pos the position
     */
    void go(int pos) throws SQLException {
        file.seek((long) pos * BLOCK_SIZE);
    }

    /**
     * Undo the changes of this transaction log entry. This method is called
     * when opening the database.
     *
     * @param pos the position of the log entry
     */
    void undo(int pos) throws SQLException {
        go(pos);
        redoOrUndo(true, false);
    }

    /**
     * Flush buffered changes to the file.
     */
    void flush() throws SQLException {
        if (bufferPos > 0) {
            if (file == null) {
                throw Message.getSQLException(ErrorCode.SIMULATED_POWER_OFF);
            }
            file.write(buffer, 0, bufferPos);
            pos = getBlock();
            for (Record r : unwritten) {
                r.setLogWritten(id, pos);
            }
            unwritten.clear();
            bufferPos = 0;
            long min = (long) pos * BLOCK_SIZE;
            min = MathUtils.scaleUp50Percent(Constants.FILE_MIN_SIZE, min, Constants.FILE_PAGE_SIZE, Constants.FILE_MAX_INCREMENT);
            if (min > file.length()) {
                file.setLength(min);
            }
        }
    }

    /**
     * Close the log file.
     *
     * @param delete if the file should be deleted shortly afterwards
     */
    void close(boolean delete) throws SQLException {
        SQLException closeException = null;
        try {
            flush();
        } catch (SQLException e) {
            closeException = e;
        }
        // continue with close even if flush was not possible (file storage problem)
        if (file != null) {
            try {
                file.close();
                file = null;
                if (delete) {
                    try {
                        database.deleteLogFileLater(fileName);
                    } catch (SQLException e) {
                        // can not delete the file now - ignore
                    }
                }
            } catch (IOException e) {
                if (closeException == null) {
                    closeException = Message.convertIOException(e, fileName);
                }
            }
            file = null;
            fileNamePrefix = null;
        }
        if (closeException != null) {
            throw closeException;
        }
    }

    /**
     * Write a file summary (storage allocation table) to the log file.
     *
     * @param dataFile if his summary is for the data file
     * @param summary the summary
     */
    void addSummary(boolean dataFile, byte[] summary) throws SQLException {
        DataPage buff = DataPage.create(database, 256);
        buff.writeInt(0);
        buff.writeByte((byte) 'S');
        buff.writeInt(0);
        // storageId
        buff.writeInt(0);
        // recordId
        buff.writeInt(0);
        // blockCount
        buff.writeInt(0);
        buff.writeByte((byte) (dataFile ? 'D' : 'I'));
        if (summary == null) {
            buff.writeInt(0);
        } else {
            buff.checkCapacity(summary.length);
            buff.writeInt(summary.length);
            buff.write(summary, 0, summary.length);
        }
        writeBuffer(buff, null);
    }

    /**
     * Append a truncate entry to the transaction log file.
     *
     * @param session the session
     * @param storageId the id of the storage that was truncated
     * @param recordId the record id
     * @param blockCount the number of blocks to delete
     */
    void addTruncate(Session session, int storageId, int recordId, int blockCount) throws SQLException {
        DataPage buff = rowBuff;
        buff.reset();
        buff.writeInt(0);
        buff.writeByte((byte) 'T');
        buff.writeInt(session.getId());
        buff.writeInt(storageId);
        buff.writeInt(recordId);
        buff.writeInt(blockCount);
        writeBuffer(buff, null);
    }

    /**
     * Add a record to the transaction log file.
     *
     * @param session the session
     * @param storageId the storage id
     * @param record the record
     */
    void add(Session session, int storageId, Record record) throws SQLException {
        record.prepareWrite();
        DataPage buff = rowBuff;
        buff.reset();
        buff.writeInt(0);
        if (record.isDeleted()) {
            buff.writeByte((byte) 'D');
        } else {
            buff.writeByte((byte) 'I');
        }
        buff.writeInt(session.getId());
        buff.writeInt(storageId);
        buff.writeInt(record.getPos());
        int blockCount = record.getBlockCount();
        buff.writeInt(blockCount);
        buff.checkCapacity(DiskFile.BLOCK_SIZE * blockCount);
        record.write(buff);
        writeBuffer(buff, record);
    }

    void setFirstUncommittedPos(int firstUncommittedPos) throws SQLException {
        this.firstUncommittedPos = firstUncommittedPos;
        int pos = getBlock();
        writeHeader();
        go(pos);
    }

    int getFirstUncommittedPos() {
        return firstUncommittedPos;
    }

    private void writeHeader() throws SQLException {
        file.seek(FileStore.HEADER_LENGTH);
        DataPage buff = getHeader();
        file.write(buff.getBytes(), 0, buff.length());
    }

    private void truncate(int pos) throws SQLException {
        go(pos);
        file.setLength((long) pos * BLOCK_SIZE);
    }

    private DataPage getHeader() {
        DataPage buff = rowBuff;
        buff.reset();
        buff.writeInt(id);
        buff.writeInt(firstUncommittedPos);
        // TODO need to update & use firstUnwrittenPos
        buff.writeInt(firstUnwrittenPos);
        buff.fill(3 * BLOCK_SIZE);
        return buff;
    }

    private void readHeader() throws SQLException {
        DataPage buff = getHeader();
        int len = buff.length();
        buff.reset();
        if (file.length() < FileStore.HEADER_LENGTH + len) {
            // this is an empty file
            return;
        }
        file.readFully(buff.getBytes(), 0, len);
        id = buff.readInt();
        firstUncommittedPos = buff.readInt();
        firstUnwrittenPos = buff.readInt();
    }

    int getPos() {
        return pos;
    }

    long getFileSize() throws SQLException {
        return file.getFilePointer();
    }

    /**
     * Write any pending changed to the log file.
     */
    void sync() {
        if (file != null) {
            file.sync();
        }
    }

    /**
     * Commit or roll back a prepared transaction.
     *
     * @param commit if the transaction should be committed (true) or rolled
     *            back (false)
     * @param pos the position of the prepare log entry
     * @param sessionId the session id
     * @param blocks the number of blocks occupied by the prepare log entry
     */
    void updatePreparedCommit(boolean commit, int pos, int sessionId, int blocks) throws SQLException {
        synchronized (database) {
            int posNow = getBlock();
            DataPage buff = rowBuff;
            buff.reset();
            buff.writeInt(blocks);
            if (commit) {
                buff.writeByte((byte) 'C');
            } else {
                buff.writeByte((byte) 'R');
            }
            buff.writeInt(sessionId);
            buff.fill(blocks * BLOCK_SIZE);
            buff.updateChecksum();
            go(pos);
            file.write(buff.getBytes(), 0, BLOCK_SIZE * blocks);
            go(posNow);
        }
    }

}
