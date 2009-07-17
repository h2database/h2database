/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.log;

import java.sql.SQLException;
import java.util.Comparator;
import java.util.HashMap;
import org.h2.api.DatabaseEventListener;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.store.DataPage;
import org.h2.store.DiskFile;
import org.h2.store.PageStore;
import org.h2.store.Record;
import org.h2.store.Storage;
import org.h2.util.FileUtils;
import org.h2.util.New;
import org.h2.util.ObjectArray;

/**
 * The transaction log system is responsible for the write ahead log mechanism
 * used in this database. A number of {@link LogFile} objects are used (one for
 * each file).
 */
public class LogSystem {

    /**
     * This special log position means that the log entry has been written.
     */
    public static final int LOG_WRITTEN = -1;

    private Database database;
    private ObjectArray<LogFile> activeLogs;
    private LogFile currentLog;
    private String fileNamePrefix;
    private HashMap<Integer, Storage> storages = New.hashMap();
    private HashMap<Integer, SessionState> sessionStates = New.hashMap();
    private DataPage rowBuff;
    private ObjectArray<LogRecord> undo;
    // TODO log file / deleteOldLogFilesAutomatically:
    // make this a setting, so they can be backed up
    private boolean deleteOldLogFilesAutomatically = true;
    private long maxLogSize = Constants.DEFAULT_MAX_LOG_SIZE;
    private boolean readOnly;
    private boolean flushOnEachCommit;
    private ObjectArray<InDoubtTransaction> inDoubtTransactions;
    private boolean disabled;
    private int keepFiles;
    private boolean closed;
    private String accessMode;
    private PageStore pageStore;

    /**
     * Create new transaction log object. This will not open or create files
     * yet.
     *
     * @param database the database
     * @param fileNamePrefix the name of the database file
     * @param readOnly if the log should be opened in read-only mode
     * @param accessMode the file access mode (r, rw, rws, rwd)
     * @param pageStore
     */
    public LogSystem(Database database, String fileNamePrefix, boolean readOnly, String accessMode, PageStore pageStore) {
        this.database = database;
        this.pageStore = pageStore;
        this.readOnly = readOnly;
        this.accessMode = accessMode;
        closed = true;
        if (database == null) {
            return;
        }
        this.fileNamePrefix = fileNamePrefix;
        rowBuff = DataPage.create(database, Constants.DEFAULT_DATA_PAGE_SIZE);
    }

    /**
     * Set the maximum log file size in megabytes.
     *
     * @param maxSize the new maximum log file size
     */
    public void setMaxLogSize(long maxSize) {
        this.maxLogSize = maxSize;
        if (pageStore != null) {
            pageStore.setMaxLogSize(maxSize);
        }
    }

    /**
     * Check if there are any in-doubt transactions.
     *
     * @return true if there are
     */
    public boolean containsInDoubtTransactions() {
        return inDoubtTransactions != null && inDoubtTransactions.size() > 0;
    }

    private void flushAndCloseUnused() throws SQLException {
        currentLog.flush();
        DiskFile file = database.getDataFile();
        if (file == null) {
            return;
        }
        file.flush();
        if (database.getLogIndexChanges()) {
            file = database.getIndexFile();
            file.flush();
        }
        if (containsInDoubtTransactions()) {
            // if there are any in-doubt transactions
            // (even if they are resolved), can't update or delete the log files
            return;
        }
        Session[] sessions = database.getSessions(true);
        int firstUncommittedLog = currentLog.getId();
        int firstUncommittedPos = currentLog.getPos();
        for (int i = 0; i < sessions.length; i++) {
            Session session = sessions[i];
            int log = session.getFirstUncommittedLog();
            int pos = session.getFirstUncommittedPos();
            if (pos != LOG_WRITTEN) {
                if (log < firstUncommittedLog || (log == firstUncommittedLog && pos < firstUncommittedPos)) {
                    firstUncommittedLog = log;
                    firstUncommittedPos = pos;
                }
            }
        }
        for (int i = activeLogs.size() - 1; i >= 0; i--) {
            LogFile l = activeLogs.get(i);
            if (l.getId() < firstUncommittedLog) {
                l.setFirstUncommittedPos(LOG_WRITTEN);
            } else if (l.getId() == firstUncommittedLog) {
                if (firstUncommittedPos == l.getPos()) {
                    // that means firstUncommittedPos is still
                    // were it was at the beginning
                    // and all sessions are committed
                    l.setFirstUncommittedPos(LOG_WRITTEN);
                } else {
                    l.setFirstUncommittedPos(firstUncommittedPos);
                }

            }
        }
        for (int i = 0; i < activeLogs.size(); i++) {
            LogFile l = activeLogs.get(i);
            if (l.getFirstUncommittedPos() == LOG_WRITTEN) {
                // must remove the log file first
                // if we don't do that, the file is closed but still in the list
                activeLogs.remove(i);
                i--;
                closeOldFile(l);
            }
        }
    }

    /**
     * Close all log files.
     *
     * @param checkpoint if a checkpoint should be written
     */
    public void close(boolean checkpoint) throws SQLException {
        if (database == null) {
            return;
        }
        synchronized (database) {
            if (closed) {
                return;
            }
            if (readOnly) {
                for (int i = 0; i < activeLogs.size(); i++) {
                    LogFile l = activeLogs.get(i);
                    l.close(false);
                }
                closed = true;
                return;
            }
            // TODO refactor flushing and closing files when we know what to do exactly
            SQLException closeException = null;
            try {
                flushAndCloseUnused();
                if (!containsInDoubtTransactions() && checkpoint) {
                    checkpoint();
                }
            } catch (SQLException e) {
                closeException = e;
            } catch (Throwable e) {
                // for example out of memory exception
                closeException = Message.convert(e);
            }
            for (int i = 0; i < activeLogs.size(); i++) {
                LogFile l = activeLogs.get(i);
                try {
                    // if there are any in-doubt transactions
                    // (even if they are resolved), can't delete the log files
                    if (l.getFirstUncommittedPos() == LOG_WRITTEN && !containsInDoubtTransactions()) {
                        closeOldFile(l);
                    } else {
                        l.close(false);
                    }
                } catch (SQLException e) {
                    // TODO log exception
                    if (closeException == null) {
                        closeException = e;
                    }
                }
            }
            closed = true;
            if (closeException != null) {
                throw closeException;
            }
        }
    }

    /**
     * Add an undo log entry. This method is called when opening the database.
     *
     * @param log the log file
     * @param logRecordId the log record id
     * @param sessionId the session id
     */
    void addUndoLogRecord(LogFile log, int logRecordId, int sessionId) {
        getOrAddSessionState(sessionId);
        LogRecord record = new LogRecord(log, logRecordId, sessionId);
        undo.add(record);
    }

    /**
     * Roll back any uncommitted transactions if required, and apply committed
     * changed to the data files.
     *
     * @return if recovery was needed
     */
    public boolean recover() throws SQLException {
        if (database == null) {
            return false;
        }
        synchronized (database) {
            if (closed) {
                return false;
            }
            undo = ObjectArray.newInstance();
            for (int i = 0; i < activeLogs.size(); i++) {
                LogFile log = activeLogs.get(i);
                log.redoAllGoEnd();
                database.getDataFile().flushRedoLog();
                database.getIndexFile().flushRedoLog();
            }
            int end = currentLog.getPos();
            Object[] states = sessionStates.values().toArray();
            inDoubtTransactions = ObjectArray.newInstance();
            for (int i = 0; i < states.length; i++) {
                SessionState state = (SessionState) states[i];
                if (state.inDoubtTransaction != null) {
                    inDoubtTransactions.add(state.inDoubtTransaction);
                }
            }
            for (int i = undo.size() - 1; i >= 0 && sessionStates.size() > 0; i--) {
                database.setProgress(DatabaseEventListener.STATE_RECOVER, null, undo.size() - 1 - i, undo.size());
                LogRecord record = undo.get(i);
                if (sessionStates.get(record.sessionId) != null) {
                    // undo only if the session is not yet committed
                    record.log.undo(record.logRecordId);
                    database.getDataFile().flushRedoLog();
                    database.getIndexFile().flushRedoLog();
                }
            }
            currentLog.go(end);
            boolean fileChanged = undo.size() > 0;
            undo = null;
            storages.clear();
            if (!readOnly && fileChanged && !containsInDoubtTransactions()) {
                checkpoint();
            }
            return fileChanged;
        }
    }

    private void closeOldFile(LogFile l) throws SQLException {
        l.close(deleteOldLogFilesAutomatically && keepFiles == 0);
    }

    /**
     * Open all existing transaction log files and create a new one if required.
     */
    public void open() throws SQLException {
        String path = FileUtils.getParent(fileNamePrefix);
        String[] list = FileUtils.listFiles(path);
        activeLogs = ObjectArray.newInstance();
        for (int i = 0; i < list.length; i++) {
            String s = list[i];
            LogFile l = null;
            try {
                l = LogFile.openIfLogFile(this, fileNamePrefix, s);
            } catch (SQLException e) {
                database.getTrace(Trace.LOG).debug("Error opening log file, header corrupt: "+s, e);
                // this can happen if the system crashes just
                // after creating a new file (before writing the header)
                // rename it, so that it doesn't get in the way the next time
                FileUtils.delete(s + ".corrupt");
                FileUtils.rename(s, s + ".corrupt");
            }
            if (l != null) {
                if (l.getPos() == LOG_WRITTEN) {
                    closeOldFile(l);
                } else {
                    activeLogs.add(l);
                }
            }
        }
        activeLogs.sort(new Comparator<LogFile>() {
            public int compare(LogFile a, LogFile b) {
                return a.getId() - b.getId();
            }
        });
        if (activeLogs.size() == 0) {
            LogFile l = new LogFile(this, 0, fileNamePrefix);
            activeLogs.add(l);
        }
        currentLog = activeLogs.get(activeLogs.size() - 1);
        closed = false;
    }

    /**
     * Get the storage object.
     *
     * @param id the storage id
     * @return the storage
     */
    Storage getStorageForRecovery(int id) {
        boolean dataFile;
        if (id < 0) {
            dataFile = false;
            id = -id;
        } else {
            dataFile = true;
        }
        Storage storage = storages.get(id);
        if (storage == null) {
            storage = database.getStorage(null, id, dataFile);
            storages.put(id, storage);
        }
        return storage;
    }

    /**
     * Check if the session contains uncommitted log entries at the given position.
     *
     * @param sessionId the session id
     * @param logId the log file id
     * @param pos the position in the log file
     * @return true if this session contains an uncommitted transaction
     */
    boolean isSessionCommitted(int sessionId, int logId, int pos) {
        SessionState state = sessionStates.get(sessionId);
        if (state == null) {
            return true;
        }
        return state.isCommitted(logId, pos);
    }

    /**
     * Set the last commit record for a session.
     *
     * @param sessionId the session id
     * @param logId the log file id
     * @param pos the position in the log file
     */
    void setLastCommitForSession(int sessionId, int logId, int pos) {
        SessionState state = getOrAddSessionState(sessionId);
        state.lastCommitLog = logId;
        state.lastCommitPos = pos;
        state.inDoubtTransaction = null;
    }

    /**
     * Get the session state for this session. A new object is created if there
     * is no session state yet.
     *
     * @param sessionId the session id
     * @return the session state object
     */
    SessionState getOrAddSessionState(int sessionId) {
        SessionState state = sessionStates.get(sessionId);
        if (state == null) {
            state = new SessionState();
            sessionStates.put(sessionId, state);
            state.sessionId = sessionId;
        }
        return state;
    }

    /**
     * This method is called when a 'prepare commit' log entry is read when
     * opening the database.
     *
     * @param log the log file
     * @param sessionId the session id
     * @param pos the position in the log file
     * @param transaction the transaction name
     * @param blocks the number of blocks the 'prepare commit' entry occupies
     */
    void setPreparedCommitForSession(LogFile log, int sessionId, int pos, String transaction, int blocks) {
        SessionState state = getOrAddSessionState(sessionId);
        // this is potentially a commit, so
        // don't roll back the action before it (currently)
        setLastCommitForSession(sessionId, log.getId(), pos);
        state.inDoubtTransaction = new InDoubtTransaction(null, log, sessionId, pos, transaction, blocks);
    }

    /**
     * Get the list of in-doubt transactions.
     *
     * @return the list
     */
    public ObjectArray<InDoubtTransaction> getInDoubtTransactions() {
        if (pageStore != null) {
            return pageStore.getInDoubtTransactions();
        }
        return inDoubtTransactions;
    }

    /**
     * Remove a session from the session state map. This is done when opening
     * the database, when all records of this session have been applied to the
     * files.
     *
     * @param sessionId the session id
     */
    void removeSession(int sessionId) {
        sessionStates.remove(sessionId);
    }

    /**
     * Prepare a transaction.
     *
     * @param session the session
     * @param transaction the name of the transaction
     */
    public void prepareCommit(Session session, String transaction) throws SQLException {
        if (database == null || readOnly) {
            return;
        }
        synchronized (database) {
            if (pageStore != null) {
                pageStore.prepareCommit(session, transaction);
            }
            if (closed) {
                return;
            }
            currentLog.prepareCommit(session, transaction);
        }
    }

    /**
     * Commit the current transaction of the given session.
     *
     * @param session the session
     */
    public void commit(Session session) throws SQLException {
        if (database == null || readOnly) {
            return;
        }
        synchronized (database) {
            if (pageStore != null) {
                pageStore.commit(session);
                session.setAllCommitted();
            }
            if (closed) {
                return;
            }
            currentLog.commit(session);
            session.setAllCommitted();
        }
    }

    /**
     * Flush all pending changes to the transaction log files.
     */
    public void flush() throws SQLException {
        if (database == null || readOnly) {
            return;
        }
        synchronized (database) {
            if (pageStore != null) {
                pageStore.flushLog();
            }
            if (closed) {
                return;
            }
            currentLog.flush();
        }
    }

    /**
     * Add a truncate entry.
     *
     * @param session the session
     * @param file the disk file
     * @param storageId the storage id
     * @param recordId the id of the first record
     * @param blockCount the number of blocks
     */
    public void addTruncate(Session session, DiskFile file, int storageId, int recordId, int blockCount)
            throws SQLException {
        if (database == null) {
            return;
        }
        synchronized (database) {
            if (disabled || closed) {
                return;
            }
            database.checkWritingAllowed();
            if (!file.isDataFile()) {
                storageId = -storageId;
            }
            currentLog.addTruncate(session, storageId, recordId, blockCount);
            if (currentLog.getFileSize() > maxLogSize) {
                checkpoint();
            }
        }
    }

    /**
     * Add an log entry to the last transaction log file.
     *
     * @param session the session
     * @param file the file
     * @param record the record to log
     */
    public void add(Session session, DiskFile file, Record record) throws SQLException {
        if (database == null) {
            return;
        }
        synchronized (database) {
            if (disabled || closed) {
                return;
            }
            database.checkWritingAllowed();
            int storageId = record.getStorageId();
            if (!file.isDataFile()) {
                storageId = -storageId;
            }
            int log = currentLog.getId();
            int pos = currentLog.getPos();
            session.addLogPos(log, pos);
            record.setLastLog(log, pos);
            currentLog.add(session, storageId, record);
            if (currentLog.getFileSize() > maxLogSize) {
                checkpoint();
            }
        }
    }

    /**
     * Flush all data to the transaction log files as well as to the data files
     * and and switch log files.
     */
    public void checkpoint() throws SQLException {
        if (readOnly || database == null) {
            return;
        }
        synchronized (database) {
            if (closed || disabled) {
                return;
            }
            database.checkWritingAllowed();
            flushAndCloseUnused();
            currentLog = new LogFile(this, currentLog.getId() + 1, fileNamePrefix);
            activeLogs.add(currentLog);
            writeSummary();
            currentLog.flush();
        }
    }

    /**
     * Get all active log files.
     *
     * @return the list of log files
     */
    public ObjectArray<LogFile> getActiveLogFiles() {
        synchronized (database) {
            ObjectArray<LogFile> list = ObjectArray.newInstance();
            list.addAll(activeLogs);
            return list;
        }
    }

    private void writeSummary() throws SQLException {
        byte[] summary;
        DiskFile file;
        file = database.getDataFile();
        if (file == null) {
            return;
        }
        summary = file.getSummary();
        if (summary != null) {
            currentLog.addSummary(true, summary);
        }
        if (database.getLogIndexChanges() || database.isIndexSummaryValid()) {
            file = database.getIndexFile();
            summary = file.getSummary();
            if (summary != null) {
                currentLog.addSummary(false, summary);
            }
        } else {
            // invalidate the index summary
            currentLog.addSummary(false, null);
        }
    }

    Database getDatabase() {
        return database;
    }

    DataPage getRowBuffer() {
        return rowBuff;
    }

    /**
     * Enable or disable-flush-on-each-commit.
     *
     * @param b the new value
     */
    public void setFlushOnEachCommit(boolean b) {
        flushOnEachCommit = b;
    }

    /**
     * Check if flush-on-each-commit is enabled.
     *
     * @return true if it is
     */
    public boolean getFlushOnEachCommit() {
        return flushOnEachCommit;
    }

    /**
     * Flush the transaction log file and sync the data to disk.
     */
    public void sync() throws SQLException {
        if (database == null || readOnly) {
            return;
        }
        synchronized (database) {
            if (currentLog != null) {
                currentLog.flush();
                currentLog.sync();
            }
        }
    }

    /**
     * Enable or disable the transaction log
     *
     * @param disabled true if the log should be switched off
     */
    public void setDisabled(boolean disabled) {
        this.disabled = disabled;
    }

    /**
     * Set the read only flag for this log system.
     *
     * @param readOnly the new value
     */
    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    /**
     * Add a redo log entry to the file. This method is called when re-applying
     * the log entries (when opening a database).
     *
     * @param storage the target storage
     * @param recordId the record id
     * @param blockCount the number of blocks
     * @param rec the record data
     */
    void addRedoLog(Storage storage, int recordId, int blockCount, DataPage rec) throws SQLException {
        DiskFile file = storage.getDiskFile();
        file.addRedoLog(storage, recordId, blockCount, rec);
    }

    /**
     * Write a log entry meaning the index summary is invalid.
     */
    public void invalidateIndexSummary() throws SQLException {
        currentLog.addSummary(false, null);
    }

    /**
     * Increment or decrement the flag to keep (not delete) old log files.
     *
     * @param incrementDecrement (1 to increment, -1 to decrement)
     */
    public synchronized void updateKeepFiles(int incrementDecrement) {
        keepFiles += incrementDecrement;
    }

    String getAccessMode() {
        return accessMode;
    }

    /**
     * Get the write position.
     *
     * @return the write position
     */
    public String getWritePos() {
        if (pageStore != null) {
            return "" + pageStore.getWriteCount();
        }
        return currentLog.getId() + "/" + currentLog.getPos();
    }

}
