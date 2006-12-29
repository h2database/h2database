/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.sql.SQLException;
import java.util.Comparator;
import java.util.HashMap;

import org.h2.api.DatabaseEventListener;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.util.FileUtils;
import org.h2.util.ObjectArray;

/**
 * @author Thomas
 */
public class LogSystem {

    public static final int LOG_WRITTEN = -1;

    private Database database;
    private ObjectArray activeLogs;
    private LogFile currentLog;
    private String fileNamePrefix;
    private HashMap storages = new HashMap();
    private HashMap sessions = new HashMap();
    private DataPage rowBuff;
    private ObjectArray undo;
    // TODO log file / deleteOldLogFilesAutomatically: make this a setting, so they can be backed up
    private boolean deleteOldLogFilesAutomatically = true;
    private long maxLogSize = Constants.DEFAULT_MAX_LOG_SIZE;
    private boolean readOnly;
    private boolean flushOnEachCommit;
    private ObjectArray inDoubtTransactions;
    private boolean disabled;

    public LogSystem(Database database, String fileNamePrefix, boolean readOnly) throws SQLException {
        this.database = database;
        this.readOnly = readOnly;
        if (database == null || readOnly) {
            return;
        }
        this.fileNamePrefix = fileNamePrefix;
        rowBuff = DataPage.create(database, Constants.DEFAULT_DATA_PAGE_SIZE);
        loadActiveLogFiles();
    }

    public void setMaxLogSize(long maxSize) {
        this.maxLogSize = maxSize;
    }

    public long getMaxLogSize() {
        return maxLogSize;
    }

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
        if (containsInDoubtTransactions()) {
            // if there are any in-doubt transactions (even if they are resolved), can't update or delete the log files
            return;
        }
        Session[] sessions = database.getSessions();
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
            LogFile l = (LogFile) activeLogs.get(i);
            if (l.getId() < firstUncommittedLog) {
                l.setFirstUncommittedPos(LOG_WRITTEN);
            } else if (l.getId() == firstUncommittedLog) {
                if (firstUncommittedPos == l.getPos()) {
                    l.setFirstUncommittedPos(LOG_WRITTEN);
                } else {
                    l.setFirstUncommittedPos(firstUncommittedPos);
                }
            }
        }
        for (int i = 0; i < activeLogs.size(); i++) {
            LogFile l = (LogFile) activeLogs.get(i);
            if (l.getFirstUncommittedPos() == LOG_WRITTEN) {
                l.close(deleteOldLogFilesAutomatically);
                activeLogs.remove(i);
                i--;
            }
        }
    }

    public void close() throws SQLException {
        if (database == null || readOnly) {
            return;
        }
        synchronized (database) {
            // TODO refactor flushing and closing files when we know what to do exactly
            SQLException closeException = null;
            try {
                flushAndCloseUnused();
                if (!containsInDoubtTransactions()) {
                    checkpoint();
                }
            } catch (SQLException e) {
                closeException = e;
            }
            for (int i = 0; i < activeLogs.size(); i++) {
                LogFile l = (LogFile) activeLogs.get(i);
                try {
                    // if there are any in-doubt transactions (even if they are resolved), can't delete the log files
                    if (l.getFirstUncommittedPos() == LOG_WRITTEN && !containsInDoubtTransactions()) {
                        l.close(deleteOldLogFilesAutomatically);
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
            database = null;
            if (closeException != null) {
                throw closeException;
            }
        }
    }

    boolean needMoreUndo() {
        return sessions.size() > 0;
    }

    void addUndoLogRecord(LogFile log, int logRecordId, int sessionId) {
        LogRecord record = new LogRecord(log, logRecordId, sessionId);
        undo.add(record);
    }

    public boolean recover() throws SQLException {
        if (database == null) {
            return false;
        }
        synchronized (database) {
            undo = new ObjectArray();
            for (int i = 0; i < activeLogs.size(); i++) {
                LogFile log = (LogFile) activeLogs.get(i);
                log.redoAllGoEnd();
            }
            database.getDataFile().flushRedoLog();
            database.getIndexFile().flushRedoLog();
            int end = currentLog.getPos();
            Object[] states = sessions.values().toArray();
            inDoubtTransactions = new ObjectArray();
            for (int i = 0; i < states.length; i++) {
                SessionState state = (SessionState) states[i];
                if (state.inDoubtTransaction != null) {
                    inDoubtTransactions.add(state.inDoubtTransaction);
                }
            }
            for (int i = undo.size() - 1; i >= 0 && sessions.size() > 0; i--) {
                database.setProgress(DatabaseEventListener.STATE_RECOVER, null, undo.size() - 1 - i, undo.size());
                LogRecord record = (LogRecord) undo.get(i);
                if (sessions.get(new Integer(record.sessionId)) != null) {
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
            if (fileChanged && !containsInDoubtTransactions()) {
                checkpoint();
            }
            return fileChanged;
        }
    }

    private void loadActiveLogFiles() throws SQLException {
        String path = FileUtils.getParent(fileNamePrefix);
        String[] list = FileUtils.listFiles(path);
        activeLogs = new ObjectArray();
        for (int i = 0; i < list.length; i++) {
            String s = list[i];
            LogFile l = LogFile.openIfLogFile(this, fileNamePrefix, s);
            if (l != null) {
                if (l.getPos() == LOG_WRITTEN) {
                    l.close(deleteOldLogFilesAutomatically);
                } else {
                    activeLogs.add(l);
                }
            }
        }
        activeLogs.sort(new Comparator() {
            public int compare(Object a, Object b) {
                return ((LogFile) a).getId() - ((LogFile) b).getId();
            }
        });
        if (activeLogs.size() == 0) {
            LogFile l = new LogFile(this, 0, fileNamePrefix);
            activeLogs.add(l);
        }
        currentLog = (LogFile) activeLogs.get(activeLogs.size() - 1);
    }

    Storage getStorageForRecovery(int id) throws SQLException {
        boolean dataFile;
        if (id < 0) {
            dataFile = false;
            id = -id;
        } else {
            dataFile = true;
        }
        Integer i = new Integer(id);
        Storage storage = (Storage) storages.get(i);
        if (storage == null) {
            storage = database.getStorage(null, id, dataFile);
            storages.put(i, storage);
        }
        return storage;
    }

    boolean isSessionCommitted(int sessionId, int logId, int pos) {
        Integer key = new Integer(sessionId);
        SessionState state = (SessionState) sessions.get(key);
        if (state == null) {
            return true;
        }
        return state.isCommitted(logId, pos);
    }

    void setLastCommitForSession(int sessionId, int logId, int pos) {
        Integer key = new Integer(sessionId);
        SessionState state = (SessionState) sessions.get(key);
        if (state == null) {
            state = new SessionState();
            sessions.put(key, state);
            state.sessionId = sessionId;
        }
        state.lastCommitLog = logId;
        state.lastCommitPos = pos;
        state.inDoubtTransaction = null;
    }

    void setPreparedCommitForSession(LogFile log, int sessionId, int pos, String transaction, int blocks) {
        Integer key = new Integer(sessionId);
        SessionState state = (SessionState) sessions.get(key);
        if (state == null) {
            state = new SessionState();
            sessions.put(key, state);
            state.sessionId = sessionId;
        }
        // this is potentially a commit, so don't roll back the action before it (currently)
        setLastCommitForSession(sessionId, log.getId(), pos);
        state.inDoubtTransaction = new InDoubtTransaction(log, sessionId, pos, transaction, blocks);
    }

    public ObjectArray getInDoubtTransactions() {
        return inDoubtTransactions;
    }

    void removeSession(int sessionId) {
        sessions.remove(new Integer(sessionId));
    }

    public void prepareCommit(Session session, String transaction) throws SQLException {
        if (database == null || readOnly) {
            return;
        }
        synchronized (database) {
            currentLog.prepareCommit(session, transaction);
        }
    }

    public void commit(Session session) throws SQLException {
        if (database == null || readOnly) {
            return;
        }
        synchronized (database) {
            currentLog.commit(session);
            session.setAllCommitted();
        }
    }

    public void flush() throws SQLException {
        if (database == null || readOnly) {
            return;
        }
        synchronized (database) {
            currentLog.flush();
        }
    }

    public void addTruncate(Session session, DiskFile file, int storageId, int recordId, int blockCount) throws SQLException {
        if (database == null || disabled) {
            return;
        }
        synchronized (database) {
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

    public void add(Session session, DiskFile file, Record record) throws SQLException {
        if (database == null || disabled) {
            return;
        }
        synchronized (database) {
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

    public void checkpoint() throws SQLException {
        if (database == null || readOnly || disabled) {
            return;
        }
        synchronized (database) {
            flushAndCloseUnused();
            currentLog = new LogFile(this, currentLog.getId() + 1, fileNamePrefix);
            activeLogs.add(currentLog);
            writeSummary();
            currentLog.flush();
        }
    }

    private void writeSummary() throws SQLException {
        if (database == null || readOnly || disabled) {
            return;
        }
        byte[] summary;
        DiskFile file;
        file = database.getDataFile();
        summary = file.getSummary();
        if (summary != null) {
            currentLog.addSummary(true, summary);
        }
        if (database.getLogIndexChanges() || database.getIndexSummaryValid()) {
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

    public void setFlushOnEachCommit(boolean b) {
        flushOnEachCommit = b;
    }

    boolean getFlushOnEachCommit() {
        return flushOnEachCommit;
    }

    public void sync() throws SQLException {
        synchronized (database) {
            if (currentLog != null) {
                currentLog.flush();
                currentLog.sync();
            }
        }
    }

    public void setDisabled(boolean disabled) {
        this.disabled = disabled;
    }

    void addRedoLog(Storage storage, int recordId, int blockCount, DataPage rec) throws SQLException {
        DiskFile file = storage.getDiskFile();
        file.addRedoLog(storage, recordId, blockCount, rec);
    }

    public void invalidateIndexSummary() throws SQLException {
        currentLog.addSummary(false, null);
    }

}
