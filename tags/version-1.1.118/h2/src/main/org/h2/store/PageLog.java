/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import org.h2.engine.Session;
import org.h2.log.InDoubtTransaction;
import org.h2.log.LogSystem;
import org.h2.log.SessionState;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.result.Row;
import org.h2.util.BitField;
import org.h2.util.IntArray;
import org.h2.util.IntIntHashMap;
import org.h2.util.New;
import org.h2.util.ObjectArray;
import org.h2.util.StringUtils;
import org.h2.value.Value;

/**
 * Transaction log mechanism. The stream contains a list of records. The data
 * format for a record is:
 * <ul>
 * <li>0-0: type (0: undo,...)</li>
 * <li>1-4: page id</li>
 * <li>5-: data</li>
 * </ul>
 * The log file is split into sections, each section starts with a new log id.
 * A checkpoint starts a new section.
 */
public class PageLog {

    /**
     * No operation.
     */
    public static final int NOOP = 0;

    /**
     * An undo log entry.
     * Format: page id, page.
     */
    public static final int UNDO = 1;

    /**
     * A commit entry of a session.
     * Format: session id.
     */
    public static final int COMMIT = 2;

    /**
     * A prepare commit entry for a session.
     * Format: session id, transaction name length, transaction name (UTF-8).
     */
    public static final int PREPARE_COMMIT = 3;

    /**
     * Roll back a prepared transaction.
     * Format: session id.
     */
    public static final int ROLLBACK = 4;

    /**
     * Add a record to a table.
     * Format: session id, table id, row.
     */
    public static final int ADD = 5;

    /**
     * Remove a record from a table.
     * Format: session id, table id, row.
     */
    public static final int REMOVE = 6;

    /**
     * Truncate a table.
     * Format: session id, table id.
     */
    public static final int TRUNCATE = 7;

    /**
     * Perform a checkpoint. The log id is incremented.
     * Format: -
     */
    public static final int CHECKPOINT = 8;

    /**
     * Free a log page.
     * Format: count, page ids
     */
    public static final int FREE_LOG = 9;

    /**
     * The recovery stage to undo changes (re-apply the backup).
     */
    static final int RECOVERY_STAGE_UNDO = 0;

    /**
     * The recovery stage to allocate pages used by the transaction log.
     */
    static final int RECOVERY_STAGE_ALLOCATE = 1;

    /**
     * The recovery stage to redo operations.
     */
    static final int RECOVERY_STAGE_REDO = 2;

    private final PageStore store;
    private int pos;
    private Trace trace;

    private DataOutputStream out;
    private ByteArrayOutputStream buffer;
    private PageInputStream pageIn;
    private PageOutputStream pageOut;
    private DataInputStream in;
    private int firstTrunkPage;
    private int firstDataPage;
    private Data data;
    private int logSectionId, logPos;
    private int firstLogId;

    /**
     * If the bit is set, the given page was written to the current log section.
     * The undo entry of these pages doesn't need to be written again.
     */
    private BitField undo = new BitField();

    /**
     * The undo entry of those pages was written in any log section.
     * These pages may not be used in the transaction log.
     */
    private BitField undoAll = new BitField();

    /**
     * The map of section ids (key) and data page where the section starts (value).
     */
    private IntIntHashMap logSectionPageMap = new IntIntHashMap();

    /**
     * The session state map.
     * Only used during recovery.
     */
    private HashMap<Integer, SessionState> sessionStates = New.hashMap();

    /**
     * The map of pages used by the transaction log.
     * Only used during recovery.
     */
    private BitField usedLogPages;

    PageLog(PageStore store) {
        this.store = store;
        data = store.createData();
        trace = store.getTrace();
    }

    /**
     * Open the log for writing. For an existing database, the recovery
     * must be run first.
     *
     * @param firstTrunkPage the first trunk page
     * @param atEnd whether only pages at the end of the file should be used
     */
    void openForWriting(int firstTrunkPage, boolean atEnd) throws SQLException {
        trace.debug("log openForWriting firstPage:" + firstTrunkPage);
        this.firstTrunkPage = firstTrunkPage;
        pageOut = new PageOutputStream(store, firstTrunkPage, undoAll, atEnd);
        pageOut.reserve(1);
        store.setLogFirstPage(firstTrunkPage, pageOut.getCurrentDataPageId());
        buffer = new ByteArrayOutputStream();
        out = new DataOutputStream(buffer);
    }

    /**
     * Free up all pages allocated by the log.
     */
    void free() throws SQLException {
        while (firstTrunkPage != 0) {
            PageStreamTrunk t = (PageStreamTrunk) store.getPage(firstTrunkPage);
            if (t == null) {
                store.freePage(firstTrunkPage, false, null);
                // EOF
                break;
            }
            t.free();
            firstTrunkPage = t.getNextTrunk();
        }
    }

    /**
     * Open the log for reading.
     *
     * @param firstTrunkPage the first trunk page
     * @param firstDataPage the index of the first data page
     */
    void openForReading(int firstTrunkPage, int firstDataPage) {
        this.firstTrunkPage = firstTrunkPage;
        this.firstDataPage = firstDataPage;
    }

    /**
     * Run one recovery stage. There are three recovery stages: 0: only the undo
     * steps are run (restoring the state before the last checkpoint). 1: the
     * pages that are used by the transaction log are allocated. 2: the
     * committed operations are re-applied.
     *
     * @param stage the recovery stage
     */
    void recover(int stage) throws SQLException {
        if (trace.isDebugEnabled()) {
            trace.debug("log recover stage:" + stage);
        }
        if (stage == RECOVERY_STAGE_ALLOCATE) {
            PageInputStream in = new PageInputStream(store, firstTrunkPage, firstDataPage);
            usedLogPages = in.allocateAllPages();
            return;
        }
        pageIn = new PageInputStream(store, firstTrunkPage, firstDataPage);
        in = new DataInputStream(pageIn);
        int logId = 0;
        Data data = store.createData();
        try {
            pos = 0;
            while (true) {
                int x = in.read();
                if (x < 0) {
                    break;
                }
                pos++;
                if (x == UNDO) {
                    int pageId = in.readInt();
                    in.readFully(data.getBytes(), 0, store.getPageSize());
                    if (stage == RECOVERY_STAGE_UNDO) {
                        if (!undo.get(pageId)) {
                            if (trace.isDebugEnabled()) {
                                trace.debug("log undo " + pageId);
                            }
                            store.writePage(pageId, data);
                            undo.set(pageId);
                            undoAll.set(pageId);
                        }
                    }
                } else if (x == ADD || x == REMOVE) {
                    int sessionId = in.readInt();
                    int tableId = in.readInt();
                    Row row = readRow(in, data);
                    if (stage == RECOVERY_STAGE_UNDO && x == ADD) {
                        store.allocateIfIndexRoot(pos, tableId, row);
                    } else if (stage == RECOVERY_STAGE_REDO) {
                        if (isSessionCommitted(sessionId, logId, pos)) {
                            if (trace.isDebugEnabled()) {
                                trace.debug("log redo " + (x == ADD ? "+" : "-") + " table:" + tableId + " " + row);
                            }
                            store.redo(pos, tableId, row, x == ADD);
                        } else {
                            if (trace.isDebugEnabled()) {
                                trace.debug("log ignore s:" + sessionId + " " + (x == ADD ? "+" : "-") + " table:" + tableId + " " + row);
                            }
                        }
                    }
                } else if (x == TRUNCATE) {
                    int sessionId = in.readInt();
                    int tableId = in.readInt();
                    if (stage == RECOVERY_STAGE_REDO) {
                        if (isSessionCommitted(sessionId, logId, pos)) {
                            if (trace.isDebugEnabled()) {
                                trace.debug("log redo truncate table:" + tableId);
                            }
                            store.redoTruncate(tableId);
                        } else {
                            if (trace.isDebugEnabled()) {
                                trace.debug("log ignore s:" + sessionId + " truncate table:" + tableId);
                            }
                        }
                    }
                } else if (x == PREPARE_COMMIT) {
                    int sessionId = in.readInt();
                    int len = in.readInt();
                    byte[] t = new byte[len];
                    in.readFully(t);
                    String transaction = StringUtils.utf8Decode(t);
                    if (trace.isDebugEnabled()) {
                        trace.debug("log prepare commit " + sessionId + " " + transaction + " pos:" + pos);
                    }
                    if (stage == RECOVERY_STAGE_UNDO) {
                        int page = pageIn.getDataPage();
                        setPrepareCommit(sessionId, page, transaction);
                    }
                } else if (x == ROLLBACK) {
                    int sessionId = in.readInt();
                    if (trace.isDebugEnabled()) {
                        trace.debug("log rollback " + sessionId + " pos:" + pos);
                    }
                    // ignore - this entry is just informational
                } else if (x == COMMIT) {
                    int sessionId = in.readInt();
                    if (trace.isDebugEnabled()) {
                        trace.debug("log commit " + sessionId + " pos:" + pos);
                    }
                    if (stage == RECOVERY_STAGE_UNDO) {
                        setLastCommitForSession(sessionId, logId, pos);
                    }
                } else  if (x == NOOP) {
                    // nothing to do
                } else if (x == CHECKPOINT) {
                    logId++;
                } else if (x == FREE_LOG) {
                    int count = in.readInt();
                    for (int i = 0; i < count; i++) {
                        int pageId = in.readInt();
                        if (stage == RECOVERY_STAGE_REDO) {
                            if (!usedLogPages.get(pageId)) {
                                store.freePage(pageId, false, null);
                            }
                        }
                    }
                } else {
                    if (trace.isDebugEnabled()) {
                        trace.debug("log end");
                        break;
                    }
                }
            }
        } catch (EOFException e) {
            trace.debug("log recovery stopped: " + e.toString());
        } catch (IOException e) {
            throw Message.convertIOException(e, "recover");
        }
        undo = new BitField();
        if (stage == RECOVERY_STAGE_REDO) {
            usedLogPages = null;
        }
    }

    /**
     * This method is called when a 'prepare commit' log entry is read when
     * opening the database.
     *
     * @param sessionId the session id
     * @param the data page with the prepare entry
     * @param transaction the transaction name, or null to rollback
     */
    private void setPrepareCommit(int sessionId, int pageId, String transaction) {
        SessionState state = getOrAddSessionState(sessionId);
        InDoubtTransaction doubt;
        if (transaction == null) {
            doubt = null;
        } else {
            doubt = new InDoubtTransaction(store, null, sessionId, pageId, transaction, 0);
        }
        state.inDoubtTransaction = doubt;
    }

    /**
     * Read a row from an input stream.
     *
     * @param in the input stream
     * @param data a temporary buffer
     * @return the row
     */
    public static Row readRow(DataInputStream in, Data data) throws IOException, SQLException {
        int pos = in.readInt();
        int len = in.readInt();
        data.reset();
        data.checkCapacity(len);
        in.readFully(data.getBytes(), 0, len);
        int columnCount = data.readInt();
        Value[] values = new Value[columnCount];
        for (int i = 0; i < columnCount; i++) {
            values[i] = data.readValue();
        }
        // TODO maybe calculate the memory usage
        Row row = new Row(values, 0);
        row.setPos(pos);
        return row;
    }

    /**
     * Add an undo entry to the log. The page data is only written once until
     * the next checkpoint.
     *
     * @param pageId the page id
     * @param page the old page data
     */
    void addUndo(int pageId, Data page) throws SQLException {
        try {
            if (undo.get(pageId)) {
                return;
            }
            if (trace.isDebugEnabled()) {
                trace.debug("log undo " + pageId);
            }
            undo.set(pageId);
            undoAll.set(pageId);
            out.write(UNDO);
            out.writeInt(pageId);
            out.write(page.getBytes(), 0, store.getPageSize());
            flushOut();
        } catch (IOException e) {
            throw Message.convertIOException(e, null);
        }
    }

    private void freeLogPages(IntArray pages) throws SQLException {
        try {
            if (trace.isDebugEnabled()) {
                trace.debug("log frees " + pages.get(0) + ".." + pages.get(pages.size() - 1));
            }
            out.write(FREE_LOG);
            out.writeInt(pages.size());
            for (int i = 0; i < pages.size(); i++) {
                out.writeInt(pages.get(i));
            }
            flushOut();
        } catch (IOException e) {
            throw Message.convertIOException(e, null);
        }
    }

    private void flushOut() throws IOException {
        out.flush();
        pageOut.write(buffer.toByteArray());
        buffer.reset();
    }

    /**
     * Mark a transaction as committed.
     *
     * @param session the session
     */
    void commit(int sessionId) throws SQLException {
        try {
            if (trace.isDebugEnabled()) {
                trace.debug("log commit s:" + sessionId);
            }
            LogSystem log = store.getDatabase().getLog();
            if (log == null) {
                // database already closed
                return;
            }
            out.write(COMMIT);
            out.writeInt(sessionId);
            flushOut();
            if (log.getFlushOnEachCommit()) {
                flush();
            }
        } catch (IOException e) {
            throw Message.convertIOException(e, null);
        }
    }

    /**
     * Prepare a transaction.
     *
     * @param session the session
     * @param transaction the name of the transaction
     */
    void prepareCommit(Session session, String transaction) throws SQLException {
        try {
            if (trace.isDebugEnabled()) {
                trace.debug("log prepare commit s:" + session.getId() + " " + transaction);
            }
            LogSystem log = store.getDatabase().getLog();
            if (log == null) {
                // database already closed
                return;
            }
            // store it on a separate log page
            int pageSize = store.getPageSize();
            byte[] t = StringUtils.utf8Encode(transaction);
            int len = t.length;
            if (1 + DataPage.LENGTH_INT * 2 + len >= PageStreamData.getCapacity(pageSize)) {
                throw Message.getInvalidValueException("transaction name too long", transaction);
            }
            pageOut.fillPage();
            out.write(PREPARE_COMMIT);
            out.writeInt(session.getId());
            out.writeInt(len);
            out.write(t);
            flushOut();
            // store it on a separate log page
            pageOut.fillPage();
            if (log.getFlushOnEachCommit()) {
                flush();
            }
        } catch (IOException e) {
            throw Message.convertIOException(e, null);
        }
    }

    /**
     * A record is added to a table, or removed from a table.
     *
     * @param session the session
     * @param tableId the table id
     * @param row the row to add
     * @param add true if the row is added, false if it is removed
     */
    void logAddOrRemoveRow(Session session, int tableId, Row row, boolean add) throws SQLException {
        try {
            if (trace.isDebugEnabled()) {
                trace.debug("log " + (add?"+":"-") + " s:" + session.getId() + " table:" + tableId +
                        " row:" + row);
            }
            session.addLogPos(logSectionId, logPos);
            row.setLastLog(logSectionId, logPos);
            logPos++;

            data.reset();
            data.checkCapacity(row.getByteCount(data));
            row.write(data);
            out.write(add ? ADD : REMOVE);
            out.writeInt(session.getId());
            out.writeInt(tableId);
            out.writeInt(row.getPos());
            out.writeInt(data.length());
            out.write(data.getBytes(), 0, data.length());
            flushOut();
        } catch (IOException e) {
            throw Message.convertIOException(e, null);
        }
    }

    /**
     * A table is truncated.
     *
     * @param session the session
     * @param tableId the table id
     */
    void logTruncate(Session session, int tableId) throws SQLException {
        try {
            if (trace.isDebugEnabled()) {
                trace.debug("log truncate s:" + session.getId() + " table:" + tableId);
            }
            session.addLogPos(logSectionId, logPos);
            logPos++;

            data.reset();
            out.write(TRUNCATE);
            out.writeInt(session.getId());
            out.writeInt(tableId);
            flushOut();
        } catch (IOException e) {
            throw Message.convertIOException(e, null);
        }
    }


    /**
     * Flush the transaction log.
     */
    void flush() throws SQLException {
        try {
            pageOut.flush();
        } catch (IOException e) {
            throw Message.convertIOException(e, null);
        }
    }

    /**
     * Switch to a new log id.
     */
    void checkpoint() throws SQLException {
        try {
            out.write(CHECKPOINT);
            flushOut();
        } catch (IOException e) {
            throw Message.convertIOException(e, null);
        }
        undo = new BitField();
        logSectionId++;
        logPos = 0;
        pageOut.fillPage();
        int currentDataPage = pageOut.getCurrentDataPageId();
        logSectionPageMap.put(logSectionId, currentDataPage);
    }

    int getLogSectionId() {
        return logSectionId;
    }

    long getLogPos() {
        return logPos;
    }

    /**
     * Remove all pages until the given log (excluding).
     *
     * @param firstUncommittedLog the first log id to keep
     */
    void removeUntil(int firstUncommittedLog) throws SQLException {
        if (firstUncommittedLog == 0) {
            return;
        }
        int firstDataPageToKeep = logSectionPageMap.get(firstUncommittedLog);
        firstTrunkPage = removeUntil(firstTrunkPage, firstDataPageToKeep);
        store.setLogFirstPage(firstTrunkPage, firstDataPageToKeep);
        while (firstLogId < firstUncommittedLog) {
            if (firstLogId > 0) {
                // there is no entry for log 0
                logSectionPageMap.remove(firstLogId);
            }
            firstLogId++;
        }
    }

    /**
     * Remove all pages until the given data page.
     *
     * @param firstTrunkPage the first trunk page
     * @param firstDataPageToKeep the first data page to keep
     * @return the trunk page of the data page to keep
     */
    private int removeUntil(int firstTrunkPage, int firstDataPageToKeep) throws SQLException {
        trace.debug("log.removeUntil " + firstDataPageToKeep);
        while (true) {
            PageStreamTrunk t = (PageStreamTrunk) store.getPage(firstTrunkPage);
            t.resetIndex();
            if (t.contains(firstDataPageToKeep)) {
                return t.getPos();
            }
            firstTrunkPage = t.getNextTrunk();
            IntArray list = new IntArray();
            list.add(t.getPos());
            while (true) {
                int next = t.getNextPageData();
                if (next == -1) {
                    break;
                }
                list.add(next);
            }
            freeLogPages(list);
            pageOut.free(t);
        }
    }

    /**
     * Close without further writing.
     */
    void close() {
        trace.debug("log close");
        if (pageOut != null) {
            pageOut.close();
            pageOut = null;
        }
        out = null;
    }

    /**
     * Check if the session committed after than the given position.
     *
     * @param sessionId the session id
     * @param logId the log file id
     * @param pos the position in the log file
     * @return true if it is committed
     */
    private boolean isSessionCommitted(int sessionId, int logId, int pos) {
        SessionState state = sessionStates.get(sessionId);
        if (state == null) {
            return false;
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
    private void setLastCommitForSession(int sessionId, int logId, int pos) {
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
    private SessionState getOrAddSessionState(int sessionId) {
        Integer key = sessionId;
        SessionState state = sessionStates.get(key);
        if (state == null) {
            state = new SessionState();
            sessionStates.put(key, state);
            state.sessionId = sessionId;
        }
        return state;
    }

    long getSize() {
        return pageOut == null ? 0 : pageOut.getSize();
    }

    ObjectArray<InDoubtTransaction> getInDoubtTransactions() {
        ObjectArray<InDoubtTransaction> list = ObjectArray.newInstance();
        for (SessionState state : sessionStates.values()) {
            InDoubtTransaction in = state.inDoubtTransaction;
            if (in != null) {
                list.add(in);
            }
        }
        return list;
    }

    /**
     * Set the state of an in-doubt transaction.
     *
     * @param sessionId the session
     * @param pageId the page where the commit was prepared
     * @param commit whether the transaction should be committed
     */
    void setInDoubtTransactionState(int sessionId, int pageId, boolean commit) throws SQLException {
        PageStreamData d = (PageStreamData) store.getPage(pageId);
        d.initWrite();
        ByteArrayOutputStream buff = new ByteArrayOutputStream();
        DataOutputStream o = new DataOutputStream(buff);
        try {
            o.write(commit ? COMMIT : ROLLBACK);
            o.writeInt(sessionId);
        } catch (IOException e) {
            throw Message.convertIOException(e, "");
        }
        byte[] bytes = buff.toByteArray();
        d.write(buff.toByteArray(), 0, bytes.length);
        bytes = new byte[d.getRemaining()];
        d.write(bytes, 0, bytes.length);
        d.write(null);
    }

    /**
     * Called after the recovery has been completed.
     */
    void recoverEnd() {
        sessionStates = New.hashMap();
    }

}
