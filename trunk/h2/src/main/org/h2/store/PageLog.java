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
import org.h2.log.LogSystem;
import org.h2.log.SessionState;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.result.Row;
import org.h2.util.BitField;
import org.h2.util.IntIntHashMap;
import org.h2.util.New;
import org.h2.value.Value;

/**
 * Transaction log mechanism. The stream contains a list of records. The data
 * format for a record is:
 * <ul>
 * <li>0-0: type (0: undo,...)</li>
 * <li>1-4: page id</li>
 * <li>5-: data</li>
 * </ul>
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
     * Add a record to a table.
     * Format: session id, table id, row.
     */
    public static final int ADD = 3;

    /**
     * Remove a record from a table.
     * Format: session id, table id, row.
     */
    public static final int REMOVE = 4;

    /**
     * Perform a checkpoint. The log id is incremented.
     * Format: -
     */
    public static final int CHECKPOINT = 5;

    private final PageStore store;
    private int pos;
    private Trace trace;

    private DataOutputStream out;
    private ByteArrayOutputStream buffer;
    private PageOutputStream pageOut;
    private DataInputStream in;
    private int firstTrunkPage;
    private int firstDataPage;
    private DataPage data;
    private int logId, logPos;
    private int firstLogId;
    private BitField undo = new BitField();
    private IntIntHashMap logIdPageMap = new IntIntHashMap();
    private HashMap<Integer, SessionState> sessionStates = New.hashMap();

    PageLog(PageStore store) {
        this.store = store;
        data = store.createDataPage();
        trace = store.getTrace();
    }

    /**
     * Open the log for writing. For an existing database, the recovery
     * must be run first.
     *
     * @param firstTrunkPage the first trunk page
     */
    void openForWriting(int firstTrunkPage) throws SQLException {
        trace.debug("log openForWriting firstPage:" + firstTrunkPage);
        this.firstTrunkPage = firstTrunkPage;
        pageOut = new PageOutputStream(store, firstTrunkPage);
        pageOut.reserve(1);
        store.setLogFirstPage(firstTrunkPage, pageOut.getCurrentDataPageId());
        buffer = new ByteArrayOutputStream();
        out = new DataOutputStream(buffer);
    }

    /**
     * Free up all pages allocated by the log.
     */
    void free() throws SQLException {
        if (this.firstTrunkPage != 0) {
            // first remove all old log pages
            PageStreamTrunk t = new PageStreamTrunk(store, this.firstTrunkPage);
            t.read();
            t.free();
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
     * Run the recovery process. There are two recovery stages: first only the
     * undo steps are run (restoring the state before the last checkpoint). In
     * the second stage the committed operations are re-applied.
     *
     * @param undo true if the undo step should be run
     */
    void recover(boolean undo) throws SQLException {
        if (trace.isDebugEnabled()) {
            trace.debug("log recover undo:" + undo);
        }
        in = new DataInputStream(new PageInputStream(store, firstTrunkPage, firstDataPage));
        int logId = 0;
        DataPage data = store.createDataPage();
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
                    if (undo) {
                        if (trace.isDebugEnabled()) {
                            trace.debug("log undo " + pageId);
                        }
                        store.writePage(pageId, data);
                    }
                } else if (x == ADD || x == REMOVE) {
                    int sessionId = in.readInt();
                    int tableId = in.readInt();
                    Row row = readRow(in, data);
                    if (!undo) {
                        if (isSessionCommitted(sessionId, logId, pos)) {
                            if (trace.isDebugEnabled()) {
                                trace.debug("log redo " + (x == ADD ? "+" : "-") + " table:" + tableId + " " + row);
                            }
                            store.redo(tableId, row, x == ADD);
                        } else {
                            if (trace.isDebugEnabled()) {
                                trace.debug("log ignore s:" + sessionId + " " + (x == ADD ? "+" : "-") + " table:" + tableId + " " + row);
                            }
                        }
                    }
                } else if (x == COMMIT) {
                    int sessionId = in.readInt();
                    if (trace.isDebugEnabled()) {
                        trace.debug("log commit " + sessionId + " pos:" + pos);
                    }
                    if (undo) {
                        setLastCommitForSession(sessionId, logId, pos);
                    }
                } else  if (x == NOOP) {
                    // nothing to do
                } else if (x == CHECKPOINT) {
                    logId++;
                } else {
                    if (trace.isDebugEnabled()) {
                        trace.debug("log end");
                        break;
                    }
                }
            }
            if (!undo) {
                // TODO probably still required for 2 phase commit
                sessionStates = New.hashMap();
            }
        } catch (EOFException e) {
            trace.debug("log recovery stopped: " + e.toString());
        } catch (IOException e) {
            throw Message.convertIOException(e, "recover");
        }
    }

    /**
     * Read a row from an input stream.
     *
     * @param in the input stream
     * @param data a temporary buffer
     * @return the row
     */
    public static Row readRow(DataInputStream in, DataPage data) throws IOException, SQLException {
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
        int todoTableDataReadRowWithMemory;
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
    void addUndo(int pageId, DataPage page) throws SQLException {
        try {
            if (undo.get(pageId)) {
                return;
            }
            if (trace.isDebugEnabled()) {
                trace.debug("log undo " + pageId);
            }
            undo.set(pageId);
            out.write(UNDO);
            out.writeInt(pageId);
            out.write(page.getBytes(), 0, store.getPageSize());
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
     * Mark a committed transaction.
     *
     * @param session the session
     */
    void commit(Session session) throws SQLException {
        try {
            if (trace.isDebugEnabled()) {
                trace.debug("log commit s:" + session.getId());
            }
            LogSystem log = store.getDatabase().getLog();
            if (log == null) {
                // database already closed
                return;
            }
            out.write(COMMIT);
            out.writeInt(session.getId());
            flushOut();
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
            session.addLogPos(logId, logPos);
            row.setLastLog(logId, logPos);

            data.reset();
            int todoWriteIntoOutputDirectly;
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
        logId++;
        pageOut.fillDataPage();
        int currentDataPage = pageOut.getCurrentDataPageId();
        logIdPageMap.put(logId, currentDataPage);
    }

    int getLogId() {
        return logId;
    }

    int getLogPos() {
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
        int firstDataPageToKeep = logIdPageMap.get(firstUncommittedLog);
        trace.debug("log.removeUntil " + firstDataPageToKeep);
        while (true) {
            // TODO keep trunk page in the cache
            PageStreamTrunk t = new PageStreamTrunk(store, firstTrunkPage);
            t.read();
            if (t.contains(firstDataPageToKeep)) {
                store.setLogFirstPage(t.getPos(), firstDataPageToKeep);
                break;
            }
            firstTrunkPage = t.getNextTrunk();
            t.free();
        }
        while (firstLogId < firstUncommittedLog) {
            if (firstLogId > 0) {
                // there is no entry for log 0
                logIdPageMap.remove(firstLogId);
            }
            firstLogId++;
        }
    }

    /**
     * Close the log.
     */
    void close() throws SQLException {
        try {
            trace.debug("log close");
            if (out != null) {
                out.close();
            }
            out = null;
        } catch (IOException e) {
            throw Message.convertIOException(e, null);
        }
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

}
