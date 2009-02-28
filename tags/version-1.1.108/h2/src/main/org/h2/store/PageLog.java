/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.index.Page;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.result.Row;
import org.h2.util.BitField;
import org.h2.value.Value;

/**
 * Transaction log mechanism.
 * The format is:
 * <ul><li>0-3: log id
 * </li><li>records
 * </li></ul>
 * The data format for a record is:
 * <ul><li>0-0: type (0: undo,...)
 * </li><li>1-4: page id
 * </li><li>5-: data
 * </li></ul>
 */
public class PageLog {

    /**
     * No operation.
     */
    public static final int NO_OP = 0;

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

    private final PageStore store;
    private int id;
    private int pos;
    private Trace trace;

    private PageOutputStream pageOut;
    private DataOutputStream out;
    private DataInputStream in;
    private int firstPage;
    private DataPage data;
    private long operation;
    private BitField undo = new BitField();
    private int[] reservedPages = new int[2];

    PageLog(PageStore store, int firstPage) {
        this.store = store;
        this.firstPage = firstPage;
        data = store.createDataPage();
        trace = store.getTrace();
    }

    /**
     * Open the log for writing. For an existing database, the recovery
     * must be run first.
     *
     * @param id the log id
     */
    void openForWriting(int id) throws SQLException {
        this.id = id;
        trace.debug("log openForWriting " + id + " firstPage:" + firstPage);
        pageOut = new PageOutputStream(store, 0, firstPage, Page.TYPE_LOG, true);
        out = new DataOutputStream(pageOut);
        try {
            out.writeInt(id);
            out.flush();
        } catch (IOException e) {
            throw Message.convertIOException(e, null);
        }
    }

    /**
     * Open the log for reading. This will also read the log id.
     *
     * @return the log id
     */
    int openForReading() throws SQLException {
        in = new DataInputStream(new PageInputStream(store, 0, firstPage, Page.TYPE_LOG));
        try {
            id = in.readInt();
            trace.debug("log openForReading " + id + " firstPage:" + firstPage + " id:" + id);
            return id;
        } catch (IOException e) {
            return 0;
        }
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
            trace.debug("log recover " + id + " undo:" + undo);
        }
        DataPage data = store.createDataPage();
        try {
            pos = 0;
            while (true) {
                int x = in.read();
                if (x < 0) {
                    break;
                }
                pos++;
                if (x == NO_OP) {
                    // nothing to do
                } else if (x == UNDO) {
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
                        Database db = store.getDatabase();
                        if (store.isSessionCommitted(sessionId, id, pos)) {
                            if (trace.isDebugEnabled()) {
                                trace.debug("log redo " + (x == ADD ? "+" : "-") + " " + row);
                            }
                            db.redo(tableId, row, x == ADD);
                        }
                    }
                } else if (x == COMMIT) {
                    int sessionId = in.readInt();
                    if (undo) {
                        store.setLastCommitForSession(sessionId, id, pos);
                    }
                }
            }
        } catch (Exception e) {
e.printStackTrace();
            int todoOnlyIOExceptionAndSQLException;
            int todoSomeExceptionAreOkSomeNot;
            trace.debug("log recovery stopped: " + e.toString());
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
            reservePages(2);
            out.write(UNDO);
            out.writeInt(pageId);
            out.write(page.getBytes(), 0, store.getPageSize());
        } catch (IOException e) {
            throw Message.convertIOException(e, null);
        }
    }

    private void reservePages(int pageCount) throws SQLException {
        int testIfRequired;
        if (pageCount > reservedPages.length) {
            reservedPages = new int[pageCount];
        }
        for (int i = 0; i < pageCount; i++) {
            reservedPages[i] = store.allocatePage();
        }
        for (int i = 0; i < pageCount; i++) {
            store.freePage(reservedPages[i]);
        }
    }

    /**
     * Mark a committed transaction.
     *
     * @param session the session
     */
    void commit(Session session) throws SQLException {
        try {
            trace.debug("log commit");
            reservePages(1);
            out.write(COMMIT);
            out.writeInt(session.getId());
            if (store.getDatabase().getLog().getFlushOnEachCommit()) {
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
                trace.debug("log " + (add?"+":"-") + " table:" + tableId +
                        " row:" + row);
            }
            int todoLogPosShouldBeLong;
            session.addLogPos(0, (int) operation);
            row.setLastLog(0, (int) operation);

            data.reset();
            int todoWriteIntoOutputDirectly;
            row.write(data);

            reservePages(1 + data.length() / store.getPageSize());

            out.write(add ? ADD : REMOVE);
            out.writeInt(session.getId());
            out.writeInt(tableId);
            out.writeInt(row.getPos());
            out.writeInt(data.length());
            out.write(data.getBytes(), 0, data.length());
        } catch (IOException e) {
            throw Message.convertIOException(e, null);
        }
    }

    /**
     * Close the log.
     */
    void close() throws SQLException {
        try {
            trace.debug("log close " + id);
            if (out != null) {
                out.close();
            }
            out = null;
        } catch (IOException e) {
            throw Message.convertIOException(e, null);
        }
    }

    /**
     * Close the log, truncate it, and re-open it.
     *
     * @param id the new log id
     */
    private void reopen(int id) throws SQLException {
        try {
            trace.debug("log reopen");
            out.close();
            openForWriting(id);
            flush();
            int todoDeleteOrReUsePages;
        } catch (IOException e) {
            throw Message.convertIOException(e, null);
        }
    }

    /**
     * Flush the transaction log.
     */
    void flush() throws SQLException {
        try {
            int todoUseLessSpace;
            trace.debug("log flush");
            out.flush();
        } catch (IOException e) {
            throw Message.convertIOException(e, null);
        }
    }

    /**
     * Get the log id.
     *
     * @return the log id
     */
    int getId() {
        return id;
    }

    /**
     * Flush and close the log.
     */
//    public void close() throws SQLException {
//        try {
//            trace.debug("log close");
//            out.close();
//        } catch (IOException e) {
//            throw Message.convertIOException(e, null);
//        }
//    }

}
