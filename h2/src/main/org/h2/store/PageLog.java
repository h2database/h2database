/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License,
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
 * The data format is:
 * <ul><li>0-0: type (0: undo,...)
 * </li><li>1-4: page id
 * </li><li>5-: data
 * </li></ul>
 */
public class PageLog {

    private static final int NO_OP = 0;
    private static final int UNDO = 1;
    private static final int COMMIT = 2;
    private static final int ADD = 3;
    private static final int REMOVE = 4;

    private PageStore store;
    private Trace trace;
    private BitField undo = new BitField();
    private PageOutputStream pageOut;
    private DataOutputStream out;
    private int firstPage;
    private DataPage data;
    private boolean recoveryRunning;

    PageLog(PageStore store, int firstPage) {
        this.store = store;
        this.firstPage = firstPage;
        data = store.createDataPage();
        trace = store.getTrace();
    }

    /**
     * Open the log file for writing. For an existing database, the recovery
     * must be run first.
     */
    void openForWriting() {
        trace.debug("openForWriting");
        pageOut = new PageOutputStream(store, 0, firstPage, Page.TYPE_LOG);
        out = new DataOutputStream(pageOut);
    }

    /**
     * Run the recovery process. There are two recovery stages: first only the
     * undo steps are run (restoring the state before the last checkpoint). In
     * the second stage the committed operations are re-applied.
     *
     * @param undo true if the undo step should be run
     */
    public void recover(boolean undo) throws SQLException {
        trace.debug("recover");
        DataInputStream in = new DataInputStream(new PageInputStream(store, 0, firstPage, Page.TYPE_LOG));
        DataPage data = store.createDataPage();
        try {
            recoveryRunning = true;
            while (true) {
                int x = in.read();
                if (x < 0) {
                    break;
                }
                if (x == NO_OP) {
                    // nothing to do
                } else if (x == UNDO) {
                    int pageId = in.readInt();
                    in.read(data.getBytes(), 0, store.getPageSize());
                    if (undo) {
                        store.writePage(pageId, data);
                    }
                } else if (x == ADD || x == REMOVE) {
                    int sessionId = in.readInt();
                    int tableId = in.readInt();
                    Row row = readRow(in);
                    Database db = store.getDatabase();
                    if (!undo) {
                        db.redo(tableId, row, x == ADD);
                    }
                } else if (x == COMMIT) {

                }
            }
        } catch (Exception e) {
            int todoOnlyIOExceptionAndSQLException;
            int todoSomeExceptionAreOkSomeNot;
//e.printStackTrace();
            trace.debug("recovery stopped: " + e.toString());
        } finally {
            recoveryRunning = false;
        }
        int todoDeleteAfterRecovering;
    }

    private Row readRow(DataInputStream in) throws IOException, SQLException {
        int len = in.readInt();
        data.reset();
        data.checkCapacity(len);
        in.read(data.getBytes(), 0, len);
        int columnCount = data.readInt();
        Value[] values = new Value[columnCount];
        for (int i = 0; i < columnCount; i++) {
            values[i] = data.readValue();
        }
        int todoTableDataReadRowWithMemory;
        Row row = new Row(values, 0);
        return row;
    }

    /**
     * Add an undo entry to the log. The page data is only written once until
     * the next checkpoint.
     *
     * @param pageId the page id
     * @param page the old page data
     */
    public void addUndo(int pageId, DataPage page) throws SQLException {
        try {
            if (undo.get(pageId)) {
                return;
            }
            out.write(UNDO);
            out.writeInt(pageId);
            out.write(page.getBytes(), 0, store.getPageSize());
            undo.set(pageId);
        } catch (IOException e) {
            throw Message.convertIOException(e, "recovering");
        }
    }

    /**
     * Mark a committed transaction.
     *
     * @param session the session
     */
    public void commit(Session session) throws SQLException {
        try {
            trace.debug("commit");
            out.write(COMMIT);
            out.writeInt(session.getId());
        } catch (IOException e) {
            throw Message.convertIOException(e, "recovering");
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
    public void addOrRemoveRow(Session session, int tableId, Row row, boolean add) throws SQLException {
        try {
            if (recoveryRunning) {
                return;
            }
            if (trace.isDebugEnabled()) {
                trace.debug((add?"+":"-") + " table:" + tableId +
                        " remaining:" + pageOut.getRemainingBytes() + " row:" + row);
            }
            out.write(add ? ADD : REMOVE);
            out.writeInt(session.getId());
            out.writeInt(tableId);
            data.reset();
            row.write(data);
            out.writeInt(data.length());
            out.write(data.getBytes(), 0, data.length());
        } catch (IOException e) {
            throw Message.convertIOException(e, null);
        }
    }

    /**
     * Close the log, truncate it, and re-open it.
     */
    void reopen() throws SQLException {
        try {
            out.close();
            openForWriting();
            int todoDeleteOrReUsePages;
        } catch (IOException e) {
            throw Message.convertIOException(e, null);
        }
    }

    /**
     * Flush the transaction log.
     */
    public void flush() throws SQLException {
        try {
            trace.debug("flush");
            out.flush();
            int filler = pageOut.getRemainingBytes();
            for (int i = 0; i < filler; i++) {
                out.writeByte(NO_OP);
            }
        } catch (IOException e) {
            throw Message.convertIOException(e, null);
        }
    }

    /**
     * Flush and close the log.
     */
    public void close() throws SQLException {
        try {
            trace.debug("close");
            out.close();
        } catch (IOException e) {
            throw Message.convertIOException(e, null);
        }
    }

}
