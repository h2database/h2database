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
import org.h2.index.Page;
import org.h2.message.Message;
import org.h2.util.BitField;

/**
 * Transaction log mechanism.
 * The data format is:
 * <ul><li>0-0: type (0: undo)
 * </li><li>1-4: page id
 * </li><li>5-: data
 * </li></ul>
 */
public class PageLog {

    private static final int UNDO = 0;
    private PageStore store;
    private BitField undo = new BitField();
    private DataOutputStream out;
    private int firstPage;

    PageLog(PageStore store, int firstPage) {
        this.store = store;
        this.firstPage = firstPage;
    }

    /**
     * Open the log file for writing. For an existing database, the recovery
     * must be run first.
     */
    void openForWriting() {
        out = new DataOutputStream(new PageOutputStream(store, 0, firstPage, Page.TYPE_LOG));
    }

    /**
     * Run the recovery process. Uncommitted transactions are rolled back.
     */
    public void recover() throws SQLException {
        DataInputStream in = new DataInputStream(new PageInputStream(store, 0, firstPage, Page.TYPE_LOG));
        DataPage data = store.createDataPage();
        try {
            while (true) {
                int x = in.read();
                if (x < 0) {
                    break;
                }
                if (x == UNDO) {
                    int pageId = in.readInt();
                    in.read(data.getBytes(), 0, store.getPageSize());
                    store.writePage(pageId, data);
                }
            }
        } catch (IOException e) {
            throw Message.convertIOException(e, "recovering");
        }
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

}
