/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import java.io.IOException;
import java.lang.ref.Reference;
import java.util.ArrayList;

import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.message.DbException;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.MVStore.Builder;
import org.h2.result.ResultExternal;
import org.h2.result.SortOrder;
import org.h2.store.fs.FileUtils;
import org.h2.util.TempFileDeleter;
import org.h2.value.Value;

/**
 * Temporary result.
 */
public abstract class MVTempResult implements ResultExternal {

    /**
     * Creates MVStore-based temporary result.
     *
     * @param session
     *                        session
     * @param expressions
     *                        expressions
     * @param distinct
     *                        is output distinct
     * @param sort
     *                        sort order, or {@code null}
     * @return temporary result
     */
    public static ResultExternal of(Session session, Expression[] expressions, boolean distinct, SortOrder sort) {
        return distinct || sort != null ? new MVSortedTempResult(session, expressions, distinct, sort)
                : new MVPlainTempResult(session, expressions);
    }

    final MVStore store;

    int rowCount;

    final MVTempResult parent;

    int childCount;

    boolean closed;

    private final TempFileDeleter tempFileDeleter;
    private final String fileName;
    private final Reference<?> fileRef;

    MVTempResult(MVTempResult parent) {
        this.parent = parent;
        this.store = parent.store;
        this.fileName = null;
        this.tempFileDeleter = null;
        this.fileRef = null;
    }

    MVTempResult(Session session) {
        try {
            fileName = FileUtils.createTempFile("h2tmp", Constants.SUFFIX_TEMP_FILE, false, true);
            Builder builder = new MVStore.Builder().autoCommitDisabled().fileName(fileName);
            byte[] key = session.getDatabase().getFileEncryptionKey();
            if (key != null) {
                builder.encryptionKey(MVTableEngine.decodePassword(key));
            }
            store = builder.open();
            tempFileDeleter = session.getDatabase().getTempFileDeleter();
            fileRef = tempFileDeleter.addFile(fileName, this);
        } catch (IOException e) {
            throw DbException.convert(e);
        }
        parent = null;
    }

    @Override
    public int addRows(ArrayList<Value[]> rows) {
        for (Value[] row : rows) {
            addRow(row);
        }
        return rowCount;
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (parent != null) {
            parent.closeChild();
        } else {
            if (childCount == 0) {
                delete();
            }
        }
    }

    private synchronized void closeChild() {
        if (--childCount == 0 && closed) {
            delete();
        }
    }

    private void delete() {
        store.closeImmediately();
        tempFileDeleter.deleteFile(fileRef, fileName);
    }

    @Override
    public void done() {
        // Do nothing
    }

}
