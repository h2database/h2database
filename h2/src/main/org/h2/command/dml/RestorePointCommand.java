/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: Enno Thieleke
 */
package org.h2.command.dml;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.command.Prepared;
import org.h2.engine.Database;
import org.h2.engine.SessionLocal;
import org.h2.message.DbException;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.db.Store;
import org.h2.mvstore.tx.TransactionStore;
import org.h2.mvstore.type.RestorePoint;
import org.h2.result.ResultInterface;
import org.h2.value.ValueBigint;
import org.h2.value.ValueTimestampTimeZone;

/**
 * This class represents the statements for dealing with restore points.
 */
public class RestorePointCommand extends Prepared {

    private final int type;
    private final String name;

    public RestorePointCommand(SessionLocal session, int type, String name) {
        super(session);
        this.type = type;
        this.name = name;
    }

    @Override
    public long update() {
        session.getUser().checkAdmin();
        switch (type) {
        case CommandInterface.CREATE_RESTORE_POINT:
            createRestorePoint();
            break;
        case CommandInterface.RESTORE_TO_POINT:
            restoreToPoint();
            break;
        case CommandInterface.DROP_RESTORE_POINT:
            dropRestorePoint();
            break;
        default: throw DbException.getInternalError("type=" + type);
        }
        return 0;
    }

    private void createRestorePoint() {
        Database db = getDatabase();
        MVStore mvStore = db.getStore().getMvStore();
        mvStore.lock();
        int autoCommitDelay = mvStore.getAutoCommitDelay();
        try {
            if (mvStore.findRestorePoint(name) != null) {
                throw DbException.get(ErrorCode.RESTORE_POINT_ALREADY_EXISTS, name);
            }
            mvStore.setAutoCommitDelay(0);
            session.commit(false); // Commit pending changes.
            ValueTimestampTimeZone createdAt = db.currentTimestamp();
            ValueBigint databaseVersion = ValueBigint.get(mvStore.getCurrentVersion() + 1);
            // The oldest database version to keep across all restore points is always the same.
            long temp = mvStore.getOldestRestorePointVersion();
            ValueBigint oldestDatabaseVersionToKeep;
            if (temp == Long.MAX_VALUE) {
                oldestDatabaseVersionToKeep = databaseVersion;
            } else {
                oldestDatabaseVersionToKeep = ValueBigint.get(temp);
            }
            RestorePoint rp = new RestorePoint(name, createdAt, oldestDatabaseVersionToKeep, databaseVersion);
            mvStore.addRestorePoint(rp.getDatabaseVersion().getLong(), rp);
            mvStore.commit(); // Flush changes to disk.
            db.getNextModificationMetaId(); // To invalidate previous results of selects from restore points.
            assert databaseVersion.getLong() == mvStore.getCurrentVersion();
        } finally {
            mvStore.setAutoCommitDelay(autoCommitDelay);
            mvStore.unlock();
        }
    }

    private void restoreToPoint() {
        Database db = getDatabase();
        try {
            if (!db.setExclusiveSession(session, true)) {
                throw DbException.get(ErrorCode.COULD_NOT_SWITCH_DATABASE_TO_EXCLUSIVE_MODE);
            }
            RestorePoint rp = db.getStore().getMvStore().findRestorePoint(name);
            if (rp == null) {
                throw DbException.get(ErrorCode.RESTORE_POINT_NOT_FOUND, name);
            }
            session.rollback(); // Discard the implicitly started transaction.
            Store store = db.getStore();
            MVStore mvStore = store.getMvStore();
            mvStore.lock();
            int autoCommitDelay = mvStore.getAutoCommitDelay();
            try {
                mvStore.setAutoCommitDelay(0);
                mvStore.rollbackTo(rp.getDatabaseVersion().getLong());

                TransactionStore txStore = store.getTransactionStore();
                txStore.reinit(); // This reads any leftover transactions back into the store...
                txStore.endLeftoverTransactions(); // ...and this ends them (i.e. removes unwanted data).

                /* This might have brought back restore points that have been deleted.
                 * Or it might have removed restore points, which hadn't been created at the point we're returning to.
                 */
                db.executeMeta(session);
            } finally {
                mvStore.setAutoCommitDelay(autoCommitDelay);
                mvStore.unlock();
            }
        } finally {
            db.unsetExclusiveSession(session);
        }
    }

    private void dropRestorePoint() {
        Database db = getDatabase();
        MVStore mvStore = db.getStore().getMvStore();
        mvStore.lock();
        try {
            RestorePoint rp = mvStore.findRestorePoint(name);
            if (rp == null) {
                throw DbException.get(ErrorCode.RESTORE_POINT_NOT_FOUND, name);
            }
            mvStore.removeRestorePoint(rp.getDatabaseVersion().getLong());
            session.commit(false);
            db.getNextModificationMetaId(); // To invalidate previous results of selects from restore points.
        } finally {
            mvStore.unlock();
        }
    }

    @Override
    public boolean isTransactional() {
        return false;
    }

    @Override
    public boolean needRecompile() {
        return false;
    }

    @Override
    public ResultInterface queryMeta() {
        return null;
    }

    @Override
    public int getType() {
        return type;
    }
}
