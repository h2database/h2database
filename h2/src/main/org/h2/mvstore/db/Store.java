/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.h2.api.ErrorCode;
import org.h2.command.ddl.CreateTableData;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.SessionLocal;
import org.h2.message.DbException;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.FileStore;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.MVStoreException;
import org.h2.mvstore.MVStoreTool;
import org.h2.mvstore.tx.Transaction;
import org.h2.mvstore.tx.TransactionStore;
import org.h2.mvstore.type.MetaType;
import org.h2.store.InDoubtTransaction;
import org.h2.store.fs.FileUtils;
import org.h2.util.HasSQL;
import org.h2.util.StringUtils;
import org.h2.util.Utils;
import org.h2.value.TypeInfo;
import org.h2.value.Typed;
import org.h2.value.Value;

/**
 * A store with open tables.
 */
public final class Store {

    /**
     * Convert password from byte[] to char[].
     *
     * @param key password as byte[]
     * @return password as char[].
     */
    static char[] decodePassword(byte[] key) {
        char[] password = new char[key.length / 2];
        for (int i = 0; i < password.length; i++) {
            password[i] = (char) (((key[i + i] & 255) << 16) | (key[i + i + 1] & 255));
        }
        return password;
    }

    /**
     * The map of open tables.
     * Key: the map name, value: the table.
     */
    private final ConcurrentHashMap<String, MVTable> tableMap = new ConcurrentHashMap<>();

    /**
     * The store.
     */
    private final MVStore mvStore;

    /**
     * The transaction store.
     */
    private final TransactionStore transactionStore;

    private long statisticsStart;

    private int temporaryMapId;

    private final boolean encrypted;

    private final String fileName;

    /**
     * Creates the store.
     *
     * @param db the database
     * @param key for file encryption
     */
    public Store(Database db, byte[] key) {
        String dbPath = db.getDatabasePath();
        MVStore.Builder builder = new MVStore.Builder();
        boolean encrypted = false;
        if (dbPath != null) {
            String fileName = dbPath + Constants.SUFFIX_MV_FILE;
            this.fileName = fileName;
            MVStoreTool.compactCleanUp(fileName);
            builder.fileName(fileName);
            builder.pageSplitSize(db.getPageSize());
            if (db.isReadOnly()) {
                builder.readOnly();
            } else {
                // possibly create the directory
                boolean exists = FileUtils.exists(fileName);
                if (exists && !FileUtils.canWrite(fileName)) {
                    // read only
                } else {
                    String dir = FileUtils.getParent(fileName);
                    FileUtils.createDirectories(dir);
                }
                int autoCompactFillRate = db.getSettings().autoCompactFillRate;
                if (autoCompactFillRate <= 100) {
                    builder.autoCompactFillRate(autoCompactFillRate);
                }
            }
            if (key != null) {
                encrypted = true;
                builder.encryptionKey(decodePassword(key));
            }
            if (db.getSettings().compressData) {
                builder.compress();
                // use a larger page split size to improve the compression ratio
                builder.pageSplitSize(64 * 1024);
            }
            builder.backgroundExceptionHandler((t, e) -> db.setBackgroundException(DbException.convert(e)));
            // always start without background thread first, and if necessary,
            // it will be set up later, after db has been fully started,
            // otherwise background thread would compete for store lock
            // with maps opening procedure
            builder.autoCommitDisabled();
        } else {
            fileName = null;
        }
        this.encrypted = encrypted;
        try {
            this.mvStore = builder.open();
            if (!db.getSettings().reuseSpace) {
                mvStore.setReuseSpace(false);
            }
            mvStore.setVersionsToKeep(0);
            this.transactionStore = new TransactionStore(mvStore,
                    new MetaType<>(db, mvStore.backgroundExceptionHandler), new ValueDataType(db, null),
                    db.getLockTimeout());
        } catch (MVStoreException e) {
            throw convertMVStoreException(e);
        }
    }

    /**
     * Convert a MVStoreException to the similar exception used
     * for the table/sql layers.
     *
     * @param e the illegal state exception
     * @return the database exception
     */
    DbException convertMVStoreException(MVStoreException e) {
        switch (e.getErrorCode()) {
        case DataUtils.ERROR_CLOSED:
            throw DbException.get(ErrorCode.DATABASE_IS_CLOSED, e, fileName);
        case DataUtils.ERROR_UNSUPPORTED_FORMAT:
            throw DbException.get(ErrorCode.FILE_VERSION_ERROR_1, e, fileName);
        case DataUtils.ERROR_FILE_CORRUPT:
            if (encrypted) {
                throw DbException.get(ErrorCode.FILE_ENCRYPTION_ERROR_1, e, fileName);
            }
            throw DbException.get(ErrorCode.FILE_CORRUPTED_1, e, fileName);
        case DataUtils.ERROR_FILE_LOCKED:
            throw DbException.get(ErrorCode.DATABASE_ALREADY_OPEN_1, e, fileName);
        case DataUtils.ERROR_READING_FAILED:
        case DataUtils.ERROR_WRITING_FAILED:
            throw DbException.get(ErrorCode.IO_EXCEPTION_1, e, fileName);
        default:
            throw DbException.get(ErrorCode.GENERAL_ERROR_1, e, e.getMessage());
        }
    }

    /**
     * Gets a SQL exception meaning the type of expression is invalid or unknown.
     *
     * @param param the name of the parameter
     * @param e the expression
     * @return the exception
     */
    public static DbException getInvalidExpressionTypeException(String param, Typed e) {
        TypeInfo type = e.getType();
        if (type.getValueType() == Value.UNKNOWN) {
            return DbException.get(ErrorCode.UNKNOWN_DATA_TYPE_1,
                                    (e instanceof HasSQL ? (HasSQL) e : type).getTraceSQL());
        }
        return DbException.get(ErrorCode.INVALID_VALUE_2, type.getTraceSQL(), param);
    }

    public MVStore getMvStore() {
        return mvStore;
    }

    public TransactionStore getTransactionStore() {
        return transactionStore;
    }

    /**
     * Get MVTable by table name.
     *
     * @param tableName table name
     * @return MVTable
     */
    public MVTable getTable(String tableName) {
        return tableMap.get(tableName);
    }

    /**
     * Create a table.
     *
     * @param data CreateTableData
     * @return table created
     */
    public MVTable createTable(CreateTableData data) {
        try {
            MVTable table = new MVTable(data, this);
            tableMap.put(table.getMapName(), table);
            return table;
        } catch (MVStoreException e) {
            throw convertMVStoreException(e);
        }
    }

    /**
     * Remove a table.
     *
     * @param table the table
     */
    public void removeTable(MVTable table) {
        try {
            tableMap.remove(table.getMapName());
        } catch (MVStoreException e) {
            throw convertMVStoreException(e);
        }
    }

    /**
     * Store all pending changes.
     */
    public void flush() {
        if (mvStore.isPersistent() && !mvStore.isReadOnly()) {
            mvStore.commit();
        }
    }

    /**
     * Close the store, without persisting changes.
     */
    public void closeImmediately() {
        mvStore.closeImmediately();
    }

    /**
     * Remove all temporary maps.
     *
     * @param objectIds the ids of the objects to keep
     */
    public void removeTemporaryMaps(BitSet objectIds) {
        for (String mapName : mvStore.getMapNames()) {
            if (mapName.startsWith("temp.")) {
                mvStore.removeMap(mapName);
            } else if (mapName.startsWith("table.") || mapName.startsWith("index.")) {
                int id = StringUtils.parseUInt31(mapName, mapName.indexOf('.') + 1, mapName.length());
                if (!objectIds.get(id)) {
                    mvStore.removeMap(mapName);
                }
            }
        }
    }

    /**
     * Get the name of the next available temporary map.
     *
     * @return the map name
     */
    public synchronized String nextTemporaryMapName() {
        return "temp." + temporaryMapId++;
    }

    /**
     * Prepare a transaction.
     *
     * @param session the session
     * @param transactionName the transaction name (may be null)
     */
    public void prepareCommit(SessionLocal session, String transactionName) {
        Transaction t = session.getTransaction();
        t.setName(transactionName);
        t.prepare();
        mvStore.commit();
    }

    public ArrayList<InDoubtTransaction> getInDoubtTransactions() {
        List<Transaction> list = transactionStore.getOpenTransactions();
        ArrayList<InDoubtTransaction> result = Utils.newSmallArrayList();
        for (Transaction t : list) {
            if (t.getStatus() == Transaction.STATUS_PREPARED) {
                result.add(new MVInDoubtTransaction(mvStore, t));
            }
        }
        return result;
    }

    /**
     * Set the maximum memory to be used by the cache.
     *
     * @param kb the maximum size in KB
     */
    public void setCacheSize(int kb) {
        mvStore.setCacheSize(kb);
    }

    /**
     * Force the changes to disk.
     */
    public void sync() {
        flush();
        mvStore.sync();
    }

    /**
     * Compact the database file, that is, compact blocks that have a low
     * fill rate, and move chunks next to each other. This will typically
     * shrink the database file. Changes are flushed to the file, and old
     * chunks are overwritten.
     *
     * @param maxCompactTime the maximum time in milliseconds to compact
     */
    @SuppressWarnings("unused")
    public void compactFile(int maxCompactTime) {
        mvStore.compactFile(maxCompactTime);
    }

    /**
     * Close the store. Pending changes are persisted.
     *
     * @param allowedCompactionTime time (in milliseconds) allotted for store
     *                              housekeeping activity, 0 means none,
     *                              -1 means unlimited time (i.e.full compaction)
     */
    public void close(int allowedCompactionTime) {
        try {
            FileStore<?> fileStore = mvStore.getFileStore();
            if (!mvStore.isClosed() && fileStore != null) {
                boolean compactFully = allowedCompactionTime == -1;
                if (fileStore.isReadOnly()) {
                    compactFully = false;
                } else {
                    transactionStore.close();
                }
                if (compactFully) {
                    allowedCompactionTime = 0;
                }

                String fileName = null;
                FileStore<?> targetFileStore = null;
                if (compactFully) {
                    fileName = fileStore.getFileName();
                    String tempName = fileName + Constants.SUFFIX_MV_STORE_TEMP_FILE;
                    FileUtils.delete(tempName);
                    targetFileStore = fileStore.open(tempName, false);
                }

                mvStore.close(allowedCompactionTime);

                if (compactFully && FileUtils.exists(fileName)) {
                    // the file could have been deleted concurrently,
                    // so only compact if the file still exists
                    compact(fileName, targetFileStore);
                }
            }
        } catch (MVStoreException e) {
            mvStore.closeImmediately();
            throw DbException.get(ErrorCode.IO_EXCEPTION_1, e, "Closing");
        }
    }


    private static void compact(String sourceFilename, FileStore<?> targetFileStore) {
        MVStore.Builder targetBuilder = new MVStore.Builder().compress().adoptFileStore(targetFileStore);
        try (MVStore targetMVStore = targetBuilder.open()) {
            FileStore<?> sourceFileStore = targetFileStore.open(sourceFilename, true);
            MVStore.Builder sourceBuilder = new MVStore.Builder();
            sourceBuilder.readOnly().adoptFileStore(sourceFileStore);
            try (MVStore sourceMVStore = sourceBuilder.open()) {
                MVStoreTool.compact(sourceMVStore, targetMVStore);
            }
        }
        MVStoreTool.moveAtomicReplace(targetFileStore.getFileName(), sourceFilename);
    }

    /**
     * Start collecting statistics.
     */
    public void statisticsStart() {
        FileStore<?> fs = mvStore.getFileStore();
        statisticsStart = fs == null ? 0 : fs.getReadCount();
    }

    /**
     * Stop collecting statistics.
     *
     * @return the statistics
     */
    public Map<String, Integer> statisticsEnd() {
        HashMap<String, Integer> map = new HashMap<>();
        FileStore<?> fs = mvStore.getFileStore();
        int reads = fs == null ? 0 : (int) (fs.getReadCount() - statisticsStart);
        map.put("reads", reads);
        return map;
    }

}
