/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

import org.h2.api.DatabaseEventListener;
import org.h2.command.dml.SetTypes;
import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.constraint.Constraint;
import org.h2.index.BtreeIndex;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.log.LogSystem;
import org.h2.log.UndoLogRecord;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.message.TraceSystem;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.schema.Schema;
import org.h2.schema.SchemaObject;
import org.h2.schema.Sequence;
import org.h2.schema.TriggerObject;
import org.h2.store.DataHandler;
import org.h2.store.DataPage;
import org.h2.store.DiskFile;
import org.h2.store.FileLock;
import org.h2.store.FileStore;
import org.h2.store.PageStore;
import org.h2.store.RecordReader;
import org.h2.store.Storage;
import org.h2.store.WriterThread;
import org.h2.store.fs.FileSystem;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.MetaTable;
import org.h2.table.Table;
import org.h2.table.TableData;
import org.h2.table.TableLinkConnection;
import org.h2.table.TableView;
import org.h2.tools.DeleteDbFiles;
import org.h2.tools.Server;
import org.h2.util.BitField;
import org.h2.util.ByteUtils;
import org.h2.util.ClassUtils;
import org.h2.util.FileUtils;
import org.h2.util.NetUtils;
import org.h2.util.New;
import org.h2.util.ObjectArray;
import org.h2.util.SmallLRUCache;
import org.h2.util.StringUtils;
import org.h2.util.TempFileDeleter;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueInt;
import org.h2.value.ValueLob;

/**
 * There is one database object per open database.
 *
 * The format of the meta data table is:
 *  id int, headPos int (for indexes), objectType int, sql varchar
 *
 * @since 2004-04-15 22:49
 */
public class Database implements DataHandler {

    private static int initialPowerOffCount;

    private final boolean persistent;
    private final String databaseName;
    private final String databaseShortName;
    private final String databaseURL;
    private final String cipher;
    private final byte[] filePasswordHash;

    private final HashMap<String, Role> roles = New.hashMap();
    private final HashMap<String, User> users = New.hashMap();
    private final HashMap<String, Setting> settings = New.hashMap();
    private final HashMap<String, Schema> schemas = New.hashMap();
    private final HashMap<String, Right> rights = New.hashMap();
    private final HashMap<String, FunctionAlias> functionAliases = New.hashMap();
    private final HashMap<String, UserDataType> userDataTypes = New.hashMap();
    private final HashMap<String, UserAggregate> aggregates = New.hashMap();
    private final HashMap<String, Comment> comments = New.hashMap();

    private final Set<Session> userSessions = Collections.synchronizedSet(new HashSet<Session>());
    private Session exclusiveSession;
    private final BitField objectIds = new BitField();
    private final Object lobSyncObject = new Object();

    private Schema mainSchema;
    private Schema infoSchema;
    private int nextSessionId;
    private User systemUser;
    private Session systemSession;
    private TableData meta;
    private Index metaIdIndex;
    private FileLock lock;
    private LogSystem log;
    private WriterThread writer;
    private HashMap<Integer, Storage> storageMap = New.hashMap();
    private boolean starting;
    private DiskFile fileData, fileIndex;
    private TraceSystem traceSystem;
    private DataPage dummy;
    private int fileLockMethod;
    private Role publicRole;
    private long modificationDataId;
    private long modificationMetaId;
    private CompareMode compareMode;
    private String cluster = Constants.CLUSTERING_DISABLED;
    private boolean readOnly;
    private boolean noDiskSpace;
    private int writeDelay = Constants.DEFAULT_WRITE_DELAY;
    private DatabaseEventListener eventListener;
    private int maxMemoryRows = Constants.DEFAULT_MAX_MEMORY_ROWS;
    private int maxMemoryUndo = SysProperties.DEFAULT_MAX_MEMORY_UNDO;
    private int lockMode = SysProperties.DEFAULT_LOCK_MODE;
    private boolean logIndexChanges;
    private int logLevel = 1;
    private int maxLengthInplaceLob = Constants.DEFAULT_MAX_LENGTH_INPLACE_LOB;
    private int allowLiterals = Constants.DEFAULT_ALLOW_LITERALS;

    private int powerOffCount = initialPowerOffCount;
    private int closeDelay;
    private DatabaseCloser delayedCloser;
    private boolean recovery;
    private volatile boolean closing;
    private boolean ignoreCase;
    private boolean deleteFilesOnDisconnect;
    private String lobCompressionAlgorithm;
    private boolean optimizeReuseResults = true;
    private String cacheType;
    private boolean indexSummaryValid = true;
    private String accessModeLog, accessModeData;
    private boolean referentialIntegrity = true;
    private boolean multiVersion;
    private DatabaseCloser closeOnExit;
    private Mode mode = Mode.getInstance(Mode.REGULAR);
    private boolean multiThreaded;
    private int maxOperationMemory = SysProperties.DEFAULT_MAX_OPERATION_MEMORY;
    private boolean lobFilesInDirectories = SysProperties.LOB_FILES_IN_DIRECTORIES;
    private SmallLRUCache<String, String[]> lobFileListCache = SmallLRUCache.newInstance(128);
    private boolean autoServerMode;
    private Server server;
    private HashMap<TableLinkConnection, TableLinkConnection> linkConnections;
    private TempFileDeleter tempFileDeleter = TempFileDeleter.getInstance();
    private PageStore pageStore;
    private boolean usePageStore;

    private Properties reconnectLastLock;
    private volatile long reconnectCheckNext;
    private volatile boolean reconnectChangePending;

    public Database(String name, ConnectionInfo ci, String cipher) throws SQLException {
        this.compareMode = CompareMode.getInstance(null, 0);
        this.persistent = ci.isPersistent();
        this.filePasswordHash = ci.getFilePasswordHash();
        this.databaseName = name;
        this.databaseShortName = parseDatabaseShortName();
        this.cipher = cipher;
        String lockMethodName = ci.getProperty("FILE_LOCK", null);
        this.accessModeLog = ci.getProperty("ACCESS_MODE_LOG", "rw").toLowerCase();
        this.accessModeData = ci.getProperty("ACCESS_MODE_DATA", "rw").toLowerCase();
        this.autoServerMode = ci.getProperty("AUTO_SERVER", false);
        this.usePageStore = ci.getProperty("PAGE_STORE", SysProperties.getPageStore());
        if ("r".equals(accessModeData)) {
            readOnly = true;
            accessModeLog = "r";
        }
        this.fileLockMethod = FileLock.getFileLockMethod(lockMethodName);
        if (fileLockMethod == FileLock.LOCK_SERIALIZED) {
            writeDelay = SysProperties.MIN_WRITE_DELAY;
        }
        this.databaseURL = ci.getURL();
        this.eventListener = ci.getDatabaseEventListenerObject();
        ci.removeDatabaseEventListenerObject();
        if (eventListener == null) {
            String listener = ci.removeProperty("DATABASE_EVENT_LISTENER", null);
            if (listener != null) {
                listener = StringUtils.trim(listener, true, true, "'");
                setEventListenerClass(listener);
            }
        }
        String logSetting = ci.getProperty(SetTypes.LOG, null);
        if (logSetting != null) {
            this.logIndexChanges = "2".equals(logSetting);
        }
        String ignoreSummary = ci.getProperty("RECOVER", null);
        if (ignoreSummary != null) {
            this.recovery = true;
        }
        this.multiVersion = ci.getProperty("MVCC", false);
        boolean closeAtVmShutdown = ci.getProperty("DB_CLOSE_ON_EXIT", true);
        int traceLevelFile = ci.getIntProperty(SetTypes.TRACE_LEVEL_FILE, TraceSystem.DEFAULT_TRACE_LEVEL_FILE);
        int traceLevelSystemOut = ci.getIntProperty(SetTypes.TRACE_LEVEL_SYSTEM_OUT,
                TraceSystem.DEFAULT_TRACE_LEVEL_SYSTEM_OUT);
        this.cacheType = StringUtils.toUpperEnglish(ci.removeProperty("CACHE_TYPE", SysProperties.CACHE_TYPE_DEFAULT));
        openDatabase(traceLevelFile, traceLevelSystemOut, closeAtVmShutdown);
    }

    private void openDatabase(int traceLevelFile, int traceLevelSystemOut, boolean closeAtVmShutdown) throws SQLException {
        try {
            open(traceLevelFile, traceLevelSystemOut);
            if (closeAtVmShutdown) {
                try {
                    closeOnExit = new DatabaseCloser(this, 0, true);
                    Runtime.getRuntime().addShutdownHook(closeOnExit);
                } catch (IllegalStateException e) {
                    // shutdown in progress - just don't register the handler
                    // (maybe an application wants to write something into a
                    // database at shutdown time)
                } catch (SecurityException  e) {
                    // applets may not do that - ignore
                    // Google App Engine doesn't allow
                    // to instantiate classes that extend Thread
                }
            }
        } catch (Throwable e) {
            if (traceSystem != null) {
                if (e instanceof SQLException) {
                    SQLException e2 = (SQLException) e;
                    if (e2.getErrorCode() != ErrorCode.DATABASE_ALREADY_OPEN_1) {
                        // only write if the database is not already in use
                        traceSystem.getTrace(Trace.DATABASE).error("opening " + databaseName, e);
                    }
                }
                traceSystem.close();
            }
            closeOpenFilesAndUnlock(false);
            throw Message.convert(e);
        }
    }

    public static void setInitialPowerOffCount(int count) {
        initialPowerOffCount = count;
    }

    public void setPowerOffCount(int count) {
        if (powerOffCount == -1) {
            return;
        }
        powerOffCount = count;
    }

    /**
     * Check if two values are equal with the current comparison mode.
     *
     * @param a the first value
     * @param b the second value
     * @return true if both objects are equal
     */
    public boolean areEqual(Value a, Value b) throws SQLException {
        // TODO optimization possible
        // boolean is = a.compareEqual(b);
        // boolean is2 = a.compareTo(b, compareMode) == 0;
        // if(is != is2) {
        // is = a.compareEqual(b);
        // System.out.println("hey!");
        // }
        // return a.compareEqual(b);
        return a.compareTo(b, compareMode) == 0;
    }

    /**
     * Compare two values with the current comparison mode. The values may not
     * be of the same type.
     *
     * @param a the first value
     * @param b the second value
     * @return 0 if both values are equal, -1 if the first value is smaller, and
     *         1 otherwise
     */
    public int compare(Value a, Value b) throws SQLException {
        return a.compareTo(b, compareMode);
    }

    /**
     * Compare two values with the current comparison mode. The values must be
     * of the same type.
     *
     * @param a the first value
     * @param b the second value
     * @return 0 if both values are equal, -1 if the first value is smaller, and
     *         1 otherwise
     */
    public int compareTypeSave(Value a, Value b) throws SQLException {
        return a.compareTypeSave(b, compareMode);
    }

    public long getModificationDataId() {
        return modificationDataId;
    }

    /**
     * Set or reset the pending change flag in the .lock.db file.
     *
     * @param pending the new value of the flag
     * @return true if the call was successful,
     *          false if another connection was faster
     */
    synchronized boolean reconnectModified(boolean pending) {
        if (readOnly || lock == null || fileLockMethod != FileLock.LOCK_SERIALIZED) {
            return true;
        }
        try {
            if (pending == reconnectChangePending) {
                long now = System.currentTimeMillis();
                if (now > reconnectCheckNext) {
                    if (pending) {
                        String pos = log == null ? null : log.getWritePos();
                        lock.setProperty("logPos", pos);
                        lock.save();
                    }
                    reconnectCheckNext = now + SysProperties.RECONNECT_CHECK_DELAY;
                }
                return true;
            }
            Properties old = lock.load();
            if (pending) {
                if (old.getProperty("changePending") != null) {
                    return false;
                }
                getTrace().debug("wait before writing");
                Thread.sleep((int) (SysProperties.RECONNECT_CHECK_DELAY * 1.1));
                Properties now = lock.load();
                if (!now.equals(old)) {
                    // somebody else was faster
                    return false;
                }
            }
            String pos = log == null ? null : log.getWritePos();
            lock.setProperty("logPos", pos);
            lock.setProperty("changePending", pending ? "true" : null);
            // ensure that the writer thread will
            // not reset the flag before we are done
            reconnectCheckNext = System.currentTimeMillis() + 2 * SysProperties.RECONNECT_CHECK_DELAY;
            old = lock.save();

            if (pending) {
                getTrace().debug("wait before writing again");
                Thread.sleep((int) (SysProperties.RECONNECT_CHECK_DELAY * 1.1));
                Properties now = lock.load();
                if (!now.equals(old)) {
                    // somebody else was faster
                    return false;
                }
            } else {
                Thread.sleep(1);
            }
            reconnectLastLock = old;
            reconnectChangePending = pending;
            reconnectCheckNext = System.currentTimeMillis() + SysProperties.RECONNECT_CHECK_DELAY;
            return true;
        } catch (Exception e) {
            getTrace().error("pending:"+ pending, e);
            return false;
        }
    }

    public long getNextModificationDataId() {
        return ++modificationDataId;
    }

    public long getModificationMetaId() {
        return modificationMetaId;
    }

    public long getNextModificationMetaId() {
        // if the meta data has been modified, the data is modified as well
        // (because MetaTable returns modificationDataId)
        modificationDataId++;
        return modificationMetaId++;
    }

    public int getPowerOffCount() {
        return powerOffCount;
    }

    public void checkPowerOff() throws SQLException {
        if (powerOffCount == 0) {
            return;
        }
        if (powerOffCount > 1) {
            powerOffCount--;
            return;
        }
        if (powerOffCount != -1) {
            try {
                powerOffCount = -1;
                if (log != null) {
                    try {
                        stopWriter();
                        log.close(false);
                    } catch (SQLException e) {
                        // ignore
                    }
                    log = null;
                }
                if (fileData != null) {
                    try {
                        fileData.close();
                    } catch (SQLException e) {
                        // ignore
                    }
                    fileData = null;
                }
                if (fileIndex != null) {
                    try {
                        fileIndex.close();
                    } catch (SQLException e) {
                        // ignore
                    }
                    fileIndex = null;
                }
                if (pageStore != null) {
                    try {
                        pageStore.close();
                    } catch (SQLException e) {
                        // ignore
                    }
                    pageStore = null;
                }
                if (lock != null) {
                    stopServer();
                    if (fileLockMethod != FileLock.LOCK_SERIALIZED) {
                        // allow testing shutdown
                        lock.unlock();
                    }
                    lock = null;
                }
            } catch (Exception e) {
                TraceSystem.traceThrowable(e);
            }
        }
        Engine.getInstance().close(databaseName);
        throw Message.getSQLException(ErrorCode.SIMULATED_POWER_OFF);
    }

    /**
     * Check if a database with the given name exists.
     *
     * @param name the name of the database (including path)
     * @return true if one exists
     */
    public static boolean exists(String name) {
        return FileUtils.exists(name + Constants.SUFFIX_PAGE_FILE) | FileUtils.exists(name + Constants.SUFFIX_DATA_FILE);
    }

    /**
     * Get the trace object for the given module.
     *
     * @param module the module name
     * @return the trace object
     */
    public Trace getTrace(String module) {
        return traceSystem.getTrace(module);
    }

    public FileStore openFile(String name, String openMode, boolean mustExist) throws SQLException {
        if (mustExist && !FileUtils.exists(name)) {
            throw Message.getSQLException(ErrorCode.FILE_NOT_FOUND_1, name);
        }
        FileStore store = FileStore.open(this, name, openMode, cipher, filePasswordHash);
        try {
            store.init();
        } catch (SQLException e) {
            store.closeSilently();
            throw e;
        }
        return store;
    }

    /**
     * Check if the file password hash is correct.
     *
     * @param testCipher the cipher algorithm
     * @param testHash the hash code
     * @return true if the cipher algorithm and the password match
     */
    public boolean validateFilePasswordHash(String testCipher, byte[] testHash) {
        if (!StringUtils.equals(testCipher, this.cipher)) {
            return false;
        }
        return ByteUtils.compareSecure(testHash, filePasswordHash);
    }

    private void openFileData() throws SQLException {
        fileData = new DiskFile(this, databaseName + Constants.SUFFIX_DATA_FILE, accessModeData, true, true,
                SysProperties.CACHE_SIZE_DEFAULT);
    }

    private void openFileIndex() throws SQLException {
        fileIndex = new DiskFile(this, databaseName + Constants.SUFFIX_INDEX_FILE, accessModeData, false,
                logIndexChanges, SysProperties.CACHE_SIZE_INDEX_DEFAULT);
    }

    public DataPage getDataPage() {
        return dummy;
    }

    private String parseDatabaseShortName() {
        String n = databaseName;
        if (n.endsWith(":")) {
            n = null;
        }
        if (n != null) {
            StringTokenizer tokenizer = new StringTokenizer(n, "/\\:,;");
            while (tokenizer.hasMoreTokens()) {
                n = tokenizer.nextToken();
            }
        }
        if (n == null || n.length() == 0) {
            n = "UNNAMED";
        }
        return StringUtils.toUpperEnglish(n);
    }

    private synchronized void open(int traceLevelFile, int traceLevelSystemOut) throws SQLException {
        if (persistent) {
            String pageFileName = databaseName + Constants.SUFFIX_PAGE_FILE;
            boolean exists = FileUtils.exists(pageFileName);
            if (exists) {
                usePageStore = true;
            }
            if (usePageStore) {
                if (exists && FileUtils.isReadOnly(pageFileName)) {
                    readOnly = true;
                }
            } else {
                String dataFileName = databaseName + Constants.SUFFIX_DATA_FILE;
                exists = FileUtils.exists(dataFileName);
                if (FileUtils.exists(dataFileName)) {
                    // if it is already read-only because ACCESS_MODE_DATA=r
                    readOnly = readOnly | FileUtils.isReadOnly(dataFileName);
                }
            }
            if (readOnly) {
                traceSystem = new TraceSystem(null, false);
            } else {
                traceSystem = new TraceSystem(databaseName + Constants.SUFFIX_TRACE_FILE, true);
            }
            traceSystem.setLevelFile(traceLevelFile);
            traceSystem.setLevelSystemOut(traceLevelSystemOut);
            traceSystem.getTrace(Trace.DATABASE)
                    .info("opening " + databaseName + " (build " + Constants.BUILD_ID + ")");
            if (autoServerMode) {
                if (readOnly || fileLockMethod == FileLock.LOCK_NO) {
                    throw Message.getUnsupportedException("autoServerMode && (readOnly || fileLockMethod == NO)");
                }
            }
            if (!readOnly && fileLockMethod != FileLock.LOCK_NO) {
                lock = new FileLock(traceSystem, databaseName + Constants.SUFFIX_LOCK_FILE, Constants.LOCK_SLEEP);
                lock.lock(fileLockMethod);
                if (autoServerMode) {
                    startServer(lock.getUniqueId());
                }
            }
            while (isReconnectNeeded() && !beforeWriting()) {
                // wait until others stopped writing and
                // until we can write (file are not open - no need to re-connect)
            }
            if (exists) {
                lobFilesInDirectories &= !ValueLob.existsLobFile(getDatabasePath());
                lobFilesInDirectories |= FileUtils.exists(databaseName + Constants.SUFFIX_LOBS_DIRECTORY);
            }
            dummy = DataPage.create(this, 0);
            deleteOldTempFiles();
            if (usePageStore) {
                starting = true;
                getPageStore();
                starting = false;
            }
            log = new LogSystem(this, databaseName, readOnly, accessModeLog, pageStore);
            if (pageStore == null) {
                openFileData();
                log.open();
                openFileIndex();
                log.recover();
                fileData.init();
                try {
                    fileIndex.init();
                } catch (Exception e) {
                    if (recovery) {
                        traceSystem.getTrace(Trace.DATABASE).error("opening index", e);
                        for (Storage s : New.arrayList(storageMap.values())) {
                            if (s.getDiskFile() == fileIndex) {
                                removeStorage(s.getId(), fileIndex);
                            }
                        }
                        fileIndex.delete();
                        openFileIndex();
                    } else {
                        throw Message.convert(e);
                    }
                }
            }
            reserveLobFileObjectIds();
            writer = WriterThread.create(this, writeDelay);
        } else {
            traceSystem = new TraceSystem(null, false);
            log = new LogSystem(null, null, false, null, null);
        }
        systemUser = new User(this, 0, Constants.DBA_NAME, true);
        mainSchema = new Schema(this, 0, Constants.SCHEMA_MAIN, systemUser, true);
        infoSchema = new Schema(this, -1, Constants.SCHEMA_INFORMATION, systemUser, true);
        schemas.put(mainSchema.getName(), mainSchema);
        schemas.put(infoSchema.getName(), infoSchema);
        publicRole = new Role(this, 0, Constants.PUBLIC_ROLE_NAME, true);
        roles.put(Constants.PUBLIC_ROLE_NAME, publicRole);
        systemUser.setAdmin(true);
        systemSession = new Session(this, systemUser, ++nextSessionId);
        ObjectArray<Column> cols = ObjectArray.newInstance();
        Column columnId = new Column("ID", Value.INT);
        columnId.setNullable(false);
        cols.add(columnId);
        cols.add(new Column("HEAD", Value.INT));
        cols.add(new Column("TYPE", Value.INT));
        cols.add(new Column("SQL", Value.STRING));
        int headPos = 0;
        if (pageStore != null) {
            headPos = pageStore.getSystemTableHeadPos();
        }
        meta = mainSchema.createTable("SYS", 0, cols, false, persistent, persistent, false, headPos, systemSession);
        IndexColumn[] pkCols = IndexColumn.wrap(new Column[] { columnId });
        metaIdIndex = meta.addIndex(systemSession, "SYS_ID", 0, pkCols, IndexType.createPrimaryKey(
                false, false), Index.EMPTY_HEAD, null);
        objectIds.set(0);
        // there could be views on system tables, so they must be added first
        for (int i = 0; i < MetaTable.getMetaTableTypeCount(); i++) {
            addMetaData(i);
        }
        starting = true;
        Cursor cursor = metaIdIndex.find(systemSession, null, null);
        // first, create all function aliases and sequences because
        // they might be used in create table / view / constraints and so on
        ObjectArray<MetaRecord> records = ObjectArray.newInstance();
        while (cursor.next()) {
            MetaRecord rec = new MetaRecord(cursor.get());
            objectIds.set(rec.getId());
            records.add(rec);
        }
        MetaRecord.sort(records);
        for (MetaRecord rec : records) {
            rec.execute(this, systemSession, eventListener);
        }
        // try to recompile the views that are invalid
        recompileInvalidViews(systemSession);
        starting = false;
        addDefaultSetting(systemSession, SetTypes.DEFAULT_LOCK_TIMEOUT, null, Constants.INITIAL_LOCK_TIMEOUT);
        addDefaultSetting(systemSession, SetTypes.DEFAULT_TABLE_TYPE, null, Constants.DEFAULT_TABLE_TYPE);
        addDefaultSetting(systemSession, SetTypes.CACHE_SIZE, null, SysProperties.CACHE_SIZE_DEFAULT);
        addDefaultSetting(systemSession, SetTypes.CLUSTER, Constants.CLUSTERING_DISABLED, 0);
        addDefaultSetting(systemSession, SetTypes.WRITE_DELAY, null, Constants.DEFAULT_WRITE_DELAY);
        addDefaultSetting(systemSession, SetTypes.CREATE_BUILD, null, Constants.BUILD_ID);
        if (!readOnly) {
            removeUnusedStorages(systemSession);
        }
        systemSession.commit(true);
        traceSystem.getTrace(Trace.DATABASE).info("opened " + databaseName);
    }

    public Schema getMainSchema() {
        return mainSchema;
    }

    private void startServer(String key) throws SQLException {
        server = Server.createTcpServer(
                "-tcpPort", "0",
                "-tcpAllowOthers",
                "-key", key, databaseName);
        server.start();
        String address = NetUtils.getLocalAddress() + ":" + server.getPort();
        lock.setProperty("server", address);
        lock.save();
    }

    private void stopServer() {
        if (server != null) {
            Server s = server;
            // avoid calling stop recursively
            // because stopping the server will
            // try to close the database as well
            server = null;
            s.stop();
        }
    }

    private void recompileInvalidViews(Session session) {
        boolean recompileSuccessful;
        do {
            recompileSuccessful = false;
            for (Table obj : getAllTablesAndViews()) {
                if (obj instanceof TableView) {
                    TableView view = (TableView) obj;
                    if (view.isInvalid()) {
                        try {
                            view.recompile(session);
                        } catch (SQLException e) {
                            // ignore
                        }
                        if (!view.isInvalid()) {
                            recompileSuccessful = true;
                        }
                    }
                }
            }
        } while (recompileSuccessful);
        // when opening a database, views are initialized before indexes,
        // so they may not have the optimal plan yet
        // this is not a problem, it is just nice to see the newest plan
        for (Table obj : getAllTablesAndViews()) {
            if (obj instanceof TableView) {
                TableView view = (TableView) obj;
                if (!view.isInvalid()) {
                    try {
                        view.recompile(systemSession);
                    } catch (SQLException e) {
                        // ignore
                    }
                }
            }
        }
    }

    private void removeUnusedStorages(Session session) throws SQLException {
        if (persistent) {
            ObjectArray<Storage> storages = getAllStorages();
            for (Storage storage : storages) {
                if (storage != null && storage.getRecordReader() == null) {
                    storage.truncate(session);
                }
            }
        }
    }

    private void addDefaultSetting(Session session, int type, String stringValue, int intValue) throws SQLException {
        if (readOnly) {
            return;
        }
        String name = SetTypes.getTypeName(type);
        if (settings.get(name) == null) {
            Setting setting = new Setting(this, allocateObjectId(false, true), name);
            if (stringValue == null) {
                setting.setIntValue(intValue);
            } else {
                setting.setStringValue(stringValue);
            }
            addDatabaseObject(session, setting);
        }
    }

    /**
     * Remove the storage object from the file.
     *
     * @param id the storage id
     * @param file the file
     */
    public void removeStorage(int id, DiskFile file) {
        if (SysProperties.CHECK) {
            Storage s = storageMap.get(id);
            if (s == null || s.getDiskFile() != file) {
                Message.throwInternalError();
            }
        }
        storageMap.remove(id);
    }

    /**
     * Get the storage object for the given file. An new object is created if
     * required.
     *
     * @param id the storage id
     * @param file the file
     * @return the storage object
     */
    public Storage getStorage(int id, DiskFile file) {
        Storage storage = storageMap.get(id);
        if (storage != null) {
            if (SysProperties.CHECK && storage.getDiskFile() != file) {
                Message.throwInternalError(storage.getDiskFile() + " != " + file + " id:" + id);
            }
        } else {
            storage = new Storage(this, file, null, id);
            storageMap.put(id, storage);
        }
        return storage;
    }

    private void addMetaData(int type) throws SQLException {
        MetaTable m = new MetaTable(infoSchema, -1 - type, type);
        infoSchema.add(m);
    }

    private synchronized void addMeta(Session session, DbObject obj) throws SQLException {
        int id = obj.getId();
        if (id > 0 && !starting && !obj.isTemporary()) {
            Row r = meta.getTemplateRow();
            MetaRecord rec = new MetaRecord(obj);
            rec.setRecord(r);
            objectIds.set(id);
            meta.lock(session, true, true);
            meta.addRow(session, r);
            if (isMultiVersion()) {
                // TODO this should work without MVCC, but avoid risks at the moment
                session.log(meta, UndoLogRecord.INSERT, r);
            }
        }
    }

    /**
     * Remove the given object from the meta data.
     *
     * @param session the session
     * @param id the id of the object to remove
     */
    public synchronized void removeMeta(Session session, int id) throws SQLException {
        if (id > 0 && !starting) {
            SearchRow r = meta.getTemplateSimpleRow(false);
            r.setValue(0, ValueInt.get(id));
            Cursor cursor = metaIdIndex.find(session, r, r);
            if (cursor.next()) {
                Row found = cursor.get();
                meta.lock(session, true, true);
                meta.removeRow(session, found);
                if (isMultiVersion()) {
                    // TODO this should work without MVCC, but avoid risks at the
                    // moment
                    session.log(meta, UndoLogRecord.DELETE, found);
                }
                objectIds.clear(id);
                if (SysProperties.CHECK) {
                    checkMetaFree(session, id);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private HashMap<String, DbObject> getMap(int type) {
        HashMap<String, ? extends DbObject> result;
        switch (type) {
        case DbObject.USER:
            result = users;
            break;
        case DbObject.SETTING:
            result = settings;
            break;
        case DbObject.ROLE:
            result = roles;
            break;
        case DbObject.RIGHT:
            result = rights;
            break;
        case DbObject.FUNCTION_ALIAS:
            result = functionAliases;
            break;
        case DbObject.SCHEMA:
            result = schemas;
            break;
        case DbObject.USER_DATATYPE:
            result = userDataTypes;
            break;
        case DbObject.COMMENT:
            result = comments;
            break;
        case DbObject.AGGREGATE:
            result = aggregates;
            break;
        default:
            throw Message.throwInternalError("type=" + type);
        }
        return (HashMap<String, DbObject>) result;
    }

    /**
     * Add a schema object to the database.
     *
     * @param session the session
     * @param obj the object to add
     */
    public synchronized void addSchemaObject(Session session, SchemaObject obj) throws SQLException {
        int id = obj.getId();
        if (id > 0 && !starting) {
            checkWritingAllowed();
        }
        obj.getSchema().add(obj);
        addMeta(session, obj);
    }

    /**
     * Add an object to the database.
     *
     * @param session the session
     * @param obj the object to add
     */
    public synchronized void addDatabaseObject(Session session, DbObject obj) throws SQLException {
        int id = obj.getId();
        if (id > 0 && !starting) {
            checkWritingAllowed();
        }
        HashMap<String, DbObject> map = getMap(obj.getType());
        if (obj.getType() == DbObject.USER) {
            User user = (User) obj;
            if (user.isAdmin() && systemUser.getName().equals(Constants.DBA_NAME)) {
                systemUser.rename(user.getName());
            }
        }
        String name = obj.getName();
        if (SysProperties.CHECK && map.get(name) != null) {
            Message.throwInternalError("object already exists");
        }
        addMeta(session, obj);
        map.put(name, obj);
    }

    /**
     * Get the user defined aggregate function if it exists, or null if not.
     *
     * @param name the name of the user defined aggregate function
     * @return the aggregate function or null
     */
    public UserAggregate findAggregate(String name) {
        return aggregates.get(name);
    }

    /**
     * Get the comment for the given database object if one exists, or null if
     * not.
     *
     * @param object the database object
     * @return the comment or null
     */
    public Comment findComment(DbObject object) {
        if (object.getType() == DbObject.COMMENT) {
            return null;
        }
        String key = Comment.getKey(object);
        return comments.get(key);
    }

    /**
     * Get the user defined function if it exists, or null if not.
     *
     * @param name the name of the user defined function
     * @return the function or null
     */
    public FunctionAlias findFunctionAlias(String name) {
        return functionAliases.get(name);
    }

    /**
     * Get the role if it exists, or null if not.
     *
     * @param roleName the name of the role
     * @return the role or null
     */
    public Role findRole(String roleName) {
        return roles.get(roleName);
    }

    /**
     * Get the schema if it exists, or null if not.
     *
     * @param schemaName the name of the schema
     * @return the schema or null
     */
    public Schema findSchema(String schemaName) {
        return schemas.get(schemaName);
    }

    /**
     * Get the setting if it exists, or null if not.
     *
     * @param name the name of the setting
     * @return the setting or null
     */
    public Setting findSetting(String name) {
        return settings.get(name);
    }

    /**
     * Get the user if it exists, or null if not.
     *
     * @param name the name of the user
     * @return the user or null
     */
    public User findUser(String name) {
        return users.get(name);
    }

    /**
     * Get the user defined data type if it exists, or null if not.
     *
     * @param name the name of the user defined data type
     * @return the user defined data type or null
     */
    public UserDataType findUserDataType(String name) {
        return userDataTypes.get(name);
    }

    /**
     * Get user with the given name. This method throws an exception if the user
     * does not exist.
     *
     * @param name the user name
     * @return the user
     * @throws SQLException if the user does not exist
     */
    public User getUser(String name) throws SQLException {
        User user = findUser(name);
        if (user == null) {
            throw Message.getSQLException(ErrorCode.USER_NOT_FOUND_1, name);
        }
        return user;
    }

    /**
     * Create a session for the given user.
     *
     * @param user the user
     * @return the session
     * @throws SQLException if the database is in exclusive mode
     */
    public synchronized Session createSession(User user) throws SQLException {
        if (exclusiveSession != null) {
            throw Message.getSQLException(ErrorCode.DATABASE_IS_IN_EXCLUSIVE_MODE);
        }
        Session session = new Session(this, user, ++nextSessionId);
        userSessions.add(session);
        traceSystem.getTrace(Trace.SESSION).info("connecting #" + session.getId() + " to " + databaseName);
        if (delayedCloser != null) {
            delayedCloser.reset();
            delayedCloser = null;
        }
        return session;
    }

    /**
     * Remove a session. This method is called after the user has disconnected.
     *
     * @param session the session
     */
    public synchronized void removeSession(Session session) {
        if (session != null) {
            if (exclusiveSession == session) {
                exclusiveSession = null;
            }
            userSessions.remove(session);
            if (session != systemSession) {
                traceSystem.getTrace(Trace.SESSION).info("disconnecting #" + session.getId());
            }
        }
        if (userSessions.size() == 0 && session != systemSession) {
            if (closeDelay == 0) {
                close(false);
            } else if (closeDelay < 0) {
                return;
            } else {
                delayedCloser = new DatabaseCloser(this, closeDelay * 1000, false);
                delayedCloser.setName("H2 Close Delay " + getShortName());
                delayedCloser.setDaemon(true);
                delayedCloser.start();
            }
        }
        if (session != systemSession && session != null) {
            traceSystem.getTrace(Trace.SESSION).info("disconnected #" + session.getId());
        }
    }

    /**
     * Close the database.
     *
     * @param fromShutdownHook true if this method is called from the shutdown
     *            hook
     */
    synchronized void close(boolean fromShutdownHook) {
        if (closing) {
            return;
        }
        if (fileLockMethod == FileLock.LOCK_SERIALIZED && !reconnectChangePending) {
            // another connection may have written something - don't write
            try {
                // make sure the log doesn't try to write
                if (log != null) {
                    log.setReadOnly(true);
                }
                closeOpenFilesAndUnlock(false);
            } catch (SQLException e) {
                // ignore
            }
            traceSystem.close();
            Engine.getInstance().close(databaseName);
            return;
        }
        closing = true;
        stopServer();
        if (userSessions.size() > 0) {
            if (!fromShutdownHook) {
                return;
            }
            traceSystem.getTrace(Trace.DATABASE).info("closing " + databaseName + " from shutdown hook");
            Session[] all = new Session[userSessions.size()];
            userSessions.toArray(all);
            for (Session s : all) {
                try {
                    // must roll back, otherwise the session is removed and
                    // the log file that contains its uncommitted operations as well
                    s.rollback();
                    s.close();
                } catch (SQLException e) {
                    traceSystem.getTrace(Trace.SESSION).error("disconnecting #" + s.getId(), e);
                }
            }
        }
        if (log != null) {
            log.setDisabled(false);
        }
        traceSystem.getTrace(Trace.DATABASE).info("closing " + databaseName);
        if (eventListener != null) {
            // allow the event listener to connect to the database
            closing = false;
            DatabaseEventListener e = eventListener;
            // set it to null, to make sure it's called only once
            eventListener = null;
            e.closingDatabase();
            if (userSessions.size() > 0) {
                // if a connection was opened, we can't close the database
                return;
            }
            closing = true;
        }
        try {
            if (systemSession != null) {
                for (Table table : getAllTablesAndViews()) {
                    table.close(systemSession);
                }
                for (SchemaObject obj : getAllSchemaObjects(DbObject.SEQUENCE)) {
                    Sequence sequence = (Sequence) obj;
                    sequence.close();
                }
                for (SchemaObject obj : getAllSchemaObjects(DbObject.TRIGGER)) {
                    TriggerObject trigger = (TriggerObject) obj;
                    trigger.close();
                }
                meta.close(systemSession);
                systemSession.commit(true);
                indexSummaryValid = true;
            }
        } catch (SQLException e) {
            traceSystem.getTrace(Trace.DATABASE).error("close", e);
        }
        // remove all session variables
        if (persistent) {
            try {
                ValueLob.removeAllForTable(this, ValueLob.TABLE_ID_SESSION_VARIABLE);
            } catch (SQLException e) {
                traceSystem.getTrace(Trace.DATABASE).error("close", e);
            }
        }
        tempFileDeleter.deleteAll();
        try {
            closeOpenFilesAndUnlock(true);
        } catch (SQLException e) {
            traceSystem.getTrace(Trace.DATABASE).error("close", e);
        }
        traceSystem.getTrace(Trace.DATABASE).info("closed");
        traceSystem.close();
        if (closeOnExit != null) {
            closeOnExit.reset();
            try {
                Runtime.getRuntime().removeShutdownHook(closeOnExit);
            } catch (IllegalStateException e) {
                // ignore
            } catch (SecurityException  e) {
                // applets may not do that - ignore
            }
            closeOnExit = null;
        }
        Engine.getInstance().close(databaseName);
        if (deleteFilesOnDisconnect && persistent) {
            deleteFilesOnDisconnect = false;
            try {
                String directory = FileUtils.getParent(databaseName);
                String name = FileUtils.getFileName(databaseName);
                DeleteDbFiles.execute(directory, name, true);
            } catch (Exception e) {
                // ignore (the trace is closed already)
            }
        }
    }

    private void stopWriter() {
        if (writer != null) {
            try {
                writer.stopThread();
            } catch (SQLException e) {
                traceSystem.getTrace(Trace.DATABASE).error("close", e);
            }
            writer = null;
        }
    }

    /**
     * Close all open files and unlock the database.
     *
     * @param flush whether writing is allowed
     */
    private synchronized void closeOpenFilesAndUnlock(boolean flush) throws SQLException {
        if (log != null) {
            stopWriter();
            try {
                log.close(flush);
            } catch (Throwable e) {
                traceSystem.getTrace(Trace.DATABASE).error("close", e);
            }
            log = null;
        }
        if (pageStore != null) {
            if (flush) {
                try {
                    pageStore.checkpoint();
                    if (!readOnly) {
                        pageStore.trim();
                    }
                } catch (Throwable e) {
                    // TODO don't ignore exceptions
                    traceSystem.getTrace(Trace.DATABASE).error("close", e);
                }
            }
        }
        reconnectModified(false);
        closeFiles();
        if (persistent && lock == null && fileLockMethod != FileLock.LOCK_NO) {
            // everything already closed (maybe in checkPowerOff)
            // don't delete temp files in this case because
            // the database could be open now (even from within another process)
            return;
        }
        if (persistent) {
            deleteOldTempFiles();
        }
        if (systemSession != null) {
            systemSession.close();
            systemSession = null;
        }
        if (lock != null) {
            if (fileLockMethod != FileLock.LOCK_SERIALIZED) {
                // must not delete the .lock file if we wrote something,
                // otherwise other connections can not detect that
                lock.unlock();
            }
            lock = null;
        }
    }

    private void closeFiles() {
        try {
            if (fileData != null) {
                fileData.close();
                fileData = null;
            }
            if (fileIndex != null) {
                fileIndex.close();
                fileIndex = null;
            }
            if (pageStore != null) {
                pageStore.close();
                pageStore = null;
            }
        } catch (SQLException e) {
            traceSystem.getTrace(Trace.DATABASE).error("close", e);
        }
        storageMap.clear();
    }

    private void checkMetaFree(Session session, int id) throws SQLException {
        SearchRow r = meta.getTemplateSimpleRow(false);
        r.setValue(0, ValueInt.get(id));
        Cursor cursor = metaIdIndex.find(session, r, r);
        if (cursor.next()) {
            Message.throwInternalError();
        }
    }

    public synchronized int allocateObjectId(boolean needFresh, boolean dataFile) {
        // TODO refactor: use hash map instead of bit field for object ids
        needFresh = true;
        int i;
        if (needFresh) {
            i = objectIds.getLastSetBit() + 1;
            if ((i & 1) != (dataFile ? 1 : 0)) {
                i++;
            }
            while (storageMap.get(i) != null || objectIds.get(i)) {
                i++;
                if ((i & 1) != (dataFile ? 1 : 0)) {
                    i++;
                }
            }
        } else {
            i = objectIds.nextClearBit(0);
        }
        if (SysProperties.CHECK && objectIds.get(i)) {
            Message.throwInternalError();
        }
        objectIds.set(i);
        return i;
    }

    public ObjectArray<UserAggregate> getAllAggregates() {
        return ObjectArray.newInstance(aggregates.values());
    }

    public ObjectArray<Comment> getAllComments() {
        return ObjectArray.newInstance(comments.values());
    }

    public ObjectArray<FunctionAlias> getAllFunctionAliases() {
        return ObjectArray.newInstance(functionAliases.values());
    }

    public int getAllowLiterals() {
        if (starting) {
            return Constants.ALLOW_LITERALS_ALL;
        }
        return allowLiterals;
    }

    public ObjectArray<Right> getAllRights() {
        return ObjectArray.newInstance(rights.values());
    }

    public ObjectArray<Role> getAllRoles() {
        return ObjectArray.newInstance(roles.values());
    }

    /**
     * Get all schema objects of the given type.
     *
     * @param type the object type
     * @return all objects of that type
     */
    public ObjectArray<SchemaObject> getAllSchemaObjects(int type) {
        ObjectArray<SchemaObject> list = ObjectArray.newInstance();
        for (Schema schema : schemas.values()) {
            list.addAll(schema.getAll(type));
        }
        return list;
    }

    /**
     * Get all tables and views.
     *
     * @return all objects of that type
     */
    public ObjectArray<Table> getAllTablesAndViews() {
        ObjectArray<Table> list = ObjectArray.newInstance();
        for (Schema schema : schemas.values()) {
            list.addAll(schema.getAllTablesAndViews());
        }
        return list;
    }

    public ObjectArray<Schema> getAllSchemas() {
        return ObjectArray.newInstance(schemas.values());
    }

    public ObjectArray<Setting> getAllSettings() {
        return ObjectArray.newInstance(settings.values());
    }

    public ObjectArray<Storage> getAllStorages() {
        return ObjectArray.newInstance(storageMap.values());
    }

    public ObjectArray<UserDataType> getAllUserDataTypes() {
        return ObjectArray.newInstance(userDataTypes.values());
    }

    public ObjectArray<User> getAllUsers() {
        return ObjectArray.newInstance(users.values());
    }

    public String getCacheType() {
        return cacheType;
    }

    public int getChecksum(byte[] data, int start, int end) {
        int x = 0;
        while (start < end) {
            x += data[start++];
        }
        return x;
    }

    public String getCluster() {
        return cluster;
    }

    public CompareMode getCompareMode() {
        return compareMode;
    }

    public String getDatabasePath() {
        if (persistent) {
            return FileUtils.getAbsolutePath(databaseName);
        }
        return null;
    }

    public String getShortName() {
        return databaseShortName;
    }

    public String getName() {
        return databaseName;
    }

    public LogSystem getLog() {
        return log;
    }

    /**
     * Get all sessions that are currently connected to the database.
     *
     * @param includingSystemSession if the system session should also be
     *            included
     * @return the list of sessions
     */
    public Session[] getSessions(boolean includingSystemSession) {
        ArrayList<Session> list = New.arrayList(userSessions);
        if (includingSystemSession && systemSession != null) {
            list.add(systemSession);
        }
        Session[] array = new Session[list.size()];
        list.toArray(array);
        return array;
    }

    /**
     * Update an object in the system table.
     *
     * @param session the session
     * @param obj the database object
     */
    public synchronized void update(Session session, DbObject obj) throws SQLException {
        int id = obj.getId();
        removeMeta(session, id);
        addMeta(session, obj);
    }

    /**
     * Rename a schema object.
     *
     * @param session the session
     * @param obj the object
     * @param newName the new name
     */
    public synchronized void renameSchemaObject(Session session, SchemaObject obj, String newName) throws SQLException {
        checkWritingAllowed();
        obj.getSchema().rename(obj, newName);
        updateWithChildren(session, obj);
    }

    private synchronized void updateWithChildren(Session session, DbObject obj) throws SQLException {
        ObjectArray<DbObject> list = obj.getChildren();
        Comment comment = findComment(obj);
        if (comment != null) {
            Message.throwInternalError();
        }
        update(session, obj);
        // remember that this scans only one level deep!
        for (int i = 0; list != null && i < list.size(); i++) {
            DbObject o = list.get(i);
            if (o.getCreateSQL() != null) {
                update(session, o);
            }
        }
    }

    /**
     * Rename a database object.
     *
     * @param session the session
     * @param obj the object
     * @param newName the new name
     */
    public synchronized void renameDatabaseObject(Session session, DbObject obj, String newName) throws SQLException {
        checkWritingAllowed();
        int type = obj.getType();
        HashMap<String, DbObject> map = getMap(type);
        if (SysProperties.CHECK) {
            if (!map.containsKey(obj.getName())) {
                Message.throwInternalError("not found: " + obj.getName());
            }
            if (obj.getName().equals(newName) || map.containsKey(newName)) {
                Message.throwInternalError("object already exists: " + newName);
            }
        }
        obj.checkRename();
        int id = obj.getId();
        removeMeta(session, id);
        map.remove(obj.getName());
        obj.rename(newName);
        map.put(newName, obj);
        updateWithChildren(session, obj);
    }

    public String createTempFile() throws SQLException {
        try {
            boolean inTempDir = readOnly;
            String name = databaseName;
            if (!persistent) {
                name = FileSystem.PREFIX_MEMORY + name;
            }
            return FileUtils.createTempFile(name, Constants.SUFFIX_TEMP_FILE, true, inTempDir);
        } catch (IOException e) {
            throw Message.convertIOException(e, databaseName);
        }
    }

    private void reserveLobFileObjectIds() throws SQLException {
        String prefix = FileUtils.normalize(databaseName) + ".";
        String path = FileUtils.getParent(databaseName);
        String[] list = FileUtils.listFiles(path);
        for (String name : list) {
            if (name.endsWith(Constants.SUFFIX_LOB_FILE) && FileUtils.fileStartsWith(name, prefix)) {
                name = name.substring(prefix.length());
                name = name.substring(0, name.length() - Constants.SUFFIX_LOB_FILE.length());
                int dot = name.indexOf('.');
                if (dot >= 0) {
                    String id = name.substring(dot + 1);
                    int objectId = Integer.parseInt(id);
                    objectIds.set(objectId);
                }
            }
        }
    }

    private void deleteOldTempFiles() throws SQLException {
        String path = FileUtils.getParent(databaseName);
        String prefix = FileUtils.normalize(databaseName);
        String[] list = FileUtils.listFiles(path);
        for (String name : list) {
            if (name.endsWith(Constants.SUFFIX_TEMP_FILE) && FileUtils.fileStartsWith(name, prefix)) {
                // can't always delete the files, they may still be open
                FileUtils.tryDelete(name);
            }
        }
    }

    /**
     * Get or create the specified storage object.
     *
     * @param reader the record reader
     * @param id the object id
     * @param dataFile true if the data is in the data file
     * @return the storage
     */
    public Storage getStorage(RecordReader reader, int id, boolean dataFile) {
        DiskFile file;
        if (dataFile) {
            file = fileData;
        } else {
            file = fileIndex;
        }
        Storage storage = getStorage(id, file);
        storage.setReader(reader);
        return storage;
    }

    /**
     * Get the schema. If the schema does not exist, an exception is thrown.
     *
     * @param schemaName the name of the schema
     * @return the schema
     * @throws SQLException no schema with that name exists
     */
    public Schema getSchema(String schemaName) throws SQLException {
        Schema schema = findSchema(schemaName);
        if (schema == null) {
            throw Message.getSQLException(ErrorCode.SCHEMA_NOT_FOUND_1, schemaName);
        }
        return schema;
    }

    /**
     * Remove the object from the database.
     *
     * @param session the session
     * @param obj the object to remove
     */
    public synchronized void removeDatabaseObject(Session session, DbObject obj) throws SQLException {
        checkWritingAllowed();
        String objName = obj.getName();
        int type = obj.getType();
        HashMap<String, DbObject> map = getMap(type);
        if (SysProperties.CHECK && !map.containsKey(objName)) {
            Message.throwInternalError("not found: " + objName);
        }
        Comment comment = findComment(obj);
        if (comment != null) {
            removeDatabaseObject(session, comment);
        }
        int id = obj.getId();
        obj.removeChildrenAndResources(session);
        map.remove(objName);
        removeMeta(session, id);
    }

    /**
     * Get the first table that depends on this object.
     *
     * @param obj the object to find
     * @param except the table to exclude (or null)
     * @return the first dependent table, or null
     */
    public Table getDependentTable(SchemaObject obj, Table except) {
        switch (obj.getType()) {
        case DbObject.COMMENT:
        case DbObject.CONSTRAINT:
        case DbObject.INDEX:
        case DbObject.RIGHT:
        case DbObject.TRIGGER:
        case DbObject.USER:
            return null;
        default:
        }
        HashSet<DbObject> set = New.hashSet();
        for (Table t : getAllTablesAndViews()) {
            if (except == t) {
                continue;
            }
            set.clear();
            t.addDependencies(set);
            if (set.contains(obj)) {
                return t;
            }
        }
        return null;
    }

    private String getFirstInvalidTable(Session session) {
        String conflict = null;
        try {
            for (Table t : getAllTablesAndViews()) {
                conflict = t.getSQL();
                session.prepare(t.getCreateSQL());
            }
        } catch (SQLException e) {
            return conflict;
        }
        return null;
    }

    /**
     * Remove an object from the system table.
     *
     * @param session the session
     * @param obj the object to be removed
     */
    public synchronized void removeSchemaObject(Session session, SchemaObject obj) throws SQLException {
        int type = obj.getType();
        if (type == DbObject.TABLE_OR_VIEW) {
            Table table = (Table) obj;
            if (table.isTemporary() && !table.isGlobalTemporary()) {
                session.removeLocalTempTable(table);
                return;
            }
        } else if (type == DbObject.INDEX) {
            Index index = (Index) obj;
            Table table = index.getTable();
            if (table.isTemporary() && !table.isGlobalTemporary()) {
                session.removeLocalTempTableIndex(index);
                return;
            }
        } else if (type == DbObject.CONSTRAINT) {
            Constraint constraint = (Constraint) obj;
            Table table = constraint.getTable();
            if (table.isTemporary() && !table.isGlobalTemporary()) {
                session.removeLocalTempTableConstraint(constraint);
                return;
            }
        }
        checkWritingAllowed();
        Comment comment = findComment(obj);
        if (comment != null) {
            removeDatabaseObject(session, comment);
        }
        obj.getSchema().remove(obj);
        if (!starting) {
            String invalid;
            if (SysProperties.OPTIMIZE_DROP_DEPENDENCIES) {
                Table t = getDependentTable(obj, null);
                invalid = t == null ? null : t.getSQL();
            } else {
                invalid = getFirstInvalidTable(session);
            }
            if (invalid != null) {
                obj.getSchema().add(obj);
                throw Message.getSQLException(ErrorCode.CANNOT_DROP_2, obj.getSQL(), invalid);
            }
            obj.removeChildrenAndResources(session);
        }
        int id = obj.getId();
        removeMeta(session, id);
    }

    /**
     * Check if this database disk-based.
     *
     * @return true if it is disk-based, false it it is in-memory only.
     */
    public boolean isPersistent() {
        return persistent;
    }

    public TraceSystem getTraceSystem() {
        return traceSystem;
    }

    public DiskFile getDataFile() {
        return fileData;
    }

    public DiskFile getIndexFile() {
        return fileIndex;
    }

    public synchronized void setCacheSize(int kb) throws SQLException {
        if (fileData != null) {
            fileData.getCache().setMaxSize(kb);
            int valueIndex = kb <= 32 ? kb : (kb >>> SysProperties.CACHE_SIZE_INDEX_SHIFT);
            fileIndex.getCache().setMaxSize(valueIndex);
        }
    }

    public synchronized void setMasterUser(User user) throws SQLException {
        addDatabaseObject(systemSession, user);
        systemSession.commit(true);
    }

    public Role getPublicRole() {
        return publicRole;
    }

    /**
     * Get a unique temporary table name.
     *
     * @param sessionId the session id
     * @return a unique name
     */
    public String getTempTableName(int sessionId) {
        String tempName;
        for (int i = 0;; i++) {
            tempName = Constants.TEMP_TABLE_PREFIX + sessionId + "_" + i;
            if (mainSchema.findTableOrView(null, tempName) == null) {
                break;
            }
        }
        return tempName;
    }

    public void setCompareMode(CompareMode compareMode) {
        this.compareMode = compareMode;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public void checkWritingAllowed() throws SQLException {
        if (readOnly) {
            throw Message.getSQLException(ErrorCode.DATABASE_IS_READ_ONLY);
        }
        if (noDiskSpace) {
            throw Message.getSQLException(ErrorCode.NO_DISK_SPACE_AVAILABLE);
        }
        if (fileLockMethod == FileLock.LOCK_SERIALIZED) {
            if (!reconnectChangePending) {
                throw Message.getSQLException(ErrorCode.DATABASE_IS_READ_ONLY);
            }
        }
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public void setWriteDelay(int value) {
        writeDelay = value;
        if (writer != null) {
            writer.setWriteDelay(value);
        }
    }

    /**
     * Delete an unused log file. It is deleted immediately if no writer thread
     * is running, or deleted later on if one is running. Deleting is delayed
     * because the hard drive otherwise may delete the file a bit before the
     * data is written to the new file, which can cause problems when
     * recovering.
     *
     * @param fileName the name of the file to be deleted
     */
    public void deleteLogFileLater(String fileName) throws SQLException {
        if (fileLockMethod == FileLock.LOCK_SERIALIZED) {
            // need to truncate the file, because another process could keep it open
            try {
                FileSystem.getInstance(fileName).openFileObject(fileName, "rw").setFileLength(0);
            } catch (IOException e) {
                throw Message.convertIOException(e, "could not truncate " + fileName);
            }
        }
        if (writer != null) {
            writer.deleteLogFileLater(fileName);
        } else {
            FileUtils.delete(fileName);
        }
    }

    public void setEventListener(DatabaseEventListener eventListener) {
        this.eventListener = eventListener;
    }

    public void setEventListenerClass(String className) throws SQLException {
        if (className == null || className.length() == 0) {
            eventListener = null;
        } else {
            try {
                eventListener = (DatabaseEventListener) ClassUtils.loadUserClass(className).newInstance();
                String url = databaseURL;
                if (cipher != null) {
                    url += ";CIPHER=" + cipher;
                }
                eventListener.init(url);
            } catch (Throwable e) {
                throw Message.getSQLException(ErrorCode.ERROR_SETTING_DATABASE_EVENT_LISTENER_2, e, className, e.toString());
            }
        }
    }

    public synchronized void freeUpDiskSpace() throws SQLException {
        if (eventListener != null) {
            eventListener.diskSpaceIsLow(0);
        }
    }

    /**
     * Set the progress of a long running operation.
     * This method calls the {@link DatabaseEventListener} if one is registered.
     *
     * @param state the {@link DatabaseEventListener} state
     * @param name the object name
     * @param x the current position
     * @param max the highest value
     */

    public void setProgress(int state, String name, int x, int max) {
        if (eventListener != null) {
            try {
                eventListener.setProgress(state, name, x, max);
            } catch (Exception e2) {
                // ignore this second (user made) exception
            }
        }
    }

    /**
     * This method is called after an exception occurred, to inform the database
     * event listener (if one is set).
     *
     * @param e the exception
     * @param sql the SQL statement
     */
    public void exceptionThrown(SQLException e, String sql) {
        if (eventListener != null) {
            try {
                eventListener.exceptionThrown(e, sql);
            } catch (Exception e2) {
                // ignore this second (user made) exception
            }
        }
    }

    /**
     * Synchronize the files with the file system. This method is called when
     * executing the SQL statement CHECKPOINT SYNC.
     */
    public void sync() throws SQLException {
        if (log != null) {
            log.sync();
        }
        if (fileData != null) {
            fileData.sync();
        }
        if (fileIndex != null) {
            fileIndex.sync();
        }
    }

    public int getMaxMemoryRows() {
        return maxMemoryRows;
    }

    public void setMaxMemoryRows(int value) {
        this.maxMemoryRows = value;
    }

    public void setMaxMemoryUndo(int value) {
        this.maxMemoryUndo = value;
    }

    public int getMaxMemoryUndo() {
        return maxMemoryUndo;
    }

    public void setLockMode(int lockMode) throws SQLException {
        switch (lockMode) {
        case Constants.LOCK_MODE_OFF:
        case Constants.LOCK_MODE_READ_COMMITTED:
        case Constants.LOCK_MODE_TABLE:
        case Constants.LOCK_MODE_TABLE_GC:
            break;
        default:
            throw Message.getInvalidValueException("lock mode", "" + lockMode);
        }
        this.lockMode = lockMode;
    }

    public int getLockMode() {
        return lockMode;
    }

    public synchronized void setCloseDelay(int value) {
        this.closeDelay = value;
    }

    public boolean getLogIndexChanges() {
        return logIndexChanges;
    }

    public synchronized void setLog(int level) throws SQLException {
        if (logLevel == level) {
            return;
        }
        boolean logData;
        boolean logIndex;
        switch (level) {
        case 0:
            logData = false;
            logIndex = false;
            break;
        case 1:
            logData = true;
            logIndex = false;
            break;
        case 2:
            logData = true;
            logIndex = true;
            break;
        default:
            throw Message.throwInternalError("level=" + level);
        }
        if (fileIndex != null) {
            fileIndex.setLogChanges(logIndex);
        }
        logIndexChanges = logIndex;
        if (log != null) {
            log.setDisabled(!logData);
            log.checkpoint();
        }
        if (level == 0) {
            traceSystem.getTrace(Trace.DATABASE).error("SET LOG " + level, null);
        }
        logLevel = level;
    }

    public boolean getRecovery() {
        return recovery;
    }

    public Session getSystemSession() {
        return systemSession;
    }

    public void handleInvalidChecksum() throws SQLException {
        SQLException e = Message.getSQLException(ErrorCode.FILE_CORRUPTED_1, "wrong checksum");
        if (!recovery) {
            throw e;
        }
        traceSystem.getTrace(Trace.DATABASE).error("recover", e);
    }

    /**
     * Check if the database is in the process of closing.
     *
     * @return true if the database is closing
     */
    public boolean isClosing() {
        return closing;
    }

    public void setMaxLengthInplaceLob(int value) {
        this.maxLengthInplaceLob = value;
    }

    public int getMaxLengthInplaceLob() {
        return persistent ? maxLengthInplaceLob : Integer.MAX_VALUE;
    }

    public void setIgnoreCase(boolean b) {
        ignoreCase = b;
    }

    public boolean getIgnoreCase() {
        if (starting) {
            // tables created at startup must not be converted to ignorecase
            return false;
        }
        return ignoreCase;
    }

    public synchronized void setDeleteFilesOnDisconnect(boolean b) {
        this.deleteFilesOnDisconnect = b;
    }

    public String getLobCompressionAlgorithm(int type) {
        return lobCompressionAlgorithm;
    }

    public void setLobCompressionAlgorithm(String stringValue) {
        this.lobCompressionAlgorithm = stringValue;
    }

    /**
     * Called when the size if the data or index file has been changed.
     *
     * @param length the new file size
     */
    public void notifyFileSize(long length) {
        // ignore
    }

    public synchronized void setMaxLogSize(long value) {
        getLog().setMaxLogSize(value);
    }

    public void setAllowLiterals(int value) {
        this.allowLiterals = value;
    }

    public boolean getOptimizeReuseResults() {
        return optimizeReuseResults;
    }

    public void setOptimizeReuseResults(boolean b) {
        optimizeReuseResults = b;
    }

    /**
     * Called when the summary of the index in the log file has become invalid.
     * This method is only called if index changes are not logged, and if an
     * index has been changed.
     */
    public void invalidateIndexSummary() throws SQLException {
        if (indexSummaryValid) {
            indexSummaryValid = false;
            log.invalidateIndexSummary();
        }
    }

    public boolean isIndexSummaryValid() {
        return indexSummaryValid;
    }

    public Object getLobSyncObject() {
        return lobSyncObject;
    }

    public int getSessionCount() {
        return userSessions.size();
    }

    public void setReferentialIntegrity(boolean b) {
        referentialIntegrity = b;
    }

    public boolean getReferentialIntegrity() {
        return referentialIntegrity;
    }

    /**
     * Check if the database is currently opening. This is true until all stored
     * SQL statements have been executed.
     *
     * @return true if the database is still starting
     */
    public boolean isStarting() {
        return starting;
    }

    /**
     * Check if multi version concurrency is enabled for this database.
     *
     * @return true if it is enabled
     */
    public boolean isMultiVersion() {
        return multiVersion;
    }

    /**
     * Called after the database has been opened and initialized. This method
     * notifies the event listener if one has been set.
     */
    public void opened() {
        if (eventListener != null) {
            eventListener.opened();
        }
    }

    public void setMode(Mode mode) {
        this.mode = mode;
    }

    public Mode getMode() {
        return mode;
    }

    public boolean isMultiThreaded() {
        return multiThreaded;
    }

    public void setMultiThreaded(boolean multiThreaded) throws SQLException {
        if (multiThreaded && multiVersion && this.multiThreaded != multiThreaded) {
            // currently the combination of MVCC and MULTI_THREADED is not supported
            throw Message.getSQLException(ErrorCode.CANNOT_CHANGE_SETTING_WHEN_OPEN_1, "MVCC & MULTI_THREADED");
        }
        this.multiThreaded = multiThreaded;
    }

    public void setMaxOperationMemory(int maxOperationMemory) {
        this.maxOperationMemory  = maxOperationMemory;
    }

    public int getMaxOperationMemory() {
        return maxOperationMemory;
    }

    public Session getExclusiveSession() {
        return exclusiveSession;
    }

    public void setExclusiveSession(Session session) {
        this.exclusiveSession = session;
    }

    public boolean getLobFilesInDirectories() {
        return lobFilesInDirectories;
    }

    public SmallLRUCache<String, String[]> getLobFileListCache() {
        return lobFileListCache;
    }

    /**
     * Checks if the system table (containing the catalog) is locked.
     *
     * @return true if it is currently locked
     */
    public boolean isSysTableLocked() {
        return meta.isLockedExclusively();
    }

    /**
     * Open a new connection or get an existing connection to another database.
     *
     * @param driver the database driver or null
     * @param url the database URL
     * @param user the user name
     * @param password the password
     * @return the connection
     */
    public TableLinkConnection getLinkConnection(String driver, String url, String user, String password) throws SQLException {
        if (linkConnections == null) {
            linkConnections = New.hashMap();
        }
        return TableLinkConnection.open(linkConnections, driver, url, user, password);
    }

    public String toString() {
        return databaseShortName + ":" + super.toString();
    }

    /**
     * Immediately close the database.
     */
    public void shutdownImmediately() {
        setPowerOffCount(1);
        try {
            checkPowerOff();
        } catch (SQLException e) {
            // ignore
        }
    }

    public TempFileDeleter getTempFileDeleter() {
        return tempFileDeleter;
    }

    public Trace getTrace() {
        return getTrace(Trace.DATABASE);
    }

    public PageStore getPageStore() throws SQLException {
        if (pageStore == null && usePageStore) {
            pageStore = new PageStore(this, databaseName + Constants.SUFFIX_PAGE_FILE, accessModeData,
                    SysProperties.CACHE_SIZE_DEFAULT);
            pageStore.open();
        }
        return pageStore;
    }

    public boolean isPageStoreEnabled() {
        return usePageStore;
    }

    /**
     * Get the first user defined table.
     *
     * @return the table or null if no table is defined
     */
    public Table getFirstUserTable() {
        for (Table table : getAllTablesAndViews()) {
            if (table.getCreateSQL() != null) {
                return table;
            }
        }
        return null;
    }

    /**
     * Check if the contents of the database was changed and therefore it is
     * required to re-connect. This method waits until pending changes are
     * completed. If a pending change takes too long (more than 2 seconds), the
     * pending change is broken (removed from the properties file).
     *
     * @return true if reconnecting is required
     */
    public boolean isReconnectNeeded() {
        if (fileLockMethod != FileLock.LOCK_SERIALIZED) {
            return false;
        }
        if (reconnectChangePending) {
            return false;
        }
        long now = System.currentTimeMillis();
        if (now < reconnectCheckNext) {
            return false;
        }
        reconnectCheckNext = now + SysProperties.RECONNECT_CHECK_DELAY;
        if (lock == null) {
            lock = new FileLock(traceSystem, databaseName + Constants.SUFFIX_LOCK_FILE, Constants.LOCK_SLEEP);
        }
        try {
            Properties prop = lock.load(), first = prop;
            while (true) {
                if (prop.equals(reconnectLastLock)) {
                    return false;
                }
                if (prop.getProperty("changePending", null) == null) {
                    break;
                }
                if (System.currentTimeMillis() > now + SysProperties.RECONNECT_CHECK_DELAY * 10) {
                    if (first.equals(prop)) {
                        // the writing process didn't update the file -
                        // it may have terminated
                        lock.setProperty("changePending", null);
                        lock.save();
                        break;
                    }
                }
                getTrace().debug("delay (change pending)");
                Thread.sleep(SysProperties.RECONNECT_CHECK_DELAY);
                prop = lock.load();
            }
            reconnectLastLock = prop;
        } catch (Exception e) {
            getTrace().error("readOnly:" + readOnly, e);
            // ignore
        }
        return true;
    }

    /**
     * Flush all changes when using the serialized mode, and if there are
     * pending changes, and some time has passed. This switches to a new
     * transaction log and resets the change pending flag in
     * the .lock.db file.
     */
    public void checkpointIfRequired() throws SQLException {
        if (fileLockMethod != FileLock.LOCK_SERIALIZED || readOnly || !reconnectChangePending || closing) {
            return;
        }
        synchronized (this) {
            long now = System.currentTimeMillis();
            if (now > reconnectCheckNext + SysProperties.RECONNECT_CHECK_DELAY) {
                getTrace().debug("checkpoint");
                flushIndexes(0);
                checkpoint();
                reconnectModified(false);
            }
        }
    }

    public boolean isFileLockSerialized() {
        return fileLockMethod == FileLock.LOCK_SERIALIZED;
    }

    /**
     * Flush the indexes that were last changed prior to some time.
     *
     * @param maxLastChange indexes that were changed
     *          afterwards are not flushed; 0 to flush all indexes
     */
    public synchronized void flushIndexes(long maxLastChange) {
        for (SchemaObject obj : getAllSchemaObjects(DbObject.INDEX)) {
            if (obj instanceof BtreeIndex) {
                BtreeIndex idx = (BtreeIndex) obj;
                if (idx.getLastChange() == 0) {
                    continue;
                }
                Table tab = idx.getTable();
                if (tab.isLockedExclusively()) {
                    continue;
                }
                if (maxLastChange != 0 && idx.getLastChange() > maxLastChange) {
                    continue;
                }
                try {
                    idx.flush(systemSession);
                } catch (SQLException e) {
                    getTrace().error("flush index " + idx.getName(), e);
                }
            }
        }
    }

    /**
     * Flush all changes and open a new log file.
     */
    public void checkpoint() throws SQLException {
        if (persistent) {
            if (pageStore != null) {
                pageStore.checkpoint();
            }
            getLog().checkpoint();
        }
        getTempFileDeleter().deleteUnused();
    }

    /**
     * This method is called before writing to the log file.
     *
     * @return true if the call was successful,
     *          false if another connection was faster
     */
    public boolean beforeWriting() {
        if (fileLockMethod == FileLock.LOCK_SERIALIZED) {

            return reconnectModified(true);
        }
        return true;
    }

    /**
     * Switch the database to read-only mode.
     *
     * @param readOnly the new value
     */
    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

}
