/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.h2.api.DatabaseEventListener;
import org.h2.api.ErrorCode;
import org.h2.api.JavaObjectSerializer;
import org.h2.api.TableEngine;
import org.h2.command.CommandInterface;
import org.h2.command.Prepared;
import org.h2.command.ddl.CreateTableData;
import org.h2.command.dml.SetTypes;
import org.h2.constraint.Constraint;
import org.h2.constraint.Constraint.Type;
import org.h2.engine.Mode.ModeEnum;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.message.TraceSystem;
import org.h2.mode.DefaultNullOrdering;
import org.h2.mode.PgCatalogSchema;
import org.h2.mvstore.MVStoreException;
import org.h2.mvstore.db.LobStorageMap;
import org.h2.mvstore.db.Store;
import org.h2.result.Row;
import org.h2.result.RowFactory;
import org.h2.result.SearchRow;
import org.h2.schema.InformationSchema;
import org.h2.schema.Schema;
import org.h2.schema.SchemaObject;
import org.h2.schema.Sequence;
import org.h2.schema.TriggerObject;
import org.h2.security.auth.Authenticator;
import org.h2.store.DataHandler;
import org.h2.store.FileLock;
import org.h2.store.FileLockMethod;
import org.h2.store.FileStore;
import org.h2.store.InDoubtTransaction;
import org.h2.store.LobStorageInterface;
import org.h2.store.fs.FileUtils;
import org.h2.store.fs.encrypt.FileEncrypt;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.table.TableLinkConnection;
import org.h2.table.TableSynonym;
import org.h2.table.TableType;
import org.h2.table.TableView;
import org.h2.tools.DeleteDbFiles;
import org.h2.tools.Server;
import org.h2.util.JdbcUtils;
import org.h2.util.MathUtils;
import org.h2.util.NetUtils;
import org.h2.util.NetworkConnectionInfo;
import org.h2.util.SmallLRUCache;
import org.h2.util.SourceCompiler;
import org.h2.util.StringUtils;
import org.h2.util.TempFileDeleter;
import org.h2.util.TimeZoneProvider;
import org.h2.util.Utils;
import org.h2.value.CaseInsensitiveConcurrentMap;
import org.h2.value.CaseInsensitiveMap;
import org.h2.value.CompareMode;
import org.h2.value.TypeInfo;
import org.h2.value.ValueInteger;
import org.h2.value.ValueTimestampTimeZone;

/**
 * There is one database object per open database.
 *
 * The format of the meta data table is:
 *  id int, 0, objectType int, sql varchar
 *
 * @since 2004-04-15 22:49
 */
public final class Database implements DataHandler, CastDataProvider {

    private static int initialPowerOffCount;

    private static final boolean ASSERT;

    private static final ThreadLocal<SessionLocal> META_LOCK_DEBUGGING;
    private static final ThreadLocal<Database> META_LOCK_DEBUGGING_DB;
    private static final ThreadLocal<Throwable> META_LOCK_DEBUGGING_STACK;
    private static final SessionLocal[] EMPTY_SESSION_ARRAY = new SessionLocal[0];

    static {
        boolean a = false;
        // Intentional side-effect
        assert a = true;
        ASSERT = a;
        if (a) {
            META_LOCK_DEBUGGING = new ThreadLocal<>();
            META_LOCK_DEBUGGING_DB = new ThreadLocal<>();
            META_LOCK_DEBUGGING_STACK = new ThreadLocal<>();
        } else {
            META_LOCK_DEBUGGING = null;
            META_LOCK_DEBUGGING_DB = null;
            META_LOCK_DEBUGGING_STACK = null;
        }
    }

    /**
     * The default name of the system user. This name is only used as long as
     * there is no administrator user registered.
     */
    private static final String SYSTEM_USER_NAME = "DBA";

    private final boolean persistent;
    private final String databaseName;
    private final String databaseShortName;
    private final String databaseURL;
    private final String cipher;
    private final byte[] filePasswordHash;

    private final ConcurrentHashMap<String, RightOwner> usersAndRoles = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Setting> settings = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Schema> schemas = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Right> rights = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Comment> comments = new ConcurrentHashMap<>();

    private final HashMap<String, TableEngine> tableEngines = new HashMap<>();

    private final Set<SessionLocal> userSessions = Collections.synchronizedSet(new HashSet<>());
    private final AtomicReference<SessionLocal> exclusiveSession = new AtomicReference<>();
    private final BitSet objectIds = new BitSet();
    private final Object lobSyncObject = new Object();

    private final Schema mainSchema;
    private final Schema infoSchema;
    private final Schema pgCatalogSchema;
    private int nextSessionId;
    private final AtomicInteger nextTempTableId = new AtomicInteger();
    private final User systemUser;
    private SessionLocal systemSession;
    private SessionLocal lobSession;
    private final Table meta;
    private final Index metaIdIndex;
    private FileLock lock;
    private volatile boolean starting;
    private final TraceSystem traceSystem;
    private final Trace trace;
    private final FileLockMethod fileLockMethod;
    private final Role publicRole;
    private final AtomicLong modificationDataId = new AtomicLong();
    private final AtomicLong modificationMetaId = new AtomicLong();
    /**
     * Used to trigger the client side to reload some of the settings.
     */
    private final AtomicLong remoteSettingsId = new AtomicLong();
    private CompareMode compareMode;
    private String cluster = Constants.CLUSTERING_DISABLED;
    private boolean readOnly;
    private DatabaseEventListener eventListener;
    private int maxMemoryRows = SysProperties.MAX_MEMORY_ROWS;
    private int lockMode;
    private int maxLengthInplaceLob;
    private int allowLiterals = Constants.ALLOW_LITERALS_ALL;

    private int powerOffCount = initialPowerOffCount;
    private volatile int closeDelay;
    private DelayedDatabaseCloser delayedCloser;
    private volatile boolean closing;
    private boolean ignoreCase;
    private boolean deleteFilesOnDisconnect;
    private boolean optimizeReuseResults = true;
    private final String cacheType;
    private boolean referentialIntegrity = true;
    private Mode mode = Mode.getRegular();
    private DefaultNullOrdering defaultNullOrdering = DefaultNullOrdering.LOW;
    private int maxOperationMemory =
            Constants.DEFAULT_MAX_OPERATION_MEMORY;
    private SmallLRUCache<String, String[]> lobFileListCache;
    private final boolean closeAtVmShutdown;
    private final boolean autoServerMode;
    private final int autoServerPort;
    private Server server;
    private HashMap<TableLinkConnection, TableLinkConnection> linkConnections;
    private final TempFileDeleter tempFileDeleter = TempFileDeleter.getInstance();
    private int compactMode;
    private SourceCompiler compiler;
    private final LobStorageInterface lobStorage;
    private final int pageSize;
    private int defaultTableType = Table.TYPE_CACHED;
    private final DbSettings dbSettings;
    private final Store store;
    private boolean allowBuiltinAliasOverride;
    private final AtomicReference<DbException> backgroundException = new AtomicReference<>();
    private JavaObjectSerializer javaObjectSerializer;
    private String javaObjectSerializerName;
    private volatile boolean javaObjectSerializerInitialized;
    private volatile boolean queryStatistics;
    private int queryStatisticsMaxEntries = Constants.QUERY_STATISTICS_MAX_ENTRIES;
    private QueryStatisticsData queryStatisticsData;
    private RowFactory rowFactory = RowFactory.getRowFactory();
    private boolean ignoreCatalogs;

    private Authenticator authenticator;

    public Database(ConnectionInfo ci, String cipher) {
        if (ASSERT) {
            META_LOCK_DEBUGGING.set(null);
            META_LOCK_DEBUGGING_DB.set(null);
            META_LOCK_DEBUGGING_STACK.set(null);
        }
        String databaseName = ci.getName();
        this.dbSettings = ci.getDbSettings();
        this.compareMode = CompareMode.getInstance(null, 0);
        this.persistent = ci.isPersistent();
        this.filePasswordHash = ci.getFilePasswordHash();
        this.databaseName = databaseName;
        this.databaseShortName = parseDatabaseShortName();
        this.maxLengthInplaceLob = persistent ? Constants.DEFAULT_MAX_LENGTH_INPLACE_LOB : Integer.MAX_VALUE - 8;
        this.cipher = cipher;
        this.autoServerMode = ci.getProperty("AUTO_SERVER", false);
        this.autoServerPort = ci.getProperty("AUTO_SERVER_PORT", 0);
        pageSize = ci.getProperty("PAGE_SIZE", Constants.DEFAULT_PAGE_SIZE);
        if (cipher != null && pageSize % FileEncrypt.BLOCK_SIZE != 0) {
            throw DbException.getUnsupportedException("CIPHER && PAGE_SIZE=" + pageSize);
        }
        String accessModeData = StringUtils.toLowerEnglish(ci.getProperty("ACCESS_MODE_DATA", "rw"));
        if ("r".equals(accessModeData)) {
            readOnly = true;
        }
        String lockMethodName = ci.getProperty("FILE_LOCK", null);
        fileLockMethod = lockMethodName != null ? FileLock.getFileLockMethod(lockMethodName) :
                            autoServerMode ? FileLockMethod.FILE : FileLockMethod.FS;
        this.databaseURL = ci.getURL();
        String s = ci.removeProperty("DATABASE_EVENT_LISTENER", null);
        if (s != null) {
            setEventListenerClass(StringUtils.trim(s, true, true, '\''));
        }
        s = ci.removeProperty("MODE", null);
        if (s != null) {
            mode = Mode.getInstance(s);
            if (mode == null) {
                throw DbException.get(ErrorCode.UNKNOWN_MODE_1, s);
            }
        }
        s = ci.removeProperty("DEFAULT_NULL_ORDERING", null);
        if (s != null) {
            try {
                defaultNullOrdering = DefaultNullOrdering.valueOf(StringUtils.toUpperEnglish(s));
            } catch (RuntimeException e) {
                throw DbException.getInvalidValueException("DEFAULT_NULL_ORDERING", s);
            }
        }
        s = ci.getProperty("JAVA_OBJECT_SERIALIZER", null);
        if (s != null) {
            s = StringUtils.trim(s, true, true, '\'');
            javaObjectSerializerName = s;
        }
        this.allowBuiltinAliasOverride = ci.getProperty("BUILTIN_ALIAS_OVERRIDE", false);
        if (autoServerMode && (readOnly || !persistent || fileLockMethod == FileLockMethod.NO
                || fileLockMethod == FileLockMethod.FS)) {
            throw DbException.getUnsupportedException(
                    "AUTO_SERVER=TRUE && (readOnly || inMemory || FILE_LOCK=NO || FILE_LOCK=FS)");
        }
        closeAtVmShutdown = ci.getProperty("DB_CLOSE_ON_EXIT", persistent);
        if (autoServerMode && !closeAtVmShutdown) {
            throw DbException.getUnsupportedException("AUTO_SERVER=TRUE && DB_CLOSE_ON_EXIT=FALSE");
        }
        int traceLevelFile = ci.getIntProperty(SetTypes.TRACE_LEVEL_FILE, TraceSystem.DEFAULT_TRACE_LEVEL_FILE);
        int traceLevelSystemOut = ci.getIntProperty(SetTypes.TRACE_LEVEL_SYSTEM_OUT,
                TraceSystem.DEFAULT_TRACE_LEVEL_SYSTEM_OUT);
        this.cacheType = StringUtils.toUpperEnglish(ci.removeProperty("CACHE_TYPE", Constants.CACHE_TYPE_DEFAULT));
        this.ignoreCatalogs = ci.getProperty("IGNORE_CATALOGS", dbSettings.ignoreCatalogs);
        this.lockMode = ci.getProperty("LOCK_MODE", Constants.DEFAULT_LOCK_MODE);
        String traceFile;
        if (persistent) {
            if (readOnly) {
                if (traceLevelFile >= TraceSystem.DEBUG) {
                    traceFile = Utils.getProperty("java.io.tmpdir", ".") + "/h2_" + System.currentTimeMillis()
                            + Constants.SUFFIX_TRACE_FILE;
                } else {
                    traceFile = null;
                }
            } else {
                traceFile = databaseName + Constants.SUFFIX_TRACE_FILE;
            }
        } else {
            traceFile = null;
        }
        traceSystem = new TraceSystem(traceFile);
        traceSystem.setLevelFile(traceLevelFile);
        traceSystem.setLevelSystemOut(traceLevelSystemOut);
        trace = traceSystem.getTrace(Trace.DATABASE);
        trace.info("opening {0} (build {1})", databaseName, Constants.BUILD_ID);
        try {
            if (persistent) {
                String lockFileName = databaseName + Constants.SUFFIX_LOCK_FILE;
                if (readOnly) {
                    if (FileUtils.exists(lockFileName)) {
                        throw DbException.get(ErrorCode.DATABASE_ALREADY_OPEN_1, "Lock file exists: " + lockFileName);
                    }
                } else if (fileLockMethod != FileLockMethod.NO && fileLockMethod != FileLockMethod.FS) {
                    lock = new FileLock(traceSystem, lockFileName, Constants.LOCK_SLEEP);
                    lock.lock(fileLockMethod);
                    if (autoServerMode) {
                        startServer(lock.getUniqueId());
                    }
                }
                deleteOldTempFiles();
            }
            starting = true;
            if (dbSettings.mvStore) {
                store = new Store(this, ci.getFileEncryptionKey());
            } else {
                throw new UnsupportedOperationException();
            }
            starting = false;
            systemUser = new User(this, 0, SYSTEM_USER_NAME, true);
            systemUser.setAdmin(true);
            mainSchema = new Schema(this, Constants.MAIN_SCHEMA_ID, sysIdentifier(Constants.SCHEMA_MAIN), systemUser,
                    true);
            infoSchema = new InformationSchema(this, systemUser);
            schemas.put(mainSchema.getName(), mainSchema);
            schemas.put(infoSchema.getName(), infoSchema);
            if (mode.getEnum() == ModeEnum.PostgreSQL) {
                pgCatalogSchema = new PgCatalogSchema(this, systemUser);
                schemas.put(pgCatalogSchema.getName(), pgCatalogSchema);
            } else {
                pgCatalogSchema = null;
            }
            publicRole = new Role(this, 0, sysIdentifier(Constants.PUBLIC_ROLE_NAME), true);
            usersAndRoles.put(publicRole.getName(), publicRole);
            systemSession = createSession(systemUser);
            lobSession = createSession(systemUser);
            Set<String> settingKeys = dbSettings.getSettings().keySet();
            store.getTransactionStore().init(lobSession);
            settingKeys.removeIf(name -> name.startsWith("PAGE_STORE_"));
            CreateTableData data = createSysTableData();
            starting = true;
            meta = mainSchema.createTable(data);
            IndexColumn[] pkCols = IndexColumn.wrap(new Column[] { data.columns.get(0) });
            metaIdIndex = meta.addIndex(systemSession, "SYS_ID", 0, pkCols, 1,
                    IndexType.createPrimaryKey(false, false), true, null);
            systemSession.commit(true);
            objectIds.set(0);
            executeMeta();
            systemSession.commit(true);
            store.getTransactionStore().endLeftoverTransactions();
            store.removeTemporaryMaps(objectIds);
            recompileInvalidViews();
            starting = false;
            if (!readOnly) {
                // set CREATE_BUILD in a new database
                String settingName = SetTypes.getTypeName(SetTypes.CREATE_BUILD);
                Setting setting = settings.get(settingName);
                if (setting == null) {
                    setting = new Setting(this, allocateObjectId(), settingName);
                    setting.setIntValue(Constants.BUILD_ID);
                    lockMeta(systemSession);
                    addDatabaseObject(systemSession, setting);
                }
            }
            lobStorage = new LobStorageMap(this);
            lobSession.commit(true);
            systemSession.commit(true);
            trace.info("opened {0}", databaseName);
            if (persistent) {
                int writeDelay = ci.getProperty("WRITE_DELAY", Constants.DEFAULT_WRITE_DELAY);
                setWriteDelay(writeDelay);
            }
            if (closeAtVmShutdown || persistent) {
                OnExitDatabaseCloser.register(this);
            }
        } catch (Throwable e) {
            try {
                if (e instanceof OutOfMemoryError) {
                    e.fillInStackTrace();
                }
                if (e instanceof DbException) {
                    if (((DbException) e).getErrorCode() == ErrorCode.DATABASE_ALREADY_OPEN_1) {
                        stopServer();
                    } else {
                        // only write if the database is not already in use
                        trace.error(e, "opening {0}", databaseName);
                    }
                }
                traceSystem.close();
                closeOpenFilesAndUnlock();
            } catch (Throwable ex) {
                e.addSuppressed(ex);
            }
            throw DbException.convert(e);
        }
    }

    public int getLockTimeout() {
        Setting setting = findSetting(SetTypes.getTypeName(SetTypes.DEFAULT_LOCK_TIMEOUT));
        return setting == null ? Constants.INITIAL_LOCK_TIMEOUT : setting.getIntValue();
    }

    public RowFactory getRowFactory() {
        return rowFactory;
    }

    public void setRowFactory(RowFactory rowFactory) {
        this.rowFactory = rowFactory;
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

    public Store getStore() {
        return store;
    }

    public long getModificationDataId() {
        return modificationDataId.get();
    }

    public long getNextModificationDataId() {
        return modificationDataId.incrementAndGet();
    }

    public long getModificationMetaId() {
        return modificationMetaId.get();
    }

    public long getNextModificationMetaId() {
        // if the meta data has been modified, the data is modified as well
        // (because MetaTable returns modificationDataId)
        modificationDataId.incrementAndGet();
        return modificationMetaId.incrementAndGet() - 1;
    }

    public long getRemoteSettingsId() {
        return remoteSettingsId.get();
    }

    public long getNextRemoteSettingsId() {
        return remoteSettingsId.incrementAndGet();
    }

    public int getPowerOffCount() {
        return powerOffCount;
    }

    @Override
    public void checkPowerOff() {
        if (powerOffCount != 0) {
            checkPowerOff2();
        }
    }

    private void checkPowerOff2() {
        if (powerOffCount > 1) {
            powerOffCount--;
            return;
        }
        if (powerOffCount != -1) {
            try {
                powerOffCount = -1;
                store.closeImmediately();
                if (lock != null) {
                    stopServer();
                    // allow testing shutdown
                    lock.unlock();
                    lock = null;
                }
                if (traceSystem != null) {
                    traceSystem.close();
                }
            } catch (DbException e) {
                DbException.traceThrowable(e);
            }
        }
        Engine.close(databaseName);
        throw DbException.get(ErrorCode.DATABASE_IS_CLOSED);
    }

    /**
     * Get the trace object for the given module id.
     *
     * @param moduleId the module id
     * @return the trace object
     */
    public Trace getTrace(int moduleId) {
        return traceSystem.getTrace(moduleId);
    }

    @Override
    public FileStore openFile(String name, String openMode, boolean mustExist) {
        if (mustExist && !FileUtils.exists(name)) {
            throw DbException.get(ErrorCode.FILE_NOT_FOUND_1, name);
        }
        FileStore store = FileStore.open(this, name, openMode, cipher,
                filePasswordHash);
        try {
            store.init();
        } catch (DbException e) {
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
    boolean validateFilePasswordHash(String testCipher, byte[] testHash) {
        if (!Objects.equals(testCipher, this.cipher)) {
            return false;
        }
        return Utils.compareSecure(testHash, filePasswordHash);
    }

    private String parseDatabaseShortName() {
        String n = databaseName;
        int l = n.length(), i = l;
        loop: while (--i >= 0) {
            char ch = n.charAt(i);
            switch (ch) {
            case '/':
            case ':':
            case '\\':
                break loop;
            }
        }
        n = ++i == l ? "UNNAMED" : n.substring(i);
        return StringUtils.truncateString(
                dbSettings.databaseToUpper ? StringUtils.toUpperEnglish(n)
                        : dbSettings.databaseToLower ? StringUtils.toLowerEnglish(n) : n,
                Constants.MAX_IDENTIFIER_LENGTH);
    }

    private CreateTableData createSysTableData() {
        CreateTableData data = new CreateTableData();
        ArrayList<Column> cols = data.columns;
        Column columnId = new Column("ID", TypeInfo.TYPE_INTEGER);
        columnId.setNullable(false);
        cols.add(columnId);
        cols.add(new Column("HEAD", TypeInfo.TYPE_INTEGER));
        cols.add(new Column("TYPE", TypeInfo.TYPE_INTEGER));
        cols.add(new Column("SQL", TypeInfo.TYPE_VARCHAR));
        data.tableName = "SYS";
        data.id = 0;
        data.temporary = false;
        data.persistData = persistent;
        data.persistIndexes = persistent;
        data.session = systemSession;
        return data;
    }

    private void executeMeta() {
        Cursor cursor = metaIdIndex.find(systemSession, null, null, false);
        ArrayList<MetaRecord> firstRecords = new ArrayList<>(), domainRecords = new ArrayList<>(),
                middleRecords = new ArrayList<>(), constraintRecords = new ArrayList<>(),
                lastRecords = new ArrayList<>();
        while (cursor.next()) {
            MetaRecord rec = new MetaRecord(cursor.get());
            objectIds.set(rec.getId());
            switch (rec.getObjectType()) {
            case DbObject.SETTING:
            case DbObject.USER:
            case DbObject.SCHEMA:
            case DbObject.FUNCTION_ALIAS:
                firstRecords.add(rec);
                break;
            case DbObject.DOMAIN:
                domainRecords.add(rec);
                break;
            case DbObject.SEQUENCE:
            case DbObject.CONSTANT:
            case DbObject.TABLE_OR_VIEW:
            case DbObject.INDEX:
                middleRecords.add(rec);
                break;
            case DbObject.CONSTRAINT:
                constraintRecords.add(rec);
                break;
            default:
                lastRecords.add(rec);
            }
        }
        final SessionLocal systemSession = this.systemSession;
        systemSession.lock();
        try {
            executeMeta(firstRecords);
            // Domains may depend on other domains
            int count = domainRecords.size();
            if (count > 0) {
                for (int j = 0;; count = j) {
                    DbException exception = null;
                    for (int i = 0; i < count; i++) {
                        MetaRecord rec = domainRecords.get(i);
                        try {
                            rec.prepareAndExecute(this, systemSession, eventListener);
                        } catch (DbException ex) {
                            if (exception == null) {
                                exception = ex;
                            }
                            domainRecords.set(j++, rec);
                        }
                    }
                    if (exception == null) {
                        break;
                    }
                    if (count == j) {
                        throw exception;
                    }
                }
            }
            executeMeta(middleRecords);
            // Prepare, but don't create all constraints and sort them
            count = constraintRecords.size();
            if (count > 0) {
                ArrayList<Prepared> constraints = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    Prepared prepared = constraintRecords.get(i).prepare(this, systemSession, eventListener);
                    if (prepared != null) {
                        constraints.add(prepared);
                    }
                }
                constraints.sort(MetaRecord.CONSTRAINTS_COMPARATOR);
                // Create constraints in order (unique and primary key before
                // all others)
                for (Prepared constraint : constraints) {
                    MetaRecord.execute(this, constraint, eventListener, constraint.getSQL());
                }
            }
            executeMeta(lastRecords);
        } finally {
            systemSession.unlock();
        }
    }

    private void executeMeta(ArrayList<MetaRecord> records) {
        if (!records.isEmpty()) {
            records.sort(null);
            for (MetaRecord rec : records) {
                rec.prepareAndExecute(this, systemSession, eventListener);
            }
        }
    }

    private void startServer(String key) {
        try {
            server = Server.createTcpServer(
                    "-tcpPort", Integer.toString(autoServerPort),
                    "-tcpAllowOthers",
                    "-tcpDaemon",
                    "-key", key, databaseName);
            server.start();
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
        String localAddress = NetUtils.getLocalAddress();
        String address = localAddress + ":" + server.getPort();
        lock.setProperty("server", address);
        String hostName = NetUtils.getHostName(localAddress);
        lock.setProperty("hostName", hostName);
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

    private void recompileInvalidViews() {
        boolean atLeastOneRecompiledSuccessfully;
        do {
            atLeastOneRecompiledSuccessfully = false;
            for (Schema schema : schemas.values()) {
                for (Table obj : schema.getAllTablesAndViews(null)) {
                    if (obj instanceof TableView) {
                        TableView view = (TableView) obj;
                        if (view.isInvalid()) {
                            view.recompile(systemSession, true, false);
                            if (!view.isInvalid()) {
                                atLeastOneRecompiledSuccessfully = true;
                            }
                        }
                    }
                }
            }
        } while (atLeastOneRecompiledSuccessfully);
        TableView.clearIndexCaches(this);
    }

    private void addMeta(SessionLocal session, DbObject obj) {
        assert Thread.holdsLock(this);
        int id = obj.getId();
        if (id > 0 && !obj.isTemporary()) {
            if (!isReadOnly()) {
                Row r = meta.getTemplateRow();
                MetaRecord.populateRowFromDBObject(obj, r);
                assert objectIds.get(id);
                if (SysProperties.CHECK) {
                    verifyMetaLocked(session);
                }
                Cursor cursor = metaIdIndex.find(session, r, r, false);
                if (!cursor.next()) {
                    meta.addRow(session, r);
                } else {
                    assert starting;
                    Row oldRow = cursor.get();
                    MetaRecord rec = new MetaRecord(oldRow);
                    assert rec.getId() == obj.getId();
                    assert rec.getObjectType() == obj.getType();
                    if (!rec.getSQL().equals(obj.getCreateSQLForMeta())) {
                        meta.updateRow(session, oldRow, r);
                    }
                }
            }
        }
    }

    /**
     * Verify the meta table is locked.
     *
     * @param session the session
     */
    public void verifyMetaLocked(SessionLocal session) {
        if (lockMode != Constants.LOCK_MODE_OFF && meta != null && !meta.isLockedExclusivelyBy(session)) {
            throw DbException.getInternalError();
        }
    }

    /**
     * Lock the metadata table for updates.
     *
     * @param session the session
     * @return whether it was already locked before by this session
     */
    public boolean lockMeta(SessionLocal session) {
        // this method can not be synchronized on the database object,
        // as unlocking is also synchronized on the database object -
        // so if locking starts just before unlocking, locking could
        // never be successful
        if (meta == null) {
            return true;
        }
        if (ASSERT) {
            lockMetaAssertion(session);
        }
        return meta.lock(session, Table.EXCLUSIVE_LOCK);
    }

    private void lockMetaAssertion(SessionLocal session) {
        // If we are locking two different databases in the same stack, just ignore it.
        // This only happens in TestLinkedTable where we connect to another h2 DB in the
        // same process.
        if (META_LOCK_DEBUGGING_DB.get() != null && META_LOCK_DEBUGGING_DB.get() != this) {
            final SessionLocal prev = META_LOCK_DEBUGGING.get();
            if (prev == null) {
                META_LOCK_DEBUGGING.set(session);
                META_LOCK_DEBUGGING_DB.set(this);
                META_LOCK_DEBUGGING_STACK.set(new Throwable("Last meta lock granted in this stack trace, "
                        + "this is debug information for following IllegalStateException"));
            } else if (prev != session) {
                META_LOCK_DEBUGGING_STACK.get().printStackTrace();
                throw new IllegalStateException("meta currently locked by " + prev + ", sessionid=" + prev.getId()
                        + " and trying to be locked by different session, " + session + ", sessionid=" //
                        + session.getId() + " on same thread");
            }
        }
    }

    /**
     * Unlock the metadata table.
     *
     * @param session the session
     */
    public void unlockMeta(SessionLocal session) {
        if (meta != null) {
            unlockMetaDebug(session);
            meta.unlock(session);
            session.unlock(meta);
        }
    }

    /**
     * This method doesn't actually unlock the metadata table, all it does it
     * reset the debugging flags.
     *
     * @param session the session
     */
    static void unlockMetaDebug(SessionLocal session) {
        if (ASSERT) {
            if (META_LOCK_DEBUGGING.get() == session) {
                META_LOCK_DEBUGGING.set(null);
                META_LOCK_DEBUGGING_DB.set(null);
                META_LOCK_DEBUGGING_STACK.set(null);
            }
        }
    }

    /**
     * Remove the given object from the meta data.
     *
     * @param session the session
     * @param id the id of the object to remove
     */
    public void removeMeta(SessionLocal session, int id) {
        if (id > 0 && !starting) {
            SearchRow r = meta.getRowFactory().createRow();
            r.setValue(0, ValueInteger.get(id));
            boolean wasLocked = lockMeta(session);
            try {
                Cursor cursor = metaIdIndex.find(session, r, r, false);
                if (cursor.next()) {
                    Row found = cursor.get();
                    meta.removeRow(session, found);
                    if (SysProperties.CHECK) {
                        checkMetaFree(session, id);
                    }
                }
            } finally {
                if (!wasLocked) {
                    // must not keep the lock if it was not locked
                    // otherwise updating sequences may cause a deadlock
                    unlockMeta(session);
                }
            }
            // release of the object id has to be postponed until the end of the transaction,
            // otherwise it might be re-used prematurely, and it would make
            // rollback impossible or lead to MVMaps name collision,
            // so until then ids are accumulated within session
            session.scheduleDatabaseObjectIdForRelease(id);
        }
    }

    /**
     * Mark some database ids as unused.
     * @param idsToRelease the ids to release
     */
    public void releaseDatabaseObjectIds(BitSet idsToRelease) {
        synchronized (objectIds) {
            objectIds.andNot(idsToRelease);
        }
    }

    @SuppressWarnings("unchecked")
    private ConcurrentHashMap<String, DbObject> getMap(int type) {
        Map<String, ? extends DbObject> result;
        switch (type) {
        case DbObject.USER:
        case DbObject.ROLE:
            result = usersAndRoles;
            break;
        case DbObject.SETTING:
            result = settings;
            break;
        case DbObject.RIGHT:
            result = rights;
            break;
        case DbObject.SCHEMA:
            result = schemas;
            break;
        case DbObject.COMMENT:
            result = comments;
            break;
        default:
            throw DbException.getInternalError("type=" + type);
        }
        return (ConcurrentHashMap<String, DbObject>) result;
    }

    /**
     * Add a schema object to the database.
     *
     * @param session the session
     * @param obj the object to add
     */
    public void addSchemaObject(SessionLocal session, SchemaObject obj) {
        int id = obj.getId();
        if (id > 0 && !starting) {
            checkWritingAllowed();
        }
        lockMeta(session);
        synchronized (this) {
            obj.getSchema().add(obj);
            addMeta(session, obj);
        }
    }

    /**
     * Add an object to the database.
     *
     * @param session the session
     * @param obj the object to add
     */
    public synchronized void addDatabaseObject(SessionLocal session, DbObject obj) {
        int id = obj.getId();
        if (id > 0 && !starting) {
            checkWritingAllowed();
        }
        ConcurrentHashMap<String, DbObject> map = getMap(obj.getType());
        if (obj.getType() == DbObject.USER) {
            User user = (User) obj;
            if (user.isAdmin() && systemUser.getName().equals(SYSTEM_USER_NAME)) {
                systemUser.rename(user.getName());
            }
        }
        String name = obj.getName();
        if (SysProperties.CHECK && map.get(name) != null) {
            throw DbException.getInternalError("object already exists");
        }
        lockMeta(session);
        addMeta(session, obj);
        map.put(name, obj);
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
     * Get the role if it exists, or null if not.
     *
     * @param roleName the name of the role
     * @return the role or null
     */
    public Role findRole(String roleName) {
        RightOwner rightOwner = findUserOrRole(roleName);
        return rightOwner instanceof Role ? (Role) rightOwner : null;
    }

    /**
     * Get the schema if it exists, or null if not.
     *
     * @param schemaName the name of the schema
     * @return the schema or null
     */
    public Schema findSchema(String schemaName) {
        if (schemaName == null) {
            return null;
        }
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
        RightOwner rightOwner = findUserOrRole(name);
        return rightOwner instanceof User ? (User) rightOwner : null;
    }

    /**
     * Get user with the given name. This method throws an exception if the user
     * does not exist.
     *
     * @param name the user name
     * @return the user
     * @throws DbException if the user does not exist
     */
    public User getUser(String name) {
        User user = findUser(name);
        if (user == null) {
            throw DbException.get(ErrorCode.USER_NOT_FOUND_1, name);
        }
        return user;
    }

    /**
     * Get the user or role if it exists, or {@code null} if not.
     *
     * @param name the name of the user or role
     * @return the user, the role, or {@code null}
     */
    public RightOwner findUserOrRole(String name) {
        return usersAndRoles.get(StringUtils.toUpperEnglish(name));
    }

    /**
     * Create a session for the given user.
     *
     * @param user the user
     * @param networkConnectionInfo the network connection information, or {@code null}
     * @return the session, or null if the database is currently closing
     * @throws DbException if the database is in exclusive mode
     */
    synchronized SessionLocal createSession(User user, NetworkConnectionInfo networkConnectionInfo) {
        if (closing) {
            return null;
        }
        if (exclusiveSession.get() != null) {
            throw DbException.get(ErrorCode.DATABASE_IS_IN_EXCLUSIVE_MODE);
        }
        SessionLocal session = createSession(user);
        session.setNetworkConnectionInfo(networkConnectionInfo);
        userSessions.add(session);
        trace.info("connecting session #{0} to {1}", session.getId(), databaseName);
        if (delayedCloser != null) {
            delayedCloser.reset();
            delayedCloser = null;
        }
        return session;
    }

    private SessionLocal createSession(User user) {
        int id = ++nextSessionId;
        return new SessionLocal(this, user, id);
    }

    /**
     * Remove a session. This method is called after the user has disconnected.
     *
     * @param session the session
     */
    public synchronized void removeSession(SessionLocal session) {
        if (session != null) {
            exclusiveSession.compareAndSet(session, null);
            if (userSessions.remove(session)) {
                trace.info("disconnecting session #{0}", session.getId());
            }
        }
        if (isUserSession(session)) {
            if (userSessions.isEmpty()) {
                if (closeDelay == 0) {
                    close();
                } else if (closeDelay < 0) {
                    return;
                } else {
                    delayedCloser = new DelayedDatabaseCloser(this, closeDelay * 1000);
                }
            }
            if (session != null) {
                trace.info("disconnected session #{0}", session.getId());
            }
        }
    }

    boolean isUserSession(SessionLocal session) {
        return session != systemSession && session != lobSession;
    }

    private synchronized void closeAllSessionsExcept(SessionLocal except) {
        SessionLocal[] all = userSessions.toArray(EMPTY_SESSION_ARRAY);
        boolean done = true;
        for (SessionLocal s : all) {
            if (s != except) {
                // indicate that session need to be closed ASAP
                s.suspend();
                done = false;
            }
        }
        if (done) {
            return;
        }
        int lockTimeout = getLockTimeout();
        // 'sleep' should be strictly greater than zero, otherwise real time is
        // not taken into consideration
        // and the thread simply waits until notified
        long sleepMillis = Math.max(lockTimeout / 10, 1);
        // LOCK_TIMEOUT * 2
        long timeoutNanos = lockTimeout * 2_000_000L;
        long start = System.nanoTime();
        do {
            done = true;
            for (SessionLocal s : all) {
                if (s != except && !s.isClosed()) {
                    done = false;
                    break;
                }
            }
            if (done) {
                return;
            }
            try {
                // although nobody going to notify us
                // it is vital to give up lock on a database
                wait(sleepMillis);
            } catch (InterruptedException e1) {
                // ignore
            }
        } while (System.nanoTime() - start <= timeoutNanos);
        for (SessionLocal s : all) {
            if (s != except && !s.isClosed()) {
                try {
                    // this will rollback outstanding transaction
                    s.close();
                } catch (Throwable e) {
                    trace.error(e, "disconnecting session #{0}", s.getId());
                }
            }
        }
    }

    /**
     * Close the database.
     */
    void close() {
        close(false);
    }

    /**
     * Invoked by shutdown hook.
     */
    void onShutdown() {
        if (closeAtVmShutdown) {
            close(true);
        } else if (persistent) {
            checkpoint();
        }
    }

    /**
     * Close the database.
     *
     * @param fromShutdownHook true if this method is called from the shutdown
     *            hook
     */
    private void close(boolean fromShutdownHook) {
        DbException b = backgroundException.getAndSet(null);
        try {
            closeImpl(fromShutdownHook);
        } catch (Throwable t) {
            if (b != null) {
                t.addSuppressed(b);
            }
            throw t;
        }
        if (b != null) {
            // wrap the exception, so we see it was thrown here
            throw DbException.get(b.getErrorCode(), b, b.getMessage());
        }
    }

    private void closeImpl(boolean fromShutdownHook) {
        synchronized (this) {
            if (closing || !fromShutdownHook && !userSessions.isEmpty()) {
                return;
            }
            closing = true;
            stopServer();
            if (!userSessions.isEmpty()) {
                assert fromShutdownHook;
                trace.info("closing {0} from shutdown hook", databaseName);
                closeAllSessionsExcept(null);
            }
            trace.info("closing {0}", databaseName);
            if (eventListener != null) {
                // allow the event listener to connect to the database
                closing = false;
                DatabaseEventListener e = eventListener;
                // set it to null, to make sure it's called only once
                eventListener = null;
                e.closingDatabase();
                closing = true;
                if (!userSessions.isEmpty()) {
                    trace.info("event listener {0} left connection open", e.getClass().getName());
                    // if listener left an open connection
                    closeAllSessionsExcept(null);
                }
            }
        }
        try {
            try {
                if (systemSession != null) {
                    if (powerOffCount != -1) {
                        for (Schema schema : schemas.values()) {
                            for (Table table : schema.getAllTablesAndViews(null)) {
                                if (table.isGlobalTemporary()) {
                                    removeSchemaObject(systemSession, table);
                                } else {
                                    table.close(systemSession);
                                }
                            }
                        }
                        for (Schema schema : schemas.values()) {
                            for (Sequence sequence : schema.getAllSequences()) {
                                sequence.close();
                            }
                        }
                    }
                    for (Schema schema : schemas.values()) {
                        for (TriggerObject trigger : schema.getAllTriggers()) {
                            try {
                                trigger.close();
                            } catch (SQLException e) {
                                trace.error(e, "close");
                            }
                        }
                    }
                    if (powerOffCount != -1) {
                        meta.close(systemSession);
                        systemSession.commit(true);
                    }
                    if (lobSession != null) {
                        lobSession.close();
                        lobSession = null;
                    }
                    systemSession.close();
                    systemSession = null;
                }
                tempFileDeleter.deleteAll();
                closeOpenFilesAndUnlock();
            } catch (DbException | MVStoreException e) {
                trace.error(e, "close");
            }
            trace.info("closed");
            traceSystem.close();
            OnExitDatabaseCloser.unregister(this);
            if (deleteFilesOnDisconnect && persistent) {
                deleteFilesOnDisconnect = false;
                try {
                    String directory = FileUtils.getParent(databaseName);
                    String name = FileUtils.getName(databaseName);
                    DeleteDbFiles.execute(directory, name, true);
                } catch (Exception e) {
                    // ignore (the trace is closed already)
                }
            }
        } finally {
            Engine.close(databaseName);
        }
    }

    /**
     * Close all open files and unlock the database.
     */
    private synchronized void closeOpenFilesAndUnlock() {
        try {
            if (lobStorage != null) {
                lobStorage.close();
            }
            if (store != null && !store.getMvStore().isClosed()) {
                if (compactMode == CommandInterface.SHUTDOWN_IMMEDIATELY) {
                    store.closeImmediately();
                } else {
                    int allowedCompactionTime =
                            compactMode == CommandInterface.SHUTDOWN_COMPACT ||
                            compactMode == CommandInterface.SHUTDOWN_DEFRAG ||
                            dbSettings.defragAlways ? -1 : dbSettings.maxCompactTime;
                    store.close(allowedCompactionTime);
                }
                if (persistent) {
                    // Don't delete temp files if everything is already closed
                    // (maybe in checkPowerOff), the database could be open now
                    // (even from within another process).
                    if (lock != null || fileLockMethod == FileLockMethod.NO || fileLockMethod == FileLockMethod.FS) {
                        deleteOldTempFiles();
                    }
                }
            }
        } finally {
            if (lock != null) {
                lock.unlock();
                lock = null;
            }
        }
    }

    private synchronized void closeFiles() {
        try {
            store.closeImmediately();
        } catch (DbException e) {
            trace.error(e, "close");
        }
    }

    private void checkMetaFree(SessionLocal session, int id) {
        SearchRow r = meta.getRowFactory().createRow();
        r.setValue(0, ValueInteger.get(id));
        Cursor cursor = metaIdIndex.find(session, r, r, false);
        if (cursor.next()) {
            throw DbException.getInternalError();
        }
    }

    /**
     * Allocate a new object id.
     *
     * @return the id
     */
    public int allocateObjectId() {
        int i;
        synchronized (objectIds) {
            i = objectIds.nextClearBit(0);
            objectIds.set(i);
        }
        return i;
    }

    /**
     * Returns system user.
     *
     * @return system user
     */
    public User getSystemUser() {
        return systemUser;
    }

    /**
     * Returns main schema (usually PUBLIC).
     *
     * @return main schema (usually PUBLIC)
     */
    public Schema getMainSchema() {
        return mainSchema;
    }

    public ArrayList<Comment> getAllComments() {
        return new ArrayList<>(comments.values());
    }

    public int getAllowLiterals() {
        if (starting) {
            return Constants.ALLOW_LITERALS_ALL;
        }
        return allowLiterals;
    }

    public ArrayList<Right> getAllRights() {
        return new ArrayList<>(rights.values());
    }

    /**
     * Get all tables and views. Meta data tables may be excluded.
     *
     * @return all objects of that type
     */
    public ArrayList<Table> getAllTablesAndViews() {
        ArrayList<Table> list = new ArrayList<>();
        for (Schema schema : schemas.values()) {
            list.addAll(schema.getAllTablesAndViews(null));
        }
        return list;
    }

    /**
     * Get all synonyms.
     *
     * @return all objects of that type
     */
    public ArrayList<TableSynonym> getAllSynonyms() {
        ArrayList<TableSynonym> list = new ArrayList<>();
        for (Schema schema : schemas.values()) {
            list.addAll(schema.getAllSynonyms());
        }
        return list;
    }

    public Collection<Schema> getAllSchemas() {
        return schemas.values();
    }

    public Collection<Schema> getAllSchemasNoMeta() {
        return schemas.values();
    }

    public Collection<Setting> getAllSettings() {
        return settings.values();
    }

    public Collection<RightOwner> getAllUsersAndRoles() {
        return usersAndRoles.values();
    }

    public String getCacheType() {
        return cacheType;
    }

    public String getCluster() {
        return cluster;
    }

    @Override
    public CompareMode getCompareMode() {
        return compareMode;
    }

    @Override
    public String getDatabasePath() {
        if (persistent) {
            return FileUtils.toRealPath(databaseName);
        }
        return null;
    }

    public String getShortName() {
        return databaseShortName;
    }

    public String getName() {
        return databaseName;
    }

    /**
     * Get all sessions that are currently connected to the database.
     *
     * @param includingSystemSession if the system session should also be
     *            included
     * @return the list of sessions
     */
    public SessionLocal[] getSessions(boolean includingSystemSession) {
        ArrayList<SessionLocal> list;
        // need to synchronized on this database,
        // otherwise the list may contain null elements
        synchronized (this) {
            list = new ArrayList<>(userSessions);
        }
        if (includingSystemSession) {
            // copy, to ensure the reference is stable
            SessionLocal s = systemSession;
            if (s != null) {
                list.add(s);
            }
            s = lobSession;
            if (s != null) {
                list.add(s);
            }
        }
        return list.toArray(new SessionLocal[0]);
    }

    /**
     * Update an object in the system table.
     *
     * @param session the session
     * @param obj the database object
     */
    public void updateMeta(SessionLocal session, DbObject obj) {
        int id = obj.getId();
        if (id > 0) {
            if (!starting && !obj.isTemporary()) {
                Row newRow = meta.getTemplateRow();
                MetaRecord.populateRowFromDBObject(obj, newRow);
                Row oldRow = metaIdIndex.getRow(session, id);
                if (oldRow != null) {
                    meta.updateRow(session, oldRow, newRow);
                }
            }
            // for temporary objects
            synchronized (objectIds) {
                objectIds.set(id);
            }
        }
    }

    /**
     * Rename a schema object.
     *
     * @param session the session
     * @param obj the object
     * @param newName the new name
     */
    public synchronized void renameSchemaObject(SessionLocal session,
            SchemaObject obj, String newName) {
        checkWritingAllowed();
        obj.getSchema().rename(obj, newName);
        updateMetaAndFirstLevelChildren(session, obj);
    }

    private synchronized void updateMetaAndFirstLevelChildren(SessionLocal session, DbObject obj) {
        ArrayList<DbObject> list = obj.getChildren();
        Comment comment = findComment(obj);
        if (comment != null) {
            throw DbException.getInternalError(comment.toString());
        }
        updateMeta(session, obj);
        // remember that this scans only one level deep!
        if (list != null) {
            for (DbObject o : list) {
                if (o.getCreateSQL() != null) {
                    updateMeta(session, o);
                }
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
    public synchronized void renameDatabaseObject(SessionLocal session,
            DbObject obj, String newName) {
        checkWritingAllowed();
        int type = obj.getType();
        ConcurrentHashMap<String, DbObject> map = getMap(type);
        if (SysProperties.CHECK) {
            if (!map.containsKey(obj.getName())) {
                throw DbException.getInternalError("not found: " + obj.getName());
            }
            if (obj.getName().equals(newName) || map.containsKey(newName)) {
                throw DbException.getInternalError("object already exists: " + newName);
            }
        }
        obj.checkRename();
        map.remove(obj.getName());
        obj.rename(newName);
        map.put(newName, obj);
        updateMetaAndFirstLevelChildren(session, obj);
    }

    private void deleteOldTempFiles() {
        String path = FileUtils.getParent(databaseName);
        for (String name : FileUtils.newDirectoryStream(path)) {
            if (name.endsWith(Constants.SUFFIX_TEMP_FILE) &&
                    name.startsWith(databaseName)) {
                // can't always delete the files, they may still be open
                FileUtils.tryDelete(name);
            }
        }
    }

    /**
     * Get the schema. If the schema does not exist, an exception is thrown.
     *
     * @param schemaName the name of the schema
     * @return the schema
     * @throws DbException no schema with that name exists
     */
    public Schema getSchema(String schemaName) {
        Schema schema = findSchema(schemaName);
        if (schema == null) {
            throw DbException.get(ErrorCode.SCHEMA_NOT_FOUND_1, schemaName);
        }
        return schema;
    }

    /**
     * Remove the object from the database.
     *
     * @param session the session
     * @param obj the object to remove
     */
    public synchronized void removeDatabaseObject(SessionLocal session, DbObject obj) {
        checkWritingAllowed();
        String objName = obj.getName();
        int type = obj.getType();
        ConcurrentHashMap<String, DbObject> map = getMap(type);
        if (SysProperties.CHECK && !map.containsKey(objName)) {
            throw DbException.getInternalError("not found: " + objName);
        }
        Comment comment = findComment(obj);
        lockMeta(session);
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
        HashSet<DbObject> set = new HashSet<>();
        for (Schema schema : schemas.values()) {
            for (Table t : schema.getAllTablesAndViews(null)) {
                if (except == t || TableType.VIEW == t.getTableType()) {
                    continue;
                }
                set.clear();
                t.addDependencies(set);
                if (set.contains(obj)) {
                    return t;
                }
            }
        }
        return null;
    }

    /**
     * Remove an object from the system table.
     *
     * @param session the session
     * @param obj the object to be removed
     */
    public void removeSchemaObject(SessionLocal session,
            SchemaObject obj) {
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
            if (constraint.getConstraintType() != Type.DOMAIN) {
                Table table = constraint.getTable();
                if (table.isTemporary() && !table.isGlobalTemporary()) {
                    session.removeLocalTempTableConstraint(constraint);
                    return;
                }
            }
        }
        checkWritingAllowed();
        lockMeta(session);
        synchronized (this) {
            Comment comment = findComment(obj);
            if (comment != null) {
                removeDatabaseObject(session, comment);
            }
            obj.getSchema().remove(obj);
            int id = obj.getId();
            if (!starting) {
                Table t = getDependentTable(obj, null);
                if (t != null) {
                    obj.getSchema().add(obj);
                    throw DbException.get(ErrorCode.CANNOT_DROP_2, obj.getTraceSQL(), t.getTraceSQL());
                }
                obj.removeChildrenAndResources(session);
            }
            removeMeta(session, id);
        }
    }

    /**
     * Check if this database is disk-based.
     *
     * @return true if it is disk-based, false if it is in-memory only.
     */
    public boolean isPersistent() {
        return persistent;
    }

    public TraceSystem getTraceSystem() {
        return traceSystem;
    }

    public synchronized void setCacheSize(int kb) {
        if (starting) {
            int max = MathUtils.convertLongToInt(Utils.getMemoryMax()) / 2;
            kb = Math.min(kb, max);
        }
        store.setCacheSize(Math.max(1, kb));
    }

    public synchronized void setMasterUser(User user) {
        lockMeta(systemSession);
        addDatabaseObject(systemSession, user);
        systemSession.commit(true);
    }

    public Role getPublicRole() {
        return publicRole;
    }

    /**
     * Get a unique temporary table name.
     *
     * @param baseName the prefix of the returned name
     * @param session the session
     * @return a unique name
     */
    public String getTempTableName(String baseName, SessionLocal session) {
        int maxBaseLength = Constants.MAX_IDENTIFIER_LENGTH - (7 + ValueInteger.DISPLAY_SIZE * 2);
        if (baseName.length() > maxBaseLength) {
            baseName = baseName.substring(0, maxBaseLength);
        }
        String tempName;
        do {
            tempName = baseName + "_COPY_" + session.getId() + '_' + nextTempTableId.getAndIncrement();
        } while (mainSchema.findTableOrView(session, tempName) != null);
        return tempName;
    }

    public void setCompareMode(CompareMode compareMode) {
        this.compareMode = compareMode;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    @Override
    public void checkWritingAllowed() {
        if (readOnly) {
            throw DbException.get(ErrorCode.DATABASE_IS_READ_ONLY);
        }
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public int getWriteDelay() {
        return store.getMvStore().getAutoCommitDelay();
    }

    public void setWriteDelay(int value) {
        store.getMvStore().setAutoCommitDelay(value < 0 ? 0 : value);
    }

    public int getRetentionTime() {
        return store.getMvStore().getRetentionTime();
    }

    public void setRetentionTime(int value) {
        store.getMvStore().setRetentionTime(value);
    }

    public void setAllowBuiltinAliasOverride(boolean b) {
        allowBuiltinAliasOverride = b;
    }

    public boolean isAllowBuiltinAliasOverride() {
        return allowBuiltinAliasOverride;
    }

    /**
     * Get the list of in-doubt transactions.
     *
     * @return the list
     */
    public ArrayList<InDoubtTransaction> getInDoubtTransactions() {
        return store.getInDoubtTransactions();
    }

    /**
     * Prepare a transaction.
     *
     * @param session the session
     * @param transaction the name of the transaction
     */
    synchronized void prepareCommit(SessionLocal session, String transaction) {
        if (!readOnly) {
            store.prepareCommit(session, transaction);
        }
    }

    /**
     * If there is a background store thread, and if there wasn an exception in
     * that thread, throw it now.
     */
    void throwLastBackgroundException() {
        DbException b = backgroundException.getAndSet(null);
        if (b != null) {
            // wrap the exception, so we see it was thrown here
            throw DbException.get(b.getErrorCode(), b, b.getMessage());
        }
    }

    public void setBackgroundException(DbException e) {
        if (backgroundException.compareAndSet(null, e)) {
            TraceSystem t = getTraceSystem();
            if (t != null) {
                t.getTrace(Trace.DATABASE).error(e, "flush");
            }
        }
    }

    public Throwable getBackgroundException() {
        MVStoreException exception = store.getMvStore().getPanicException();
        if(exception != null) {
            return exception;
        }
        return backgroundException.getAndSet(null);
    }


    /**
     * Flush all pending changes to the transaction log.
     */
    public synchronized void flush() {
        if (!readOnly) {
            try {
                store.flush();
            } catch (RuntimeException e) {
                backgroundException.compareAndSet(null, DbException.convert(e));
                throw e;
            }
        }
    }

    public void setEventListener(DatabaseEventListener eventListener) {
        this.eventListener = eventListener;
    }

    public void setEventListenerClass(String className) {
        if (className == null || className.isEmpty()) {
            eventListener = null;
        } else {
            try {
                eventListener = (DatabaseEventListener)
                        JdbcUtils.loadUserClass(className).getDeclaredConstructor().newInstance();
                String url = databaseURL;
                if (cipher != null) {
                    url += ";CIPHER=" + cipher;
                }
                eventListener.init(url);
            } catch (Throwable e) {
                throw DbException.get(
                        ErrorCode.ERROR_SETTING_DATABASE_EVENT_LISTENER_2, e,
                        className, e.toString());
            }
        }
    }

    /**
     * Set the progress of a long running operation.
     * This method calls the {@link DatabaseEventListener} if one is registered.
     *
     * @param state the {@link DatabaseEventListener} state
     * @param name the object name
     * @param x the current position
     * @param max the highest value or 0 if unknown
     */
    public void setProgress(int state, String name, long x, long max) {
        if (eventListener != null) {
            try {
                eventListener.setProgress(state, name, x, max);
            } catch (Exception e2) {
                // ignore this (user made) exception
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
                // ignore this (user made) exception
            }
        }
    }

    /**
     * Synchronize the files with the file system. This method is called when
     * executing the SQL statement CHECKPOINT SYNC.
     */
    public synchronized void sync() {
        if (readOnly) {
            return;
        }
        store.sync();
    }

    public int getMaxMemoryRows() {
        return maxMemoryRows;
    }

    public void setMaxMemoryRows(int value) {
        this.maxMemoryRows = value;
    }

    public void setLockMode(int lockMode) {
        switch (lockMode) {
        case Constants.LOCK_MODE_OFF:
        case Constants.LOCK_MODE_READ_COMMITTED:
            break;
        case Constants.LOCK_MODE_TABLE:
        case Constants.LOCK_MODE_TABLE_GC:
            lockMode = Constants.LOCK_MODE_READ_COMMITTED;
            break;
        default:
            throw DbException.getInvalidValueException("lock mode", lockMode);
        }
        this.lockMode = lockMode;
    }

    public int getLockMode() {
        return lockMode;
    }

    public void setCloseDelay(int value) {
        this.closeDelay = value;
    }

    public SessionLocal getSystemSession() {
        return systemSession;
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

    @Override
    public int getMaxLengthInplaceLob() {
        return maxLengthInplaceLob;
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

    public void setIgnoreCatalogs(boolean b) {
        ignoreCatalogs = b;
    }

    public boolean getIgnoreCatalogs() {
        return ignoreCatalogs;
    }


    public synchronized void setDeleteFilesOnDisconnect(boolean b) {
        this.deleteFilesOnDisconnect = b;
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

    @Override
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

    public void setQueryStatistics(boolean b) {
        queryStatistics = b;
        synchronized (this) {
            if (!b) {
                queryStatisticsData = null;
            }
        }
    }

    public boolean getQueryStatistics() {
        return queryStatistics;
    }

    public void setQueryStatisticsMaxEntries(int n) {
        queryStatisticsMaxEntries = n;
        if (queryStatisticsData != null) {
            synchronized (this) {
                if (queryStatisticsData != null) {
                    queryStatisticsData.setMaxQueryEntries(queryStatisticsMaxEntries);
                }
            }
        }
    }

    public QueryStatisticsData getQueryStatisticsData() {
        if (!queryStatistics) {
            return null;
        }
        if (queryStatisticsData == null) {
            synchronized (this) {
                if (queryStatisticsData == null) {
                    queryStatisticsData = new QueryStatisticsData(queryStatisticsMaxEntries);
                }
            }
        }
        return queryStatisticsData;
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
     * Called after the database has been opened and initialized. This method
     * notifies the event listener if one has been set.
     */
    void opened() {
        if (eventListener != null) {
            eventListener.opened();
        }
    }

    public void setMode(Mode mode) {
        this.mode = mode;
        getNextRemoteSettingsId();
    }

    @Override
    public Mode getMode() {
        return mode;
    }

    public void setDefaultNullOrdering(DefaultNullOrdering defaultNullOrdering) {
        this.defaultNullOrdering = defaultNullOrdering;
    }

    public DefaultNullOrdering getDefaultNullOrdering() {
        return defaultNullOrdering;
    }

    public void setMaxOperationMemory(int maxOperationMemory) {
        this.maxOperationMemory  = maxOperationMemory;
    }

    public int getMaxOperationMemory() {
        return maxOperationMemory;
    }

    public SessionLocal getExclusiveSession() {
        return exclusiveSession.get();
    }

    /**
     * Set the session that can exclusively access the database.
     *
     * @param session the session
     * @param closeOthers whether other sessions are closed
     * @return true if success or if database is in exclusive mode
     *         set by this session already, false otherwise
     */
    public boolean setExclusiveSession(SessionLocal session, boolean closeOthers) {
        if (exclusiveSession.get() != session &&
                !exclusiveSession.compareAndSet(null, session)) {
            return false;
        }
        if (closeOthers) {
            closeAllSessionsExcept(session);
        }
        return true;
    }

    /**
     * Stop exclusive access the database by provided session.
     *
     * @param session the session
     * @return true if success or if database is in non-exclusive mode already,
     *         false otherwise
     */
    public boolean unsetExclusiveSession(SessionLocal session) {
        return exclusiveSession.get() == null
            || exclusiveSession.compareAndSet(session, null);
    }

    @Override
    public SmallLRUCache<String, String[]> getLobFileListCache() {
        if (lobFileListCache == null) {
            lobFileListCache = SmallLRUCache.newInstance(128);
        }
        return lobFileListCache;
    }

    /**
     * Checks if the system table (containing the catalog) is locked.
     *
     * @return true if it is currently locked
     */
    public boolean isSysTableLocked() {
        return meta == null || meta.isLockedExclusively();
    }

    /**
     * Checks if the system table (containing the catalog) is locked by the
     * given session.
     *
     * @param session the session
     * @return true if it is currently locked
     */
    public boolean isSysTableLockedBy(SessionLocal session) {
        return meta == null || meta.isLockedExclusivelyBy(session);
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
    public TableLinkConnection getLinkConnection(String driver, String url,
            String user, String password) {
        if (linkConnections == null) {
            linkConnections = new HashMap<>();
        }
        return TableLinkConnection.open(linkConnections, driver, url, user,
                password, dbSettings.shareLinkedConnections);
    }

    @Override
    public String toString() {
        return databaseShortName + ":" + super.toString();
    }

    /**
     * Immediately close the database.
     */
    public void shutdownImmediately() {
        closing = true;
        setPowerOffCount(1);
        try {
            checkPowerOff();
        } catch (DbException e) {
            // ignore
        }
        closeFiles();
        powerOffCount = 0;
    }

    @Override
    public TempFileDeleter getTempFileDeleter() {
        return tempFileDeleter;
    }

    /**
     * Get the first user defined table, excluding the LOB_BLOCKS table that the
     * Recover tool creates.
     *
     * @return the table or null if no table is defined
     */
    public Table getFirstUserTable() {
        for (Schema schema : schemas.values()) {
            for (Table table : schema.getAllTablesAndViews(null)) {
                if (table.getCreateSQL() == null) {
                    continue;
                }
                // exclude the LOB_MAP that the Recover tool creates
                if (schema.getId() == Constants.INFORMATION_SCHEMA_ID
                        && table.getName().equalsIgnoreCase("LOB_BLOCKS")) {
                    continue;
                }
                return table;
            }
        }
        return null;
    }

    /**
     * Flush all changes and open a new transaction log.
     */
    public void checkpoint() {
        if (persistent) {
            store.flush();
        }
        getTempFileDeleter().deleteUnused();
    }

    /**
     * Switch the database to read-only mode.
     *
     * @param readOnly the new value
     */
    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    public void setCompactMode(int compactMode) {
        this.compactMode = compactMode;
    }

    public SourceCompiler getCompiler() {
        if (compiler == null) {
            compiler = new SourceCompiler();
        }
        return compiler;
    }

    @Override
    public LobStorageInterface getLobStorage() {
        return lobStorage;
    }

    public SessionLocal getLobSession() {
        return lobSession;
    }

    public int getDefaultTableType() {
        return defaultTableType;
    }

    public void setDefaultTableType(int defaultTableType) {
        this.defaultTableType = defaultTableType;
    }

    public DbSettings getSettings() {
        return dbSettings;
    }

    /**
     * Create a new hash map. Depending on the configuration, the key is case
     * sensitive or case insensitive.
     *
     * @param <V> the value type
     * @return the hash map
     */
    public <V> HashMap<String, V> newStringMap() {
        return dbSettings.caseInsensitiveIdentifiers ? new CaseInsensitiveMap<>() : new HashMap<>();
    }

    /**
     * Create a new hash map. Depending on the configuration, the key is case
     * sensitive or case insensitive.
     *
     * @param <V> the value type
     * @param  initialCapacity the initial capacity
     * @return the hash map
     */
    public <V> HashMap<String, V> newStringMap(int initialCapacity) {
        return dbSettings.caseInsensitiveIdentifiers ? new CaseInsensitiveMap<>(initialCapacity)
                : new HashMap<>(initialCapacity);
    }

    /**
     * Create a new hash map. Depending on the configuration, the key is case
     * sensitive or case insensitive.
     *
     * @param <V> the value type
     * @return the hash map
     */
    public <V> ConcurrentHashMap<String, V> newConcurrentStringMap() {
        return dbSettings.caseInsensitiveIdentifiers ? new CaseInsensitiveConcurrentMap<>()
                : new ConcurrentHashMap<>();
    }

    /**
     * Compare two identifiers (table names, column names,...) and verify they
     * are equal. Case sensitivity depends on the configuration.
     *
     * @param a the first identifier
     * @param b the second identifier
     * @return true if they match
     */
    public boolean equalsIdentifiers(String a, String b) {
        return a.equals(b) || dbSettings.caseInsensitiveIdentifiers && a.equalsIgnoreCase(b);
    }

    /**
     * Returns identifier in upper or lower case depending on database settings.
     *
     * @param upperName
     *            identifier in the upper case
     * @return identifier in upper or lower case
     */
    public String sysIdentifier(String upperName) {
        assert isUpperSysIdentifier(upperName);
        return dbSettings.databaseToLower ? StringUtils.toLowerEnglish(upperName) : upperName;
    }

    private static boolean isUpperSysIdentifier(String upperName) {
        int l = upperName.length();
        if (l == 0) {
            return false;
        }
        char c = upperName.charAt(0);
        if (c < 'A' || c > 'Z') {
            return false;
        }
        l--;
        for (int i = 1; i < l; i++) {
            c = upperName.charAt(i);
            if ((c < 'A' || c > 'Z') && c != '_') {
                return false;
            }
        }
        if (l > 0) {
            c = upperName.charAt(l);
            if (c < 'A' || c > 'Z') {
                return false;
            }
        }
        return true;
    }

    @Override
    public int readLob(long lobId, byte[] hmac, long offset, byte[] buff, int off, int length) {
        throw DbException.getInternalError();
    }

    public int getPageSize() {
        return pageSize;
    }

    @Override
    public JavaObjectSerializer getJavaObjectSerializer() {
        initJavaObjectSerializer();
        return javaObjectSerializer;
    }

    private void initJavaObjectSerializer() {
        if (javaObjectSerializerInitialized) {
            return;
        }
        synchronized (this) {
            if (javaObjectSerializerInitialized) {
                return;
            }
            String serializerName = javaObjectSerializerName;
            if (serializerName != null) {
                serializerName = serializerName.trim();
                if (!serializerName.isEmpty() &&
                        !serializerName.equals("null")) {
                    try {
                        javaObjectSerializer = (JavaObjectSerializer)
                                JdbcUtils.loadUserClass(serializerName).getDeclaredConstructor().newInstance();
                    } catch (Exception e) {
                        throw DbException.convert(e);
                    }
                }
            }
            javaObjectSerializerInitialized = true;
        }
    }

    public void setJavaObjectSerializerName(String serializerName) {
        synchronized (this) {
            javaObjectSerializerInitialized = false;
            javaObjectSerializerName = serializerName;
            getNextRemoteSettingsId();
        }
    }

    /**
     * Get the table engine class, loading it if needed.
     *
     * @param tableEngine the table engine name
     * @return the class
     */
    public TableEngine getTableEngine(String tableEngine) {
        assert Thread.holdsLock(this);

        TableEngine engine = tableEngines.get(tableEngine);
        if (engine == null) {
            try {
                engine = (TableEngine) JdbcUtils.loadUserClass(tableEngine).getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                throw DbException.convert(e);
            }
            tableEngines.put(tableEngine, engine);
        }
        return engine;
    }

    /**
     * get authenticator for database users
     *
     * @return authenticator set for database
     */
    public Authenticator getAuthenticator() {
        return authenticator;
    }

    /**
     * Set current database authenticator
     *
     * @param authenticator
     *            = authenticator to set, null to revert to the Internal
     *            authenticator
     */
    public void setAuthenticator(Authenticator authenticator) {
        if (authenticator != null) {
            authenticator.init(this);
        }
        this.authenticator = authenticator;
    }

    @Override
    public ValueTimestampTimeZone currentTimestamp() {
        Session session = SessionLocal.getThreadLocalSession();
        if (session != null) {
            return session.currentTimestamp();
        }
        throw DbException.getUnsupportedException("Unsafe comparison or cast");
    }

    @Override
    public TimeZoneProvider currentTimeZone() {
        Session session = SessionLocal.getThreadLocalSession();
        if (session != null) {
            return session.currentTimeZone();
        }
        throw DbException.getUnsupportedException("Unsafe comparison or cast");
    }

    @Override
    public boolean zeroBasedEnums() {
        return dbSettings.zeroBasedEnums;
    }

}
