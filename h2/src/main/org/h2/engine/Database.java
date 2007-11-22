/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.h2.api.DatabaseEventListener;
import org.h2.command.dml.SetTypes;
import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.jdbc.JdbcSQLException;
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
import org.h2.store.DataHandler;
import org.h2.store.DataPage;
import org.h2.store.DiskFile;
import org.h2.store.FileLock;
import org.h2.store.FileStore;
import org.h2.store.RecordReader;
import org.h2.store.Storage;
import org.h2.store.WriterThread;
import org.h2.store.fs.FileSystem;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.MetaTable;
import org.h2.table.Table;
import org.h2.table.TableData;
import org.h2.table.TableView;
import org.h2.tools.DeleteDbFiles;
import org.h2.util.BitField;
import org.h2.util.ByteUtils;
import org.h2.util.CacheLRU;
import org.h2.util.ClassUtils;
import org.h2.util.FileUtils;
import org.h2.util.IOUtils;
import org.h2.util.IntHashMap;
import org.h2.util.ObjectArray;
import org.h2.util.StringUtils;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueInt;

/**
 * There is one database object per open database.
 * 
 * @author Thomas
 * @since 2004-04-15 22:49
 */
/*
 * MetaData format: int id int headPos (for indexes) int objectType String sql
 */
public class Database implements DataHandler {

    private final boolean persistent;
    private final String databaseName;
    private final String databaseShortName;
    private final String databaseURL;
    private final String cipher;
    private final byte[] filePasswordHash;

    private final HashMap roles = new HashMap();
    private final HashMap users = new HashMap();
    private final HashMap settings = new HashMap();
    private final HashMap schemas = new HashMap();
    private final HashMap rights = new HashMap();
    private final HashMap functionAliases = new HashMap();
    private final HashMap userDataTypes = new HashMap();
    private final HashMap aggregates = new HashMap();
    private final HashMap comments = new HashMap();
    private final HashSet sessions = new HashSet();
    private final BitField objectIds = new BitField();
    private final Object lobSyncObject = new Object();

    private boolean textStorage;
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
    private IntHashMap storageMap = new IntHashMap();
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
    private FileStore emergencyReserve;
    private int maxMemoryRows = Constants.DEFAULT_MAX_MEMORY_ROWS;
    private int maxMemoryUndo = SysProperties.DEFAULT_MAX_MEMORY_UNDO;
    private int lockMode = SysProperties.DEFAULT_LOCK_MODE;
    private boolean logIndexChanges;
    private int logLevel = 1;
    private int cacheSize;
    private int maxLengthInplaceLob = Constants.DEFAULT_MAX_LENGTH_INPLACE_LOB;
    private long biggestFileSize;
    private int allowLiterals = Constants.DEFAULT_ALLOW_LITERALS;

    private static int initialPowerOffCount;
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
    private int maxOperationMemory = SysProperties.DEFAULT_MAX_OPERERATION_MEMORY;

    public Database(String name, ConnectionInfo ci, String cipher) throws SQLException {
        this.compareMode = new CompareMode(null, null);
        this.persistent = ci.isPersistent();
        this.filePasswordHash = ci.getFilePasswordHash();
        this.databaseName = name;
        this.databaseShortName = parseDatabaseShortName();
        this.cipher = cipher;
        String lockMethodName = ci.removeProperty("FILE_LOCK", null);
        this.accessModeLog = ci.removeProperty("ACCESS_MODE_LOG", "rw").toLowerCase();
        this.accessModeData = ci.removeProperty("ACCESS_MODE_DATA", "rw").toLowerCase();
        this.fileLockMethod = FileLock.getFileLockMethod(lockMethodName);
        this.textStorage = ci.getTextStorage();
        this.databaseURL = ci.getURL();
        this.eventListener = ci.removeDatabaseEventListenerObject();
        if (eventListener == null) {
            String listener = ci.removeProperty("DATABASE_EVENT_LISTENER", null);
            if (listener != null) {
                if (listener.startsWith("'")) {
                    listener = listener.substring(1);
                }
                if (listener.endsWith("'")) {
                    listener = listener.substring(0, listener.length() - 1);
                }
                setEventListener(listener);
            }
        }
        String log = ci.getProperty(SetTypes.LOG, null);
        if (log != null) {
            this.logIndexChanges = "2".equals(log);
        }
        String ignoreSummary = ci.getProperty("RECOVER", null);
        if (ignoreSummary != null) {
            this.recovery = true;
        }
        this.multiVersion = ci.removeProperty("MVCC", false);
        boolean closeAtVmShutdown = ci.removeProperty("DB_CLOSE_ON_EXIT", true);
        int traceLevelFile = ci.getIntProperty(SetTypes.TRACE_LEVEL_FILE, TraceSystem.DEFAULT_TRACE_LEVEL_FILE);
        int traceLevelSystemOut = ci.getIntProperty(SetTypes.TRACE_LEVEL_SYSTEM_OUT,
                TraceSystem.DEFAULT_TRACE_LEVEL_SYSTEM_OUT);
        this.cacheType = StringUtils.toUpperEnglish(ci.removeProperty("CACHE_TYPE", CacheLRU.TYPE_NAME));
        try {
            open(traceLevelFile, traceLevelSystemOut);
            if (closeAtVmShutdown) {
                closeOnExit = new DatabaseCloser(this, 0, true);
                try {
                    Runtime.getRuntime().addShutdownHook(closeOnExit);
                } catch (IllegalStateException e) {
                    // shutdown in progress - just don't register the handler
                    // (maybe an application wants to write something into a
                    // database at shutdown time)
                }
            }
        } catch (Throwable e) {
            if (traceSystem != null) {
                traceSystem.getTrace(Trace.DATABASE).error("opening " + databaseName, e);
                traceSystem.close();
            }
            closeOpenFilesAndUnlock();
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

    public boolean getTextStorage() {
        return textStorage;
    }

    public static boolean isTextStorage(String fileName, boolean defaultValue) throws SQLException {
        byte[] magicText = Constants.MAGIC_FILE_HEADER_TEXT.getBytes();
        byte[] magicBinary = Constants.MAGIC_FILE_HEADER.getBytes();
        try {
            InputStream fin = FileUtils.openFileInputStream(fileName);
            byte[] magic = IOUtils.readBytesAndClose(fin, magicBinary.length);
            if (ByteUtils.compareNotNull(magic, magicText) == 0) {
                return true;
            } else if (ByteUtils.compareNotNull(magic, magicBinary) == 0) {
                return false;
            } else if (magic.length < magicText.length) {
                // file size is 0 or too small
                return defaultValue;
            }
            throw Message.getSQLException(ErrorCode.FILE_VERSION_ERROR_1, fileName);
        } catch (IOException e) {
            throw Message.convertIOException(e, fileName);
        }
    }

    public static byte[] getMagic(boolean textStorage) {
        if (textStorage) {
            return Constants.MAGIC_FILE_HEADER_TEXT.getBytes();
        } else {
            return Constants.MAGIC_FILE_HEADER.getBytes();
        }
    }

    public byte[] getMagic() {
        return getMagic(textStorage);
    }

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

    public int compare(Value a, Value b) throws SQLException {
        return a.compareTo(b, compareMode);
    }

    public int compareTypeSave(Value a, Value b) throws SQLException {
        return a.compareTypeSave(b, compareMode);
    }

    public long getModificationDataId() {
        return modificationDataId;
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
                        log.close();
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
                if (lock != null) {
                    lock.unlock();
                    lock = null;
                }
                if (emergencyReserve != null) {
                    emergencyReserve.closeAndDeleteSilently();
                    emergencyReserve = null;
                }
            } catch (Exception e) {
                TraceSystem.traceThrowable(e);
            }
        }
        Engine.getInstance().close(databaseName);
        throw Message.getSQLException(ErrorCode.SIMULATED_POWER_OFF);
    }

    public static boolean exists(String name) {
        return FileUtils.exists(name + Constants.SUFFIX_DATA_FILE);
    }

    public Trace getTrace(String module) {
        return traceSystem.getTrace(module);
    }

    public FileStore openFile(String name, String mode, boolean mustExist) throws SQLException {
        if (mustExist && !FileUtils.exists(name)) {
            throw Message.getSQLException(ErrorCode.FILE_NOT_FOUND_1, name);
        }
        FileStore store = FileStore.open(this, name, mode, getMagic(), cipher, filePasswordHash);
        try {
            store.init();
        } catch (SQLException e) {
            store.closeSilently();
            throw e;
        }
        return store;
    }

    public void checkFilePasswordHash(String c, byte[] hash) throws JdbcSQLException {
        if (!ByteUtils.compareSecure(hash, filePasswordHash) || !StringUtils.equals(c, cipher)) {
            throw Message.getSQLException(ErrorCode.WRONG_USER_OR_PASSWORD);
        }
    }

    private void openFileData() throws SQLException {
        fileData = new DiskFile(this, databaseName + Constants.SUFFIX_DATA_FILE, accessModeData, true, true,
                SysProperties.CACHE_SIZE_DEFAULT);
    }

    private void openFileIndex() throws SQLException {
        fileIndex = new DiskFile(this, databaseName + Constants.SUFFIX_INDEX_FILE, accessModeData, false,
                logIndexChanges, Constants.CACHE_SIZE_INDEX_DEFAULT);
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
            String dataFileName = databaseName + Constants.SUFFIX_DATA_FILE;
            if (FileUtils.exists(dataFileName)) {
                readOnly = FileUtils.isReadOnly(dataFileName);
                textStorage = isTextStorage(dataFileName, textStorage);
            }
        }
        dummy = DataPage.create(this, 0);
        if (persistent) {
            if (readOnly) {
                traceSystem = new TraceSystem(null, false);
            } else {
                traceSystem = new TraceSystem(databaseName + Constants.SUFFIX_TRACE_FILE, true);
            }
            if (cipher != null) {
                traceSystem.setManualEnabling(false);
            }
            traceSystem.setLevelFile(traceLevelFile);
            traceSystem.setLevelSystemOut(traceLevelSystemOut);
            traceSystem.getTrace(Trace.DATABASE)
                    .info("opening " + databaseName + " (build " + Constants.BUILD_ID + ")");
            if (!readOnly && fileLockMethod != FileLock.LOCK_NO) {
                lock = new FileLock(traceSystem, Constants.LOCK_SLEEP);
                lock.lock(databaseName + Constants.SUFFIX_LOCK_FILE, fileLockMethod == FileLock.LOCK_SOCKET);
            }
            deleteOldTempFiles();
            log = new LogSystem(this, databaseName, readOnly, accessModeLog);
            openFileData();
            openFileIndex();
            log.recover();
            fileData.init();
            try {
                fileIndex.init();
            } catch (SQLException e) {
                if (recovery) {
                    traceSystem.getTrace(Trace.DATABASE).error("opening index", e);
                    fileIndex.close();
                    fileIndex.delete();
                    openFileIndex();
                } else {
                    throw e;
                }
            }
            reserveLobFileObjectIds();
            writer = WriterThread.create(this, writeDelay);
        } else {
            traceSystem = new TraceSystem(null, false);
            log = new LogSystem(null, null, false, null);
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
        // TODO storage: antivir scans .script files, maybe other scanners scan .db files?
        ObjectArray cols = new ObjectArray();
        Column columnId = new Column("ID", Value.INT);
        columnId.setNullable(false);
        cols.add(columnId);
        cols.add(new Column("HEAD", Value.INT));
        cols.add(new Column("TYPE", Value.INT));
        cols.add(new Column("SQL", Value.STRING));
        meta = mainSchema.createTable("SYS", 0, cols, persistent, false);
        IndexColumn[] pkCols = IndexColumn.wrap(new Column[] { columnId });
        metaIdIndex = meta.addIndex(systemSession, "SYS_ID", 0, pkCols, IndexType.createPrimaryKey(
                false, false), Index.EMPTY_HEAD, null);
        objectIds.set(0);
        // there could be views on system tables, so they must be added first
        addMetaData(MetaTable.TABLES);
        addMetaData(MetaTable.COLUMNS);
        addMetaData(MetaTable.INDEXES);
        addMetaData(MetaTable.TABLE_TYPES);
        addMetaData(MetaTable.TYPE_INFO);
        addMetaData(MetaTable.CATALOGS);
        addMetaData(MetaTable.SETTINGS);
        addMetaData(MetaTable.HELP);
        addMetaData(MetaTable.SEQUENCES);
        addMetaData(MetaTable.USERS);
        addMetaData(MetaTable.ROLES);
        addMetaData(MetaTable.RIGHTS);
        addMetaData(MetaTable.FUNCTION_ALIASES);
        addMetaData(MetaTable.SCHEMATA);
        addMetaData(MetaTable.TABLE_PRIVILEGES);
        addMetaData(MetaTable.COLUMN_PRIVILEGES);
        addMetaData(MetaTable.COLLATIONS);
        addMetaData(MetaTable.VIEWS);
        addMetaData(MetaTable.IN_DOUBT);
        addMetaData(MetaTable.CROSS_REFERENCES);
        addMetaData(MetaTable.CONSTRAINTS);
        addMetaData(MetaTable.FUNCTION_COLUMNS);
        addMetaData(MetaTable.CONSTANTS);
        addMetaData(MetaTable.DOMAINS);
        addMetaData(MetaTable.TRIGGERS);
        starting = true;
        Cursor cursor = metaIdIndex.find(systemSession, null, null);
        // first, create all function aliases and sequences because
        // they might be used in create table / view / constraints and so on
        ObjectArray records = new ObjectArray();
        while (cursor.next()) {
            MetaRecord rec = new MetaRecord(cursor.get());
            objectIds.set(rec.getId());
            records.add(rec);
        }
        MetaRecord.sort(records);
        for (int i = 0; i < records.size(); i++) {
            MetaRecord rec = (MetaRecord) records.get(i);
            rec.execute(this, systemSession, eventListener);
        }
        // try to recompile the views that are invalid
        recompileInvalidViews(systemSession);
        starting = false;
        addDefaultSetting(systemSession, SetTypes.DEFAULT_LOCK_TIMEOUT, null, Constants.INITIAL_LOCK_TIMEOUT);
        addDefaultSetting(systemSession, SetTypes.DEFAULT_TABLE_TYPE, null, Constants.DEFAULT_TABLE_TYPE);
        addDefaultSetting(systemSession, SetTypes.TRACE_LEVEL_FILE, null, traceSystem.getLevelFile());
        addDefaultSetting(systemSession, SetTypes.TRACE_LEVEL_SYSTEM_OUT, null, traceSystem.getLevelSystemOut());
        addDefaultSetting(systemSession, SetTypes.CACHE_SIZE, null, SysProperties.CACHE_SIZE_DEFAULT);
        addDefaultSetting(systemSession, SetTypes.CLUSTER, Constants.CLUSTERING_DISABLED, 0);
        addDefaultSetting(systemSession, SetTypes.WRITE_DELAY, null, Constants.DEFAULT_WRITE_DELAY);
        if (!readOnly) {
            removeUnusedStorages(systemSession);
        }
        systemSession.commit(true);
        if (!readOnly && persistent) {
            emergencyReserve = openFile(createTempFile(), "rw", false);
            emergencyReserve.autoDelete();
            emergencyReserve.setLength(SysProperties.EMERGENCY_SPACE_INITIAL);
        }
        traceSystem.getTrace(Trace.DATABASE).info("opened " + databaseName);     
    }

    private void recompileInvalidViews(Session session) {
        boolean recompileSuccessful;
        do {
            recompileSuccessful = false;
            ObjectArray list = getAllSchemaObjects(DbObject.TABLE_OR_VIEW);
            for (int i = 0; i < list.size(); i++) {
                DbObject obj = (DbObject) list.get(i);
                if (obj instanceof TableView) {
                    TableView view = (TableView) obj;
                    if (view.getInvalid()) {
                        try {
                            view.recompile(session);
                        } catch (Throwable e) {
                            // ignore
                        }
                        if (!view.getInvalid()) {
                            recompileSuccessful = true;
                        }
                    }
                }
            }
        } while (recompileSuccessful);
        // when opening a database, views are initialized before indexes, so they may not have the optimal plan yet
        // this is not a problem, it is just nice to see the newest plan
        ObjectArray list = getAllSchemaObjects(DbObject.TABLE_OR_VIEW);
        for (int i = 0; i < list.size(); i++) {
            DbObject obj = (DbObject) list.get(i);
            if (obj instanceof TableView) {
                TableView view = (TableView) obj;
                if (!view.getInvalid()) {
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
            ObjectArray storages = getAllStorages();
            for (int i = 0; i < storages.size(); i++) {
                Storage storage = (Storage) storages.get(i);
                if (storage != null && storage.getRecordReader() == null) {
                    storage.delete(session);
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

    public void removeStorage(int id, DiskFile file) {
        if (SysProperties.CHECK) {
            Storage s = (Storage) storageMap.get(id);
            if (s == null || s.getDiskFile() != file) {
                throw Message.getInternalError();
            }
        }
        storageMap.remove(id);
    }

    public Storage getStorage(int id, DiskFile file) {
        Storage storage = (Storage) storageMap.get(id);
        if (storage != null) {
            if (SysProperties.CHECK && storage.getDiskFile() != file) {
                throw Message.getInternalError();
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
        if (obj.getTemporary()) {
            return;
        }
        Row r = meta.getTemplateRow();
        MetaRecord rec = new MetaRecord(obj);
        rec.setRecord(r);
        objectIds.set(obj.getId());
        meta.lock(session, true, true);
        meta.addRow(session, r);
        if (isMultiVersion()) {
            // TODO this should work without MVCC, but avoid risks at the moment
            session.log(meta, UndoLogRecord.INSERT, r);
        }
    }

    private synchronized void removeMeta(Session session, int id) throws SQLException {
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

    private HashMap getMap(int type) {
        switch (type) {
        case DbObject.USER:
            return users;
        case DbObject.SETTING:
            return settings;
        case DbObject.ROLE:
            return roles;
        case DbObject.RIGHT:
            return rights;
        case DbObject.FUNCTION_ALIAS:
            return functionAliases;
        case DbObject.SCHEMA:
            return schemas;
        case DbObject.USER_DATATYPE:
            return userDataTypes;
        case DbObject.COMMENT:
            return comments;
        case DbObject.AGGREGATE:
            return aggregates;
        default:
            throw Message.getInternalError("type=" + type);
        }
    }

    public synchronized void addSchemaObject(Session session, SchemaObject obj) throws SQLException {
        obj.getSchema().add(obj);
        int id = obj.getId();
        if (id > 0 && !starting) {
            addMeta(session, obj);
        }
    }

    public synchronized void addDatabaseObject(Session session, DbObject obj) throws SQLException {
        HashMap map = getMap(obj.getType());
        if (obj.getType() == DbObject.USER) {
            User user = (User) obj;
            if (user.getAdmin() && systemUser.getName().equals(Constants.DBA_NAME)) {
                systemUser.rename(user.getName());
            }
        }
        String name = obj.getName();
        if (SysProperties.CHECK && map.get(name) != null) {
            throw Message.getInternalError("object already exists");
        }
        int id = obj.getId();
        if (id > 0 && !starting) {
            addMeta(session, obj);
        }
        map.put(name, obj);
    }

    public Setting findSetting(String name) {
        return (Setting) settings.get(name);
    }

    public Comment findComment(DbObject object) {
        if (object.getType() == DbObject.COMMENT) {
            return null;
        }
        String key = Comment.getKey(object);
        return (Comment) comments.get(key);
    }

    public User findUser(String name) {
        return (User) users.get(name);
    }

    public FunctionAlias findFunctionAlias(String name) {
        return (FunctionAlias) functionAliases.get(name);
    }
    
    public UserAggregate findAggregate(String name) {
        return (UserAggregate) aggregates.get(name);
    }

    public UserDataType findUserDataType(String name) {
        return (UserDataType) userDataTypes.get(name);
    }

    public User getUser(String name, SQLException notFound) throws SQLException {
        User user = (User) users.get(name);
        if (user == null) {
            throw notFound;
        }
        return user;
    }
    
    public User getUser(String name) throws SQLException {
        return getUser(name, Message.getSQLException(ErrorCode.USER_NOT_FOUND_1, name));
    }

    public synchronized Session createSession(User user) {
        Session session = new Session(this, user, ++nextSessionId);
        sessions.add(session);
        traceSystem.getTrace(Trace.SESSION).info("connecting #" + session.getId() + " to " + databaseName);
        if (delayedCloser != null) {
            delayedCloser.reset();
            delayedCloser = null;
        }
        return session;
    }

    public synchronized void removeSession(Session session) throws SQLException {
        if (session != null) {
            sessions.remove(session);
            if (session != systemSession) {
                traceSystem.getTrace(Trace.SESSION).info("disconnecting #" + session.getId());
            }
        }
        if (sessions.size() == 0 && session != systemSession) {
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

    synchronized void close(boolean fromShutdownHook) {
        closing = true;
        if (sessions.size() > 0) {
            if (!fromShutdownHook) {
                return;
            }
            traceSystem.getTrace(Trace.DATABASE).info("closing " + databaseName + " from shutdown hook");
            Session[] all = new Session[sessions.size()];
            sessions.toArray(all);
            for (int i = 0; i < all.length; i++) {
                Session s = all[i];
                try {
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
            if (sessions.size() > 0) {
                // if a connection was opened, we can't close the database
                return;
            }
            closing = true;
        }
        try {
            if (systemSession != null) {
                ObjectArray tablesAndViews = getAllSchemaObjects(DbObject.TABLE_OR_VIEW);
                for (int i = 0; i < tablesAndViews.size(); i++) {
                    Table table = (Table) tablesAndViews.get(i);
                    table.close(systemSession);
                }
                ObjectArray sequences = getAllSchemaObjects(DbObject.SEQUENCE);
                for (int i = 0; i < sequences.size(); i++) {
                    Sequence sequence = (Sequence) sequences.get(i);
                    sequence.close();
                }
                meta.close(systemSession);
                indexSummaryValid = true;
            }
        } catch (SQLException e) {
            traceSystem.getTrace(Trace.DATABASE).error("close", e);
        }
        try {
            closeOpenFilesAndUnlock();
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
            writer.stopThread();
            writer = null;
        }
    }

    private synchronized void closeOpenFilesAndUnlock() throws SQLException {
        if (log != null) {
            stopWriter();
            log.close();
            log = null;
        }
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
            lock.unlock();
            lock = null;
        }
    }

    private void closeFiles() throws SQLException {
        try {
            if (fileData != null) {
                fileData.close();
                fileData = null;
            }
            if (fileIndex != null) {
                fileIndex.close();
                fileIndex = null;
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
            throw Message.getInternalError();
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
            throw Message.getInternalError();
        }
        objectIds.set(i);
        return i;
    }

    public ObjectArray getAllSettings() {
        return new ObjectArray(settings.values());
    }

    public ObjectArray getAllUsers() {
        return new ObjectArray(users.values());
    }

    public ObjectArray getAllRoles() {
        return new ObjectArray(roles.values());
    }

    public ObjectArray getAllRights() {
        return new ObjectArray(rights.values());
    }

    public ObjectArray getAllComments() {
        return new ObjectArray(comments.values());
    }

    public ObjectArray getAllSchemas() {
        return new ObjectArray(schemas.values());
    }

    public ObjectArray getAllFunctionAliases() {
        return new ObjectArray(functionAliases.values());
    }
    
    public ObjectArray getAllAggregates() {
        return new ObjectArray(aggregates.values());
    }

    public ObjectArray getAllUserDataTypes() {
        return new ObjectArray(userDataTypes.values());
    }

    public ObjectArray getAllSchemaObjects(int type) {
        ObjectArray list = new ObjectArray();
        for (Iterator it = schemas.values().iterator(); it.hasNext();) {
            Schema schema = (Schema) it.next();
            list.addAll(schema.getAll(type));
        }
        return list;
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

    public synchronized Session[] getSessions() {
        Session[] list = new Session[sessions.size()];
        sessions.toArray(list);
        return list;
    }

    public synchronized void update(Session session, DbObject obj) throws SQLException {
        int id = obj.getId();
        removeMeta(session, id);
        addMeta(session, obj);
    }

    public synchronized void renameSchemaObject(Session session, SchemaObject obj, String newName) throws SQLException {
        obj.getSchema().rename(obj, newName);
        updateWithChildren(session, obj);
    }

    private synchronized void updateWithChildren(Session session, DbObject obj) throws SQLException {
        ObjectArray list = obj.getChildren();
        Comment comment = findComment(obj);
        if (comment != null) {
            throw Message.getInternalError();
        }
        update(session, obj);
        // remember that this scans only one level deep!
        for (int i = 0; list != null && i < list.size(); i++) {
            DbObject o = (DbObject) list.get(i);
            if (o.getCreateSQL() != null) {
                update(session, o);
            }
        }
    }

    public synchronized void renameDatabaseObject(Session session, DbObject obj, String newName) throws SQLException {
        int type = obj.getType();
        HashMap map = getMap(type);
        if (SysProperties.CHECK) {
            if (!map.containsKey(obj.getName())) {
                throw Message.getInternalError("not found: " + obj.getName());
            }
            if (obj.getName().equals(newName) || map.containsKey(newName)) {
                throw Message.getInternalError("object already exists: " + newName);
            }
        }
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
                name = FileSystem.MEMORY_PREFIX + name;
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
        for (int i = 0; i < list.length; i++) {
            String name = list[i];
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
        if (emergencyReserve != null) {
            emergencyReserve.closeAndDeleteSilently();
            emergencyReserve = null;
        }
        String path = FileUtils.getParent(databaseName);
        String prefix = FileUtils.normalize(databaseName);
        String[] list = FileUtils.listFiles(path);
        for (int i = 0; i < list.length; i++) {
            String name = list[i];
            if (name.endsWith(Constants.SUFFIX_TEMP_FILE) && FileUtils.fileStartsWith(name, prefix)) {
                // can't always delete the files, they may still be open
                FileUtils.tryDelete(name);
            }
        }
    }

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

    public Role findRole(String roleName) {
        return (Role) roles.get(roleName);
    }

    public Schema findSchema(String schemaName) {
        return (Schema) schemas.get(schemaName);
    }

    public Schema getSchema(String schemaName) throws SQLException {
        Schema schema = findSchema(schemaName);
        if (schema == null) {
            throw Message.getSQLException(ErrorCode.SCHEMA_NOT_FOUND_1, schemaName);
        }
        return schema;
    }

    public synchronized void removeDatabaseObject(Session session, DbObject obj) throws SQLException {
        String objName = obj.getName();
        int type = obj.getType();
        HashMap map = getMap(type);
        if (SysProperties.CHECK && !map.containsKey(objName)) {
            throw Message.getInternalError("not found: " + objName);
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

    private String getFirstInvalidTable(Session session) {
        String conflict = null;
        try {
            ObjectArray list = getAllSchemaObjects(DbObject.TABLE_OR_VIEW);
            for (int i = 0; i < list.size(); i++) {
                Table t = (Table) list.get(i);
                conflict = t.getSQL();
                session.prepare(t.getCreateSQL());
            }
        } catch (SQLException e) {
            return conflict;
        }
        return null;
    }

    public synchronized void removeSchemaObject(Session session, SchemaObject obj) throws SQLException {
        if (obj.getType() == DbObject.TABLE_OR_VIEW) {
            Table table = (Table) obj;
            if (table.getTemporary() && !table.getGlobalTemporary()) {
                session.removeLocalTempTable(table);
                return;
            }
        }
        Comment comment = findComment(obj);
        if (comment != null) {
            removeDatabaseObject(session, comment);
        }
        obj.getSchema().remove(session, obj);
        String invalid = getFirstInvalidTable(session);
        if (invalid != null) {
            obj.getSchema().add(obj);
            throw Message.getSQLException(ErrorCode.CANNOT_DROP_2, new String[] { obj.getSQL(), invalid });
        }
        int id = obj.getId();
        obj.removeChildrenAndResources(session);
        removeMeta(session, id);
    }

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
            cacheSize = kb;
        }
    }

    public synchronized void setMasterUser(User user) throws SQLException {
        addDatabaseObject(systemSession, user);
        systemSession.commit(true);
    }

    public Role getPublicRole() {
        return publicRole;
    }

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

    public CompareMode getCompareMode() {
        return compareMode;
    }

    public String getCluster() {
        return cluster;
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
    }

    public boolean getReadOnly() {
        return readOnly;
    }

    public void setWriteDelay(int value) {
        writeDelay = value;
        if (writer != null) {
            writer.setWriteDelay(value);
        }
    }

    public Class loadClass(String className) throws SQLException {
        try {
            return ClassUtils.loadClass(className);
        } catch (ClassNotFoundException e) {
            throw Message.getSQLException(ErrorCode.CLASS_NOT_FOUND_1, new String[] { className }, e);
        }            
    }

    public void setEventListener(String className) throws SQLException {
        if (className == null || className.length() == 0) {
            eventListener = null;
        } else {
            try {
                eventListener = (DatabaseEventListener) loadClass(className).newInstance();
                String url = databaseURL;
                if (cipher != null) {
                    url += ";CIPHER=" + cipher;
                }
                eventListener.init(url);
            } catch (Throwable e) {
                throw Message.getSQLException(ErrorCode.ERROR_SETTING_DATABASE_EVENT_LISTENER_2, new String[] {
                        className, e.toString() }, e);
            }
        }
    }

    public synchronized void freeUpDiskSpace() throws SQLException {
        long sizeAvailable = 0;
        if (emergencyReserve != null) {
            sizeAvailable = emergencyReserve.length();
            long newLength = sizeAvailable / 4;
            if (newLength < SysProperties.EMERGENCY_SPACE_MIN) {
                newLength = 0;
                noDiskSpace = true;
            }
            emergencyReserve.setLength(newLength);
        }
        if (eventListener != null) {
            eventListener.diskSpaceIsLow(sizeAvailable);
        }
    }

    public void setProgress(int state, String name, int x, int max) {
        if (eventListener != null) {
            try {
                eventListener.setProgress(state, name, x, max);
            } catch (Exception e2) {
                // ignore this second (user made) exception
            }
        }
    }

    public void exceptionThrown(SQLException e, String sql) {
        if (eventListener != null) {
            try {
                eventListener.exceptionThrown(e, sql);
            } catch (Exception e2) {
                // ignore this second (user made) exception
            }
        }
    }

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

    public int getChecksum(byte[] data, int start, int end) {
        int x = 0;
        while (start < end) {
            x += data[start++];
        }
        return x;
    }

    public void setLockMode(int lockMode) {
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
            throw Message.getInternalError("level=" + level);
        }
        if (fileIndex != null) {
            fileIndex.setLogChanges(logIndex);
        }
        if (log != null) {
            log.setDisabled(!logData);
            log.checkpoint();
        }
        logLevel = level;
    }

    public ObjectArray getAllStorages() {
        return new ObjectArray(storageMap.values());
    }

    public boolean getRecovery() {
        return recovery;
    }

    public Session getSystemSession() {
        return systemSession;
    }

    public String getDatabasePath() {
        if (persistent) {
            return FileUtils.getAbsolutePath(databaseName);
        } else {
            return null;
        }
    }

    public void handleInvalidChecksum() throws SQLException {
        SQLException e = Message.getSQLException(ErrorCode.FILE_CORRUPTED_1, "wrong checksum");
        if (!recovery) {
            throw e;
        } else {
            traceSystem.getTrace(Trace.DATABASE).error("recover", e);
        }
    }

    public boolean isClosing() {
        return closing;
    }

    public int getWriteDelay() {
        return writeDelay;
    }

    public int getCacheSize() {
        return cacheSize;
    }

    public void setMaxLengthInplaceLob(int value) {
        this.maxLengthInplaceLob = value;
    }

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

    public synchronized void setDeleteFilesOnDisconnect(boolean b) {
        this.deleteFilesOnDisconnect = b;
    }

    public String getLobCompressionAlgorithm(int type) {
        return lobCompressionAlgorithm;
    }

    public void setLobCompressionAlgorithm(String stringValue) {
        this.lobCompressionAlgorithm = stringValue;
    }

    public void notifyFileSize(long length) {
        if (length > biggestFileSize) {
            biggestFileSize = length;
            setMaxLogSize(0);
        }
    }

    public synchronized void setMaxLogSize(long value) {
        long minLogSize = biggestFileSize / Constants.LOG_SIZE_DIVIDER;
        minLogSize = Math.max(value, minLogSize);
        long currentLogSize = getLog().getMaxLogSize();
        if (minLogSize > currentLogSize || (value > 0 && minLogSize > value)) {
            // works for currentLogSize <= 0 as well
            value = minLogSize;
        }
        if (value > 0) {
            getLog().setMaxLogSize(value);
        }
    }

    public void setAllowLiterals(int value) {
        this.allowLiterals = value;
    }

    public int getAllowLiterals() {
        if (starting) {
            return Constants.ALLOW_LITERALS_ALL;
        }
        return allowLiterals;
    }

    public boolean getOptimizeReuseResults() {
        return optimizeReuseResults;
    }

    public void setOptimizeReuseResults(boolean b) {
        optimizeReuseResults = b;
    }

    public String getCacheType() {
        return cacheType;
    }

    public void invalidateIndexSummary() throws SQLException {
        if (indexSummaryValid) {
            indexSummaryValid = false;
            log.invalidateIndexSummary();
        }
    }

    public boolean getIndexSummaryValid() {
        return indexSummaryValid;
    }

    public Object getLobSyncObject() {
        return lobSyncObject;
    }

    public int getSessionCount() {
        return sessions.size();
    }

    public void setReferentialIntegrity(boolean b) {
        referentialIntegrity = b;
    }

    public boolean getReferentialIntegrity() {
        return referentialIntegrity;
    }

    public boolean isStarting() {
        return starting;
    }

    public boolean isMultiVersion() {
        return multiVersion;
    }

    public void opened() throws SQLException {
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

    public boolean getMultiThreaded() {
        return multiThreaded;
    }

    public void setMultiThreaded(boolean multiThreaded) {
        this.multiThreaded = multiThreaded;
    }
    
    public void setMaxOperationMemory(int maxOperationMemory) {
        this.maxOperationMemory  = maxOperationMemory;
    }

    public int getMaxOperationMemory() {
        return maxOperationMemory;
    }

}
