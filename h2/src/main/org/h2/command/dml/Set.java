/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.text.Collator;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.command.ParserBase;
import org.h2.command.Prepared;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Mode;
import org.h2.engine.SessionLocal;
import org.h2.engine.Setting;
import org.h2.expression.Expression;
import org.h2.expression.TimeZoneOperation;
import org.h2.expression.ValueExpression;
import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.mode.DefaultNullOrdering;
import org.h2.result.ResultInterface;
import org.h2.schema.Schema;
import org.h2.security.auth.AuthenticatorFactory;
import org.h2.table.Table;
import org.h2.util.DateTimeUtils;
import org.h2.util.StringUtils;
import org.h2.util.TimeZoneProvider;
import org.h2.value.CompareMode;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueInteger;
import org.h2.value.ValueNull;

/**
 * This class represents the statement
 * SET
 */
public class Set extends Prepared {

    private final int type;
    private Expression expression;
    private String stringValue;
    private String[] stringValueList;

    public Set(SessionLocal session, int type) {
        super(session);
        this.type = type;
    }

    public void setString(String v) {
        this.stringValue = v;
    }

    @Override
    public boolean isTransactional() {
        switch (type) {
        case SetTypes.CLUSTER:
        case SetTypes.VARIABLE:
        case SetTypes.QUERY_TIMEOUT:
        case SetTypes.LOCK_TIMEOUT:
        case SetTypes.TRACE_LEVEL_SYSTEM_OUT:
        case SetTypes.TRACE_LEVEL_FILE:
        case SetTypes.THROTTLE:
        case SetTypes.SCHEMA:
        case SetTypes.SCHEMA_SEARCH_PATH:
        case SetTypes.CATALOG:
        case SetTypes.RETENTION_TIME:
        case SetTypes.LAZY_QUERY_EXECUTION:
        case SetTypes.NON_KEYWORDS:
        case SetTypes.TIME_ZONE:
        case SetTypes.VARIABLE_BINARY:
        case SetTypes.TRUNCATE_LARGE_LENGTH:
        case SetTypes.WRITE_DELAY:
            return true;
        default:
        }
        return false;
    }

    @Override
    public long update() {
        Database database = getDatabase();
        String name = SetTypes.getTypeName(type);
        switch (type) {
        case SetTypes.ALLOW_LITERALS: {
            session.getUser().checkAdmin();
            int value = getIntValue();
            if (value < 0 || value > 2) {
                throw DbException.getInvalidValueException("ALLOW_LITERALS", value);
            }
            synchronized (database) {
                database.setAllowLiterals(value);
                addOrUpdateSetting(name, null, value);
            }
            break;
        }
        case SetTypes.CACHE_SIZE: {
            session.getUser().checkAdmin();
            int value = getIntValue();
            if (value < 0) {
                throw DbException.getInvalidValueException("CACHE_SIZE", value);
            }
            synchronized (database) {
                database.setCacheSize(value);
                addOrUpdateSetting(name, null, value);
            }
            break;
        }
        case SetTypes.CLUSTER: {
            if (Constants.CLUSTERING_ENABLED.equals(stringValue)) {
                // this value is used when connecting
                // ignore, as the cluster setting is checked later
                break;
            }
            String value = StringUtils.quoteStringSQL(stringValue);
            if (!value.equals(database.getCluster())) {
                if (!value.equals(Constants.CLUSTERING_DISABLED)) {
                    // anybody can disable the cluster
                    // (if he can't access a cluster node)
                    session.getUser().checkAdmin();
                }
                database.setCluster(value);
                // use the system session so that the current transaction
                // (if any) is not committed
                SessionLocal sysSession = database.getSystemSession();
                synchronized (sysSession) {
                    synchronized (database) {
                        addOrUpdateSetting(sysSession, name, value, 0);
                        sysSession.commit(true);
                    }
                }
            }
            break;
        }
        case SetTypes.COLLATION: {
            session.getUser().checkAdmin();
            CompareMode compareMode;
            StringBuilder buff = new StringBuilder(stringValue);
            if (stringValue.equals(CompareMode.OFF)) {
                compareMode = CompareMode.getInstance(null, 0);
            } else {
                int strength = getIntValue();
                buff.append(" STRENGTH ");
                if (strength == Collator.IDENTICAL) {
                    buff.append("IDENTICAL");
                } else if (strength == Collator.PRIMARY) {
                    buff.append("PRIMARY");
                } else if (strength == Collator.SECONDARY) {
                    buff.append("SECONDARY");
                } else if (strength == Collator.TERTIARY) {
                    buff.append("TERTIARY");
                }
                compareMode = CompareMode.getInstance(stringValue, strength);
            }
            synchronized (database) {
                CompareMode old = database.getCompareMode();
                if (old.equals(compareMode)) {
                    break;
                }
                Table table = database.getFirstUserTable();
                if (table != null) {
                    throw DbException.get(ErrorCode.COLLATION_CHANGE_WITH_DATA_TABLE_1, table.getTraceSQL());
                }
                addOrUpdateSetting(name, buff.toString(), 0);
                database.setCompareMode(compareMode);
            }
            break;
        }
        case SetTypes.CREATE_BUILD: {
            session.getUser().checkAdmin();
            if (database.isStarting()) {
                // just ignore the command if not starting
                // this avoids problems when running recovery scripts
                int value = getIntValue();
                synchronized (database) {
                    addOrUpdateSetting(name, null, value);
                }
            }
            break;
        }
        case SetTypes.DATABASE_EVENT_LISTENER: {
            session.getUser().checkAdmin();
            database.setEventListenerClass(stringValue);
            break;
        }
        case SetTypes.DB_CLOSE_DELAY: {
            session.getUser().checkAdmin();
            int value = getIntValue();
            if (value == -1) {
                // -1 is a special value for in-memory databases,
                // which means "keep the DB alive and use the same
                // DB for all connections"
            } else if (value < 0) {
                throw DbException.getInvalidValueException("DB_CLOSE_DELAY", value);
            }
            synchronized (database) {
                database.setCloseDelay(value);
                addOrUpdateSetting(name, null, value);
            }
            break;
        }
        case SetTypes.DEFAULT_LOCK_TIMEOUT: {
            session.getUser().checkAdmin();
            int value = getIntValue();
            if (value < 0) {
                throw DbException.getInvalidValueException("DEFAULT_LOCK_TIMEOUT", value);
            }
            synchronized (database) {
                addOrUpdateSetting(name, null, value);
            }
            break;
        }
        case SetTypes.DEFAULT_TABLE_TYPE: {
            session.getUser().checkAdmin();
            int value = getIntValue();
            synchronized (database) {
                database.setDefaultTableType(value);
                addOrUpdateSetting(name, null, value);
            }
            break;
        }
        case SetTypes.EXCLUSIVE: {
            session.getUser().checkAdmin();
            int value = getIntValue();
            switch (value) {
            case 0:
                if (!database.unsetExclusiveSession(session)) {
                    throw DbException.get(ErrorCode.DATABASE_IS_IN_EXCLUSIVE_MODE);
                }
                break;
            case 1:
                if (!database.setExclusiveSession(session, false)) {
                    throw DbException.get(ErrorCode.DATABASE_IS_IN_EXCLUSIVE_MODE);
                }
                break;
            case 2:
                if (!database.setExclusiveSession(session, true)) {
                    throw DbException.get(ErrorCode.DATABASE_IS_IN_EXCLUSIVE_MODE);
                }
                break;
            default:
                throw DbException.getInvalidValueException("EXCLUSIVE", value);
            }
            break;
        }
        case SetTypes.JAVA_OBJECT_SERIALIZER: {
            session.getUser().checkAdmin();
            synchronized (database) {
                Table table = database.getFirstUserTable();
                if (table != null) {
                    throw DbException.get(ErrorCode.JAVA_OBJECT_SERIALIZER_CHANGE_WITH_DATA_TABLE,
                            table.getTraceSQL());
                }
                database.setJavaObjectSerializerName(stringValue);
                addOrUpdateSetting(name, stringValue, 0);
            }
            break;
        }
        case SetTypes.IGNORECASE: {
            session.getUser().checkAdmin();
            int value = getIntValue();
            synchronized (database) {
                database.setIgnoreCase(value == 1);
                addOrUpdateSetting(name, null, value);
            }
            break;
        }
        case SetTypes.LOCK_MODE: {
            session.getUser().checkAdmin();
            int value = getIntValue();
            synchronized (database) {
                database.setLockMode(value);
                addOrUpdateSetting(name, null, value);
            }
            break;
        }
        case SetTypes.LOCK_TIMEOUT: {
            int value = getIntValue();
            if (value < 0) {
                throw DbException.getInvalidValueException("LOCK_TIMEOUT", value);
            }
            session.setLockTimeout(value);
            break;
        }
        case SetTypes.MAX_LENGTH_INPLACE_LOB: {
            session.getUser().checkAdmin();
            int value = getIntValue();
            if (value < 0) {
                throw DbException.getInvalidValueException("MAX_LENGTH_INPLACE_LOB", value);
            }
            synchronized (database) {
                database.setMaxLengthInplaceLob(value);
                addOrUpdateSetting(name, null, value);
            }
            break;
        }
        case SetTypes.MAX_LOG_SIZE: {
            session.getUser().checkAdmin();
            int value = getIntValue();
            if (value < 0) {
                throw DbException.getInvalidValueException("MAX_LOG_SIZE", value);
            }
            break;
        }
        case SetTypes.MAX_MEMORY_ROWS: {
            session.getUser().checkAdmin();
            int value = getIntValue();
            if (value < 0) {
                throw DbException.getInvalidValueException("MAX_MEMORY_ROWS", value);
            }
            synchronized (database) {
                database.setMaxMemoryRows(value);
                addOrUpdateSetting(name, null, value);
            }
            break;
        }
        case SetTypes.MAX_MEMORY_UNDO: {
            session.getUser().checkAdmin();
            int value = getIntValue();
            if (value < 0) {
                throw DbException.getInvalidValueException("MAX_MEMORY_UNDO", value);
            }
            synchronized (database) {
                addOrUpdateSetting(name, null, value);
            }
            break;
        }
        case SetTypes.MAX_OPERATION_MEMORY: {
            session.getUser().checkAdmin();
            int value = getIntValue();
            if (value < 0) {
                throw DbException.getInvalidValueException("MAX_OPERATION_MEMORY", value);
            }
            database.setMaxOperationMemory(value);
            break;
        }
        case SetTypes.MODE: {
            Mode mode = Mode.getInstance(stringValue);
            if (mode == null) {
                throw DbException.get(ErrorCode.UNKNOWN_MODE_1, stringValue);
            }
            if (database.getMode() != mode) {
                session.getUser().checkAdmin();
                database.setMode(mode);
            }
            break;
        }
        case SetTypes.OPTIMIZE_REUSE_RESULTS: {
            session.getUser().checkAdmin();
            database.setOptimizeReuseResults(getIntValue() != 0);
            break;
        }
        case SetTypes.QUERY_TIMEOUT: {
            int value = getIntValue();
            if (value < 0) {
                throw DbException.getInvalidValueException("QUERY_TIMEOUT", value);
            }
            session.setQueryTimeout(value);
            break;
        }
        case SetTypes.REDO_LOG_BINARY: {
            DbException.getUnsupportedException("MV_STORE + SET REDO_LOG_BINARY");
            break;
        }
        case SetTypes.REFERENTIAL_INTEGRITY: {
            session.getUser().checkAdmin();
            int value = getIntValue();
            if (value < 0 || value > 1) {
                throw DbException.getInvalidValueException("REFERENTIAL_INTEGRITY", value);
            }
            database.setReferentialIntegrity(value == 1);
            break;
        }
        case SetTypes.QUERY_STATISTICS: {
            session.getUser().checkAdmin();
            int value = getIntValue();
            if (value < 0 || value > 1) {
                throw DbException.getInvalidValueException("QUERY_STATISTICS", value);
            }
            database.setQueryStatistics(value == 1);
            break;
        }
        case SetTypes.QUERY_STATISTICS_MAX_ENTRIES: {
            session.getUser().checkAdmin();
            int value = getIntValue();
            if (value < 1) {
                throw DbException.getInvalidValueException("QUERY_STATISTICS_MAX_ENTRIES", value);
            }
            database.setQueryStatisticsMaxEntries(value);
            break;
        }
        case SetTypes.SCHEMA: {
            Schema schema = database.getSchema(expression.optimize(session).getValue(session).getString());
            session.setCurrentSchema(schema);
            break;
        }
        case SetTypes.SCHEMA_SEARCH_PATH: {
            session.setSchemaSearchPath(stringValueList);
            break;
        }
        case SetTypes.CATALOG: {
            String shortName = database.getShortName();
            String value = expression.optimize(session).getValue(session).getString();
            if (value == null || !database.equalsIdentifiers(shortName, value)
                    && !database.equalsIdentifiers(shortName, value.trim())) {
                throw DbException.get(ErrorCode.DATABASE_NOT_FOUND_1, stringValue);
            }
            break;
        }
        case SetTypes.TRACE_LEVEL_FILE:
            session.getUser().checkAdmin();
            if (getPersistedObjectId() == 0) {
                // don't set the property when opening the database
                // this is for compatibility with older versions, because
                // this setting was persistent
                database.getTraceSystem().setLevelFile(getIntValue());
            }
            break;
        case SetTypes.TRACE_LEVEL_SYSTEM_OUT:
            session.getUser().checkAdmin();
            if (getPersistedObjectId() == 0) {
                // don't set the property when opening the database
                // this is for compatibility with older versions, because
                // this setting was persistent
                database.getTraceSystem().setLevelSystemOut(getIntValue());
            }
            break;
        case SetTypes.TRACE_MAX_FILE_SIZE: {
            session.getUser().checkAdmin();
            int value = getIntValue();
            if (value < 0) {
                throw DbException.getInvalidValueException("TRACE_MAX_FILE_SIZE", value);
            }
            int size = value * (1024 * 1024);
            synchronized (database) {
                database.getTraceSystem().setMaxFileSize(size);
                addOrUpdateSetting(name, null, value);
            }
            break;
        }
        case SetTypes.THROTTLE: {
            int value = getIntValue();
            if (value < 0) {
                throw DbException.getInvalidValueException("THROTTLE", value);
            }
            session.setThrottle(value);
            break;
        }
        case SetTypes.VARIABLE: {
            Expression expr = expression.optimize(session);
            session.setVariable(stringValue, expr.getValue(session));
            break;
        }
        case SetTypes.WRITE_DELAY: {
            session.getUser().checkAdmin();
            int value = getIntValue();
            if (value < 0) {
                throw DbException.getInvalidValueException("WRITE_DELAY", value);
            }
            synchronized (database) {
                database.setWriteDelay(value);
                addOrUpdateSetting(name, null, value);
            }
            break;
        }
        case SetTypes.RETENTION_TIME: {
            session.getUser().checkAdmin();
            int value = getIntValue();
            if (value < 0) {
                throw DbException.getInvalidValueException("RETENTION_TIME", value);
            }
            synchronized (database) {
                database.setRetentionTime(value);
                addOrUpdateSetting(name, null, value);
            }
            break;
        }
        case SetTypes.LAZY_QUERY_EXECUTION: {
            int value = getIntValue();
            if (value != 0 && value != 1) {
                throw DbException.getInvalidValueException("LAZY_QUERY_EXECUTION",
                        value);
            }
            session.setLazyQueryExecution(value == 1);
            break;
        }
        case SetTypes.BUILTIN_ALIAS_OVERRIDE: {
            session.getUser().checkAdmin();
            int value = getIntValue();
            if (value != 0 && value != 1) {
                throw DbException.getInvalidValueException("BUILTIN_ALIAS_OVERRIDE",
                        value);
            }
            database.setAllowBuiltinAliasOverride(value == 1);
            break;
        }
        case SetTypes.AUTHENTICATOR: {
            session.getUser().checkAdmin();
            boolean value = expression.optimize(session).getBooleanValue(session);
            try {
                synchronized (database) {
                    if (value) {
                        database.setAuthenticator(AuthenticatorFactory.createAuthenticator());
                    } else {
                        database.setAuthenticator(null);
                    }
                    addOrUpdateSetting(name, value ? "TRUE" : "FALSE", 0);
                }
            } catch (Exception e) {
                // Errors during start are ignored to allow to open the database
                if (database.isStarting()) {
                    database.getTrace(Trace.DATABASE).error(e,
                            "{0}: failed to set authenticator during database start ", expression.toString());
                } else {
                    throw DbException.convert(e);
                }
            }
            break;
        }
        case SetTypes.IGNORE_CATALOGS: {
            session.getUser().checkAdmin();
            int value = getIntValue();
            synchronized (database) {
                database.setIgnoreCatalogs(value == 1);
                addOrUpdateSetting(name, null, value);
            }
            break;
        }
        case SetTypes.NON_KEYWORDS:
            session.setNonKeywords(ParserBase.parseNonKeywords(stringValueList));
            break;
        case SetTypes.TIME_ZONE:
            session.setTimeZone(expression == null ? DateTimeUtils.getTimeZone()
                    : parseTimeZone(expression.getValue(session)));
            break;
        case SetTypes.VARIABLE_BINARY:
            session.setVariableBinary(expression.getBooleanValue(session));
            break;
        case SetTypes.DEFAULT_NULL_ORDERING: {
            DefaultNullOrdering defaultNullOrdering;
            try {
                defaultNullOrdering = DefaultNullOrdering.valueOf(StringUtils.toUpperEnglish(stringValue));
            } catch (RuntimeException e) {
                throw DbException.getInvalidValueException("DEFAULT_NULL_ORDERING", stringValue);
            }
            if (database.getDefaultNullOrdering() != defaultNullOrdering) {
                session.getUser().checkAdmin();
                database.setDefaultNullOrdering(defaultNullOrdering);
            }
            break;
        }
        case SetTypes.TRUNCATE_LARGE_LENGTH:
            session.setTruncateLargeLength(expression.getBooleanValue(session));
            break;
        default:
            throw DbException.getInternalError("type="+type);
        }
        // the meta data information has changed
        database.getNextModificationDataId();
        // query caches might be affected as well, for example
        // when changing the compatibility mode
        database.getNextModificationMetaId();
        return 0;
    }

    private static TimeZoneProvider parseTimeZone(Value v) {
        if (DataType.isCharacterStringType(v.getValueType())) {
            TimeZoneProvider timeZone;
            try {
                timeZone = TimeZoneProvider.ofId(v.getString());
            } catch (RuntimeException ex) {
                throw DbException.getInvalidValueException("TIME ZONE", v.getTraceSQL());
            }
            return timeZone;
        } else if (v == ValueNull.INSTANCE) {
            throw DbException.getInvalidValueException("TIME ZONE", v);
        }
        return TimeZoneProvider.ofOffset(TimeZoneOperation.parseInterval(v));
    }

    private int getIntValue() {
        expression = expression.optimize(session);
        return expression.getValue(session).getInt();
    }

    public void setInt(int value) {
        this.expression = ValueExpression.get(ValueInteger.get(value));
    }

    public void setExpression(Expression expression) {
        this.expression = expression;
    }

    private void addOrUpdateSetting(String name, String s, int v) {
        addOrUpdateSetting(session, name, s, v);
    }

    private void addOrUpdateSetting(SessionLocal session, String name, String s, int v) {
        Database database = session.getDatabase();
        assert Thread.holdsLock(database);
        if (database.isReadOnly()) {
            return;
        }
        Setting setting = database.findSetting(name);
        boolean addNew = false;
        if (setting == null) {
            addNew = true;
            int id = getObjectId();
            setting = new Setting(database, id, name);
        }
        if (s != null) {
            if (!addNew && setting.getStringValue().equals(s)) {
                return;
            }
            setting.setStringValue(s);
        } else {
            if (!addNew && setting.getIntValue() == v) {
                return;
            }
            setting.setIntValue(v);
        }
        if (addNew) {
            database.addDatabaseObject(session, setting);
        } else {
            database.updateMeta(session, setting);
        }
    }

    @Override
    public boolean needRecompile() {
        return false;
    }

    @Override
    public ResultInterface queryMeta() {
        return null;
    }

    public void setStringArray(String[] list) {
        this.stringValueList = list;
    }

    @Override
    public int getType() {
        return CommandInterface.SET;
    }

}
