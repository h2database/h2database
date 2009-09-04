/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.Collator;
import java.util.Locale;

import org.h2.command.Command;
import org.h2.constant.SysProperties;
import org.h2.constraint.Constraint;
import org.h2.constraint.ConstraintCheck;
import org.h2.constraint.ConstraintReferential;
import org.h2.constraint.ConstraintUnique;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.FunctionAlias;
import org.h2.engine.Right;
import org.h2.engine.Role;
import org.h2.engine.Session;
import org.h2.engine.Setting;
import org.h2.engine.User;
import org.h2.engine.UserAggregate;
import org.h2.engine.UserDataType;
import org.h2.expression.ValueExpression;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.index.MetaIndex;
import org.h2.log.InDoubtTransaction;
import org.h2.message.Message;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.schema.Constant;
import org.h2.schema.Schema;
import org.h2.schema.SchemaObject;
import org.h2.schema.Sequence;
import org.h2.schema.TriggerObject;
import org.h2.store.DiskFile;
import org.h2.tools.Csv;
import org.h2.util.MathUtils;
import org.h2.util.ObjectArray;
import org.h2.util.Resources;
import org.h2.util.StatementBuilder;
import org.h2.util.StringUtils;
import org.h2.value.CompareMode;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueString;

/**
 * This class is responsible to build the database meta data pseudo tables.
 */
public class MetaTable extends Table {

    /**
     * The approximate number of rows of a meta table.
     */
    public static final long ROW_COUNT_APPROXIMATION = 1000;

    private static final int TABLES = 0;
    private static final int COLUMNS = 1;
    private static final int INDEXES = 2;
    private static final int TABLE_TYPES = 3;
    private static final int TYPE_INFO = 4;
    private static final int CATALOGS = 5;
    private static final int SETTINGS = 6;
    private static final int HELP = 7;
    private static final int SEQUENCES = 8;
    private static final int USERS = 9;
    private static final int ROLES = 10;
    private static final int RIGHTS = 11;
    private static final int FUNCTION_ALIASES = 12;
    private static final int SCHEMATA = 13;
    private static final int TABLE_PRIVILEGES = 14;
    private static final int COLUMN_PRIVILEGES = 15;
    private static final int COLLATIONS = 16;
    private static final int VIEWS = 17;
    private static final int IN_DOUBT = 18;
    private static final int CROSS_REFERENCES = 19;
    private static final int CONSTRAINTS = 20;
    private static final int FUNCTION_COLUMNS = 21;
    private static final int CONSTANTS = 22;
    private static final int DOMAINS = 23;
    private static final int TRIGGERS = 24;
    private static final int SESSIONS = 25;
    private static final int LOCKS = 26;
    private static final int SESSION_STATE = 27;
    private static final int META_TABLE_TYPE_COUNT = SESSION_STATE + 1;

    private final int type;
    private final int indexColumn;
    private MetaIndex index;

    /**
     * Create a new metadata table.
     *
     * @param schema the schema
     * @param id the object id
     * @param type the meta table type
     */
    public MetaTable(Schema schema, int id, int type) throws SQLException {
        // tableName will be set later
        super(schema, id, null, true, true);
        this.type = type;
        Column[] cols;
        String indexColumnName = null;
        switch(type) {
        case TABLES:
            setObjectName("TABLES");
            cols = createColumns(
                    "TABLE_CATALOG",
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "TABLE_TYPE",
                    // extensions
                    "STORAGE_TYPE",
                    "SQL",
                    "REMARKS",
                    "LAST_MODIFICATION BIGINT",
                    "ID INT",
                    "TYPE_NAME"
            );
            indexColumnName = "TABLE_NAME";
            break;
        case COLUMNS:
            setObjectName("COLUMNS");
            cols = createColumns(
                    "TABLE_CATALOG",
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "COLUMN_NAME",
                    "ORDINAL_POSITION INT",
                    "COLUMN_DEFAULT",
                    "IS_NULLABLE",
                    "DATA_TYPE INT",
                    "CHARACTER_MAXIMUM_LENGTH INT",
                    "CHARACTER_OCTET_LENGTH INT",
                    "NUMERIC_PRECISION INT",
                    "NUMERIC_PRECISION_RADIX INT",
                    "NUMERIC_SCALE INT",
                    "CHARACTER_SET_NAME",
                    "COLLATION_NAME",
                    // extensions
                    "TYPE_NAME",
                    "NULLABLE INT",
                    "IS_COMPUTED BIT",
                    "SELECTIVITY INT",
                    "CHECK_CONSTRAINT",
                    "SEQUENCE_NAME",
                    "REMARKS",
                    "SOURCE_DATA_TYPE SMALLINT"
            );
            indexColumnName = "TABLE_NAME";
            break;
        case INDEXES:
            setObjectName("INDEXES");
            cols = createColumns(
                    "TABLE_CATALOG",
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "NON_UNIQUE BIT",
                    "INDEX_NAME",
                    "ORDINAL_POSITION SMALLINT",
                    "COLUMN_NAME",
                    "CARDINALITY INT",
                    "PRIMARY_KEY BIT",
                    "INDEX_TYPE_NAME",
                    "IS_GENERATED BIT",
                    "INDEX_TYPE SMALLINT",
                    "ASC_OR_DESC",
                    "PAGES INT",
                    "FILTER_CONDITION",
                    "REMARKS",
                    "SQL",
                    "ID INT",
                    "SORT_TYPE INT",
                    "CONSTRAINT_NAME"
            );
            indexColumnName = "TABLE_NAME";
            break;
        case TABLE_TYPES:
            setObjectName("TABLE_TYPES");
            cols = createColumns("TYPE");
            break;
        case TYPE_INFO:
            setObjectName("TYPE_INFO");
            cols = createColumns(
                "TYPE_NAME",
                "DATA_TYPE INT",
                "PRECISION INT",
                "PREFIX",
                "SUFFIX",
                "PARAMS",
                "AUTO_INCREMENT BIT",
                "MINIMUM_SCALE SMALLINT",
                "MAXIMUM_SCALE SMALLINT",
                "RADIX INT",
                "POS INT",
                "CASE_SENSITIVE BIT",
                "NULLABLE SMALLINT",
                "SEARCHABLE SMALLINT"
            );
            break;
        case CATALOGS:
            setObjectName("CATALOGS");
            cols = createColumns("CATALOG_NAME");
            break;
        case SETTINGS:
            setObjectName("SETTINGS");
            cols = createColumns("NAME", "VALUE");
            break;
        case HELP:
            setObjectName("HELP");
            cols = createColumns(
                    "ID INT",
                    "SECTION",
                    "TOPIC",
                    "SYNTAX",
                    "TEXT"
            );
            break;
        case SEQUENCES:
            setObjectName("SEQUENCES");
            cols = createColumns(
                    "SEQUENCE_CATALOG",
                    "SEQUENCE_SCHEMA",
                    "SEQUENCE_NAME",
                    "CURRENT_VALUE BIGINT",
                    "INCREMENT BIGINT",
                    "IS_GENERATED BIT",
                    "REMARKS",
                    "CACHE BIGINT",
                    "ID INT"
            );
            break;
        case USERS:
            setObjectName("USERS");
            cols = createColumns(
                    "NAME",
                    "ADMIN",
                    "REMARKS",
                    "ID INT"
            );
            break;
        case ROLES:
            setObjectName("ROLES");
            cols = createColumns(
                    "NAME",
                    "REMARKS",
                    "ID INT"
            );
            break;
        case RIGHTS:
            setObjectName("RIGHTS");
            cols = createColumns(
                    "GRANTEE",
                    "GRANTEETYPE",
                    "GRANTEDROLE",
                    "RIGHTS",
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "ID INT"
            );
            indexColumnName = "TABLE_NAME";
            break;
        case FUNCTION_ALIASES:
            setObjectName("FUNCTION_ALIASES");
            cols = createColumns(
                    "ALIAS_CATALOG",
                    "ALIAS_SCHEMA",
                    "ALIAS_NAME",
                    "JAVA_CLASS",
                    "JAVA_METHOD",
                    "DATA_TYPE INT",
                    "COLUMN_COUNT INT",
                    "RETURNS_RESULT SMALLINT",
                    "REMARKS",
                    "ID INT"
            );
            break;
        case FUNCTION_COLUMNS:
            setObjectName("FUNCTION_COLUMNS");
            cols = createColumns(
                    "ALIAS_CATALOG",
                    "ALIAS_SCHEMA",
                    "ALIAS_NAME",
                    "JAVA_CLASS",
                    "JAVA_METHOD",
                    "COLUMN_COUNT INT",
                    "POS INT",
                    "COLUMN_NAME",
                    "DATA_TYPE INT",
                    "TYPE_NAME",
                    "PRECISION INT",
                    "SCALE SMALLINT",
                    "RADIX SMALLINT",
                    "NULLABLE SMALLINT",
                    "COLUMN_TYPE SMALLINT",
                    "REMARKS",
                    "COLUMN_DEFAULT"
            );
            break;
        case SCHEMATA:
            setObjectName("SCHEMATA");
            cols = createColumns(
                    "CATALOG_NAME",
                    "SCHEMA_NAME",
                    "SCHEMA_OWNER",
                    "DEFAULT_CHARACTER_SET_NAME",
                    "DEFAULT_COLLATION_NAME",
                    "IS_DEFAULT BIT",
                    "REMARKS",
                    "ID INT"
            );
            break;
        case TABLE_PRIVILEGES:
            setObjectName("TABLE_PRIVILEGES");
            cols = createColumns(
                    "GRANTOR",
                    "GRANTEE",
                    "TABLE_CATALOG",
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "PRIVILEGE_TYPE",
                    "IS_GRANTABLE"
            );
            indexColumnName = "TABLE_NAME";
            break;
        case COLUMN_PRIVILEGES:
            setObjectName("COLUMN_PRIVILEGES");
            cols = createColumns(
                    "GRANTOR",
                    "GRANTEE",
                    "TABLE_CATALOG",
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "COLUMN_NAME",
                    "PRIVILEGE_TYPE",
                    "IS_GRANTABLE"
            );
            indexColumnName = "TABLE_NAME";
            break;
        case COLLATIONS:
            setObjectName("COLLATIONS");
            cols = createColumns(
                    "NAME",
                    "KEY"
            );
            break;
        case VIEWS:
            setObjectName("VIEWS");
            cols = createColumns(
                    "TABLE_CATALOG",
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "VIEW_DEFINITION",
                    "CHECK_OPTION",
                    "IS_UPDATABLE",
                    "STATUS",
                    "REMARKS",
                    "ID INT"
            );
            indexColumnName = "TABLE_NAME";
            break;
        case IN_DOUBT:
            setObjectName("IN_DOUBT");
            cols = createColumns(
                    "TRANSACTION",
                    "STATE"
            );
            break;
        case CROSS_REFERENCES:
            setObjectName("CROSS_REFERENCES");
            cols = createColumns(
                    "PKTABLE_CATALOG",
                    "PKTABLE_SCHEMA",
                    "PKTABLE_NAME",
                    "PKCOLUMN_NAME",
                    "FKTABLE_CATALOG",
                    "FKTABLE_SCHEMA",
                    "FKTABLE_NAME",
                    "FKCOLUMN_NAME",
                    "ORDINAL_POSITION SMALLINT",
                    "UPDATE_RULE SMALLINT",
                    "DELETE_RULE SMALLINT",
                    "FK_NAME",
                    "PK_NAME",
                    "DEFERRABILITY SMALLINT"
            );
            indexColumnName = "PKTABLE_NAME";
            break;
        case CONSTRAINTS:
            setObjectName("CONSTRAINTS");
            cols = createColumns(
                    "CONSTRAINT_CATALOG",
                    "CONSTRAINT_SCHEMA",
                    "CONSTRAINT_NAME",
                    "CONSTRAINT_TYPE",
                    "TABLE_CATALOG",
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "UNIQUE_INDEX_NAME",
                    "CHECK_EXPRESSION",
                    "COLUMN_LIST",
                    "REMARKS",
                    "SQL",
                    "ID INT"
            );
            indexColumnName = "TABLE_NAME";
            break;
        case CONSTANTS:
            setObjectName("CONSTANTS");
            cols = createColumns(
                    "CONSTANT_CATALOG",
                    "CONSTANT_SCHEMA",
                    "CONSTANT_NAME",
                    "DATA_TYPE INT",
                    "REMARKS",
                    "SQL",
                    "ID INT"
            );
            break;
        case DOMAINS:
            setObjectName("DOMAINS");
            cols = createColumns(
                    "DOMAIN_CATALOG",
                    "DOMAIN_SCHEMA",
                    "DOMAIN_NAME",
                    "COLUMN_DEFAULT",
                    "IS_NULLABLE",
                    "DATA_TYPE INT",
                    "PRECISION INT",
                    "SCALE INT",
                    "TYPE_NAME",
                    "SELECTIVITY INT",
                    "CHECK_CONSTRAINT",
                    "REMARKS",
                    "SQL",
                    "ID INT"
            );
            break;
        case TRIGGERS:
            setObjectName("TRIGGERS");
            cols = createColumns(
                    "TRIGGER_CATALOG",
                    "TRIGGER_SCHEMA",
                    "TRIGGER_NAME",
                    "TRIGGER_TYPE",
                    "TABLE_CATALOG",
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "BEFORE BIT",
                    "JAVA_CLASS",
                    "QUEUE_SIZE INT",
                    "NO_WAIT BIT",
                    "REMARKS",
                    "SQL",
                    "ID INT"
            );
            break;
        case SESSIONS: {
            setObjectName("SESSIONS");
            cols = createColumns(
                    "ID INT",
                    "USER_NAME",
                    "SESSION_START",
                    "STATEMENT",
                    "STATEMENT_START"
            );
            break;
        }
        case LOCKS: {
            setObjectName("LOCKS");
            cols = createColumns(
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "SESSION_ID INT",
                    "LOCK_TYPE"
            );
            break;
        }
        case SESSION_STATE: {
            setObjectName("SESSION_STATE");
            cols = createColumns(
                    "KEY",
                    "SQL"
            );
            break;
        }
        default:
            throw Message.throwInternalError("type="+type);
        }
        setColumns(cols);

        if (indexColumnName == null) {
            indexColumn = -1;
        } else {
            indexColumn = getColumn(indexColumnName).getColumnId();
            IndexColumn[] indexCols = IndexColumn.wrap(new Column[] { cols[indexColumn] });
            index = new MetaIndex(this, indexCols, false);
        }
    }

    private Column[] createColumns(String... names) {
        Column[] cols = new Column[names.length];
        for (int i = 0; i < names.length; i++) {
            String nameType = names[i];
            int idx = nameType.indexOf(' ');
            int type;
            String name;
            if (idx < 0) {
                type = Value.STRING;
                name = nameType;
            } else {
                type = DataType.getTypeByName(nameType.substring(idx + 1)).type;
                name = nameType.substring(0, idx);
            }
            cols[i] = new Column(name, type);
        }
        return cols;
    }

    public String getDropSQL() {
        return null;
    }

    public String getCreateSQL() {
        return null;
    }

    public Index addIndex(Session session, String indexName, int indexId, IndexColumn[] cols, IndexType indexType,
            int headPos, String comment) throws SQLException {
        throw Message.getUnsupportedException("META");
    }

    public void lock(Session session, boolean exclusive, boolean force) {
        // nothing to do
    }

    public boolean isLockedExclusively() {
        return false;
    }

    private String identifier(String s) {
        if (database.getMode().lowerCaseIdentifiers) {
            s = s == null ? null : StringUtils.toLowerEnglish(s);
        }
        return s;
    }

    private ObjectArray<Table> getAllTables(Session session) {
        ObjectArray<Table> tables = database.getAllTablesAndViews();
        ObjectArray<Table> tempTables = session.getLocalTempTables();
        tables.addAll(tempTables);
        return tables;
    }

    private boolean checkIndex(Session session, String value, Value indexFrom, Value indexTo) throws SQLException {
        if (value == null || (indexFrom == null && indexTo == null)) {
            return true;
        }
        Database db = session.getDatabase();
        Value v = ValueString.get(value);
        if (indexFrom != null && db.compare(v, indexFrom) < 0) {
            return false;
        }
        if (indexTo != null && db.compare(v, indexTo) > 0) {
            return false;
        }
        return true;
    }

    private String replaceNullWithEmpty(String s) {
        return s == null ? "" : s;
    }

    /**
     * Generate the data for the given metadata table using the given first and
     * last row filters.
     *
     * @param session the session
     * @param first the first row to return
     * @param last the last row to return
     * @return the generated rows
     */
    public ObjectArray<Row> generateRows(Session session, SearchRow first, SearchRow last) throws SQLException {
        Value indexFrom = null, indexTo = null;

        if (indexColumn >= 0) {
            if (first != null) {
                indexFrom = first.getValue(indexColumn);
            }
            if (last != null) {
                indexTo = last.getValue(indexColumn);
            }
        }

        ObjectArray<Row> rows = ObjectArray.newInstance();
        String catalog = identifier(database.getShortName());
        switch (type) {
        case TABLES: {
            for (Table table : getAllTables(session)) {
                String tableName = identifier(table.getName());
                if (!checkIndex(session, tableName, indexFrom, indexTo)) {
                    continue;
                }
                String storageType;
                if (table.isTemporary()) {
                    if (table.isGlobalTemporary()) {
                        storageType = "GLOBAL TEMPORARY";
                    } else {
                        storageType = "LOCAL TEMPORARY";
                    }
                } else {
                    storageType = table.isPersistIndexes() ? "CACHED" : "MEMORY";
                }
                add(rows,
                        // TABLE_CATALOG
                        catalog,
                        // TABLE_SCHEMA
                        identifier(table.getSchema().getName()),
                        // TABLE_NAME
                        tableName,
                        // TABLE_TYPE
                        table.getTableType(),
                        // STORAGE_TYPE
                        storageType,
                        // SQL
                        table.getCreateSQL(),
                        // REMARKS
                        replaceNullWithEmpty(table.getComment()),
                        // LAST_MODIFICATION
                        "" + table.getMaxDataModificationId(),
                        // ID
                        "" + table.getId(),
                        // TYPE_NAME
                        null
                );
            }
            break;
        }
        case COLUMNS: {
            for (Table table : getAllTables(session)) {
                String tableName = identifier(table.getName());
                if (!checkIndex(session, tableName, indexFrom, indexTo)) {
                    continue;
                }
                Column[] cols = table.getColumns();
                String collation = database.getCompareMode().getName();
                for (int j = 0; j < cols.length; j++) {
                    Column c = cols[j];
                    Sequence sequence = c.getSequence();
                    add(rows,
                            // TABLE_CATALOG
                            catalog,
                            // TABLE_SCHEMA
                            identifier(table.getSchema().getName()),
                            // TABLE_NAME
                            tableName,
                            // COLUMN_NAME
                            identifier(c.getName()),
                            // ORDINAL_POSITION
                            String.valueOf(j + 1),
                            // COLUMN_DEFAULT
                            c.getDefaultSQL(),
                            // IS_NULLABLE
                            c.isNullable() ? "YES" : "NO",
                            // DATA_TYPE
                            "" + DataType.convertTypeToSQLType(c.getType()),
                            // CHARACTER_MAXIMUM_LENGTH
                            "" + c.getPrecisionAsInt(),
                            // CHARACTER_OCTET_LENGTH
                            "" + c.getPrecisionAsInt(),
                            // NUMERIC_PRECISION
                            "" + c.getPrecisionAsInt(),
                            // NUMERIC_PRECISION_RADIX
                            "10",
                            // NUMERIC_SCALE
                            "" + c.getScale(),
                            // CHARACTER_SET_NAME
                            Constants.CHARACTER_SET_NAME,
                            // COLLATION_NAME
                            collation,
                            // TYPE_NAME
                            identifier(DataType.getDataType(c.getType()).name),
                            // NULLABLE
                            "" + (c.isNullable() ? DatabaseMetaData.columnNullable : DatabaseMetaData.columnNoNulls) ,
                            // IS_COMPUTED
                            "" + (c.getComputed() ? "TRUE" : "FALSE"),
                            // SELECTIVITY
                            "" + (c.getSelectivity()),
                            // CHECK_CONSTRAINT
                            c.getCheckConstraintSQL(session, c.getName()),
                            // SEQUENCE_NAME
                            sequence == null ? null : sequence.getName(),
                            // REMARKS
                            replaceNullWithEmpty(c.getComment()),
                            // SOURCE_DATA_TYPE
                            null
                    );
                }
            }
            break;
        }
        case INDEXES: {
            for (Table table : getAllTables(session)) {
                String tableName = identifier(table.getName());
                if (!checkIndex(session, tableName, indexFrom, indexTo)) {
                    continue;
                }
                ObjectArray<Index> indexes = table.getIndexes();
                ObjectArray<Constraint> constraints = table.getConstraints();
                for (int j = 0; indexes != null && j < indexes.size(); j++) {
                    Index index = indexes.get(j);
                    if (index.getCreateSQL() == null) {
                        continue;
                    }
                    String constraintName = null;
                    for (int k = 0; constraints != null && k < constraints.size(); k++) {
                        Constraint constraint = constraints.get(k);
                        if (constraint.getUniqueIndex() == index) {
                            if (index.getIndexType().isPrimaryKey()) {
                                if (constraint.getConstraintType().equals(Constraint.PRIMARY_KEY)) {
                                    constraintName = constraint.getName();
                                }
                            } else {
                                constraintName = constraint.getName();
                            }
                        }
                    }
                    IndexColumn[] cols = index.getIndexColumns();
                    for (int k = 0; k < cols.length; k++) {
                        IndexColumn idxCol = cols[k];
                        Column column = idxCol.column;
                        add(rows,
                                // TABLE_CATALOG
                                catalog,
                                // TABLE_SCHEMA
                                identifier(table.getSchema().getName()),
                                // TABLE_NAME
                                tableName,
                                // NON_UNIQUE
                                index.getIndexType().isUnique() ? "FALSE" : "TRUE",
                                        // INDEX_NAME
                                identifier(index.getName()),
                                // ORDINAL_POSITION
                                "" + (k+1),
                                // COLUMN_NAME
                                identifier(column.getName()),
                                // CARDINALITY
                                "0",
                                // PRIMARY_KEY
                                index.getIndexType().isPrimaryKey() ? "TRUE" : "FALSE",
                                // INDEX_TYPE_NAME
                                index.getIndexType().getSQL(),
                                // IS_GENERATED
                                index.getIndexType().getBelongsToConstraint() ? "TRUE" : "FALSE",
                                // INDEX_TYPE
                                "" + DatabaseMetaData.tableIndexOther,
                                // ASC_OR_DESC
                                (idxCol.sortType & SortOrder.DESCENDING) != 0 ? "D" : "A",
                                // PAGES
                                "0",
                                // FILTER_CONDITION
                                "",
                                // REMARKS
                                replaceNullWithEmpty(index.getComment()),
                                // SQL
                                index.getSQL(),
                                // ID
                                "" + index.getId(),
                                // SORT_TYPE
                                "" + idxCol.sortType,
                                // CONSTRAINT_NAME
                                constraintName
                            );
                    }
                }
            }
            break;
        }
        case TABLE_TYPES: {
            add(rows, Table.TABLE);
            add(rows, Table.TABLE_LINK);
            add(rows, Table.SYSTEM_TABLE);
            add(rows, Table.VIEW);
            break;
        }
        case CATALOGS: {
            add(rows, catalog);
            break;
        }
        case SETTINGS: {
            for (Setting s : database.getAllSettings()) {
                String value = s.getStringValue();
                if (value == null) {
                    value = "" + s.getIntValue();
                }
                add(rows,
                        identifier(s.getName()),
                        value
                );
            }
            add(rows, "info.BUILD_ID", "" + Constants.BUILD_ID);
            add(rows, "info.VERSION_MAJOR", "" + Constants.VERSION_MAJOR);
            add(rows, "info.VERSION_MINOR", "" + Constants.VERSION_MINOR);
            add(rows, "info.VERSION", "" + Constants.getFullVersion());
            if (session.getUser().isAdmin()) {
                String[] settings = new String[]{
                        "java.runtime.version",
                        "java.vm.name", "java.vendor",
                        "os.name", "os.arch", "os.version", "sun.os.patch.level",
                        "file.separator", "path.separator", "line.separator",
                        "user.country", "user.language", "user.variant", "file.encoding"
                };
                for (String s : settings) {
                    add(rows, "property." + s, SysProperties.getStringSetting(s, ""));
                }
            }
            add(rows, "EXCLUSIVE", database.getExclusiveSession() == null ? "FALSE" : "TRUE");
            add(rows, "MODE", database.getMode().getName());
            add(rows, "MULTI_THREADED", database.isMultiThreaded() ? "1" : "0");
            add(rows, "MVCC", database.isMultiVersion() ? "TRUE" : "FALSE");
            add(rows, "QUERY_TIMEOUT", "" + session.getQueryTimeout());
            // the setting for the current database
            add(rows, "LOB_FILES_IN_DIRECTORIES", "" + database.getLobFilesInDirectories());
            add(rows, "h2.allowBigDecimalExtensions", "" + SysProperties.ALLOW_BIG_DECIMAL_EXTENSIONS);
            add(rows, "h2.baseDir", "" + SysProperties.getBaseDir());
            add(rows, "h2.check", "" + SysProperties.CHECK);
            add(rows, "h2.check2", "" + SysProperties.CHECK2);
            add(rows, "h2.clientTraceDirectory", SysProperties.CLIENT_TRACE_DIRECTORY);
            add(rows, SysProperties.H2_COLLATOR_CACHE_SIZE, "" + SysProperties.getCollatorCacheSize());
            add(rows, "h2.defaultMaxMemoryUndo", "" + SysProperties.DEFAULT_MAX_MEMORY_UNDO);
            add(rows, "h2.lobFilesInDirectories", "" + SysProperties.LOB_FILES_IN_DIRECTORIES);
            add(rows, "h2.lobFilesPerDirectory", "" + SysProperties.LOB_FILES_PER_DIRECTORY);
            add(rows, "h2.logAllErrors", "" + SysProperties.LOG_ALL_ERRORS);
            add(rows, "h2.logAllErrorsFile", "" + SysProperties.LOG_ALL_ERRORS_FILE);
            add(rows, "h2.maxFileRetry", "" + SysProperties.MAX_FILE_RETRY);
            add(rows, SysProperties.H2_MAX_QUERY_TIMEOUT, "" + SysProperties.getMaxQueryTimeout());
            add(rows, "h2.lobCloseBetweenReads", "" + SysProperties.lobCloseBetweenReads);
            add(rows, "h2.objectCache", "" + SysProperties.OBJECT_CACHE);
            add(rows, "h2.objectCacheSize", "" + SysProperties.OBJECT_CACHE_SIZE);
            add(rows, "h2.objectCacheMaxPerElementSize", "" + SysProperties.OBJECT_CACHE_MAX_PER_ELEMENT_SIZE);
            add(rows, "h2.optimizeIn", "" + SysProperties.OPTIMIZE_IN);
            add(rows, "h2.optimizeInJoin", "" + SysProperties.optimizeInJoin);
            add(rows, "h2.optimizeInList", "" + SysProperties.OPTIMIZE_IN_LIST);
            add(rows, "h2.optimizeMinMax", "" + SysProperties.OPTIMIZE_MIN_MAX);
            add(rows, "h2.optimizeSubqueryCache", "" + SysProperties.OPTIMIZE_SUBQUERY_CACHE);
            add(rows, "h2.overflowExceptions", "" + SysProperties.OVERFLOW_EXCEPTIONS);
            add(rows, "h2.recompileAlways", "" + SysProperties.RECOMPILE_ALWAYS);
            add(rows, "h2.redoBufferSize", "" + SysProperties.REDO_BUFFER_SIZE);
            add(rows, "h2.runFinalize", "" + SysProperties.runFinalize);
            add(rows, "h2.scriptDirectory", SysProperties.getScriptDirectory());
            add(rows, "h2.serverCachedObjects", "" + SysProperties.SERVER_CACHED_OBJECTS);
            add(rows, "h2.serverResultSetFetchSize", "" + SysProperties.SERVER_RESULT_SET_FETCH_SIZE);
            add(rows, "h2.sortNullsHigh", "" + SysProperties.SORT_NULLS_HIGH);
            DiskFile dataFile = database.getDataFile();
            if (dataFile != null) {
                add(rows, "CACHE_TYPE", dataFile.getCache().getTypeName());
                if (session.getUser().isAdmin()) {
                    DiskFile indexFile = database.getIndexFile();
                    add(rows, "info.FILE_DISK_WRITE", "" + dataFile.getWriteCount());
                    add(rows, "info.FILE_DISK_READ", "" + dataFile.getReadCount());
                    add(rows, "info.FILE_INDEX_WRITE", "" + database.getIndexFile().getWriteCount());
                    add(rows, "info.FILE_INDEX_READ", "" + database.getIndexFile().getReadCount());
                    add(rows, "info.CACHE_DATA_MAX_SIZE", "" + dataFile.getCache().getMaxSize());
                    add(rows, "info.CACHE_DATA_SIZE", "" + dataFile.getCache().getSize());
                    add(rows, "info.CACHE_INDEX_MAX_SIZE", "" + indexFile.getCache().getMaxSize());
                    add(rows, "info.CACHE_INDEX_SIZE", "" + indexFile.getCache().getSize());
                }
            }
            break;
        }
        case TYPE_INFO: {
            for (DataType t : DataType.getTypes()) {
                if (t.hidden || t.sqlType == Value.NULL) {
                    continue;
                }
                add(rows,
                        // TYPE_NAME
                        t.name,
                        // DATA_TYPE
                        String.valueOf(t.sqlType),
                        // PRECISION
                        String.valueOf(MathUtils.convertLongToInt(t.maxPrecision)),
                        // PREFIX
                        t.prefix,
                        // SUFFIX
                        t.suffix,
                        // PARAMS
                        t.params,
                        // AUTO_INCREMENT
                        String.valueOf(t.autoIncrement),
                        // MINIMUM_SCALE
                        String.valueOf(t.minScale),
                        // MAXIMUM_SCALE
                        String.valueOf(t.maxScale),
                        // RADIX
                        t.decimal ? "10" : null,
                        // POS
                        String.valueOf(t.sqlTypePos),
                        // CASE_SENSITIVE
                        String.valueOf(t.caseSensitive),
                        // NULLABLE
                        "" + DatabaseMetaData.typeNullable,
                        // SEARCHABLE
                        "" + DatabaseMetaData.typeSearchable
                );
            }
            break;
        }
        case HELP: {
            String resource = "/org/h2/res/help.csv";
            try {
                byte[] data = Resources.get(resource);
                Reader reader = new InputStreamReader(new ByteArrayInputStream(data));
                ResultSet rs = Csv.getInstance().read(reader, null);
                for (int i = 0; rs.next(); i++) {
                    add(rows,
                        // ID
                        String.valueOf(i),
                        // SECTION
                        rs.getString(1).trim(),
                        // TOPIC
                        rs.getString(2).trim(),
                        // SYNTAX
                        rs.getString(3).trim(),
                        // TEXT
                        rs.getString(4).trim()
                    );
                }
            } catch (IOException e) {
                throw Message.convertIOException(e, resource);
            }
            break;
        }
        case SEQUENCES: {
            for (SchemaObject obj : database.getAllSchemaObjects(DbObject.SEQUENCE)) {
                Sequence s = (Sequence) obj;
                add(rows,
                        // SEQUENCE_CATALOG
                        catalog,
                        // SEQUENCE_SCHEMA
                        identifier(s.getSchema().getName()),
                        // SEQUENCE_NAME
                        identifier(s.getName()),
                        // CURRENT_VALUE
                        String.valueOf(s.getCurrentValue()),
                        // INCREMENT
                        String.valueOf(s.getIncrement()),
                        // IS_GENERATED
                        s.getBelongsToTable() ? "TRUE" : "FALSE",
                                // REMARKS
                        replaceNullWithEmpty(s.getComment()),
                        // CACHE
                        String.valueOf(s.getCacheSize()),
                        // ID
                        "" + s.getId()
                    );
            }
            break;
        }
        case USERS: {
            for (User u : database.getAllUsers()) {
                add(rows,
                        // NAME
                        identifier(u.getName()),
                        // ADMIN
                        String.valueOf(u.isAdmin()),
                        // REMARKS
                        replaceNullWithEmpty(u.getComment()),
                        // ID
                        "" + u.getId()
                );
            }
            break;
        }
        case ROLES: {
            for (Role r : database.getAllRoles()) {
                add(rows,
                        // NAME
                        identifier(r.getName()),
                        // REMARKS
                        replaceNullWithEmpty(r.getComment()),
                        // ID
                        "" + r.getId()
                );
            }
            break;
        }
        case RIGHTS: {
            for (Right r : database.getAllRights()) {
                Role role = r.getGrantedRole();
                DbObject grantee = r.getGrantee();
                String type = grantee.getType() == DbObject.USER ? "USER" : "ROLE";
                if (role == null) {
                    Table granted = r.getGrantedTable();
                    String tableName = identifier(granted.getName());
                    if (!checkIndex(session, tableName, indexFrom, indexTo)) {
                        continue;
                    }
                    add(rows,
                            // GRANTEE
                            identifier(grantee.getName()),
                            // GRANTEETYPE
                            type,
                            // GRANTEDROLE
                            "",
                            // RIGHTS
                            r.getRights(),
                            // TABLE_SCHEMA
                            identifier(granted.getSchema().getName()),
                            // TABLE_NAME
                            identifier(granted.getName()),
                            // ID
                            "" + r.getId()
                    );
                } else {
                    add(rows,
                            // GRANTEE
                            identifier(grantee.getName()),
                            // GRANTEETYPE
                            type,
                            // GRANTEDROLE
                            identifier(role.getName()),
                            // RIGHTS
                            "",
                            // TABLE_SCHEMA
                            "",
                            // TABLE_NAME
                            "",
                            // ID
                            "" + r.getId()
                    );
                }
            }
            break;
        }
        case FUNCTION_ALIASES: {
            for (FunctionAlias alias : database.getAllFunctionAliases()) {
                for (FunctionAlias.JavaMethod method : alias.getJavaMethods()) {
                    int returnsResult = method.getDataType() == Value.NULL ? DatabaseMetaData.procedureNoResult
                            : DatabaseMetaData.procedureReturnsResult;
                    add(rows,
                            // ALIAS_CATALOG
                            catalog,
                            // ALIAS_SCHEMA
                            Constants.SCHEMA_MAIN,
                            // ALIAS_NAME
                            identifier(alias.getName()),
                            // JAVA_CLASS
                            alias.getJavaClassName(),
                            // JAVA_METHOD
                            alias.getJavaMethodName(),
                            // DATA_TYPE
                            ""+DataType.convertTypeToSQLType(method.getDataType()),
                            // COLUMN_COUNT INT
                            ""+ method.getColumnClasses().length,
                            // RETURNS_RESULT SMALLINT
                            ""+ returnsResult,
                            // REMARKS
                            replaceNullWithEmpty(alias.getComment()),
                            // ID
                            "" + alias.getId()
                    );
                }
            }
            for (UserAggregate agg : database.getAllAggregates()) {
                int returnsResult = DatabaseMetaData.procedureReturnsResult;
                add(rows,
                        // ALIAS_CATALOG
                        catalog,
                        // ALIAS_SCHEMA
                        Constants.SCHEMA_MAIN,
                        // ALIAS_NAME
                        identifier(agg.getName()),
                        // JAVA_CLASS
                        agg.getJavaClassName(),
                        // JAVA_METHOD
                        "",
                        // DATA_TYPE
                        ""+DataType.convertTypeToSQLType(Value.NULL),
                        // COLUMN_COUNT INT
                        "1",
                        // RETURNS_RESULT SMALLINT
                        ""+ returnsResult,
                        // REMARKS
                        replaceNullWithEmpty(agg.getComment()),
                        // ID
                        "" + agg.getId()
                );
            }
            break;
        }
        case FUNCTION_COLUMNS: {
            for (FunctionAlias alias : database.getAllFunctionAliases()) {
                for (FunctionAlias.JavaMethod method : alias.getJavaMethods()) {
                    Class< ? >[] columns = method.getColumnClasses();
                    for (int k = 0; k < columns.length; k++) {
                        Class< ? > clazz = columns[k];
                        int type = DataType.getTypeFromClass(clazz);
                        DataType dt = DataType.getDataType(type);
                        int nullable = clazz.isPrimitive() ? DatabaseMetaData.columnNoNulls
                                : DatabaseMetaData.columnNullable;
                        add(rows,
                                // ALIAS_CATALOG
                                catalog,
                                // ALIAS_SCHEMA
                                Constants.SCHEMA_MAIN,
                                // ALIAS_NAME
                                identifier(alias.getName()),
                                // JAVA_CLASS
                                alias.getJavaClassName(),
                                // JAVA_METHOD
                                alias.getJavaMethodName(),
                                // COLUMN_COUNT
                                "" + method.getParameterCount(),
                                // POS INT
                                "" + k,
                                // COLUMN_NAME
                                "P" + (k+1),
                                // DATA_TYPE
                                "" + DataType.convertTypeToSQLType(dt.type),
                                // TYPE_NAME
                                dt.name,
                                // PRECISION
                                "" + dt.defaultPrecision,
                                // SCALE
                                "" + dt.defaultScale,
                                // RADIX
                                "10",
                                // NULLABLE SMALLINT
                                "" + nullable,
                                // COLUMN_TYPE
                                "" + DatabaseMetaData.procedureColumnIn,
                                // REMARKS
                                "",
                                // COLUMN_DEFAULT
                                null
                        );
                    }
                }
            }
            break;
        }
        case SCHEMATA: {
            String collation = database.getCompareMode().getName();
            for (Schema schema : database.getAllSchemas()) {
                add(rows,
                        // CATALOG_NAME
                        catalog,
                        // SCHEMA_NAME
                        identifier(schema.getName()),
                        // SCHEMA_OWNER
                        identifier(schema.getOwner().getName()),
                        // DEFAULT_CHARACTER_SET_NAME
                        Constants.CHARACTER_SET_NAME,
                        // DEFAULT_COLLATION_NAME
                        collation,
                        // IS_DEFAULT
                        Constants.SCHEMA_MAIN.equals(schema.getName()) ? "TRUE" : "FALSE",
                        // REMARKS
                        replaceNullWithEmpty(schema.getComment()),
                        // ID
                        "" + schema.getId()
                );
            }
            break;
        }
        case TABLE_PRIVILEGES: {
            for (Right r : database.getAllRights()) {
                Table table = r.getGrantedTable();
                if (table == null) {
                    continue;
                }
                String tableName = identifier(table.getName());
                if (!checkIndex(session, tableName, indexFrom, indexTo)) {
                    continue;
                }
                addPrivileges(rows, r.getGrantee(), catalog, table, null, r.getRightMask());
            }
            break;
        }
        case COLUMN_PRIVILEGES: {
            for (Right r : database.getAllRights()) {
                Table table = r.getGrantedTable();
                if (table == null) {
                    continue;
                }
                String tableName = identifier(table.getName());
                if (!checkIndex(session, tableName, indexFrom, indexTo)) {
                    continue;
                }
                DbObject grantee = r.getGrantee();
                int mask = r.getRightMask();
                for (Column column : table.getColumns()) {
                    addPrivileges(rows, grantee, catalog, table, column.getName(), mask);
                }
            }
            break;
        }
        case COLLATIONS: {
            for (Locale l : Collator.getAvailableLocales()) {
                add(rows,
                        // NAME
                        CompareMode.getName(l),
                        // KEY
                        l.toString()
                );
            }
            break;
        }
        case VIEWS: {
            for (Table table : getAllTables(session)) {
                if (!table.getTableType().equals(Table.VIEW)) {
                    continue;
                }
                String tableName = identifier(table.getName());
                if (!checkIndex(session, tableName, indexFrom, indexTo)) {
                    continue;
                }
                TableView view = (TableView) table;
                add(rows,
                        // TABLE_CATALOG
                        catalog,
                        // TABLE_SCHEMA
                        identifier(table.getSchema().getName()),
                        // TABLE_NAME
                        tableName,
                        // VIEW_DEFINITION
                        table.getCreateSQL(),
                        // CHECK_OPTION
                        "NONE",
                        // IS_UPDATABLE
                        "NO",
                        // STATUS
                        view.isInvalid() ? "INVALID" : "VALID",
                        // REMARKS
                        replaceNullWithEmpty(view.getComment()),
                        // ID
                        "" + view.getId()
                );
            }
            break;
        }
        case IN_DOUBT: {
            ObjectArray<InDoubtTransaction> prepared = database.getLog().getInDoubtTransactions();
            for (int i = 0; prepared != null && i < prepared.size(); i++) {
                InDoubtTransaction prep = prepared.get(i);
                add(rows,
                        // TRANSACTION
                        prep.getTransaction(),
                        // STATE
                        prep.getState()
                );
            }
            break;
        }
        case CROSS_REFERENCES: {
            for (SchemaObject obj : database.getAllSchemaObjects(DbObject.CONSTRAINT)) {
                Constraint constraint = (Constraint) obj;
                if (!(constraint.getConstraintType().equals(Constraint.REFERENTIAL))) {
                    continue;
                }
                ConstraintReferential ref = (ConstraintReferential) constraint;
                IndexColumn[] cols = ref.getColumns();
                IndexColumn[] refCols = ref.getRefColumns();
                Table tab = ref.getTable();
                Table refTab = ref.getRefTable();
                String tableName = identifier(refTab.getName());
                if (!checkIndex(session, tableName, indexFrom, indexTo)) {
                    continue;
                }
                int update = getRefAction(ref.getUpdateAction());
                int delete = getRefAction(ref.getDeleteAction());
                for (int j = 0; j < cols.length; j++) {
                    add(rows,
                            // PKTABLE_CATALOG
                            catalog,
                            // PKTABLE_SCHEMA
                            identifier(refTab.getSchema().getName()),
                            // PKTABLE_NAME
                            identifier(refTab.getName()),
                            // PKCOLUMN_NAME
                            identifier(refCols[j].column.getName()),
                            // FKTABLE_CATALOG
                            catalog,
                            // FKTABLE_SCHEMA
                            identifier(tab.getSchema().getName()),
                            // FKTABLE_NAME
                            identifier(tab.getName()),
                            // FKCOLUMN_NAME
                            identifier(cols[j].column.getName()),
                            // ORDINAL_POSITION
                            String.valueOf(j + 1),
                            // UPDATE_RULE SMALLINT
                            String.valueOf(update),
                            // DELETE_RULE SMALLINT
                            String.valueOf(delete),
                            // FK_NAME
                            identifier(ref.getName()),
                            // PK_NAME
                            null,
                            // DEFERRABILITY
                            "" + DatabaseMetaData.importedKeyNotDeferrable
                    );
                }
            }
            break;
        }
        case CONSTRAINTS: {
            for (SchemaObject obj : database.getAllSchemaObjects(DbObject.CONSTRAINT)) {
                Constraint constraint = (Constraint) obj;
                String type = constraint.getConstraintType();
                String checkExpression = null;
                IndexColumn[] columns = null;
                Table table = constraint.getTable();
                Index index = constraint.getUniqueIndex();
                String uniqueIndexName = null;
                if (index != null) {
                    uniqueIndexName = index.getName();
                }
                String tableName = identifier(table.getName());
                if (!checkIndex(session, tableName, indexFrom, indexTo)) {
                    continue;
                }
                if (type.equals(Constraint.CHECK)) {
                    checkExpression = ((ConstraintCheck) constraint).getExpression().getSQL();
                } else if (type.equals(Constraint.UNIQUE) || type.equals(Constraint.PRIMARY_KEY)) {
                    columns = ((ConstraintUnique) constraint).getColumns();
                } else if (type.equals(Constraint.REFERENTIAL)) {
                    columns = ((ConstraintReferential) constraint).getColumns();
                }
                String columnList = null;
                if (columns != null) {
                    StatementBuilder buff = new StatementBuilder();
                    for (IndexColumn col : columns) {
                        buff.appendExceptFirst(",");
                        buff.append(col.column.getName());
                    }
                    columnList = buff.toString();
                }
                add(rows,
                        // CONSTRAINT_CATALOG
                        catalog,
                        // CONSTRAINT_SCHEMA
                        identifier(constraint.getSchema().getName()),
                        // CONSTRAINT_NAME
                        identifier(constraint.getName()),
                        // CONSTRAINT_TYPE
                        type,
                        // TABLE_CATALOG
                        catalog,
                        // TABLE_SCHEMA
                        identifier(table.getSchema().getName()),
                        // TABLE_NAME
                        tableName,
                        // UNIQUE_INDEX_NAME
                        uniqueIndexName,
                        // CHECK_EXPRESSION
                        checkExpression,
                        // COLUMN_LIST
                        columnList,
                        // REMARKS
                        replaceNullWithEmpty(constraint.getComment()),
                        // SQL
                        constraint.getCreateSQL(),
                        // ID
                        "" + constraint.getId()
                    );
            }
            break;
        }
        case CONSTANTS: {
            for (SchemaObject obj : database.getAllSchemaObjects(DbObject.CONSTANT)) {
                Constant constant = (Constant) obj;
                ValueExpression expr = constant.getValue();
                add(rows,
                        // CONSTANT_CATALOG
                        catalog,
                        // CONSTANT_SCHEMA
                        identifier(constant.getSchema().getName()),
                        // CONSTANT_NAME
                        identifier(constant.getName()),
                        // CONSTANT_TYPE
                        "" + DataType.convertTypeToSQLType(expr.getType()),
                        // REMARKS
                        replaceNullWithEmpty(constant.getComment()),
                        // SQL
                        expr.getSQL(),
                        // ID
                        "" + constant.getId()
                    );
            }
            break;
        }
        case DOMAINS: {
            for (UserDataType dt : database.getAllUserDataTypes()) {
                Column col = dt.getColumn();
                add(rows,
                        // DOMAIN_CATALOG
                        catalog,
                        // DOMAIN_SCHEMA
                        Constants.SCHEMA_MAIN,
                        // DOMAIN_NAME
                        identifier(dt.getName()),
                        // COLUMN_DEFAULT
                        col.getDefaultSQL(),
                        // IS_NULLABLE
                        col.isNullable() ? "YES" : "NO",
                        // DATA_TYPE
                        "" + col.getDataType().sqlType,
                        // PRECISION INT
                        "" + col.getPrecisionAsInt(),
                        // SCALE INT
                        "" + col.getScale(),
                        // TYPE_NAME
                        col.getDataType().name,
                        // SELECTIVITY INT
                        "" + col.getSelectivity(),
                        // CHECK_CONSTRAINT
                        "" + col.getCheckConstraintSQL(session, "VALUE"),
                        // REMARKS
                        replaceNullWithEmpty(dt.getComment()),
                        // SQL
                        "" + dt.getCreateSQL(),
                        // ID
                        "" + dt.getId()
                );
            }
            break;
        }
        case TRIGGERS: {
            for (SchemaObject obj : database.getAllSchemaObjects(DbObject.TRIGGER)) {
                TriggerObject trigger = (TriggerObject) obj;
                Table table = trigger.getTable();
                add(rows,
                        // TRIGGER_CATALOG
                        catalog,
                        // TRIGGER_SCHEMA
                        identifier(trigger.getSchema().getName()),
                        // TRIGGER_NAME
                        identifier(trigger.getName()),
                        // TRIGGER_TYPE
                        trigger.getTypeNameList(),
                        // TABLE_CATALOG
                        catalog,
                        // TABLE_SCHEMA
                        identifier(table.getSchema().getName()),
                        // TABLE_NAME
                        identifier(table.getName()),
                        // BEFORE BIT
                        "" + trigger.isBefore(),
                        // JAVA_CLASS
                        trigger.getTriggerClassName(),
                        // QUEUE_SIZE INT
                        "" + trigger.getQueueSize(),
                        // NO_WAIT BIT
                        "" + trigger.isNoWait(),
                        // REMARKS
                        replaceNullWithEmpty(trigger.getComment()),
                        // SQL
                        trigger.getCreateSQL(),
                        // ID
                        "" + trigger.getId()
                );
            }
            break;
        }
        case SESSIONS: {
            boolean admin = session.getUser().isAdmin();
            for (Session s : database.getSessions(false)) {
                if (admin || s == session) {
                    Command command = s.getCurrentCommand();
                    add(rows,
                            // ID
                            "" + s.getId(),
                            // USER_NAME
                            s.getUser().getName(),
                            // SESSION_START
                            new Timestamp(s.getSessionStart()).toString(),
                            // STATEMENT
                            command == null ? null : command.toString(),
                            // STATEMENT_START
                            new Timestamp(s.getCurrentCommandStart()).toString()
                    );
                }
            }
            break;
        }
        case LOCKS: {
            boolean admin = session.getUser().isAdmin();
            for (Session s : database.getSessions(false)) {
                if (admin || s == session) {
                    for (Table table : s.getLocks()) {
                        add(rows,
                                // TABLE_SCHEMA
                                table.getSchema().getName(),
                                // TABLE_NAME
                                table.getName(),
                                // SESSION_ID
                                "" + s.getId(),
                                // LOCK_TYPE
                                table.isLockedExclusivelyBy(s) ? "WRITE" : "READ"
                        );
                    }
                }
            }
            break;
        }
        case SESSION_STATE: {
            for (String name : session.getVariableNames()) {
                Value v = session.getVariable(name);
                add(rows,
                        // KEY
                        "@" + name,
                        // SQL
                        "SET @" + name + " " + v.getSQL()
                );
            }
            for (Table table : session.getLocalTempTables()) {
                add(rows,
                        // KEY
                        "TABLE " + table.getName(),
                        // SQL
                        table.getCreateSQL()
                );
            }
            String[] path = session.getSchemaSearchPath();
            if (path != null && path.length > 0) {
                StatementBuilder buff = new StatementBuilder("SET SCHEMA_SEARCH_PATH ");
                for (String p : path) {
                    buff.appendExceptFirst(", ");
                    buff.append(StringUtils.quoteIdentifier(p));
                }
                add(rows,
                        // KEY
                        "SCHEMA_SEARCH_PATH",
                        // SQL
                        buff.toString()
                );
            }
            String schema = session.getCurrentSchemaName();
            if (schema != null) {
                add(rows,
                        // KEY
                        "SCHEMA",
                        // SQL
                        "SET SCHEMA " + StringUtils.quoteIdentifier(schema)
                );
            }
            break;
        }
        default:
            Message.throwInternalError("type="+type);
        }
        return rows;
    }

    private int getRefAction(int action) {
        switch(action) {
        case ConstraintReferential.CASCADE:
            return DatabaseMetaData.importedKeyCascade;
        case ConstraintReferential.RESTRICT:
            return DatabaseMetaData.importedKeyRestrict;
        case ConstraintReferential.SET_DEFAULT:
            return DatabaseMetaData.importedKeySetDefault;
        case ConstraintReferential.SET_NULL:
            return DatabaseMetaData.importedKeySetNull;
        default:
            throw Message.throwInternalError("action="+action);
        }
    }

    public void removeRow(Session session, Row row) throws SQLException {
        throw Message.getUnsupportedException("META");
    }

    public void addRow(Session session, Row row) throws SQLException {
        throw Message.getUnsupportedException("META");
    }

    public void removeChildrenAndResources(Session session) throws SQLException {
        throw Message.getUnsupportedException("META");
    }

    public void close(Session session) {
        // nothing to do
    }

    public void unlock(Session s) {
        // nothing to do
    }

    private void addPrivileges(ObjectArray<Row> rows, DbObject grantee, String catalog, Table table, String column,
            int rightMask) throws SQLException {
        if ((rightMask & Right.SELECT) != 0) {
            addPrivilege(rows, grantee, catalog, table, column, "SELECT");
        }
        if ((rightMask & Right.INSERT) != 0) {
            addPrivilege(rows, grantee, catalog, table, column, "INSERT");
        }
        if ((rightMask & Right.UPDATE) != 0) {
            addPrivilege(rows, grantee, catalog, table, column, "UPDATE");
        }
        if ((rightMask & Right.DELETE) != 0) {
            addPrivilege(rows, grantee, catalog, table, column, "DELETE");
        }
    }

    private void addPrivilege(ObjectArray<Row> rows, DbObject grantee, String catalog, Table table, String column,
            String right) throws SQLException {
        String isGrantable = "NO";
        if (grantee.getType() == DbObject.USER) {
            User user = (User) grantee;
            if (user.isAdmin()) {
                // the right is grantable if the grantee is an admin
                isGrantable = "YES";
            }
        }
        if (column == null) {
            add(rows,
                    // GRANTOR
                    null,
                    // GRANTEE
                    identifier(grantee.getName()),
                    // TABLE_CATALOG
                    catalog,
                    // TABLE_SCHEMA
                    identifier(table.getSchema().getName()),
                    // TABLE_NAME
                    identifier(table.getName()),
                    // PRIVILEGE_TYPE
                    right,
                    // IS_GRANTABLE
                    isGrantable
            );
        } else {
            add(rows,
                    // GRANTOR
                    null,
                    // GRANTEE
                    identifier(grantee.getName()),
                    // TABLE_CATALOG
                    catalog,
                    // TABLE_SCHEMA
                    identifier(table.getSchema().getName()),
                    // TABLE_NAME
                    identifier(table.getName()),
                    // COLUMN_NAME
                    identifier(column),
                    // PRIVILEGE_TYPE
                    right,
                    // IS_GRANTABLE
                    isGrantable
            );
        }
    }

    private void add(ObjectArray<Row> rows, String... strings) throws SQLException {
        Value[] values = new Value[strings.length];
        for (int i = 0; i < strings.length; i++) {
            String s = strings[i];
            Value v = (s == null) ? (Value) ValueNull.INSTANCE : ValueString.get(s);
            Column col = columns[i];
            v = v.convertTo(col.getType());
            values[i] = v;
        }
        Row row = new Row(values, 0);
        row.setPos(rows.size());
        rows.add(row);
    }

    public void checkRename() throws SQLException {
        throw Message.getUnsupportedException("META");
    }

    public void checkSupportAlter() throws SQLException {
        throw Message.getUnsupportedException("META");
    }

    public void truncate(Session session) throws SQLException {
        throw Message.getUnsupportedException("META");
    }

    public long getRowCount(Session session) {
        throw Message.throwInternalError();
    }

    public boolean canGetRowCount() {
        return false;
    }

    public boolean canDrop() {
        return false;
    }

    public String getTableType() {
        return Table.SYSTEM_TABLE;
    }

    public Index getScanIndex(Session session) {
        return new MetaIndex(this, IndexColumn.wrap(columns), true);
    }

    public ObjectArray<Index> getIndexes() {
        if (index == null) {
            return null;
        }
        ObjectArray <Index>list = ObjectArray.newInstance();
        list.add(new MetaIndex(this, IndexColumn.wrap(columns), true));
        // TODO fixed scan index
        list.add(index);
        return list;
    }

    public long getMaxDataModificationId() {
        return database.getModificationDataId();
    }

    public Index getUniqueIndex() {
        return null;
    }

    /**
     * Get the number of meta table types. Supported meta table
     * types are 0 .. this value - 1.
     *
     * @return the number of meta table types
     */
    public static int getMetaTableTypeCount() {
        return META_TABLE_TYPE_COUNT;
    }

    public long getRowCountApproximation() {
        return ROW_COUNT_APPROXIMATION;
    }

    public boolean isDeterministic() {
        return true;
    }

}
