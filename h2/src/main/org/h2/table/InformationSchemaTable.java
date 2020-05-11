/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Types;
import java.text.Collator;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;

import org.h2.command.Command;
import org.h2.command.Parser;
import org.h2.constraint.Constraint;
import org.h2.constraint.Constraint.Type;
import org.h2.constraint.ConstraintActionType;
import org.h2.constraint.ConstraintCheck;
import org.h2.constraint.ConstraintDomain;
import org.h2.constraint.ConstraintReferential;
import org.h2.constraint.ConstraintUnique;
import org.h2.engine.Constants;
import org.h2.engine.DbObject;
import org.h2.engine.FunctionAlias;
import org.h2.engine.FunctionAlias.JavaMethod;
import org.h2.engine.QueryStatisticsData;
import org.h2.engine.Right;
import org.h2.engine.Role;
import org.h2.engine.Session;
import org.h2.engine.Session.State;
import org.h2.engine.Setting;
import org.h2.engine.User;
import org.h2.engine.UserAggregate;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.ValueExpression;
import org.h2.index.Index;
import org.h2.index.MetaIndex;
import org.h2.message.DbException;
import org.h2.mvstore.FileStore;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.db.MVTableEngine.Store;
import org.h2.pagestore.PageStore;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.schema.Constant;
import org.h2.schema.Domain;
import org.h2.schema.Schema;
import org.h2.schema.SchemaObject;
import org.h2.schema.Sequence;
import org.h2.schema.TriggerObject;
import org.h2.store.InDoubtTransaction;
import org.h2.tools.Csv;
import org.h2.util.DateTimeUtils;
import org.h2.util.MathUtils;
import org.h2.util.NetworkConnectionInfo;
import org.h2.util.StringUtils;
import org.h2.util.TimeZoneProvider;
import org.h2.util.Utils;
import org.h2.value.CompareMode;
import org.h2.value.DataType;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueBigint;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueDouble;
import org.h2.value.ValueInteger;
import org.h2.value.ValueSmallint;

/**
 * This class is responsible to build the INFORMATION_SCHEMA tables.
 */
public final class InformationSchemaTable extends MetaTable {

    private static final String CHARACTER_SET_NAME = "Unicode";

    private static final int TABLES = 0;
    private static final int COLUMNS = TABLES + 1;
    private static final int INDEXES = COLUMNS + 1;
    private static final int TABLE_TYPES = INDEXES + 1;
    private static final int TYPE_INFO = TABLE_TYPES + 1;
    private static final int CATALOGS = TYPE_INFO + 1;
    private static final int SETTINGS = CATALOGS + 1;
    private static final int HELP = SETTINGS + 1;
    private static final int SEQUENCES = HELP + 1;
    private static final int USERS = SEQUENCES + 1;
    private static final int ROLES = USERS + 1;
    private static final int RIGHTS = ROLES + 1;
    private static final int FUNCTION_ALIASES = RIGHTS + 1;
    private static final int SCHEMATA = FUNCTION_ALIASES + 1;
    private static final int TABLE_PRIVILEGES = SCHEMATA + 1;
    private static final int COLUMN_PRIVILEGES = TABLE_PRIVILEGES + 1;
    private static final int COLLATIONS = COLUMN_PRIVILEGES + 1;
    private static final int VIEWS = COLLATIONS + 1;
    private static final int IN_DOUBT = VIEWS + 1;
    private static final int CROSS_REFERENCES = IN_DOUBT + 1;
    private static final int FUNCTION_COLUMNS = CROSS_REFERENCES + 1;
    private static final int CONSTANTS = FUNCTION_COLUMNS + 1;
    private static final int DOMAINS = CONSTANTS + 1;
    private static final int TRIGGERS = DOMAINS + 1;
    private static final int SESSIONS = TRIGGERS + 1;
    private static final int LOCKS = SESSIONS + 1;
    private static final int SESSION_STATE = LOCKS + 1;
    private static final int QUERY_STATISTICS = SESSION_STATE + 1;
    private static final int SYNONYMS = QUERY_STATISTICS + 1;
    private static final int TABLE_CONSTRAINTS = SYNONYMS + 1;
    private static final int DOMAIN_CONSTRAINTS = TABLE_CONSTRAINTS + 1;
    private static final int KEY_COLUMN_USAGE = DOMAIN_CONSTRAINTS + 1;
    private static final int REFERENTIAL_CONSTRAINTS = KEY_COLUMN_USAGE + 1;
    private static final int CHECK_CONSTRAINTS = REFERENTIAL_CONSTRAINTS + 1;
    private static final int CONSTRAINT_COLUMN_USAGE = CHECK_CONSTRAINTS + 1;

    private static final int META_TABLE_TYPE_COUNT = CONSTRAINT_COLUMN_USAGE + 1;

    /**
     * Get the number of meta table types. Supported meta table
     * types are 0 .. this value - 1.
     *
     * @return the number of meta table types
     */
    public static int getMetaTableTypeCount() {
        return META_TABLE_TYPE_COUNT;
    }

    /**
     * Create a new metadata table.
     *
     * @param schema the schema
     * @param id the object id
     * @param type the meta table type
     */
    public InformationSchemaTable(Schema schema, int id, int type) {
        super(schema, id, type);
        Column[] cols;
        String indexColumnName = null;
        switch (type) {
        case TABLES:
            setMetaTableName("TABLES");
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
                    "TYPE_NAME",
                    "TABLE_CLASS",
                    "ROW_COUNT_ESTIMATE BIGINT"
            );
            indexColumnName = "TABLE_NAME";
            break;
        case COLUMNS:
            setMetaTableName("COLUMNS");
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
                    "DATETIME_PRECISION INT",
                    "INTERVAL_TYPE",
                    "INTERVAL_PRECISION INT",
                    "CHARACTER_SET_NAME",
                    "COLLATION_NAME",
                    "DOMAIN_CATALOG",
                    "DOMAIN_SCHEMA",
                    "DOMAIN_NAME",
                    "IS_GENERATED",
                    "GENERATION_EXPRESSION",
                    // extensions
                    "TYPE_NAME",
                    "NULLABLE INT",
                    "IS_COMPUTED BIT",
                    "SELECTIVITY INT",
                    "SEQUENCE_NAME",
                    "REMARKS",
                    "SOURCE_DATA_TYPE SMALLINT",
                    "COLUMN_TYPE",
                    "COLUMN_ON_UPDATE",
                    "IS_VISIBLE"
            );
            indexColumnName = "TABLE_NAME";
            break;
        case INDEXES:
            setMetaTableName("INDEXES");
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
                    "CONSTRAINT_NAME",
                    "INDEX_CLASS"
            );
            indexColumnName = "TABLE_NAME";
            break;
        case TABLE_TYPES:
            setMetaTableName("TABLE_TYPES");
            cols = createColumns("TYPE");
            break;
        case TYPE_INFO:
            setMetaTableName("TYPE_INFO");
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
            setMetaTableName("CATALOGS");
            cols = createColumns("CATALOG_NAME");
            break;
        case SETTINGS:
            setMetaTableName("SETTINGS");
            cols = createColumns("NAME", "VALUE");
            break;
        case HELP:
            setMetaTableName("HELP");
            cols = createColumns(
                    "ID INT",
                    "SECTION",
                    "TOPIC",
                    "SYNTAX",
                    "TEXT"
            );
            break;
        case SEQUENCES:
            setMetaTableName("SEQUENCES");
            cols = createColumns(
                    "SEQUENCE_CATALOG",
                    "SEQUENCE_SCHEMA",
                    "SEQUENCE_NAME",
                    "DATA_TYPE",
                    "NUMERIC_PRECISION INT",
                    "NUMERIC_PRECISION_RADIX INT",
                    "NUMERIC_SCALE INT",
                    "START_VALUE BIGINT",
                    "MINIMUM_VALUE BIGINT",
                    "MAXIMUM_VALUE BIGINT",
                    "INCREMENT BIGINT",
                    "CYCLE_OPTION",
                    "CURRENT_VALUE BIGINT",
                    "IS_GENERATED BIT",
                    "REMARKS",
                    "CACHE BIGINT",
                    "ID INT"
            );
            break;
        case USERS:
            setMetaTableName("USERS");
            cols = createColumns(
                    "NAME",
                    "ADMIN",
                    "REMARKS",
                    "ID INT"
            );
            break;
        case ROLES:
            setMetaTableName("ROLES");
            cols = createColumns(
                    "NAME",
                    "REMARKS",
                    "ID INT"
            );
            break;
        case RIGHTS:
            setMetaTableName("RIGHTS");
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
            setMetaTableName("FUNCTION_ALIASES");
            cols = createColumns(
                    "ALIAS_CATALOG",
                    "ALIAS_SCHEMA",
                    "ALIAS_NAME",
                    "JAVA_CLASS",
                    "JAVA_METHOD",
                    "DATA_TYPE INT",
                    "TYPE_NAME",
                    "COLUMN_COUNT INT",
                    "RETURNS_RESULT SMALLINT",
                    "REMARKS",
                    "ID INT",
                    "SOURCE"
            );
            break;
        case FUNCTION_COLUMNS:
            setMetaTableName("FUNCTION_COLUMNS");
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
            setMetaTableName("SCHEMATA");
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
            setMetaTableName("TABLE_PRIVILEGES");
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
            setMetaTableName("COLUMN_PRIVILEGES");
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
            setMetaTableName("COLLATIONS");
            cols = createColumns(
                    "NAME",
                    "KEY"
            );
            break;
        case VIEWS:
            setMetaTableName("VIEWS");
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
            setMetaTableName("IN_DOUBT");
            cols = createColumns(
                    "TRANSACTION",
                    "STATE"
            );
            break;
        case CROSS_REFERENCES:
            setMetaTableName("CROSS_REFERENCES");
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
        case CONSTANTS:
            setMetaTableName("CONSTANTS");
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
            setMetaTableName("DOMAINS");
            cols = createColumns(
                    "DOMAIN_CATALOG",
                    "DOMAIN_SCHEMA",
                    "DOMAIN_NAME",
                    "DOMAIN_DEFAULT",
                    "DOMAIN_ON_UPDATE",
                    "DATA_TYPE INT",
                    "PRECISION INT",
                    "SCALE INT",
                    "TYPE_NAME",
                    "PARENT_DOMAIN_CATALOG",
                    "PARENT_DOMAIN_SCHEMA",
                    "PARENT_DOMAIN_NAME",
                    "SELECTIVITY INT",
                    "REMARKS",
                    "SQL",
                    "ID INT"
            );
            break;
        case TRIGGERS:
            setMetaTableName("TRIGGERS");
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
            setMetaTableName("SESSIONS");
            cols = createColumns(
                    "ID INT",
                    "USER_NAME",
                    "SERVER",
                    "CLIENT_ADDR",
                    "CLIENT_INFO",
                    "SESSION_START TIMESTAMP WITH TIME ZONE",
                    "ISOLATION_LEVEL",
                    "STATEMENT",
                    "STATEMENT_START TIMESTAMP WITH TIME ZONE",
                    "CONTAINS_UNCOMMITTED BIT",
                    "STATE",
                    "BLOCKER_ID INT",
                    "SLEEP_SINCE TIMESTAMP WITH TIME ZONE"
            );
            break;
        }
        case LOCKS: {
            setMetaTableName("LOCKS");
            cols = createColumns(
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "SESSION_ID INT",
                    "LOCK_TYPE"
            );
            break;
        }
        case SESSION_STATE: {
            setMetaTableName("SESSION_STATE");
            cols = createColumns(
                    "KEY",
                    "SQL"
            );
            break;
        }
        case QUERY_STATISTICS: {
            setMetaTableName("QUERY_STATISTICS");
            cols = createColumns(
                    "SQL_STATEMENT",
                    "EXECUTION_COUNT INT",
                    "MIN_EXECUTION_TIME DOUBLE",
                    "MAX_EXECUTION_TIME DOUBLE",
                    "CUMULATIVE_EXECUTION_TIME DOUBLE",
                    "AVERAGE_EXECUTION_TIME DOUBLE",
                    "STD_DEV_EXECUTION_TIME DOUBLE",
                    "MIN_ROW_COUNT INT",
                    "MAX_ROW_COUNT INT",
                    "CUMULATIVE_ROW_COUNT LONG",
                    "AVERAGE_ROW_COUNT DOUBLE",
                    "STD_DEV_ROW_COUNT DOUBLE"
            );
            break;
        }
        case SYNONYMS: {
            setMetaTableName("SYNONYMS");
            cols = createColumns(
                    "SYNONYM_CATALOG",
                    "SYNONYM_SCHEMA",
                    "SYNONYM_NAME",
                    "SYNONYM_FOR",
                    "SYNONYM_FOR_SCHEMA",
                    "TYPE_NAME",
                    "STATUS",
                    "REMARKS",
                    "ID INT"
            );
            indexColumnName = "SYNONYM_NAME";
            break;
        }
        case TABLE_CONSTRAINTS: {
            setMetaTableName("TABLE_CONSTRAINTS");
            cols = createColumns(
                    "CONSTRAINT_CATALOG",
                    "CONSTRAINT_SCHEMA",
                    "CONSTRAINT_NAME",
                    "CONSTRAINT_TYPE",
                    "TABLE_CATALOG",
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "IS_DEFERRABLE",
                    "INITIALLY_DEFERRED",
                    "REMARKS",
                    "SQL",
                    "ID INT"
            );
            indexColumnName = "TABLE_NAME";
            break;
        }
        case DOMAIN_CONSTRAINTS: {
            setMetaTableName("DOMAIN_CONSTRAINTS");
            cols = createColumns(
                    "CONSTRAINT_CATALOG",
                    "CONSTRAINT_SCHEMA",
                    "CONSTRAINT_NAME",
                    "DOMAIN_CATALOG",
                    "DOMAIN_SCHEMA",
                    "DOMAIN_NAME",
                    "IS_DEFERRABLE",
                    "INITIALLY_DEFERRED",
                    "REMARKS",
                    "SQL",
                    "ID INT"
            );
            break;
        }
        case KEY_COLUMN_USAGE: {
            setMetaTableName("KEY_COLUMN_USAGE");
            cols = createColumns(
                    "CONSTRAINT_CATALOG",
                    "CONSTRAINT_SCHEMA",
                    "CONSTRAINT_NAME",
                    "TABLE_CATALOG",
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "COLUMN_NAME",
                    "ORDINAL_POSITION INT",
                    "POSITION_IN_UNIQUE_CONSTRAINT INT",
                    "INDEX_CATALOG",
                    "INDEX_SCHEMA",
                    "INDEX_NAME"
            );
            indexColumnName = "TABLE_NAME";
            break;
        }
        case REFERENTIAL_CONSTRAINTS: {
            setMetaTableName("REFERENTIAL_CONSTRAINTS");
            cols = createColumns(
                    "CONSTRAINT_CATALOG",
                    "CONSTRAINT_SCHEMA",
                    "CONSTRAINT_NAME",
                    "UNIQUE_CONSTRAINT_CATALOG",
                    "UNIQUE_CONSTRAINT_SCHEMA",
                    "UNIQUE_CONSTRAINT_NAME",
                    "MATCH_OPTION",
                    "UPDATE_RULE",
                    "DELETE_RULE"
            );
            break;
        }
        case CHECK_CONSTRAINTS: {
            setMetaTableName("CHECK_CONSTRAINTS");
            cols = createColumns(
                    "CONSTRAINT_CATALOG",
                    "CONSTRAINT_SCHEMA",
                    "CONSTRAINT_NAME",
                    "CHECK_CLAUSE"
            );
            break;
        }
        case CONSTRAINT_COLUMN_USAGE: {
            setMetaTableName("CONSTRAINT_COLUMN_USAGE");
            cols = createColumns(
                    "TABLE_CATALOG",
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "COLUMN_NAME",
                    "CONSTRAINT_CATALOG",
                    "CONSTRAINT_SCHEMA",
                    "CONSTRAINT_NAME"
            );
            indexColumnName = "TABLE_NAME";
            break;
        }
        default:
            throw DbException.throwInternalError("type="+type);
        }
        setColumns(cols);

        if (indexColumnName == null) {
            indexColumn = -1;
            metaIndex = null;
        } else {
            indexColumn = getColumn(database.sysIdentifier(indexColumnName)).getColumnId();
            IndexColumn[] indexCols = IndexColumn.wrap(
                    new Column[] { cols[indexColumn] });
            metaIndex = new MetaIndex(this, indexCols, false);
        }
    }

    private static String replaceNullWithEmpty(String s) {
        return s == null ? "" : s;
    }

    @Override
    public ArrayList<Row> generateRows(Session session, SearchRow first, SearchRow last) {
        Value indexFrom = null, indexTo = null;

        if (indexColumn >= 0) {
            if (first != null) {
                indexFrom = first.getValue(indexColumn);
            }
            if (last != null) {
                indexTo = last.getValue(indexColumn);
            }
        }

        ArrayList<Row> rows = Utils.newSmallArrayList();
        String catalog = database.getShortName();
        boolean admin = session.getUser().isAdmin();
        switch (type) {
        case TABLES: {
            for (Table table : getAllTables(session)) {
                String tableName = table.getName();
                if (!checkIndex(session, tableName, indexFrom, indexTo)) {
                    continue;
                }
                if (hideTable(table, session)) {
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
                    storageType = table.isPersistIndexes() ?
                            "CACHED" : "MEMORY";
                }
                String sql = table.getCreateSQL();
                if (!admin) {
                    if (sql != null && sql.contains(DbException.HIDE_SQL)) {
                        // hide the password of linked tables
                        sql = "-";
                    }
                }
                add(session,
                        rows,
                        // TABLE_CATALOG
                        catalog,
                        // TABLE_SCHEMA
                        table.getSchema().getName(),
                        // TABLE_NAME
                        tableName,
                        // TABLE_TYPE
                        table.getTableType().toString(),
                        // STORAGE_TYPE
                        storageType,
                        // SQL
                        sql,
                        // REMARKS
                        replaceNullWithEmpty(table.getComment()),
                        // LAST_MODIFICATION
                        ValueBigint.get(table.getMaxDataModificationId()),
                        // ID
                        ValueInteger.get(table.getId()),
                        // TYPE_NAME
                        null,
                        // TABLE_CLASS
                        table.getClass().getName(), // ROW_COUNT_ESTIMATE
                        ValueBigint.get(table.getRowCountApproximation())
                );
            }
            break;
        }
        case COLUMNS: {
            // reduce the number of tables to scan - makes some metadata queries
            // 10x faster
            final ArrayList<Table> tablesToList;
            if (indexFrom != null && indexFrom.equals(indexTo)) {
                String tableName = indexFrom.getString();
                if (tableName == null) {
                    break;
                }
                tablesToList = getTablesByName(session, tableName);
            } else {
                tablesToList = getAllTables(session);
            }
            for (Table table : tablesToList) {
                String tableName = table.getName();
                if (!checkIndex(session, tableName, indexFrom, indexTo)) {
                    continue;
                }
                if (hideTable(table, session)) {
                    continue;
                }
                Column[] cols = table.getColumns();
                String collation = database.getCompareMode().getName();
                for (int j = 0; j < cols.length; j++) {
                    Column c = cols[j];
                    Domain domain = c.getDomain();
                    TypeInfo typeInfo = c.getType();
                    DataType dataType = DataType.getDataType(typeInfo.getValueType());
                    ValueInteger precision = ValueInteger.get(MathUtils.convertLongToInt(typeInfo.getPrecision()));
                    ValueInteger scale = ValueInteger.get(typeInfo.getScale());
                    Sequence sequence = c.getSequence();
                    boolean hasDateTimePrecision;
                    int type = typeInfo.getValueType();
                    switch (type) {
                    case Value.TIME:
                    case Value.TIME_TZ:
                    case Value.DATE:
                    case Value.TIMESTAMP:
                    case Value.TIMESTAMP_TZ:
                    case Value.INTERVAL_SECOND:
                    case Value.INTERVAL_DAY_TO_SECOND:
                    case Value.INTERVAL_HOUR_TO_SECOND:
                    case Value.INTERVAL_MINUTE_TO_SECOND:
                        hasDateTimePrecision = true;
                        break;
                    default:
                        hasDateTimePrecision = false;
                    }
                    boolean isGenerated = c.getGenerated();
                    boolean isInterval = DataType.isIntervalType(type);
                    String createSQLWithoutName = c.getCreateSQLWithoutName();
                    add(session,
                            rows,
                            // TABLE_CATALOG
                            catalog,
                            // TABLE_SCHEMA
                            table.getSchema().getName(),
                            // TABLE_NAME
                            tableName,
                            // COLUMN_NAME
                            c.getName(),
                            // ORDINAL_POSITION
                            ValueInteger.get(j + 1),
                            // COLUMN_DEFAULT
                            isGenerated ? null : c.getDefaultSQL(),
                            // IS_NULLABLE
                            c.isNullable() ? "YES" : "NO",
                            // DATA_TYPE
                            ValueInteger.get(dataType.sqlType),
                            // CHARACTER_MAXIMUM_LENGTH
                            precision,
                            // CHARACTER_OCTET_LENGTH
                            precision,
                            // NUMERIC_PRECISION
                            precision,
                            // NUMERIC_PRECISION_RADIX
                            ValueInteger.get(10),
                            // NUMERIC_SCALE
                            scale,
                            // DATETIME_PRECISION
                            hasDateTimePrecision ? scale : null,
                            // INTERVAL_TYPE
                            isInterval ? createSQLWithoutName.substring(9) : null,
                            // INTERVAL_PRECISION
                            isInterval ? precision : null,
                            // CHARACTER_SET_NAME
                            CHARACTER_SET_NAME,
                            // COLLATION_NAME
                            collation,
                            // DOMAIN_CATALOG
                            domain != null ? catalog : null,
                            // DOMAIN_SCHEMA
                            domain != null ? domain.getSchema().getName() : null,
                            // DOMAIN_NAME
                            domain != null ? domain.getName() : null,
                            // IS_GENERATED
                            isGenerated ? "ALWAYS" : "NEVER",
                            // GENERATION_EXPRESSION
                            isGenerated ? c.getDefaultSQL() : null,
                            // TYPE_NAME
                            identifier(isInterval ? "INTERVAL" : getDataTypeName(dataType, typeInfo)),
                            // NULLABLE
                            ValueInteger.get(c.isNullable()
                                    ? DatabaseMetaData.columnNullable : DatabaseMetaData.columnNoNulls),
                            // IS_COMPUTED
                            ValueBoolean.get(isGenerated),
                            // SELECTIVITY
                            ValueInteger.get(c.getSelectivity()),
                            // SEQUENCE_NAME
                            sequence == null ? null : sequence.getName(),
                            // REMARKS
                            replaceNullWithEmpty(c.getComment()),
                            // SOURCE_DATA_TYPE
                            // SMALLINT
                            null,
                            // COLUMN_TYPE
                            createSQLWithoutName,
                            // COLUMN_ON_UPDATE
                            c.getOnUpdateSQL(), // IS_VISIBLE
                            ValueBoolean.get(c.getVisible())
                    );
                }
            }
            break;
        }
        case INDEXES: {
            // reduce the number of tables to scan - makes some metadata queries
            // 10x faster
            final ArrayList<Table> tablesToList;
            if (indexFrom != null && indexFrom.equals(indexTo)) {
                String tableName = indexFrom.getString();
                if (tableName == null) {
                    break;
                }
                tablesToList = getTablesByName(session, tableName);
            } else {
                tablesToList = getAllTables(session);
            }
            for (Table table : tablesToList) {
                String tableName = table.getName();
                if (!checkIndex(session, tableName, indexFrom, indexTo)) {
                    continue;
                }
                if (hideTable(table, session)) {
                    continue;
                }
                ArrayList<Index> indexes = table.getIndexes();
                ArrayList<Constraint> constraints = table.getConstraints();
                for (int j = 0; indexes != null && j < indexes.size(); j++) {
                    Index index = indexes.get(j);
                    if (index.getCreateSQL() == null) {
                        continue;
                    }
                    String constraintName = null;
                    for (int k = 0; constraints != null && k < constraints.size(); k++) {
                        Constraint constraint = constraints.get(k);
                        if (constraint.usesIndex(index)) {
                            if (index.getIndexType().isPrimaryKey()) {
                                if (constraint.getConstraintType() == Constraint.Type.PRIMARY_KEY) {
                                    constraintName = constraint.getName();
                                }
                            } else {
                                constraintName = constraint.getName();
                            }
                        }
                    }
                    IndexColumn[] cols = index.getIndexColumns();
                    String indexClass = index.getClass().getName();
                    for (int k = 0; k < cols.length; k++) {
                        IndexColumn idxCol = cols[k];
                        Column column = idxCol.column;
                        add(session,
                                rows,
                                // TABLE_CATALOG
                                catalog,
                                // TABLE_SCHEMA
                                table.getSchema().getName(),
                                // TABLE_NAME
                                tableName,
                                // NON_UNIQUE
                                ValueBoolean.get(!index.getIndexType().isUnique()),
                                // INDEX_NAME
                                index.getName(),
                                // ORDINAL_POSITION
                                ValueSmallint.get((short) (k + 1)),
                                // COLUMN_NAME
                                column.getName(),
                                // CARDINALITY
                                ValueInteger.get(0),
                                // PRIMARY_KEY
                                ValueBoolean.get(index.getIndexType().isPrimaryKey()),
                                // INDEX_TYPE_NAME
                                index.getIndexType().getSQL(),
                                // IS_GENERATED
                                ValueBoolean.get(index.getIndexType().getBelongsToConstraint()),
                                // INDEX_TYPE
                                ValueSmallint.get(DatabaseMetaData.tableIndexOther),
                                // ASC_OR_DESC
                                (idxCol.sortType & SortOrder.DESCENDING) != 0 ? "D" : "A",
                                // PAGES
                                ValueInteger.get(0),
                                // FILTER_CONDITION
                                "",
                                // REMARKS
                                replaceNullWithEmpty(index.getComment()),
                                // SQL
                                index.getCreateSQL(),
                                // ID
                                ValueInteger.get(index.getId()),
                                // SORT_TYPE
                                ValueInteger.get(idxCol.sortType),
                                // CONSTRAINT_NAME
                                constraintName, // INDEX_CLASS
                                indexClass
                            );
                    }
                }
            }
            break;
        }
        case TABLE_TYPES: {
            add(session, rows, TableType.TABLE.toString());
            add(session, rows, TableType.TABLE_LINK.toString());
            add(session, rows, TableType.SYSTEM_TABLE.toString());
            add(session, rows, TableType.VIEW.toString());
            add(session, rows, TableType.EXTERNAL_TABLE_ENGINE.toString());
            break;
        }
        case TYPE_INFO: {
            for (DataType t : DataType.getTypes()) {
                if (t.hidden || t.sqlType == Value.NULL) {
                    continue;
                }
                add(session,
                        rows,
                        // TYPE_NAME
                        t.name,
                        // DATA_TYPE
                        ValueInteger.get(t.sqlType),
                        // PRECISION
                        ValueInteger.get(MathUtils.convertLongToInt(t.maxPrecision)),
                        // PREFIX
                        t.prefix,
                        // SUFFIX
                        t.suffix,
                        // PARAMS
                        t.params,
                        // AUTO_INCREMENT
                        ValueBoolean.get(t.autoIncrement),
                        // MINIMUM_SCALE
                        ValueSmallint.get(MathUtils.convertIntToShort(t.minScale)),
                        // MAXIMUM_SCALE
                        ValueSmallint.get(MathUtils.convertIntToShort(t.maxScale)),
                        // RADIX
                        t.decimal ? ValueInteger.get(10) : null,
                        // POS
                        ValueInteger.get(t.type),
                        // CASE_SENSITIVE
                        ValueBoolean.get(t.caseSensitive),
                        // NULLABLE
                        ValueSmallint.get((short) DatabaseMetaData.typeNullable), // SEARCHABLE
                        ValueSmallint.get((short) DatabaseMetaData.typeSearchable)
                );
            }
            break;
        }
        case CATALOGS: {
            add(session, rows, catalog);
            break;
        }
        case SETTINGS: {
            for (Setting s : database.getAllSettings()) {
                String value = s.getStringValue();
                if (value == null) {
                    value = Integer.toString(s.getIntValue());
                }
                add(session,
                        rows,
                        identifier(s.getName()), value
                );
            }
            add(session, rows, "info.BUILD_ID", "" + Constants.BUILD_ID);
            add(session, rows, "info.VERSION_MAJOR", "" + Constants.VERSION_MAJOR);
            add(session, rows, "info.VERSION_MINOR", "" + Constants.VERSION_MINOR);
            add(session, rows, "info.VERSION", Constants.FULL_VERSION);
            if (admin) {
                String[] settings = {
                        "java.runtime.version", "java.vm.name",
                        "java.vendor", "os.name", "os.arch", "os.version",
                        "sun.os.patch.level", "file.separator",
                        "path.separator", "line.separator", "user.country",
                        "user.language", "user.variant", "file.encoding" };
                for (String s : settings) {
                    add(session, rows, "property." + s, Utils.getProperty(s, ""));
                }
            }
            add(session, rows, "EXCLUSIVE", database.getExclusiveSession() == null ?
                    "FALSE" : "TRUE");
            add(session, rows, "MODE", database.getMode().getName());
            add(session, rows, "QUERY_TIMEOUT", Integer.toString(session.getQueryTimeout()));
            add(session, rows, "TIME ZONE", session.currentTimeZone().getId());
            add(session, rows, "VARIABLE_BINARY", session.isVariableBinary() ? "TRUE" : "FALSE");
            BitSet nonKeywords = session.getNonKeywords();
            if (nonKeywords != null) {
                add(session, rows, "NON_KEYWORDS", Parser.formatNonKeywords(nonKeywords));
            }
            add(session, rows, "RETENTION_TIME", Integer.toString(database.getRetentionTime()));
            add(session, rows, "LOG", Integer.toString(database.getLogMode()));
            // database settings
            for (Map.Entry<String, String> entry : database.getSettings().getSortedSettings()) {
                add(session, rows, entry.getKey(), entry.getValue());
            }
            if (database.isPersistent()) {
                PageStore pageStore = database.getPageStore();
                if (pageStore != null) {
                    add(session, rows,
                            "info.FILE_WRITE_TOTAL", Long.toString(pageStore.getWriteCountTotal()));
                    add(session, rows,
                            "info.FILE_WRITE", Long.toString(pageStore.getWriteCount()));
                    add(session, rows,
                            "info.FILE_READ", Long.toString(pageStore.getReadCount()));
                    add(session, rows,
                            "info.PAGE_COUNT", Integer.toString(pageStore.getPageCount()));
                    add(session, rows,
                            "info.PAGE_SIZE", Integer.toString(pageStore.getPageSize()));
                    add(session, rows,
                            "info.CACHE_MAX_SIZE", Integer.toString(pageStore.getCache().getMaxMemory()));
                    add(session, rows,
                            "info.CACHE_SIZE", Integer.toString(pageStore.getCache().getMemory()));
                }
                Store store = database.getStore();
                if (store != null) {
                    MVStore mvStore = store.getMvStore();
                    FileStore fs = mvStore.getFileStore();
                    if (fs != null) {
                        add(session, rows,
                                "info.FILE_WRITE", Long.toString(fs.getWriteCount()));
                        add(session, rows,
                                "info.FILE_WRITE_BYTES", Long.toString(fs.getWriteBytes()));
                        add(session, rows,
                                "info.FILE_READ", Long.toString(fs.getReadCount()));
                        add(session, rows,
                                "info.FILE_READ_BYTES", Long.toString(fs.getReadBytes()));
                        add(session, rows,
                                "info.UPDATE_FAILURE_PERCENT",
                                String.format(Locale.ENGLISH, "%.2f%%", 100 * mvStore.getUpdateFailureRatio()));
                        add(session, rows,
                                "info.FILL_RATE", Integer.toString(mvStore.getFillRate()));
                        add(session, rows,
                                "info.CHUNKS_FILL_RATE", Integer.toString(mvStore.getChunksFillRate()));
                        add(session, rows,
                                "info.CHUNKS_FILL_RATE_RW", Integer.toString(mvStore.getRewritableChunksFillRate()));
                        try {
                            add(session, rows,
                                    "info.FILE_SIZE", Long.toString(fs.getFile().size()));
                        } catch (IOException ignore) {/**/}
                        add(session, rows,
                                "info.CHUNK_COUNT", Long.toString(mvStore.getChunkCount()));
                        add(session, rows,
                                "info.PAGE_COUNT", Long.toString(mvStore.getPageCount()));
                        add(session, rows,
                                "info.PAGE_COUNT_LIVE", Long.toString(mvStore.getLivePageCount()));
                        add(session, rows,
                                "info.PAGE_SIZE", Integer.toString(mvStore.getPageSplitSize()));
                        add(session, rows,
                                "info.CACHE_MAX_SIZE", Integer.toString(mvStore.getCacheSize()));
                        add(session, rows,
                                "info.CACHE_SIZE", Integer.toString(mvStore.getCacheSizeUsed()));
                        add(session, rows,
                                "info.CACHE_HIT_RATIO", Integer.toString(mvStore.getCacheHitRatio()));
                        add(session, rows, "info.TOC_CACHE_HIT_RATIO",
                                Integer.toString(mvStore.getTocCacheHitRatio()));
                        add(session, rows,
                                "info.LEAF_RATIO", Integer.toString(mvStore.getLeafRatio()));
                    }
                }
            }
            break;
        }
        case HELP: {
            String resource = "/org/h2/res/help.csv";
            try {
                byte[] data = Utils.getResource(resource);
                Reader reader = new InputStreamReader(
                        new ByteArrayInputStream(data));
                Csv csv = new Csv();
                csv.setLineCommentCharacter('#');
                ResultSet rs = csv.read(reader, null);
                for (int i = 0; rs.next(); i++) {
                    add(session,
                        rows,
                        // ID
                        ValueInteger.get(i),
                        // SECTION
                        rs.getString(1).trim(),
                        // TOPIC
                        rs.getString(2).trim(),
                        // SYNTAX
                        rs.getString(3).trim(), // TEXT
                        rs.getString(4).trim()
                    );
                }
            } catch (Exception e) {
                throw DbException.convert(e);
            }
            break;
        }
        case SEQUENCES: {
            for (SchemaObject obj : database.getAllSchemaObjects(DbObject.SEQUENCE)) {
                Sequence s = (Sequence) obj;
                add(session,
                        rows,
                        // SEQUENCE_CATALOG
                        catalog,
                        // SEQUENCE_SCHEMA
                        s.getSchema().getName(),
                        // SEQUENCE_NAME
                        s.getName(),
                        // DATA_TYPE
                        "BIGINT",
                        // NUMERIC_PRECISION
                        ValueInteger.get(ValueBigint.PRECISION),
                        // NUMERIC_PRECISION_RADIX
                        ValueInteger.get(10),
                        // NUMERIC_SCALE
                        ValueInteger.get(0),
                        // START_VALUE
                        ValueBigint.get(s.getStartValue()),
                        // MINIMUM_VALUE
                        ValueBigint.get(s.getMinValue()),
                        // MAXIMUM_VALUE
                        ValueBigint.get(s.getMaxValue()),
                        // INCREMENT
                        ValueBigint.get(s.getIncrement()),
                        // CYCLE_OPTION
                        s.getCycle() ? "YES" : "NO",
                        // CURRENT_VALUE
                        ValueBigint.get(s.getCurrentValue()),
                        // IS_GENERATED
                        ValueBoolean.get(s.getBelongsToTable()),
                        // REMARKS
                        replaceNullWithEmpty(s.getComment()),
                        // CACHE
                        ValueBigint.get(s.getCacheSize()), // ID
                        ValueInteger.get(s.getId())
                    );
            }
            break;
        }
        case USERS: {
            for (User u : database.getAllUsers()) {
                if (admin || session.getUser() == u) {
                    add(session,
                            rows,
                            // NAME
                            identifier(u.getName()),
                            // ADMIN
                            String.valueOf(u.isAdmin()),
                            // REMARKS
                            replaceNullWithEmpty(u.getComment()), // ID
                            ValueInteger.get(u.getId())
                    );
                }
            }
            break;
        }
        case ROLES: {
            for (Role r : database.getAllRoles()) {
                if (admin || session.getUser().isRoleGranted(r)) {
                    add(session,
                            rows,
                            // NAME
                            identifier(r.getName()),
                            // REMARKS
                            replaceNullWithEmpty(r.getComment()), // ID
                            ValueInteger.get(r.getId())
                    );
                }
            }
            break;
        }
        case RIGHTS: {
            if (admin) {
                for (Right r : database.getAllRights()) {
                    Role role = r.getGrantedRole();
                    DbObject grantee = r.getGrantee();
                    String rightType = grantee.getType() == DbObject.USER ? "USER" : "ROLE";
                    if (role == null) {
                        DbObject object = r.getGrantedObject();
                        Schema schema = null;
                        Table table = null;
                        if (object != null) {
                            if (object instanceof Schema) {
                                schema = (Schema) object;
                            } else if (object instanceof Table) {
                                table = (Table) object;
                                schema = table.getSchema();
                            }
                        }
                        String tableName = (table != null) ? table.getName() : "";
                        String schemaName = (schema != null) ? schema.getName() : "";
                        if (!checkIndex(session, tableName, indexFrom, indexTo)) {
                            continue;
                        }
                        add(session,
                                rows,
                                // GRANTEE
                                identifier(grantee.getName()),
                                // GRANTEETYPE
                                rightType,
                                // GRANTEDROLE
                                "",
                                // RIGHTS
                                r.getRights(),
                                // TABLE_SCHEMA
                                schemaName,
                                // TABLE_NAME
                                tableName, // ID
                                ValueInteger.get(r.getId())
                        );
                    } else {
                        add(session,
                                rows,
                                // GRANTEE
                                identifier(grantee.getName()),
                                // GRANTEETYPE
                                rightType,
                                // GRANTEDROLE
                                identifier(role.getName()),
                                // RIGHTS
                                "",
                                // TABLE_SCHEMA
                                "",
                                // TABLE_NAME
                                "", // ID
                                ValueInteger.get(r.getId())
                        );
                    }
                }
            }
            break;
        }
        case FUNCTION_ALIASES: {
            for (SchemaObject aliasAsSchemaObject :
                    database.getAllSchemaObjects(DbObject.FUNCTION_ALIAS)) {
                FunctionAlias alias = (FunctionAlias) aliasAsSchemaObject;
                JavaMethod[] methods;
                try {
                    methods = alias.getJavaMethods();
                } catch (DbException e) {
                    methods = new JavaMethod[0];
                }
                for (FunctionAlias.JavaMethod method : methods) {
                    TypeInfo typeInfo = method.getDataType();
                    int valueType = typeInfo.getValueType();
                    add(session,
                            rows,
                            // ALIAS_CATALOG
                            catalog,
                            // ALIAS_SCHEMA
                            alias.getSchema().getName(),
                            // ALIAS_NAME
                            alias.getName(),
                            // JAVA_CLASS
                            alias.getJavaClassName(),
                            // JAVA_METHOD
                            alias.getJavaMethodName(),
                            // DATA_TYPE
                            ValueInteger.get(DataType.convertTypeToSQLType(valueType)),
                            // TYPE_NAME
                            getDataTypeName(DataType.getDataType(valueType), typeInfo),
                            // COLUMN_COUNT
                            ValueInteger.get(method.getParameterCount()),
                            // RETURNS_RESULT
                            ValueSmallint.get(valueType == Value.NULL
                                    ? (short) DatabaseMetaData.procedureNoResult
                                    : (short) DatabaseMetaData.procedureReturnsResult),
                            // REMARKS
                            replaceNullWithEmpty(alias.getComment()),
                            // ID
                            ValueInteger.get(alias.getId()),
                            // SOURCE
                            alias.getSource()
                            // when adding more columns, see also below
                    );
                }
            }
            for (UserAggregate agg : database.getAllAggregates()) {
                add(session,
                        rows,
                        // ALIAS_CATALOG
                        catalog,
                        // ALIAS_SCHEMA
                        database.getMainSchema().getName(),
                        // ALIAS_NAME
                        agg.getName(),
                        // JAVA_CLASS
                        agg.getJavaClassName(),
                        // JAVA_METHOD
                        "",
                        // DATA_TYPE
                        ValueInteger.get(Types.NULL),
                        // TYPE_NAME
                        DataType.getDataType(Value.NULL).name,
                        // COLUMN_COUNT
                        ValueInteger.get(1),
                        // RETURNS_RESULT
                        ValueSmallint.get((short) DatabaseMetaData.procedureReturnsResult),
                        // REMARKS
                        replaceNullWithEmpty(agg.getComment()),
                        // ID
                        ValueInteger.get(agg.getId()),
                        // SOURCE
                        ""
                        // when adding more columns, see also below
                );
            }
            break;
        }
        case FUNCTION_COLUMNS: {
            for (SchemaObject aliasAsSchemaObject :
                    database.getAllSchemaObjects(DbObject.FUNCTION_ALIAS)) {
                FunctionAlias alias = (FunctionAlias) aliasAsSchemaObject;
                JavaMethod[] methods;
                try {
                    methods = alias.getJavaMethods();
                } catch (DbException e) {
                    methods = new JavaMethod[0];
                }
                for (FunctionAlias.JavaMethod method : methods) {
                    // Add return column index 0
                    TypeInfo typeInfo = method.getDataType();
                    if (typeInfo.getValueType() != Value.NULL) {
                        DataType dt = DataType.getDataType(typeInfo.getValueType());
                        add(session,
                                rows,
                                // ALIAS_CATALOG
                                catalog,
                                // ALIAS_SCHEMA
                                alias.getSchema().getName(),
                                // ALIAS_NAME
                                alias.getName(),
                                // JAVA_CLASS
                                alias.getJavaClassName(),
                                // JAVA_METHOD
                                alias.getJavaMethodName(),
                                // COLUMN_COUNT
                                ValueInteger.get(method.getParameterCount()),
                                // POS
                                ValueInteger.get(0),
                                // COLUMN_NAME
                                "P0",
                                // DATA_TYPE
                                ValueInteger.get(DataType.convertTypeToSQLType(typeInfo.getValueType())),
                                // TYPE_NAME
                                getDataTypeName(dt, typeInfo),
                                // PRECISION
                                ValueInteger.get(MathUtils.convertLongToInt(dt.defaultPrecision)),
                                // SCALE
                                ValueSmallint.get(MathUtils.convertIntToShort(dt.defaultScale)),
                                // RADIX
                                ValueSmallint.get((short) 10),
                                // NULLABLE
                                ValueSmallint.get((short) DatabaseMetaData.columnNullableUnknown),
                                // COLUMN_TYPE
                                ValueSmallint.get((short) DatabaseMetaData.procedureColumnReturn),
                                // REMARKS
                                "", // COLUMN_DEFAULT
                                null
                        );
                    }
                    Class<?>[] columnList = method.getColumnClasses();
                    for (int k = 0; k < columnList.length; k++) {
                        if (method.hasConnectionParam() && k == 0) {
                            continue;
                        }
                        Class<?> clazz = columnList[k];
                        TypeInfo columnTypeInfo = DataType.getTypeFromClass(clazz);
                        int dataType = columnTypeInfo.getValueType();
                        DataType dt = DataType.getDataType(dataType);
                        add(session,
                                rows,
                                // ALIAS_CATALOG
                                catalog,
                                // ALIAS_SCHEMA
                                alias.getSchema().getName(),
                                // ALIAS_NAME
                                alias.getName(),
                                // JAVA_CLASS
                                alias.getJavaClassName(),
                                // JAVA_METHOD
                                alias.getJavaMethodName(),
                                // COLUMN_COUNT
                                ValueInteger.get(method.getParameterCount()),
                                // POS
                                ValueInteger.get(k + (method.hasConnectionParam() ? 0 : 1)),
                                // COLUMN_NAME
                                "P" + (k + 1),
                                // DATA_TYPE
                                ValueInteger.get(DataType.convertTypeToSQLType(dt.type)),
                                // TYPE_NAME
                                getDataTypeName(dt, columnTypeInfo),
                                // PRECISION
                                ValueInteger.get(MathUtils.convertLongToInt(dt.defaultPrecision)),
                                // SCALE
                                ValueSmallint.get(MathUtils.convertIntToShort(dt.defaultScale)),
                                // RADIX
                                ValueSmallint.get((short) 10),
                                // NULLABLE
                                ValueSmallint.get(clazz.isPrimitive()
                                        ? (short) DatabaseMetaData.columnNoNulls
                                        : (short) DatabaseMetaData.columnNullable),
                                // COLUMN_TYPE
                                ValueSmallint.get((short) DatabaseMetaData.procedureColumnIn),
                                // REMARKS
                                "", // COLUMN_DEFAULT
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
                add(session,
                        rows,
                        // CATALOG_NAME
                        catalog,
                        // SCHEMA_NAME
                        schema.getName(),
                        // SCHEMA_OWNER
                        identifier(schema.getOwner().getName()),
                        // DEFAULT_CHARACTER_SET_NAME
                        CHARACTER_SET_NAME,
                        // DEFAULT_COLLATION_NAME
                        collation,
                        // IS_DEFAULT
                        ValueBoolean.get(schema.getId() == Constants.MAIN_SCHEMA_ID),
                        // REMARKS
                        replaceNullWithEmpty(schema.getComment()), // ID
                        ValueInteger.get(schema.getId())
                );
            }
            break;
        }
        case TABLE_PRIVILEGES: {
            for (Right r : database.getAllRights()) {
                DbObject object = r.getGrantedObject();
                if (!(object instanceof Table)) {
                    continue;
                }
                Table table = (Table) object;
                if (hideTable(table, session)) {
                    continue;
                }
                String tableName = table.getName();
                if (!checkIndex(session, tableName, indexFrom, indexTo)) {
                    continue;
                }
                addPrivileges(session, rows, r.getGrantee(), catalog, table, null, r.getRightMask());
            }
            break;
        }
        case COLUMN_PRIVILEGES: {
            for (Right r : database.getAllRights()) {
                DbObject object = r.getGrantedObject();
                if (!(object instanceof Table)) {
                    continue;
                }
                Table table = (Table) object;
                if (hideTable(table, session)) {
                    continue;
                }
                String tableName = table.getName();
                if (!checkIndex(session, tableName, indexFrom, indexTo)) {
                    continue;
                }
                DbObject grantee = r.getGrantee();
                int mask = r.getRightMask();
                for (Column column : table.getColumns()) {
                    addPrivileges(session, rows, grantee, catalog, table, column.getName(), mask);
                }
            }
            break;
        }
        case COLLATIONS: {
            for (Locale l : Collator.getAvailableLocales()) {
                add(session,
                        rows,
                        // NAME
                        CompareMode.getName(l), // KEY
                        l.toString()
                );
            }
            break;
        }
        case VIEWS: {
            for (Table table : getAllTables(session)) {
                if (table.getTableType() != TableType.VIEW) {
                    continue;
                }
                String tableName = table.getName();
                if (!checkIndex(session, tableName, indexFrom, indexTo)) {
                    continue;
                }
                TableView view = (TableView) table;
                add(session,
                        rows,
                        // TABLE_CATALOG
                        catalog,
                        // TABLE_SCHEMA
                        table.getSchema().getName(),
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
                        replaceNullWithEmpty(view.getComment()), // ID
                        ValueInteger.get(view.getId())
                );
            }
            break;
        }
        case IN_DOUBT: {
            ArrayList<InDoubtTransaction> prepared = database.getInDoubtTransactions();
            if (prepared != null && admin) {
                for (InDoubtTransaction prep : prepared) {
                    add(session,
                            rows,
                            // TRANSACTION
                            prep.getTransactionName(), // STATE
                            prep.getStateDescription()
                    );
                }
            }
            break;
        }
        case CROSS_REFERENCES: {
            for (SchemaObject obj : database.getAllSchemaObjects(
                    DbObject.CONSTRAINT)) {
                Constraint constraint = (Constraint) obj;
                if (constraint.getConstraintType() != Constraint.Type.REFERENTIAL) {
                    continue;
                }
                ConstraintReferential ref = (ConstraintReferential) constraint;
                IndexColumn[] cols = ref.getColumns();
                IndexColumn[] refCols = ref.getRefColumns();
                Table tab = ref.getTable();
                Table refTab = ref.getRefTable();
                String tableName = refTab.getName();
                if (!checkIndex(session, tableName, indexFrom, indexTo)) {
                    continue;
                }
                ValueSmallint update = ValueSmallint.get(getRefAction(ref.getUpdateAction()));
                ValueSmallint delete = ValueSmallint.get(getRefAction(ref.getDeleteAction()));
                for (int j = 0; j < cols.length; j++) {
                    add(session,
                            rows,
                            // PKTABLE_CATALOG
                            catalog,
                            // PKTABLE_SCHEMA
                            refTab.getSchema().getName(),
                            // PKTABLE_NAME
                            refTab.getName(),
                            // PKCOLUMN_NAME
                            refCols[j].column.getName(),
                            // FKTABLE_CATALOG
                            catalog,
                            // FKTABLE_SCHEMA
                            tab.getSchema().getName(),
                            // FKTABLE_NAME
                            tab.getName(),
                            // FKCOLUMN_NAME
                            cols[j].column.getName(),
                            // ORDINAL_POSITION
                            ValueSmallint.get((short) (j + 1)),
                            // UPDATE_RULE
                            update,
                            // DELETE_RULE
                            delete,
                            // FK_NAME
                            ref.getName(),
                            // PK_NAME
                            ref.getReferencedConstraint().getName(), // DEFERRABILITY
                            ValueSmallint.get((short) DatabaseMetaData.importedKeyNotDeferrable)
                    );
                }
            }
            break;
        }
        case CONSTANTS: {
            for (SchemaObject obj : database.getAllSchemaObjects(
                    DbObject.CONSTANT)) {
                Constant constant = (Constant) obj;
                ValueExpression expr = constant.getValue();
                add(session,
                        rows,
                        // CONSTANT_CATALOG
                        catalog,
                        // CONSTANT_SCHEMA
                        constant.getSchema().getName(),
                        // CONSTANT_NAME
                        constant.getName(),
                        // DATA_TYPE
                        ValueInteger.get(DataType.convertTypeToSQLType(expr.getType().getValueType())),
                        // REMARKS
                        replaceNullWithEmpty(constant.getComment()),
                        // SQL
                        expr.getSQL(DEFAULT_SQL_FLAGS), // ID
                        ValueInteger.get(constant.getId())
                    );
            }
            break;
        }
        case DOMAINS: {
            for (SchemaObject obj : database.getAllSchemaObjects(DbObject.DOMAIN)) {
                Domain domain = (Domain) obj;
                Column col = domain.getColumn();
                Domain parentDomain = col.getDomain();
                TypeInfo typeInfo = col.getType();
                DataType dataType = DataType.getDataType(typeInfo.getValueType());
                add(session,
                        rows,
                        // DOMAIN_CATALOG
                        catalog,
                        // DOMAIN_SCHEMA
                        domain.getSchema().getName(),
                        // DOMAIN_NAME
                        domain.getName(),
                        // DOMAIN_DEFAULT
                        col.getDefaultSQL(),
                        // DOMAIN_ON_UPDATE
                        col.getOnUpdateSQL(),
                        // DATA_TYPE
                        ValueInteger.get(dataType.sqlType),
                        // PRECISION
                        ValueInteger.get(MathUtils.convertLongToInt(typeInfo.getPrecision())),
                        // SCALE
                        ValueInteger.get(typeInfo.getScale()),
                        // TYPE_NAME
                        getDataTypeName(dataType, typeInfo),
                        // PARENT_DOMAIN_CATALOG
                        parentDomain != null ? catalog : null,
                        // PARENT_DOMAIN_SCHEMA
                        parentDomain != null ? parentDomain.getSchema().getName() : null,
                        // PARENT_DOMAIN_NAME
                        parentDomain != null ? parentDomain.getName() : null,
                        // SELECTIVITY INT
                        ValueInteger.get(col.getSelectivity()),
                        // REMARKS
                        replaceNullWithEmpty(domain.getComment()),
                        // SQL
                        domain.getCreateSQL(), // ID
                        ValueInteger.get(domain.getId())
                );
            }
            break;
        }
        case TRIGGERS: {
            for (SchemaObject obj : database.getAllSchemaObjects(
                    DbObject.TRIGGER)) {
                TriggerObject trigger = (TriggerObject) obj;
                Table table = trigger.getTable();
                add(session,
                        rows,
                        // TRIGGER_CATALOG
                        catalog,
                        // TRIGGER_SCHEMA
                        trigger.getSchema().getName(),
                        // TRIGGER_NAME
                        trigger.getName(),
                        // TRIGGER_TYPE
                        trigger.getTypeNameList(new StringBuilder()).toString(),
                        // TABLE_CATALOG
                        catalog,
                        // TABLE_SCHEMA
                        table.getSchema().getName(),
                        // TABLE_NAME
                        table.getName(),
                        // BEFORE
                        ValueBoolean.get(trigger.isBefore()),
                        // JAVA_CLASS
                        trigger.getTriggerClassName(),
                        // QUEUE_SIZE
                        ValueInteger.get(trigger.getQueueSize()),
                        // NO_WAIT
                        ValueBoolean.get(trigger.isNoWait()),
                        // REMARKS
                        replaceNullWithEmpty(trigger.getComment()),
                        // SQL
                        trigger.getCreateSQL(),
                        // ID
                        ValueInteger.get(trigger.getId())
                );
            }
            break;
        }
        case SESSIONS: {
            for (Session s : database.getSessions(false)) {
                if (admin || s == session) {
                    NetworkConnectionInfo networkConnectionInfo = s.getNetworkConnectionInfo();
                    Command command = s.getCurrentCommand();
                    int blockingSessionId = s.getBlockingSessionId();
                    add(session,
                            rows,
                            // ID
                            ValueInteger.get(s.getId()),
                            // USER_NAME
                            s.getUser().getName(),
                            // SERVER
                            networkConnectionInfo == null ? null : networkConnectionInfo.getServer(),
                            // CLIENT_ADDR
                            networkConnectionInfo == null ? null : networkConnectionInfo.getClient(),
                            // CLIENT_INFO
                            networkConnectionInfo == null ? null : networkConnectionInfo.getClientInfo(),
                            // SESSION_START
                            s.getSessionStart(),
                            // ISOLATION_LEVEL
                            session.getIsolationLevel().getSQL(),
                            // STATEMENT
                            command == null ? null : command.toString(),
                            // STATEMENT_START
                            command == null ? null : s.getCommandStartOrEnd(),
                            // CONTAINS_UNCOMMITTED
                            ValueBoolean.get(s.containsUncommitted()),
                            // STATE
                            String.valueOf(s.getState()),
                            // BLOCKER_ID
                            blockingSessionId == 0 ? null : ValueInteger.get(blockingSessionId),
                            // SLEEP_SINCE
                            s.getState() == State.SLEEP ? s.getCommandStartOrEnd() : null
                    );
                }
            }
            break;
        }
        case LOCKS: {
            for (Session s : database.getSessions(false)) {
                if (admin || s == session) {
                    for (Table table : s.getLocks()) {
                        add(session,
                                rows,
                                // TABLE_SCHEMA
                                table.getSchema().getName(),
                                // TABLE_NAME
                                table.getName(),
                                // SESSION_ID
                                ValueInteger.get(s.getId()), // LOCK_TYPE
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
                StringBuilder builder = new StringBuilder().append("SET @").append(name).append(' ');
                v.getSQL(builder, DEFAULT_SQL_FLAGS);
                add(session,
                        rows,
                        // KEY
                        "@" + name, builder.toString()
                );
            }
            for (Table table : session.getLocalTempTables()) {
                add(session,
                        rows,
                        // KEY
                        "TABLE " + table.getName(), // SQL
                        table.getCreateSQL()
                );
            }
            String[] path = session.getSchemaSearchPath();
            if (path != null && path.length > 0) {
                StringBuilder builder = new StringBuilder("SET SCHEMA_SEARCH_PATH ");
                for (int i = 0, l = path.length; i < l; i++) {
                    if (i > 0) {
                        builder.append(", ");
                    }
                    StringUtils.quoteIdentifier(builder, path[i]);
                }
                add(session,
                        rows,
                        // KEY
                        "SCHEMA_SEARCH_PATH", // SQL
                        builder.toString()
                );
            }
            String schema = session.getCurrentSchemaName();
            if (schema != null) {
                add(session,
                        rows,
                        // KEY
                        "SCHEMA", // SQL
                        StringUtils.quoteIdentifier(new StringBuilder("SET SCHEMA "), schema).toString()
                );
            }
            TimeZoneProvider currentTimeZone = session.currentTimeZone();
            if (!currentTimeZone.equals(DateTimeUtils.getTimeZone())) {
                add(session,
                        rows,
                        // KEY
                        "TIME ZONE", // SQL
                        StringUtils.quoteStringSQL(new StringBuilder("SET TIME ZONE "), currentTimeZone.getId())
                                .toString()
                );
            }
            break;
        }
        case QUERY_STATISTICS: {
            QueryStatisticsData control = database.getQueryStatisticsData();
            if (control != null) {
                for (QueryStatisticsData.QueryEntry entry : control.getQueries()) {
                    add(session,
                            rows,
                            // SQL_STATEMENT
                            entry.sqlStatement,
                            // EXECUTION_COUNT
                            ValueInteger.get(entry.count),
                            // MIN_EXECUTION_TIME
                            ValueDouble.get(entry.executionTimeMinNanos / 1_000_000d),
                            // MAX_EXECUTION_TIME
                            ValueDouble.get(entry.executionTimeMaxNanos / 1_000_000d),
                            // CUMULATIVE_EXECUTION_TIME
                            ValueDouble.get(entry.executionTimeCumulativeNanos / 1_000_000d),
                            // AVERAGE_EXECUTION_TIME
                            ValueDouble.get(entry.executionTimeMeanNanos / 1_000_000d),
                            // STD_DEV_EXECUTION_TIME
                            ValueDouble.get(entry.getExecutionTimeStandardDeviation() / 1_000_000d),
                            // MIN_ROW_COUNT
                            ValueInteger.get(entry.rowCountMin),
                            // MAX_ROW_COUNT
                            ValueInteger.get(entry.rowCountMax),
                            // CUMULATIVE_ROW_COUNT
                            ValueBigint.get(entry.rowCountCumulative),
                            // AVERAGE_ROW_COUNT
                            ValueDouble.get(entry.rowCountMean), // STD_DEV_ROW_COUNT
                            ValueDouble.get(entry.getRowCountStandardDeviation())
                    );
                }
            }
            break;
        }
        case SYNONYMS: {
            for (TableSynonym synonym : database.getAllSynonyms()) {
                add(session,
                        rows,
                        // SYNONYM_CATALOG
                        catalog,
                        // SYNONYM_SCHEMA
                        synonym.getSchema().getName(),
                        // SYNONYM_NAME
                        synonym.getName(),
                        // SYNONYM_FOR
                        synonym.getSynonymForName(),
                        // SYNONYM_FOR_SCHEMA
                        synonym.getSynonymForSchema().getName(),
                        // TYPE NAME
                        "SYNONYM",
                        // STATUS
                        "VALID",
                        // REMARKS
                        replaceNullWithEmpty(synonym.getComment()), // ID
                        ValueInteger.get(synonym.getId())
                );
            }
            break;
        }
        case TABLE_CONSTRAINTS: {
            for (SchemaObject obj : database.getAllSchemaObjects(DbObject.CONSTRAINT)) {
                Constraint constraint = (Constraint) obj;
                Constraint.Type constraintType = constraint.getConstraintType();
                if (constraintType == Constraint.Type.DOMAIN) {
                    continue;
                }
                Table table = constraint.getTable();
                if (hideTable(table, session)) {
                    continue;
                }
                String tableName = table.getName();
                if (!checkIndex(session, tableName, indexFrom, indexTo)) {
                    continue;
                }
                add(session,
                        rows,
                        // CONSTRAINT_CATALOG
                        catalog,
                        // CONSTRAINT_SCHEMA
                        constraint.getSchema().getName(),
                        // CONSTRAINT_NAME
                        constraint.getName(),
                        // CONSTRAINT_TYPE
                        constraintType.getSqlName(),
                        // TABLE_CATALOG
                        catalog,
                        // TABLE_SCHEMA
                        table.getSchema().getName(),
                        // TABLE_NAME
                        tableName,
                        // IS_DEFERRABLE
                        "NO",
                        // INITIALLY_DEFERRED
                        "NO",
                        // REMARKS
                        replaceNullWithEmpty(constraint.getComment()),
                        // SQL
                        constraint.getCreateSQL(), // ID
                        ValueInteger.get(constraint.getId())
                );
            }
            break;
        }
        case DOMAIN_CONSTRAINTS: {
            for (SchemaObject obj : database.getAllSchemaObjects(DbObject.CONSTRAINT)) {
                if (((Constraint) obj).getConstraintType() != Constraint.Type.DOMAIN) {
                    continue;
                }
                ConstraintDomain constraint = (ConstraintDomain) obj;
                Domain domain = constraint.getDomain();
                add(session,
                        rows,
                        // CONSTRAINT_CATALOG
                        catalog,
                        // CONSTRAINT_SCHEMA
                        constraint.getSchema().getName(),
                        // CONSTRAINT_NAME
                        constraint.getName(),
                        // DOMAIN_CATALOG
                        catalog,
                        // DOMAIN_SCHEMA
                        domain.getSchema().getName(),
                        // DOMAIN_NAME
                        domain.getName(),
                        // IS_DEFERRABLE
                        "NO",
                        // INITIALLY_DEFERRED
                        "NO",
                        // REMARKS
                        replaceNullWithEmpty(constraint.getComment()),
                        // SQL
                        constraint.getCreateSQL(), // ID
                        ValueInteger.get(constraint.getId())
                );
            }
            break;
        }
        case KEY_COLUMN_USAGE: {
            for (SchemaObject obj : database.getAllSchemaObjects(DbObject.CONSTRAINT)) {
                Constraint constraint = (Constraint) obj;
                Constraint.Type constraintType = constraint.getConstraintType();
                IndexColumn[] indexColumns = null;
                if (constraintType == Constraint.Type.UNIQUE || constraintType == Constraint.Type.PRIMARY_KEY) {
                    indexColumns = ((ConstraintUnique) constraint).getColumns();
                } else if (constraintType == Constraint.Type.REFERENTIAL) {
                    indexColumns = ((ConstraintReferential) constraint).getColumns();
                }
                if (indexColumns == null) {
                    continue;
                }
                Table table = constraint.getTable();
                if (hideTable(table, session)) {
                    continue;
                }
                String tableName = table.getName();
                if (!checkIndex(session, tableName, indexFrom, indexTo)) {
                    continue;
                }
                ConstraintUnique referenced;
                if (constraintType == Constraint.Type.REFERENTIAL) {
                    referenced = ((ConstraintReferential) constraint).getReferencedConstraint();
                } else {
                    referenced = null;
                }
                Index index = constraint.getIndex();
                for (int i = 0; i < indexColumns.length; i++) {
                    IndexColumn indexColumn = indexColumns[i];
                    ValueInteger ordinalPosition = ValueInteger.get(i + 1);
                    ValueInteger positionInUniqueConstraint = null;
                    if (referenced != null) {
                        Column c = ((ConstraintReferential) constraint).getRefColumns()[i].column;
                        IndexColumn[] refColumns = referenced.getColumns();
                        for (int j = 0; j < refColumns.length; j++) {
                            if (refColumns[j].column.equals(c)) {
                                positionInUniqueConstraint = ValueInteger.get(j + 1);
                                break;
                            }
                        }
                    }
                    add(session,
                            rows,
                            // CONSTRAINT_CATALOG
                            catalog,
                            // CONSTRAINT_SCHEMA
                            constraint.getSchema().getName(),
                            // CONSTRAINT_NAME
                            constraint.getName(),
                            // TABLE_CATALOG
                            catalog,
                            // TABLE_SCHEMA
                            table.getSchema().getName(),
                            // TABLE_NAME
                            tableName,
                            // COLUMN_NAME
                            indexColumn.columnName,
                            // ORDINAL_POSITION
                            ordinalPosition,
                            // POSITION_IN_UNIQUE_CONSTRAINT
                            positionInUniqueConstraint,
                            // INDEX_CATALOG
                            index != null ? catalog : null,
                            // INDEX_SCHEMA
                            index != null ? index.getSchema().getName() : null, // INDEX_NAME
                            index != null ? index.getName() : null
                    );
                }
            }
            break;
        }
        case REFERENTIAL_CONSTRAINTS: {
            for (SchemaObject obj : database.getAllSchemaObjects(DbObject.CONSTRAINT)) {
                if (((Constraint) obj).getConstraintType() != Constraint.Type.REFERENTIAL) {
                    continue;
                }
                ConstraintReferential constraint = (ConstraintReferential) obj;
                Table table = constraint.getTable();
                if (hideTable(table, session)) {
                    continue;
                }
                ConstraintUnique unique = constraint.getReferencedConstraint();
                add(session,
                        rows,
                        // CONSTRAINT_CATALOG
                        catalog,
                        // CONSTRAINT_SCHEMA
                        constraint.getSchema().getName(),
                        // CONSTRAINT_NAME
                        constraint.getName(),
                        // UNIQUE_CONSTRAINT_CATALOG
                        catalog,
                        // UNIQUE_CONSTRAINT_SCHEMA
                        unique.getSchema().getName(),
                        // UNIQUE_CONSTRAINT_NAME
                        unique.getName(),
                        // MATCH_OPTION
                        "NONE",
                        // UPDATE_RULE
                        constraint.getUpdateAction().getSqlName(), // DELETE_RULE
                        constraint.getDeleteAction().getSqlName()
                );
            }
            break;
        }
        case CHECK_CONSTRAINTS: {
            for (SchemaObject obj : database.getAllSchemaObjects(DbObject.CONSTRAINT)) {
                Constraint constraint = (Constraint) obj;
                Type constraintType = constraint.getConstraintType();
                if (constraintType == Constraint.Type.CHECK) {
                    ConstraintCheck check = (ConstraintCheck) obj;
                    Table table = check.getTable();
                    if (hideTable(table, session)) {
                        continue;
                    }
                } else if (constraintType != Constraint.Type.DOMAIN) {
                    continue;
                }
                add(session,
                        rows,
                        // CONSTRAINT_CATALOG
                        catalog,
                        // CONSTRAINT_SCHEMA
                        obj.getSchema().getName(),
                        // CONSTRAINT_NAME
                        obj.getName(), // CHECK_CLAUSE
                        constraint.getExpression().getUnenclosedSQL(new StringBuilder(), DEFAULT_SQL_FLAGS).toString()
                );
            }
            break;
        }
        case CONSTRAINT_COLUMN_USAGE: {
            for (SchemaObject obj : database.getAllSchemaObjects(DbObject.CONSTRAINT)) {
                Constraint constraint = (Constraint) obj;
                switch (constraint.getConstraintType()) {
                case CHECK:
                case DOMAIN: {
                    HashSet<Column> columns = new HashSet<>();
                    constraint.getExpression().isEverything(ExpressionVisitor.getColumnsVisitor(columns, null));
                    for (Column column: columns) {
                        Table table = column.getTable();
                        if (checkIndex(session, table.getName(), indexFrom, indexTo) && !hideTable(table, session)) {
                            addConstraintColumnUsage(session, rows, catalog, constraint, column);
                        }
                    }
                    break;
                }
                case REFERENTIAL: {
                    Table table = constraint.getRefTable();
                    if (checkIndex(session, table.getName(), indexFrom, indexTo) && !hideTable(table, session)) {
                        for (Column column : constraint.getReferencedColumns(table)) {
                            addConstraintColumnUsage(session, rows, catalog, constraint, column);
                        }
                    }
                }
                //$FALL-THROUGH$
                case PRIMARY_KEY:
                case UNIQUE: {
                    Table table = constraint.getTable();
                    if (checkIndex(session, table.getName(), indexFrom, indexTo) && !hideTable(table, session)) {
                        for (Column column : constraint.getReferencedColumns(table)) {
                            addConstraintColumnUsage(session, rows, catalog, constraint, column);
                        }
                    }
                }
                }
            }
            break;
        }
        default:
            DbException.throwInternalError("type="+type);
        }
        return rows;
    }

    private static String getDataTypeName(DataType dt, TypeInfo typeInfo) {
        if (typeInfo.getValueType() == Value.ARRAY) {
            typeInfo = (TypeInfo) typeInfo.getExtTypeInfo();
            // Use full type names with parameters for elements
            return typeInfo.getSQL(new StringBuilder()).append(" ARRAY").toString();
        }
        return dt.name;
    }

    private static short getRefAction(ConstraintActionType action) {
        switch (action) {
        case CASCADE:
            return DatabaseMetaData.importedKeyCascade;
        case RESTRICT:
            return DatabaseMetaData.importedKeyRestrict;
        case SET_DEFAULT:
            return DatabaseMetaData.importedKeySetDefault;
        case SET_NULL:
            return DatabaseMetaData.importedKeySetNull;
        default:
            throw DbException.throwInternalError("action="+action);
        }
    }

    private void addConstraintColumnUsage(Session session, ArrayList<Row> rows, String catalog, Constraint constraint,
            Column column) {
        Table table = column.getTable();
        add(session,
                rows,
                // TABLE_CATALOG
                catalog,
                // TABLE_SCHEMA
                table.getSchema().getName(),
                // TABLE_NAME
                table.getName(),
                // COLUMN_NAME
                column.getName(),
                // CONSTRAINT_CATALOG
                catalog,
                // CONSTRAINT_SCHEMA
                constraint.getSchema().getName(), // CONSTRAINT_NAME
                constraint.getName()
        );
    }

    private void addPrivileges(Session session, ArrayList<Row> rows, DbObject grantee,
            String catalog, Table table, String column, int rightMask) {
        if ((rightMask & Right.SELECT) != 0) {
            addPrivilege(session, rows, grantee, catalog, table, column, "SELECT");
        }
        if ((rightMask & Right.INSERT) != 0) {
            addPrivilege(session, rows, grantee, catalog, table, column, "INSERT");
        }
        if ((rightMask & Right.UPDATE) != 0) {
            addPrivilege(session, rows, grantee, catalog, table, column, "UPDATE");
        }
        if ((rightMask & Right.DELETE) != 0) {
            addPrivilege(session, rows, grantee, catalog, table, column, "DELETE");
        }
    }

    private void addPrivilege(Session session, ArrayList<Row> rows, DbObject grantee,
            String catalog, Table table, String column, String right) {
        String isGrantable = "NO";
        if (grantee.getType() == DbObject.USER) {
            User user = (User) grantee;
            if (user.isAdmin()) {
                // the right is grantable if the grantee is an admin
                isGrantable = "YES";
            }
        }
        if (column == null) {
            add(session,
                    rows,
                    // GRANTOR
                    null,
                    // GRANTEE
                    identifier(grantee.getName()),
                    // TABLE_CATALOG
                    catalog,
                    // TABLE_SCHEMA
                    table.getSchema().getName(),
                    // TABLE_NAME
                    table.getName(),
                    // PRIVILEGE_TYPE
                    right, // IS_GRANTABLE
                    isGrantable
            );
        } else {
            add(session,
                    rows,
                    // GRANTOR
                    null,
                    // GRANTEE
                    identifier(grantee.getName()),
                    // TABLE_CATALOG
                    catalog,
                    // TABLE_SCHEMA
                    table.getSchema().getName(),
                    // TABLE_NAME
                    table.getName(),
                    // COLUMN_NAME
                    column,
                    // PRIVILEGE_TYPE
                    right, // IS_GRANTABLE
                    isGrantable
            );
        }
    }

    @Override
    public long getMaxDataModificationId() {
        switch (type) {
        case SETTINGS:
        case SEQUENCES:
        case IN_DOUBT:
        case SESSIONS:
        case LOCKS:
        case SESSION_STATE:
            return Long.MAX_VALUE;
        }
        return database.getModificationDataId();
    }

}
