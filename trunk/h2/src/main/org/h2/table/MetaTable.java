/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
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
import java.text.Collator;
import java.util.Locale;

import org.h2.constraint.Constraint;
import org.h2.constraint.ConstraintCheck;
import org.h2.constraint.ConstraintReferential;
import org.h2.constraint.ConstraintUnique;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.FunctionAlias;
import org.h2.engine.Mode;
import org.h2.engine.Right;
import org.h2.engine.Role;
import org.h2.engine.Session;
import org.h2.engine.Setting;
import org.h2.engine.User;
import org.h2.engine.UserDataType;
import org.h2.expression.ValueExpression;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.index.MetaIndex;
import org.h2.message.Message;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.schema.Constant;
import org.h2.schema.Schema;
import org.h2.schema.Sequence;
import org.h2.schema.TriggerObject;
import org.h2.store.DiskFile;
import org.h2.store.InDoubtTransaction;
import org.h2.tools.Csv;
import org.h2.util.ObjectArray;
import org.h2.util.Resources;
import org.h2.util.StringUtils;
import org.h2.value.CompareMode;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueString;

/**
 * @author Thomas
 */

public class MetaTable extends Table {

    // TODO INFORMATION_SCHEMA.tables: select table_name from INFORMATION_SCHEMA.tables where TABLE_TYPE = 'BASE TABLE'

    public static final int TABLES = 0, COLUMNS = 1, INDEXES = 2, TABLE_TYPES=3,
    TYPE_INFO=4, CATALOGS=5, SETTINGS=6, HELP=7, SEQUENCES=8, USERS=9,
    ROLES=10, RIGHTS=11, FUNCTION_ALIASES = 12, SCHEMATA = 13, TABLE_PRIVILEGES = 14,
    COLUMN_PRIVILEGES = 15, COLLATIONS = 16, VIEWS = 17, IN_DOUBT = 18, CROSS_REFERENCES = 19,
    CONSTRAINTS = 20, FUNCTION_COLUMNS = 21, CONSTANTS = 22, DOMAINS = 23, TRIGGERS = 24;

    private int type;
    private MetaIndex index;
    private int indexColumn;

    public MetaTable(Schema schema, int type) throws SQLException {
        // tableName will be set later
        super(schema, 0, null, true);
        this.type = type;
        Column[] cols;
        String indexColumnName = null;
        switch(type) {
        case TABLES:
            setObjectName("TABLES");
            cols = createColumns(new String[]{
                    "TABLE_CATALOG",
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "TABLE_TYPE",
                    // extensions
                    "STORAGE_TYPE",
                    "SQL",
                    "REMARKS"
            });
            indexColumnName = "TABLE_NAME";
            break;
        case COLUMNS:
            setObjectName("COLUMNS");
            cols = createColumns(new String[]{
                    "TABLE_CATALOG",
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "COLUMN_NAME",
                    "ORDINAL_POSITION INT",
                    "COLUMN_DEFAULT",
                    "IS_NULLABLE",
                    "DATA_TYPE SMALLINT",
                    "CHARACTER_MAXIMUM_LENGTH INT",
                    "CHARACTER_OCTET_LENGTH INT",
                    "NUMERIC_PRECISION INT",
                    "NUMERIC_PRECISION_RADIX INT",
                    "NUMERIC_SCALE INT",
                    "CHARACTER_SET_NAME",
                    "COLLATION_NAME",
                    // extensions
                    "TYPE_NAME",
                    "NULLABLE SMALLINT",
                    "IS_COMPUTED BIT",
                    "SELECTIVITY INT",
                    "CHECK_CONSTRAINT",
                    "REMARKS"
            });
            indexColumnName = "TABLE_NAME";
            break;
        case INDEXES:
            setObjectName("INDEXES");
            cols = createColumns(new String[]{
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
                    "REMARKS"
            });
            indexColumnName = "TABLE_NAME";
            break;
        case TABLE_TYPES:
            setObjectName("TABLE_TYPES");
            cols = createColumns(
                    new String[]{"TYPE"});
            break;
        case TYPE_INFO:
            setObjectName("TYPE_INFO");
            cols = createColumns(new String[]{
                "TYPE_NAME",
                "DATA_TYPE SMALLINT",
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
            });
            break;
        case CATALOGS:
            setObjectName("CATALOGS");
            cols = createColumns(
                    new String[]{"CATALOG_NAME"});
            break;
        case SETTINGS:
            setObjectName("SETTINGS");
            cols = createColumns(
                    new String[]{"NAME", "VALUE"});
            break;
        case HELP:
            setObjectName("HELP");
            cols = createColumns(new String[]{
                    "ID INT",
                    "SECTION",
                    "TOPIC",
                    "SYNTAX",
                    "TEXT",
                    "EXAMPLE"
            });
            break;
        case SEQUENCES:
            setObjectName("SEQUENCES");
            cols = createColumns(new String[]{
                    "SEQUENCE_CATALOG",
                    "SEQUENCE_SCHEMA",
                    "SEQUENCE_NAME",
                    "CURRENT_VALUE BIGINT",
                    "INCREMENT BIGINT",
                    "IS_GENERATED BIT",
                    "REMARKS"
            });
            break;
        case USERS:
            setObjectName("USERS");
            cols = createColumns(new String[]{
                    "NAME",
                    "ADMIN",
                    "REMARKS"
            });
            break;
        case ROLES:
            setObjectName("ROLES");
            cols = createColumns(new String[]{
                    "NAME",
                    "REMARKS"
            });
            break;
        case RIGHTS:
            setObjectName("RIGHTS");
            cols = createColumns(new String[]{
                    "GRANTEE",
                    "GRANTEETYPE",
                    "GRANTEDROLE",
                    "RIGHTS",
                    "TABLE_SCHEMA",
                    "TABLE_NAME"
            });
            indexColumnName = "TABLE_NAME";
            break;
        case FUNCTION_ALIASES:
            setObjectName("FUNCTION_ALIASES");
            cols = createColumns(new String[]{
                    "ALIAS_CATALOG",
                    "ALIAS_SCHEMA",
                    "ALIAS_NAME",
                    "JAVA_CLASS",
                    "JAVA_METHOD",
                    "DATA_TYPE INT",
                    "COLUMN_COUNT INT",
                    "RETURNS_RESULT SMALLINT",
                    "REMARKS"
            });
            break;
        case FUNCTION_COLUMNS:
            setObjectName("FUNCTION_COLUMNS");
            cols = createColumns(new String[]{
                    "ALIAS_CATALOG",
                    "ALIAS_SCHEMA",
                    "ALIAS_NAME",
                    "JAVA_CLASS",
                    "JAVA_METHOD",
                    "POS INT",
                    "COLUMN_NAME",
                    "DATA_TYPE INT",
                    "TYPE_NAME",
                    "PRECISION INT",
                    "SCALE SMALLINT",
                    "RADIX SMALLINT",
                    "NULLABLE SMALLINT",
                    "COLUMN_TYPE SMALLINT",
                    "REMARKS"
            });
            break;
        case SCHEMATA:
            setObjectName("SCHEMATA");
            cols = createColumns(new String[]{
                    "CATALOG_NAME",
                    "SCHEMA_NAME",
                    "SCHEMA_OWNER",
                    "DEFAULT_CHARACTER_SET_NAME",
                    "DEFAULT_COLLATION_NAME",
                    "IS_DEFAULT BIT",
                    "REMARKS"
            });
            break;
        case TABLE_PRIVILEGES:
            setObjectName("TABLE_PRIVILEGES");
            cols = createColumns(new String[]{
                    "GRANTOR",
                    "GRANTEE",
                    "TABLE_CATALOG",
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "PRIVILEGE_TYPE",
                    "IS_GRANTABLE",
            });
            indexColumnName = "TABLE_NAME";
            break;
        case COLUMN_PRIVILEGES:
            setObjectName("COLUMN_PRIVILEGES");
            cols = createColumns(new String[]{
                    "GRANTOR",
                    "GRANTEE",
                    "TABLE_CATALOG",
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "COLUMN_NAME",
                    "PRIVILEGE_TYPE",
                    "IS_GRANTABLE",
            });
            indexColumnName = "TABLE_NAME";
            break;
        case COLLATIONS:
            setObjectName("COLLATIONS");
            cols = createColumns(new String[]{
                    "NAME",
                    "KEY"
            });
            break;
        case VIEWS:
            setObjectName("VIEWS");
            cols = createColumns(new String[]{
                    "TABLE_CATALOG",
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "VIEW_DEFINITION",
                    "CHECK_OPTION",
                    "IS_UPDATABLE",
                    "STATUS",
                    "REMARKS"
            });
            indexColumnName = "TABLE_NAME";
            break;
        case IN_DOUBT:
            setObjectName("IN_DOUBT");
            cols = createColumns(new String[]{
                    "TRANSACTION",
                    "STATE",
            });
            break;
        case CROSS_REFERENCES:
            setObjectName("CROSS_REFERENCES");
            cols = createColumns(new String[]{
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
            });
            indexColumnName = "PKTABLE_NAME";
            break;
        case CONSTRAINTS:
            setObjectName("CONSTRAINTS");
            cols = createColumns(new String[]{
                    "CONSTRAINT_CATALOG",
                    "CONSTRAINT_SCHEMA",
                    "CONSTRAINT_NAME",
                    "CONSTRAINT_TYPE",
                    "TABLE_CATALOG",
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "CHECK_EXPRESSION",
                    "COLUMN_LIST",
                    "REMARKS",
                    "SQL",
            });
            indexColumnName = "TABLE_NAME";
            break;
        case CONSTANTS:
            setObjectName("CONSTANTS");
            cols = createColumns(new String[]{
                    "CONSTANT_CATALOG",
                    "CONSTANT_SCHEMA",
                    "CONSTANT_NAME",
                    "DATA_TYPE SMALLINT",
                    "REMARKS",
                    "SQL",
            });
            break;
        case DOMAINS:
            setObjectName("DOMAINS");
            cols = createColumns(new String[]{
                    "DOMAIN_CATALOG",
                    "DOMAIN_SCHEMA",
                    "DOMAIN_NAME",
                    "COLUMN_DEFAULT",
                    "IS_NULLABLE",
                    "DATA_TYPE SMALLINT",
                    "PRECISION INT",
                    "SCALE INT",
                    "TYPE_NAME",
                    "SELECTIVITY INT",
                    "CHECK_CONSTRAINT",
                    "REMARKS",
                    "SQL",
            });
            break;
        case TRIGGERS:
            setObjectName("TRIGGERS");
            cols = createColumns(new String[]{
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
            });
            break;
        default:
            throw Message.getInternalError("type="+type);
        }
        setColumns(cols);

        if(indexColumnName == null) {
            indexColumn = -1;
        } else {
            indexColumn = getColumn(indexColumnName).getColumnId();
            Column[] indexCols = new Column[]{cols[indexColumn]};
            index = new MetaIndex(this, indexCols, false);
        }
    }

    private Column[] createColumns(String[] names) {
        Column[] cols = new Column[names.length];
        for(int i=0; i<names.length; i++) {
            String nameType = names[i];
            int idx = nameType.indexOf(' ');
            int type;
            String name;
            if(idx < 0) {
                type = Value.STRING;
                name = nameType;
            } else {
                type = DataType.getTypeByName(nameType.substring(idx+1)).type;
                name = nameType.substring(0, idx);
            }
            cols[i] = new Column(name, type, 0, 0);
        }
        return cols;
    }

    public String getCreateSQL() {
        return null;
    }

    public Index addIndex(Session session, String indexName, int indexId, Column[] cols, IndexType indexType, int headPos, String comment) throws SQLException {
        throw Message.getUnsupportedException();
    }

    public void lock(Session session, boolean exclusive) throws SQLException {
        // nothing to do
    }

    public boolean isLockedExclusively() {
        return false;
    }

    private String identifier(String s) {
        if(Mode.getCurrentMode().lowerCaseIdentifiers) {
            s = (s==null ? s : StringUtils.toLowerEnglish(s));
        }
        return s;
    }

    private ObjectArray getAllTables(Session session) {
        ObjectArray tables = database.getAllSchemaObjects(DbObject.TABLE_OR_VIEW);
        ObjectArray tempTables = session.getLocalTempTables();
        tables.addAll(tempTables);
        return tables;
    }

    private boolean checkIndex(Session session, String value, Value indexFrom, Value indexTo) throws SQLException {
        if(value == null || (indexFrom == null && indexTo == null)) {
            return true;
        }
        Database db = session.getDatabase();
        Value v = value == null ? (Value)ValueNull.INSTANCE : ValueString.get(value);
        if(indexFrom != null && db.compare(v, indexFrom) < 0) {
            return false;
        }
        if(indexTo != null && db.compare(v, indexTo) > 0) {
            return false;
        }
        return true;
    }

    private String replaceNullWithEmpty(String s) {
        return s == null ? "" : s;
    }

    public ObjectArray generateRows(Session session, SearchRow first, SearchRow last) throws SQLException {
        Value indexFrom = null, indexTo = null;
        if(indexColumn >= 0) {
            if(first != null) {
                indexFrom = first.getValue(indexColumn);
            }
            if(last != null) {
                indexTo = last.getValue(indexColumn);
            }
        }
        ObjectArray rows = new ObjectArray();
        String catalog = identifier(database.getShortName());
        switch(type) {
        case TABLES: {
            ObjectArray tables = getAllTables(session);
            for(int i=0; i<tables.size(); i++) {
                Table table = (Table) tables.get(i);
                String tableName = identifier(table.getName());
                if(!checkIndex(session, tableName, indexFrom, indexTo)) {
                    continue;
                }
                String storageType;
                if(table.getTemporary()) {
                    if(table.getGlobalTemporary()) {
                        storageType = "GLOBAL TEMPORARY";
                    } else {
                        storageType = "LOCAL TEMPORARY";
                    }
                } else {
                    storageType = table.isPersistent() ? "CACHED" : "MEMORY";
                }
                add(rows, new String[]{
                        catalog, // TABLE_CATALOG
                        identifier(table.getSchema().getName()), // TABLE_SCHEMA
                        tableName, // TABLE_NAME
                        table.getTableType(), // TABLE_TYPE
                        storageType, // STORAGE_TYPE
                        table.getCreateSQL(), // SQL
                        replaceNullWithEmpty(table.getComment()) // REMARKS
                });
            }
            break;
        }
        case COLUMNS: {
            ObjectArray tables = getAllTables(session);
            for(int i=0; i<tables.size(); i++) {
                Table table = (Table) tables.get(i);
                String tableName = identifier(table.getName());
                if(!checkIndex(session, tableName, indexFrom, indexTo)) {
                    continue;
                }
                Column[] cols = table.getColumns();
                String collation = database.getCompareMode().getName();
                for(int j=0; j<cols.length; j++) {
                    Column c = cols[j];
                    add(rows, new String[]{
                            catalog, // TABLE_CATALOG
                            identifier(table.getSchema().getName()), // TABLE_SCHEMA
                            tableName, // TABLE_NAME
                            identifier(c.getName()), // COLUMN_NAME
                            String.valueOf(j + 1), // ORDINAL_POSITION
                            c.getDefaultSQL(), // COLUMN_DEFAULT
                            c.getNullable() ? "YES" : "NO", // IS_NULLABLE
                            "" + DataType.convertTypeToSQLType(c.getType()), // DATA_TYPE
                            "" + c.getPrecisionAsInt(), // CHARACTER_MAXIMUM_LENGTH
                            "" + c.getPrecisionAsInt(), // CHARACTER_OCTET_LENGTH
                            "" + c.getPrecisionAsInt(), // NUMERIC_PRECISION
                            "10", // NUMERIC_PRECISION_RADIX
                            "" + c.getScale(), // NUMERIC_SCALE
                            Constants.CHARACTER_SET_NAME, // CHARACTER_SET_NAME
                            collation, // COLLATION_NAME
                            identifier(DataType.getDataType(c.getType()).name), // TYPE_NAME
                            "" + (c.getNullable() ? DatabaseMetaData.columnNullable : DatabaseMetaData.columnNoNulls) , // NULLABLE
                            "" + (c.getComputed() ? "TRUE" : "FALSE"), // IS_COMPUTED
                            "" + (c.getSelectivity()), // SELECTIVITY
                            c.getCheckConstraintSQL(session, c.getName()),  // CHECK_CONSTRAINT
                            replaceNullWithEmpty(c.getComment()) // REMARKS
                    });
                }
            }
            break;
        }
        case INDEXES: {
            ObjectArray tables = getAllTables(session);
            for(int i=0; i<tables.size(); i++) {
                Table table = (Table) tables.get(i);
                String tableName = identifier(table.getName());
                if(!checkIndex(session, tableName, indexFrom, indexTo)) {
                    continue;
                }
                ObjectArray idx = table.getIndexes();
                for(int j=0; idx != null && j<idx.size(); j++) {
                    Index index = (Index) idx.get(j);
                    if(index.getCreateSQL() == null) {
                        continue;
                    }
                    Column[] cols = index.getColumns();
                    for(int k=0; k<cols.length; k++) {
                        Column column = cols[k];
                        add(rows,new String[]{
                                catalog, // TABLE_CATALOG
                                identifier(table.getSchema().getName()), // TABLE_SCHEMA
                                tableName, // TABLE_NAME
                                index.getIndexType().isUnique() ? "FALSE" : "TRUE", // NON_UNIQUE
                                identifier(index.getName()), // INDEX_NAME
                                "" + (k+1), // ORDINAL_POSITION
                                identifier(column.getName()), // COLUMN_NAME
                                "0", // CARDINALITY
                                index.getIndexType().isPrimaryKey() ? "TRUE" : "FALSE", // PRIMARY_KEY
                                index.getIndexType().getSQL(), // INDEX_TYPE_NAME
                                index.getIndexType().belongsToConstraint() ? "TRUE" : "FALSE", // IS_GENERATED
                                "" + DatabaseMetaData.tableIndexOther, // INDEX_TYPE
                                "A", // ASC_OR_DESC
                                "0", // PAGES
                                "", // FILTER_CONDITION
                                replaceNullWithEmpty(index.getComment()) // REMARKS
                            });
                    }
                }
            }
            break;
        }
        case TABLE_TYPES: {
            add(rows,new String[]{Table.TABLE});
            add(rows,new String[]{Table.TABLE_LINK});
            add(rows,new String[]{Table.SYSTEM_TABLE});
            add(rows,new String[]{Table.VIEW});
            break;
        }
        case CATALOGS: {
            add(rows,new String[]{catalog});
            break;
        }
        case SETTINGS: {
            ObjectArray list = database.getAllSettings();
            for(int i=0; i<list.size(); i++) {
                Setting s = (Setting)list.get(i);
                String value = s.getStringValue();
                if(value == null) {
                    value = "" + s.getIntValue();
                }
                add(rows,new String[]{
                        identifier(s.getName()),
                        value
                });
            }
            add(rows, new String[]{"MODE", Mode.getCurrentMode().getName()});
            DiskFile dataFile = database.getDataFile();
            if(dataFile != null) {
                add(rows, new String[]{"CACHE_TYPE", dataFile.getCache().getTypeName()});
                if(session.getUser().getAdmin()) {
                    add(rows, new String[]{"FILE_DISK_WRITE", "" + dataFile.getWriteCount()});
                    add(rows, new String[]{"FILE_DISK_READ", "" + dataFile.getReadCount()});
                    add(rows, new String[]{"FILE_INDEX_WRITE", "" + database.getIndexFile().getWriteCount()});
                    add(rows, new String[]{"FILE_INDEX_READ", "" + database.getIndexFile().getReadCount()});
                }
            }
            break;
        }
        case TYPE_INFO: {
            ObjectArray types = DataType.getTypes();
            for(int i=0; i<types.size(); i++) {
                DataType t = (DataType) types.get(i);
                if(t.hidden || t.sqlType == Value.NULL) {
                    continue;
                }
                add(rows,new String[]{
                        t.name, // TYPE_NAME
                        String.valueOf(t.sqlType), // DATA_TYPE
                        String.valueOf(t.maxPrecision), // PRECISION
                        t.prefix, // PREFIX
                        t.suffix, // SUFFIX
                        t.params, // PARAMS
                        String.valueOf(t.autoInc), // AUTO_INCREMENT
                        String.valueOf(t.minScale), // MINIMUM_SCALE
                        String.valueOf(t.maxScale), // MAXIMUM_SCALE
                        t.decimal ? "10" : null, // RADIX
                        String.valueOf(t.order), // POS
                        String.valueOf(t.caseSensitive), // CASE_SENSITIVE
                        "" + DatabaseMetaData.typeNullable, // NULLABLE
                        "" + DatabaseMetaData.typeSearchable // SEARCHABLE
                });
            }
            break;
        }
        case HELP: {
            try {
                byte[] data = Resources.get("/org/h2/res/help.csv");
                Reader reader = new InputStreamReader(new ByteArrayInputStream(data));
                ResultSet rs = Csv.getInstance().read(reader, null);
                for(int i=0; rs.next(); i++) {
                    add(rows, new String[]{
                        String.valueOf(i), // ID
                        rs.getString(1).trim(), // SECTION
                        rs.getString(2).trim(), // TOPIC
                        rs.getString(3).trim(), // SYNTAX
                        rs.getString(4).trim(), // TEXT
                        rs.getString(5).trim(), // EXAMPLE
                    });
                }
            } catch (IOException e) {
                throw Message.convert(e);
            }
            break;
        }
        case SEQUENCES: {
            ObjectArray sequences = database.getAllSchemaObjects(DbObject.SEQUENCE);
            for(int i=0; i<sequences.size(); i++) {
                Sequence s = (Sequence) sequences.get(i);
                add(rows,new String[]{
                        catalog, // SEQUENCE_CATALOG
                        identifier(s.getSchema().getName()), // SEQUENCE_SCHEMA
                        identifier(s.getName()), // SEQUENCE_NAME
                        String.valueOf(s.getCurrentValue()), // CURRENT_VALUE
                        String.valueOf(s.getIncrement()), // INCREMENT
                        s.getBelongsToTable() ? "TRUE" : "FALSE", // IS_GENERATED
                        replaceNullWithEmpty(s.getComment()) // REMARKS
                    });
            }
            break;
        }
        case USERS: {
            ObjectArray users = database.getAllUsers();
            for(int i=0; i<users.size(); i++) {
                User u = (User) users.get(i);
                add(rows,new String[]{
                        identifier(u.getName()), // NAME
                        String.valueOf(u.getAdmin()), // ADMIN
                        replaceNullWithEmpty(u.getComment()) // REMARKS
                });
            }
            break;
        }
        case ROLES: {
            ObjectArray roles = database.getAllRoles();
            for(int i=0; i<roles.size(); i++) {
                Role r = (Role) roles.get(i);
                add(rows,new String[]{
                        identifier(r.getName()), // NAME
                        replaceNullWithEmpty(r.getComment()) // REMARKS
                });
            }
            break;
        }
        case RIGHTS: {
            ObjectArray rights = database.getAllRights();
            for(int i=0; i<rights.size(); i++) {
                Right r = (Right) rights.get(i);
                // "GRANTEE", "GRANTEETYPE", "GRANTEDROLE", "RIGHTS", "TABLE"
                Role role = r.getGrantedRole();
                DbObject grantee = r.getGrantee();
                String type = grantee.getType() == DbObject.USER ? "USER" : "ROLE";
                if(role == null) {
                    Table granted = r.getGrantedTable();
                    String tableName = identifier(granted.getName());
                    if(!checkIndex(session, tableName, indexFrom, indexTo)) {
                        continue;
                    }
                    add(rows,new String[]{
                            identifier(grantee.getName()),
                            type,
                            "",
                            r.getRights(),
                            identifier(granted.getSchema().getName()),
                            identifier(granted.getName())
                    });
                } else {
                    add(rows,new String[]{
                            identifier(grantee.getName()),
                            type,
                            identifier(role.getName()),
                            "",
                            "",
                            ""
                    });
                }
            }
            break;
        }
        case FUNCTION_ALIASES: {
            ObjectArray aliases = database.getAllFunctionAliases();
            for(int i=0; i<aliases.size(); i++) {
                FunctionAlias alias = (FunctionAlias) aliases.get(i);
                int returnsResult = alias.getDataType() == Value.NULL ? DatabaseMetaData.procedureNoResult : DatabaseMetaData.procedureReturnsResult;
                add(rows,new String[]{
                        catalog, // ALIAS_CATALOG
                        Constants.SCHEMA_MAIN, // ALIAS_SCHEMA
                        identifier(alias.getName()), // ALIAS_NAME
                        alias.getJavaClassName(), // JAVA_CLASS
                        alias.getJavaMethodName(), // JAVA_METHOD
                        ""+DataType.convertTypeToSQLType(alias.getDataType()), // DATA_TYPE
                        ""+ alias.getColumnClasses().length, // COLUMN_COUNT INT
                        ""+ returnsResult, // RETURNS_RESULT SMALLINT
                        replaceNullWithEmpty(alias.getComment()) // REMARKS
                });
            }
            break;
        }
        case FUNCTION_COLUMNS: {
            ObjectArray aliases = database.getAllFunctionAliases();
            for(int i=0; i<aliases.size(); i++) {
                FunctionAlias alias = (FunctionAlias) aliases.get(i);
                Class[] columns = alias.getColumnClasses();
                for(int j=0; j<columns.length; j++) {
                    Class clazz = columns[j];
                    int type = DataType.getTypeFromClass(clazz);
                    DataType dt = DataType.getDataType(type);
                    int nullable = clazz.isPrimitive() ? DatabaseMetaData.columnNoNulls : DatabaseMetaData.columnNullable;
                    add(rows,new String[]{
                            catalog, // ALIAS_CATALOG
                            Constants.SCHEMA_MAIN, // ALIAS_SCHEMA
                            identifier(alias.getName()), // ALIAS_NAME
                            alias.getJavaClassName(), // JAVA_CLASS
                            alias.getJavaMethodName(), // JAVA_METHOD
                            "" + j, // POS INT
                            "P" + (j+1), // COLUMN_NAME
                            "" + DataType.convertTypeToSQLType(dt.type), // DATA_TYPE
                            dt.name, // TYPE_NAME
                            "" + dt.defaultPrecision, // PRECISION
                            "" + dt.defaultScale, // SCALE
                            "10", // RADIX
                            "" + nullable, // NULLABLE SMALLINT
                            "" + DatabaseMetaData.procedureColumnIn, // COLUMN_TYPE
                            "" // REMARKS
                    });
                }
            }
            break;
        }
        case SCHEMATA: {
            ObjectArray schemas = database.getAllSchemas();
            String collation = database.getCompareMode().getName();
            for(int i=0; i<schemas.size(); i++) {
                Schema schema = (Schema) schemas.get(i);
                add(rows,new String[]{
                        catalog, // CATALOG_NAME
                        identifier(schema.getName()), // SCHEMA_NAME
                        identifier(schema.getOwner().getName()), // SCHEMA_OWNER
                        Constants.CHARACTER_SET_NAME, // DEFAULT_CHARACTER_SET_NAME
                        collation, // DEFAULT_COLLATION_NAME
                        Constants.SCHEMA_MAIN.equals(schema.getName()) ? "TRUE" : "FALSE", // IS_DEFAULT
                        replaceNullWithEmpty(schema.getComment()) // REMARKS
                });
            }
            break;
        }
        case TABLE_PRIVILEGES: {
            ObjectArray rights = database.getAllRights();
            for(int i=0; i<rights.size(); i++) {
                Right r = (Right) rights.get(i);
                Table table = r.getGrantedTable();
                if(table == null) {
                    continue;
                }
                String tableName = identifier(table.getName());
                if(!checkIndex(session, tableName, indexFrom, indexTo)) {
                    continue;
                }
                addPrivileges(rows, r.getGrantee(), catalog, table, null, r.getRightMask());
            }
            break;
        }
        case COLUMN_PRIVILEGES: {
            ObjectArray rights = database.getAllRights();
            for(int i=0; i<rights.size(); i++) {
                Right r = (Right) rights.get(i);
                Table table = r.getGrantedTable();
                if(table == null) {
                    continue;
                }
                String tableName = identifier(table.getName());
                if(!checkIndex(session, tableName, indexFrom, indexTo)) {
                    continue;
                }
                DbObject grantee = r.getGrantee();
                int mask = r.getRightMask();
                Column[] columns = table.getColumns();
                for(int j=0; j<columns.length; j++) {
                    String column = columns[j].getName();
                    addPrivileges(rows, grantee, catalog, table, column, mask);
                }
            }
            break;
        }
        case COLLATIONS: {
            Locale[] locales = Collator.getAvailableLocales();
            for(int i=0; i<locales.length; i++) {
                Locale l = locales[i];
                add(rows,new String[]{
                        CompareMode.getName(l), // NAME
                        l.toString(), // KEY
                });
            }
            break;
        }
        case VIEWS: {
            ObjectArray tables = getAllTables(session);
            for(int i=0; i<tables.size(); i++) {
                Table table = (Table) tables.get(i);
                if(!table.getTableType().equals(Table.VIEW)) {
                    continue;
                }
                String tableName = identifier(table.getName());
                if(!checkIndex(session, tableName, indexFrom, indexTo)) {
                    continue;
                }
                TableView view = (TableView)table;
                add(rows, new String[]{
                        catalog, // TABLE_CATALOG
                        identifier(table.getSchema().getName()), // TABLE_SCHEMA
                        tableName, // TABLE_NAME
                        table.getCreateSQL(), // VIEW_DEFINITION
                        "NONE", // CHECK_OPTION
                        "NO", // IS_UPDATABLE
                        view.getInvalid() ? "INVALID" : "VALID", // STATUS
                        replaceNullWithEmpty(view.getComment()) // REMARKS
                });
            }
            break;
        }
        case IN_DOUBT: {
            ObjectArray prepared = database.getLog().getInDoubtTransactions();
            for(int i=0; prepared != null && i<prepared.size(); i++) {
                InDoubtTransaction prep = (InDoubtTransaction) prepared.get(i);
                add(rows, new String[] {
                        prep.getTransaction(), // TRANSACTION
                        prep.getState(), // STATE
                });
            }
            break;
        }
        case CROSS_REFERENCES: {
            ObjectArray constraints = database.getAllSchemaObjects(DbObject.CONSTRAINT);
            for(int i=0; i<constraints.size(); i++) {
                Constraint constraint = (Constraint) constraints.get(i);
                if(!(constraint.getConstraintType().equals(Constraint.REFERENTIAL))) {
                    continue;
                }
                ConstraintReferential ref = (ConstraintReferential) constraint;
                Column[] cols = ref.getColumns();
                Column[] refCols = ref.getRefColumns();
                Table tab = ref.getTable();
                Table refTab = ref.getRefTable();
                String tableName = identifier(refTab.getName());
                if(!checkIndex(session, tableName, indexFrom, indexTo)) {
                    continue;
                }
                int upd = getRefAction(ref.getUpdateAction());
                int del = getRefAction(ref.getDeleteAction());
                for(int j=0; j<cols.length; j++) {
                    add(rows, new String[] {
                            catalog, // PKTABLE_CATALOG
                            identifier(refTab.getSchema().getName()), // PKTABLE_SCHEMA
                            identifier(refTab.getName()), // PKTABLE_NAME
                            identifier(refCols[j].getName()), // PKCOLUMN_NAME
                            catalog, // FKTABLE_CATALOG
                            identifier(tab.getSchema().getName()), // FKTABLE_SCHEMA
                            identifier(tab.getName()), // FKTABLE_NAME
                            identifier(cols[j].getName()), // FKCOLUMN_NAME
                            String.valueOf(j + 1), // ORDINAL_POSITION
                            String.valueOf(upd), // UPDATE_RULE SMALLINT
                            String.valueOf(del), // DELETE_RULE SMALLINT
                            identifier(ref.getName()), // FK_NAME
                            null, // PK_NAME
                            "" + DatabaseMetaData.importedKeyNotDeferrable, // DEFERRABILITY
                    });
                }
            }
            break;
        }
        case CONSTRAINTS: {
            ObjectArray constraints = database.getAllSchemaObjects(DbObject.CONSTRAINT);
            for(int i=0; i<constraints.size(); i++) {
                Constraint constraint = (Constraint) constraints.get(i);
                String type = constraint.getConstraintType();
                String checkExpression = null;
                Column[] columns = null;
                Table table = constraint.getTable();
                String tableName = identifier(table.getName());
                if(!checkIndex(session, tableName, indexFrom, indexTo)) {
                    continue;
                }
                if(type.equals(Constraint.CHECK)) {
                    checkExpression = ((ConstraintCheck)constraint).getExpression().getSQL();
                } else if(type.equals(Constraint.UNIQUE)) {
                    columns = ((ConstraintUnique)constraint).getColumns();
                } else if(type.equals(Constraint.REFERENTIAL)) {
                    columns = ((ConstraintReferential)constraint).getColumns();
                }
                String columnList = null;
                if(columns != null) {
                    columnList = "";
                    for(int j=0; j<columns.length; j++) {
                        if(j>0) {
                            columnList += ",";
                        }
                        columnList += columns[j].getName();
                    }
                }
                add(rows, new String[] {
                        catalog, // CONSTRAINT_CATALOG
                        identifier(constraint.getSchema().getName()), // CONSTRAINT_SCHEMA
                        identifier(constraint.getName()), // CONSTRAINT_NAME
                        type, // CONSTRAINT_TYPE
                        catalog, // TABLE_CATALOG
                        identifier(table.getSchema().getName()), // TABLE_SCHEMA
                        tableName, // TABLE_NAME
                        checkExpression, // CHECK_EXPRESSION
                        columnList, // COLUMN_LIST
                        replaceNullWithEmpty(constraint.getComment()), // REMARKS
                        constraint.getCreateSQL(), // SQL
                    });
            }
            break;
        }
        case CONSTANTS: {
            ObjectArray constants = database.getAllSchemaObjects(DbObject.CONSTANT);
            for(int i=0; i<constants.size(); i++) {
                Constant constant = (Constant) constants.get(i);
                ValueExpression expr = constant.getValue();
                add(rows, new String[] {
                        catalog, // CONSTANT_CATALOG
                        identifier(constant.getSchema().getName()), // CONSTANT_SCHEMA
                        identifier(constant.getName()), // CONSTANT_NAME
                        "" + DataType.convertTypeToSQLType(expr.getType()), // CONSTANT_TYPE
                        replaceNullWithEmpty(constant.getComment()), // REMARKS
                        expr.getSQL(), // SQL
                    });
            }
            break;
        }
        case DOMAINS: {
            ObjectArray userDataTypes = database.getAllUserDataTypes();
            for(int i=0; i<userDataTypes.size(); i++) {
                UserDataType dt = (UserDataType) userDataTypes.get(i);
                Column col = dt.getColumn();
                add(rows, new String[] {
                        catalog, // DOMAIN_CATALOG
                        Constants.SCHEMA_MAIN, // DOMAIN_SCHEMA
                        identifier(dt.getName()), // DOMAIN_NAME
                        col.getDefaultSQL(), // COLUMN_DEFAULT
                        col.getNullable() ? "YES" : "NO", // IS_NULLABLE
                        "" + col.getDataType().sqlType, // DATA_TYPE
                        "" + col.getPrecisionAsInt(), // PRECISION INT
                        "" + col.getScale(), // SCALE INT
                        col.getDataType().name, // TYPE_NAME
                        "" + col.getSelectivity(), // SELECTIVITY INT
                        "" + col.getCheckConstraintSQL(session, "VALUE"), // CHECK_CONSTRAINT
                        replaceNullWithEmpty(dt.getComment()), // REMARKS
                        "" + dt.getCreateSQL() // SQL
                });
            }
            break;
        }
        case TRIGGERS: {
            ObjectArray triggers = database.getAllSchemaObjects(DbObject.TRIGGER);
            for(int i=0; i<triggers.size(); i++) {
                TriggerObject trigger = (TriggerObject) triggers.get(i);
                Table table = trigger.getTable();
                add(rows, new String[] {
                        catalog, // TRIGGER_CATALOG
                        identifier(trigger.getSchema().getName()), // TRIGGER_SCHEMA
                        identifier(trigger.getName()), // TRIGGER_NAME
                        trigger.getTypeNameList(), // TRIGGER_TYPE
                        catalog, // TABLE_CATALOG
                        identifier(table.getSchema().getName()), // TABLE_SCHEMA
                        identifier(table.getName()), // TABLE_NAME
                        "" + trigger.getBefore(), // BEFORE BIT
                        trigger.getTriggerClassName(), // JAVA_CLASS
                        "" + trigger.getQueueSize(), // QUEUE_SIZE INT
                        "" + trigger.getNoWait(), // NO_WAIT BIT
                        replaceNullWithEmpty(trigger.getComment()), // REMARKS
                        trigger.getSQL() // SQL
                });
            }
            break;
        }
        default:
            throw Message.getInternalError("type="+type);
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
            throw Message.getInternalError("action="+action);
        }
    }

    public void removeRow(Session session, Row row) throws SQLException {
        throw Message.getUnsupportedException();
    }

    public void addRow(Session session, Row row) throws SQLException {
        throw Message.getUnsupportedException();
    }

    public void removeIndex(String indexName) throws SQLException {
        throw Message.getUnsupportedException();
    }

    public void removeChildrenAndResources(Session session) throws SQLException {
        throw Message.getUnsupportedException();
    }

    public void close(Session session) throws SQLException {
        // nothing to do
    }

    public void unlock(Session s) {
        // nothing to do
    }

    private void addPrivileges(ObjectArray rows, DbObject grantee, String catalog, Table table, String column, int rightMask) throws SQLException {
        if((rightMask & Right.SELECT) != 0) {
            addPrivileg(rows, grantee, catalog, table, column, "SELECT");
        }
        if((rightMask & Right.INSERT) != 0) {
            addPrivileg(rows, grantee, catalog, table, column, "INSERT");
        }
        if((rightMask & Right.UPDATE) != 0) {
            addPrivileg(rows, grantee, catalog, table, column, "UPDATE");
        }
        if((rightMask & Right.DELETE) != 0) {
            addPrivileg(rows, grantee, catalog, table, column, "DELETE");
        }
    }

    private void addPrivileg(ObjectArray rows, DbObject grantee, String catalog, Table table, String column, String right) throws SQLException {
        String isGrantable = "NO";
        if(grantee.getType() == DbObject.USER) {
            User user = (User)grantee;
            if(user.getAdmin()) {
                // the right is grantable if the grantee is an admin
                isGrantable = "YES";
            }
        }
        if(column == null) {
            add(rows,new String[]{
                    null, // GRANTOR
                    identifier(grantee.getName()), // GRANTEE
                    catalog, // TABLE_CATALOG
                    identifier(table.getSchema().getName()), // TABLE_SCHEMA
                    identifier(table.getName()), // TABLE_NAME
                    right, // PRIVILEGE_TYPE
                    isGrantable // IS_GRANTABLE
            });
        } else {
            add(rows,new String[]{
                    null, // GRANTOR
                    identifier(grantee.getName()), // GRANTEE
                    catalog, // TABLE_CATALOG
                    identifier(table.getSchema().getName()), // TABLE_SCHEMA
                    identifier(table.getName()), // TABLE_NAME
                    identifier(column), // COLUMN_NAME
                    right, // PRIVILEGE_TYPE
                    isGrantable // IS_GRANTABLE
            });
        }
    }

    private void add(ObjectArray rows, String[] strings) throws SQLException {
        Value[] values = new Value[strings.length];
        for(int i=0; i<strings.length; i++) {
            String s = strings[i];
            Value v = (s == null) ? (Value)ValueNull.INSTANCE : ValueString.get(s);
            Column col = columns[i];
            v = v.convertTo(col.getType());
            values[i] = v;
        }
        Row row = new Row(values);
        row.setPos(rows.size());
        rows.add(row);
    }

    public void checkRename() throws SQLException {
        throw Message.getUnsupportedException();
    }

    public void checkSupportAlter() throws SQLException {
        throw Message.getUnsupportedException();
    }

    public void truncate(Session session) throws SQLException {
        throw Message.getUnsupportedException();
    }

    public int getRowCount() {
        throw Message.getInternalError();
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

    public Index getScanIndex(Session session) throws SQLException {
        return new MetaIndex(this, columns, true);
    }

    public ObjectArray getIndexes() {
        if(index == null) {
            return null;
        }
        ObjectArray list = new ObjectArray();
        list.add(new MetaIndex(this, columns, true));
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

}
