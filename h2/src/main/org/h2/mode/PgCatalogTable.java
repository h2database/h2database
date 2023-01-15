/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.h2.constraint.Constraint;
import org.h2.engine.Constants;
import org.h2.engine.RightOwner;
import org.h2.engine.SessionLocal;
import org.h2.engine.User;
import org.h2.index.Index;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.schema.Schema;
import org.h2.schema.TriggerObject;
import org.h2.server.pg.PgServer;
import org.h2.table.Column;
import org.h2.table.MetaTable;
import org.h2.table.Table;
import org.h2.util.StringUtils;
import org.h2.util.Utils;
import org.h2.value.DataType;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueDouble;
import org.h2.value.ValueInteger;
import org.h2.value.ValueSmallint;

/**
 * This class is responsible to build the pg_catalog tables.
 */
public final class PgCatalogTable extends MetaTable {

    private static final int PG_AM = 0;

    private static final int PG_ATTRDEF = PG_AM + 1;

    private static final int PG_ATTRIBUTE = PG_ATTRDEF + 1;

    private static final int PG_AUTHID = PG_ATTRIBUTE + 1;

    private static final int PG_CLASS = PG_AUTHID + 1;

    private static final int PG_CONSTRAINT = PG_CLASS + 1;

    private static final int PG_DATABASE = PG_CONSTRAINT + 1;

    private static final int PG_DESCRIPTION = PG_DATABASE + 1;

    private static final int PG_GROUP = PG_DESCRIPTION + 1;

    private static final int PG_INDEX = PG_GROUP + 1;

    private static final int PG_INHERITS = PG_INDEX + 1;

    private static final int PG_NAMESPACE = PG_INHERITS + 1;

    private static final int PG_PROC = PG_NAMESPACE + 1;

    private static final int PG_ROLES = PG_PROC + 1;

    private static final int PG_SETTINGS = PG_ROLES + 1;

    private static final int PG_TABLESPACE = PG_SETTINGS + 1;

    private static final int PG_TRIGGER = PG_TABLESPACE + 1;

    private static final int PG_TYPE = PG_TRIGGER + 1;

    private static final int PG_USER = PG_TYPE + 1;

    /**
     * The number of meta table types. Supported meta table types are
     * {@code 0..META_TABLE_TYPE_COUNT - 1}.
     */
    public static final int META_TABLE_TYPE_COUNT = PG_USER + 1;

    private static final Object[][] PG_EXTRA_TYPES = {
            { 18, "char", 1, 0 },
            { 19, "name", 64, 18 },
            { 22, "int2vector", -1, 21 },
            { 24, "regproc", 4, 0 },
            { PgServer.PG_TYPE_INT2_ARRAY, "_int2", -1, PgServer.PG_TYPE_INT2 },
            { PgServer.PG_TYPE_INT4_ARRAY, "_int4", -1, PgServer.PG_TYPE_INT4 },
            { PgServer.PG_TYPE_VARCHAR_ARRAY, "_varchar", -1, PgServer.PG_TYPE_VARCHAR },
            { 2205, "regclass", 4, 0 },
    };

    /**
     * Create a new metadata table.
     *
     * @param schema
     *            the schema
     * @param id
     *            the object id
     * @param type
     *            the meta table type
     */
    public PgCatalogTable(Schema schema, int id, int type) {
        super(schema, id, type);
        Column[] cols;
        switch (type) {
        case PG_AM:
            setMetaTableName("PG_AM");
            cols = new Column[] { //
                    column("OID", TypeInfo.TYPE_INTEGER), //
                    column("AMNAME", TypeInfo.TYPE_VARCHAR), //
            };
            break;
        case PG_ATTRDEF:
            setMetaTableName("PG_ATTRDEF");
            cols = new Column[] { //
                    column("OID", TypeInfo.TYPE_INTEGER), //
                    column("ADSRC", TypeInfo.TYPE_INTEGER), //
                    column("ADRELID", TypeInfo.TYPE_INTEGER), //
                    column("ADNUM", TypeInfo.TYPE_INTEGER), //
                    column("ADBIN", TypeInfo.TYPE_VARCHAR), // pg_node_tree
            };
            break;
        case PG_ATTRIBUTE:
            setMetaTableName("PG_ATTRIBUTE");
            cols = new Column[] { //
                    column("OID", TypeInfo.TYPE_INTEGER), //
                    column("ATTRELID", TypeInfo.TYPE_INTEGER), //
                    column("ATTNAME", TypeInfo.TYPE_VARCHAR), //
                    column("ATTTYPID", TypeInfo.TYPE_INTEGER), //
                    column("ATTLEN", TypeInfo.TYPE_INTEGER), //
                    column("ATTNUM", TypeInfo.TYPE_INTEGER), //
                    column("ATTTYPMOD", TypeInfo.TYPE_INTEGER), //
                    column("ATTNOTNULL", TypeInfo.TYPE_BOOLEAN), //
                    column("ATTISDROPPED", TypeInfo.TYPE_BOOLEAN), //
                    column("ATTHASDEF", TypeInfo.TYPE_BOOLEAN), //
            };
            break;
        case PG_AUTHID:
            setMetaTableName("PG_AUTHID");
            cols = new Column[] { //
                    column("OID", TypeInfo.TYPE_INTEGER), //
                    column("ROLNAME", TypeInfo.TYPE_VARCHAR), //
                    column("ROLSUPER", TypeInfo.TYPE_BOOLEAN), //
                    column("ROLINHERIT", TypeInfo.TYPE_BOOLEAN), //
                    column("ROLCREATEROLE", TypeInfo.TYPE_BOOLEAN), //
                    column("ROLCREATEDB", TypeInfo.TYPE_BOOLEAN), //
                    column("ROLCATUPDATE", TypeInfo.TYPE_BOOLEAN), //
                    column("ROLCANLOGIN", TypeInfo.TYPE_BOOLEAN), //
                    column("ROLCONNLIMIT", TypeInfo.TYPE_BOOLEAN), //
                    column("ROLPASSWORD", TypeInfo.TYPE_BOOLEAN), //
                    column("ROLVALIDUNTIL", TypeInfo.TYPE_TIMESTAMP_TZ), //
                    column("ROLCONFIG", TypeInfo.getTypeInfo(Value.ARRAY, -1L, 0, TypeInfo.TYPE_VARCHAR)), //
            };
            break;
        case PG_CLASS:
            setMetaTableName("PG_CLASS");
            cols = new Column[] { //
                    column("OID", TypeInfo.TYPE_INTEGER), //
                    column("RELNAME", TypeInfo.TYPE_VARCHAR), //
                    column("RELNAMESPACE", TypeInfo.TYPE_INTEGER), //
                    column("RELKIND", TypeInfo.TYPE_CHAR), //
                    column("RELAM", TypeInfo.TYPE_INTEGER), //
                    column("RELTUPLES", TypeInfo.TYPE_DOUBLE), //
                    column("RELTABLESPACE", TypeInfo.TYPE_INTEGER), //
                    column("RELPAGES", TypeInfo.TYPE_INTEGER), //
                    column("RELHASINDEX", TypeInfo.TYPE_BOOLEAN), //
                    column("RELHASRULES", TypeInfo.TYPE_BOOLEAN), //
                    column("RELHASOIDS", TypeInfo.TYPE_BOOLEAN), //
                    column("RELCHECKS", TypeInfo.TYPE_SMALLINT), //
                    column("RELTRIGGERS", TypeInfo.TYPE_INTEGER), //
            };
            break;
        case PG_CONSTRAINT:
            setMetaTableName("PG_CONSTRAINT");
            cols = new Column[] { //
                    column("OID", TypeInfo.TYPE_INTEGER), //
                    column("CONNAME", TypeInfo.TYPE_VARCHAR), //
                    column("CONTYPE", TypeInfo.TYPE_VARCHAR), //
                    column("CONRELID", TypeInfo.TYPE_INTEGER), //
                    column("CONFRELID", TypeInfo.TYPE_INTEGER), //
                    column("CONKEY", TypeInfo.getTypeInfo(Value.ARRAY, -1L, 0, TypeInfo.TYPE_SMALLINT)), //
            };
            break;
        case PG_DATABASE:
            setMetaTableName("PG_DATABASE");
            cols = new Column[] { //
                    column("OID", TypeInfo.TYPE_INTEGER), //
                    column("DATNAME", TypeInfo.TYPE_VARCHAR), //
                    column("ENCODING", TypeInfo.TYPE_INTEGER), //
                    column("DATLASTSYSOID", TypeInfo.TYPE_INTEGER), //
                    column("DATALLOWCONN", TypeInfo.TYPE_BOOLEAN), //
                    column("DATCONFIG", TypeInfo.getTypeInfo(Value.ARRAY, -1L, 0, TypeInfo.TYPE_VARCHAR)), //
                    column("DATACL", TypeInfo.getTypeInfo(Value.ARRAY, -1L, 0, TypeInfo.TYPE_VARCHAR)), // aclitem[]
                    column("DATDBA", TypeInfo.TYPE_INTEGER), //
                    column("DATTABLESPACE", TypeInfo.TYPE_INTEGER), //
            };
            break;
        case PG_DESCRIPTION:
            setMetaTableName("PG_DESCRIPTION");
            cols = new Column[] { //
                    column("OBJOID", TypeInfo.TYPE_INTEGER), //
                    column("OBJSUBID", TypeInfo.TYPE_INTEGER), //
                    column("CLASSOID", TypeInfo.TYPE_INTEGER), //
                    column("DESCRIPTION", TypeInfo.TYPE_VARCHAR), //
            };
            break;
        case PG_GROUP:
            setMetaTableName("PG_GROUP");
            cols = new Column[] { //
                    column("OID", TypeInfo.TYPE_INTEGER), //
                    column("GRONAME", TypeInfo.TYPE_VARCHAR), //
            };
            break;
        case PG_INDEX:
            setMetaTableName("PG_INDEX");
            cols = new Column[] { //
                    column("OID", TypeInfo.TYPE_INTEGER), //
                    column("INDEXRELID", TypeInfo.TYPE_INTEGER), //
                    column("INDRELID", TypeInfo.TYPE_INTEGER), //
                    column("INDISCLUSTERED", TypeInfo.TYPE_BOOLEAN), //
                    column("INDISUNIQUE", TypeInfo.TYPE_BOOLEAN), //
                    column("INDISPRIMARY", TypeInfo.TYPE_BOOLEAN), //
                    column("INDEXPRS", TypeInfo.TYPE_VARCHAR), //
                    column("INDKEY", TypeInfo.getTypeInfo(Value.ARRAY, -1L, 0, TypeInfo.TYPE_INTEGER)), //
                    column("INDPRED", TypeInfo.TYPE_VARCHAR), // pg_node_tree
            };
            break;
        case PG_INHERITS:
            setMetaTableName("PG_INHERITS");
            cols = new Column[] { //
                    column("INHRELID", TypeInfo.TYPE_INTEGER), //
                    column("INHPARENT", TypeInfo.TYPE_INTEGER), //
                    column("INHSEQNO", TypeInfo.TYPE_INTEGER), //
            };
            break;
        case PG_NAMESPACE:
            setMetaTableName("PG_NAMESPACE");
            cols = new Column[] { //
                    column("ID", TypeInfo.TYPE_INTEGER), //
                    column("NSPNAME", TypeInfo.TYPE_VARCHAR), //
            };
            break;
        case PG_PROC:
            setMetaTableName("PG_PROC");
            cols = new Column[] { //
                    column("OID", TypeInfo.TYPE_INTEGER), //
                    column("PRONAME", TypeInfo.TYPE_VARCHAR), //
                    column("PRORETTYPE", TypeInfo.TYPE_INTEGER), //
                    column("PROARGTYPES", TypeInfo.getTypeInfo(Value.ARRAY, -1L, 0, TypeInfo.TYPE_INTEGER)), //
                    column("PRONAMESPACE", TypeInfo.TYPE_INTEGER), //
            };
            break;
        case PG_ROLES:
            setMetaTableName("PG_ROLES");
            cols = new Column[] { //
                    column("OID", TypeInfo.TYPE_INTEGER), //
                    column("ROLNAME", TypeInfo.TYPE_VARCHAR), //
                    column("ROLSUPER", TypeInfo.TYPE_CHAR), //
                    column("ROLCREATEROLE", TypeInfo.TYPE_CHAR), //
                    column("ROLCREATEDB", TypeInfo.TYPE_CHAR), //
            };
            break;
        case PG_SETTINGS:
            setMetaTableName("PG_SETTINGS");
            cols = new Column[] { //
                    column("OID", TypeInfo.TYPE_INTEGER), //
                    column("NAME", TypeInfo.TYPE_VARCHAR), //
                    column("SETTING", TypeInfo.TYPE_VARCHAR), //
            };
            break;
        case PG_TABLESPACE:
            setMetaTableName("PG_TABLESPACE");
            cols = new Column[] { //
                    column("OID", TypeInfo.TYPE_INTEGER), //
                    column("SPCNAME", TypeInfo.TYPE_VARCHAR), //
                    column("SPCLOCATION", TypeInfo.TYPE_VARCHAR), //
                    column("SPCOWNER", TypeInfo.TYPE_INTEGER), //
                    column("SPCACL", TypeInfo.getTypeInfo(Value.ARRAY, -1L, 0, TypeInfo.TYPE_VARCHAR)), // ACLITEM[]
            };
            break;
        case PG_TRIGGER:
            setMetaTableName("PG_TRIGGER");
            cols = new Column[] { //
                    column("OID", TypeInfo.TYPE_INTEGER), //
                    column("TGCONSTRRELID", TypeInfo.TYPE_INTEGER), //
                    column("TGFOID", TypeInfo.TYPE_INTEGER), //
                    column("TGARGS", TypeInfo.TYPE_INTEGER), //
                    column("TGNARGS", TypeInfo.TYPE_INTEGER), //
                    column("TGDEFERRABLE", TypeInfo.TYPE_BOOLEAN), //
                    column("TGINITDEFERRED", TypeInfo.TYPE_BOOLEAN), //
                    column("TGCONSTRNAME", TypeInfo.TYPE_VARCHAR), //
                    column("TGRELID", TypeInfo.TYPE_INTEGER), //
            };
            break;
        case PG_TYPE:
            setMetaTableName("PG_TYPE");
            cols = new Column[] { //
                    column("OID", TypeInfo.TYPE_INTEGER), //
                    column("TYPNAME", TypeInfo.TYPE_VARCHAR), //
                    column("TYPNAMESPACE", TypeInfo.TYPE_INTEGER), //
                    column("TYPLEN", TypeInfo.TYPE_INTEGER), //
                    column("TYPTYPE", TypeInfo.TYPE_VARCHAR), //
                    column("TYPDELIM", TypeInfo.TYPE_VARCHAR), //
                    column("TYPRELID", TypeInfo.TYPE_INTEGER), //
                    column("TYPELEM", TypeInfo.TYPE_INTEGER), //
                    column("TYPBASETYPE", TypeInfo.TYPE_INTEGER), //
                    column("TYPTYPMOD", TypeInfo.TYPE_INTEGER), //
                    column("TYPNOTNULL", TypeInfo.TYPE_BOOLEAN), //
                    column("TYPINPUT", TypeInfo.TYPE_VARCHAR), //
            };
            break;
        case PG_USER:
            setMetaTableName("PG_USER");
            cols = new Column[] { //
                    column("OID", TypeInfo.TYPE_INTEGER), //
                    column("USENAME", TypeInfo.TYPE_VARCHAR), //
                    column("USECREATEDB", TypeInfo.TYPE_BOOLEAN), //
                    column("USESUPER", TypeInfo.TYPE_BOOLEAN), //
            };
            break;
        default:
            throw DbException.getInternalError("type=" + type);
        }
        setColumns(cols);
        indexColumn = -1;
        metaIndex = null;
    }

    @Override
    public ArrayList<Row> generateRows(SessionLocal session, SearchRow first, SearchRow last) {
        ArrayList<Row> rows = Utils.newSmallArrayList();
        String catalog = database.getShortName();
        boolean admin = session.getUser().isAdmin();
        switch (type) {
        case PG_AM: {
            String[] am = { "btree", "hash" };
            for (int i = 0, l = am.length; i < l; i++) {
                add(session, rows,
                        // OID
                        ValueInteger.get(i),
                        // AMNAME
                        am[i]);
            }
            break;
        }
        case PG_ATTRDEF:
            break;
        case PG_ATTRIBUTE:
            for (Schema schema : database.getAllSchemas()) {
                for (Table table : schema.getAllTablesAndViews(session)) {
                    if (!hideTable(table, session)) {
                        pgAttribute(session, rows, table);
                    }
                }
            }
            for (Table table: session.getLocalTempTables()) {
                if (!hideTable(table, session)) {
                    pgAttribute(session, rows, table);
                }
            }
            break;
        case PG_AUTHID:
            break;
        case PG_CLASS:
            for (Schema schema : database.getAllSchemas()) {
                for (Table table : schema.getAllTablesAndViews(session)) {
                    if (!hideTable(table, session)) {
                        pgClass(session, rows, table);
                    }
                }
            }
            for (Table table: session.getLocalTempTables()) {
                if (!hideTable(table, session)) {
                    pgClass(session, rows, table);
                }
            }
            break;
        case PG_CONSTRAINT:
            pgConstraint(session, rows);
            break;
        case PG_DATABASE: {
            int uid = Integer.MAX_VALUE;
            for (RightOwner rightOwner : database.getAllUsersAndRoles()) {
                if (rightOwner instanceof User && ((User) rightOwner).isAdmin()) {
                    int id = rightOwner.getId();
                    if (id < uid) {
                        uid = id;
                    }
                }
            }
            add(session, rows,
                    // OID
                    ValueInteger.get(100_001),
                    // DATNAME
                    catalog,
                    // ENCODING INT,
                    ValueInteger.get(6), // UTF-8
                    // DATLASTSYSOID INT,
                    ValueInteger.get(100_000),
                    // DATALLOWCONN BOOLEAN,
                    ValueBoolean.TRUE,
                    // DATCONFIG ARRAY, -- TEXT[]
                    null,
                    // DATACL ARRAY, -- ACLITEM[]
                    null,
                    // DATDBA INT,
                    ValueInteger.get(uid),
                    // DATTABLESPACE INT
                    ValueInteger.get(0));
            break;
        }
        case PG_DESCRIPTION:
            add(session, rows,
                    // OBJOID
                    ValueInteger.get(0),
                    // OBJSUBID
                    ValueInteger.get(0),
                    // CLASSOID
                    ValueInteger.get(-1),
                    // DESCRIPTION
                    catalog);
            break;
        case PG_GROUP:
            // The next one returns no rows due to MS Access problem opening
            // tables with primary key
        case PG_INDEX:
        case PG_INHERITS:
            break;
        case PG_NAMESPACE:
            for (Schema schema : database.getAllSchemas()) {
                add(session, rows,
                        // ID
                        ValueInteger.get(schema.getId()),
                        // NSPNAME
                        schema.getName());
            }
            break;
        case PG_PROC:
            break;
        case PG_ROLES:
            for (RightOwner rightOwner : database.getAllUsersAndRoles()) {
                if (admin || session.getUser() == rightOwner) {
                    String r = rightOwner instanceof User && ((User) rightOwner).isAdmin() ? "t" : "f";
                    add(session, rows,
                            // OID
                            ValueInteger.get(rightOwner.getId()),
                            // ROLNAME
                            identifier(rightOwner.getName()),
                            // ROLSUPER
                            r,
                            // ROLCREATEROLE
                            r,
                            // ROLCREATEDB;
                            r);
                }
            }
            break;
        case PG_SETTINGS: {
            String[][] settings = { { "autovacuum", "on" }, { "stats_start_collector", "on" },
                    { "stats_row_level", "on" } };
            for (int i = 0, l = settings.length; i < l; i++) {
                String[] setting = settings[i];
                add(session, rows,
                        // OID
                        ValueInteger.get(i),
                        // NAME
                        setting[0],
                        // SETTING
                        setting[1]);
            }
            break;
        }
        case PG_TABLESPACE:
            add(session, rows,
                    // OID INTEGER
                    ValueInteger.get(0),
                    // SPCNAME
                    "main",
                    // SPCLOCATION
                    "?",
                    // SPCOWNER
                    ValueInteger.get(0),
                    // SPCACL
                    null);
            break;
        case PG_TRIGGER:
            break;
        case PG_TYPE: {
            HashSet<Integer> types = new HashSet<>();
            for (int i = 1, l = Value.TYPE_COUNT; i < l; i++) {
                DataType t = DataType.getDataType(i);
                if (t.type == Value.ARRAY) {
                    continue;
                }
                int pgType = PgServer.convertType(TypeInfo.getTypeInfo(t.type));
                if (pgType == PgServer.PG_TYPE_UNKNOWN || !types.add(pgType)) {
                    continue;
                }
                add(session, rows,
                        // OID
                        ValueInteger.get(pgType),
                        // TYPNAME
                        Value.getTypeName(t.type),
                        // TYPNAMESPACE
                        ValueInteger.get(Constants.PG_CATALOG_SCHEMA_ID),
                        // TYPLEN
                        ValueInteger.get(-1),
                        // TYPTYPE
                        "b",
                        // TYPDELIM
                        ",",
                        // TYPRELID
                        ValueInteger.get(0),
                        // TYPELEM
                        ValueInteger.get(0),
                        // TYPBASETYPE
                        ValueInteger.get(0),
                        // TYPTYPMOD
                        ValueInteger.get(-1),
                        // TYPNOTNULL
                        ValueBoolean.FALSE,
                        // TYPINPUT
                        null);
            }
            for (Object[] pgType : PG_EXTRA_TYPES) {
                add(session, rows,
                        // OID
                        ValueInteger.get((int) pgType[0]),
                        // TYPNAME
                        pgType[1],
                        // TYPNAMESPACE
                        ValueInteger.get(Constants.PG_CATALOG_SCHEMA_ID),
                        // TYPLEN
                        ValueInteger.get((int) pgType[2]),
                        // TYPTYPE
                        "b",
                        // TYPDELIM
                        ",",
                        // TYPRELID
                        ValueInteger.get(0),
                        // TYPELEM
                        ValueInteger.get((int) pgType[3]),
                        // TYPBASETYPE
                        ValueInteger.get(0),
                        // TYPTYPMOD
                        ValueInteger.get(-1),
                        // TYPNOTNULL
                        ValueBoolean.FALSE,
                        // TYPINPUT
                        null);
            }
            break;
        }
        case PG_USER:
            for (RightOwner rightOwner : database.getAllUsersAndRoles()) {
                if (rightOwner instanceof User) {
                    User u = (User) rightOwner;
                    if (admin || session.getUser() == u) {
                        ValueBoolean r = ValueBoolean.get(u.isAdmin());
                        add(session, rows,
                                // OID
                                ValueInteger.get(u.getId()),
                                // USENAME
                                identifier(u.getName()),
                                // USECREATEDB
                                r,
                                // USESUPER;
                                r);
                    }
                }
            }
            break;
        default:
            throw DbException.getInternalError("type=" + type);
        }
        return rows;

    }

    private void pgAttribute(SessionLocal session, ArrayList<Row> rows, Table table) {
        Column[] cols = table.getColumns();
        int tableId = table.getId();
        for (int i = 0; i < cols.length;) {
            Column column = cols[i++];
            addAttribute(session, rows, tableId * 10_000 + i, tableId, column, i);
        }
        for (Index index : table.getIndexes()) {
            if (index.getCreateSQL() == null) {
                continue;
            }
            cols = index.getColumns();
            for (int i = 0; i < cols.length;) {
                Column column = cols[i++];
                int indexId = index.getId();
                addAttribute(session, rows, 1_000_000 * indexId + tableId * 10_000 + i, indexId, column, i);
            }
        }
    }

    private void pgClass(SessionLocal session, ArrayList<Row> rows, Table table) {
        ArrayList<TriggerObject> triggers = table.getTriggers();
        addClass(session, rows, table.getId(), table.getName(), table.getSchema().getId(),
                table.isView() ? "v" : "r", false, triggers != null ? triggers.size() : 0);
        ArrayList<Index> indexes = table.getIndexes();
        if (indexes != null) {
            for (Index index : indexes) {
                if (index.getCreateSQL() == null) {
                    continue;
                }
                addClass(session, rows, index.getId(), index.getName(), index.getSchema().getId(), "i", true,
                        0);
            }
        }
    }

    private void pgConstraint(SessionLocal session, ArrayList<Row> rows) {
        for (Schema schema : database.getAllSchemasNoMeta()) {
            for (Constraint constraint : schema.getAllConstraints()) {
                Constraint.Type constraintType = constraint.getConstraintType();
                if (constraintType == Constraint.Type.DOMAIN) {
                    continue;
                }
                Table table = constraint.getTable();
                if (hideTable(table, session)) {
                    continue;
                }
                List<ValueSmallint> conkey = new ArrayList<>();
                for (Column column : constraint.getReferencedColumns(table)) {
                    conkey.add(ValueSmallint.get((short) (column.getColumnId() + 1)));
                }
                Table refTable = constraint.getRefTable();
                add(session,
                        rows,
                        // OID
                        ValueInteger.get(constraint.getId()),
                        // CONNAME
                        constraint.getName(),
                        // CONTYPE
                        StringUtils.toLowerEnglish(constraintType.getSqlName().substring(0, 1)),
                        // CONRELID
                        ValueInteger.get(table.getId()),
                        // CONFRELID
                        ValueInteger.get(refTable != null && refTable != table
                                && !hideTable(refTable, session) ? table.getId() : 0),
                        // CONKEY
                        ValueArray.get(TypeInfo.TYPE_SMALLINT, conkey.toArray(Value.EMPTY_VALUES), null)
                );
            }
        }
    }

    private void addAttribute(SessionLocal session, ArrayList<Row> rows, int id, int relId, Column column,
            int ordinal) {
        long precision = column.getType().getPrecision();
        add(session, rows,
                // OID
                ValueInteger.get(id),
                // ATTRELID
                ValueInteger.get(relId),
                // ATTNAME
                column.getName(),
                // ATTTYPID
                ValueInteger.get(PgServer.convertType(column.getType())),
                // ATTLEN
                ValueInteger.get(precision > 255 ? -1 : (int) precision),
                // ATTNUM
                ValueInteger.get(ordinal),
                // ATTTYPMOD
                ValueInteger.get(-1),
                // ATTNOTNULL
                ValueBoolean.get(!column.isNullable()),
                // ATTISDROPPED
                ValueBoolean.FALSE,
                // ATTHASDEF
                ValueBoolean.FALSE);
    }

    private void addClass(SessionLocal session, ArrayList<Row> rows, int id, String name, int schema, String kind,
            boolean index, int triggers) {
        add(session, rows,
                // OID
                ValueInteger.get(id),
                // RELNAME
                name,
                // RELNAMESPACE
                ValueInteger.get(schema),
                // RELKIND
                kind,
                // RELAM
                ValueInteger.get(0),
                // RELTUPLES
                ValueDouble.get(0d),
                // RELTABLESPACE
                ValueInteger.get(0),
                // RELPAGES
                ValueInteger.get(0),
                // RELHASINDEX
                ValueBoolean.get(index),
                // RELHASRULES
                ValueBoolean.FALSE,
                // RELHASOIDS
                ValueBoolean.FALSE,
                // RELCHECKS
                ValueSmallint.get((short) 0),
                // RELTRIGGERS
                ValueInteger.get(triggers));
    }

    @Override
    public long getMaxDataModificationId() {
        return database.getModificationDataId();
    }

}
