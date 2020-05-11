/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.util.ArrayList;

import org.h2.engine.Mode;
import org.h2.engine.Session;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.index.MetaIndex;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.schema.Schema;
import org.h2.util.StringUtils;
import org.h2.value.DataType;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueVarchar;
import org.h2.value.ValueVarcharIgnoreCase;

/**
 * This class is responsible to build the database meta data pseudo tables.
 */
public abstract class MetaTable extends Table {

    /**
     * The approximate number of rows of a meta table.
     */
    public static final long ROW_COUNT_APPROXIMATION = 1000;

    /**
     * The table type.
     */
    protected final int type;

    /**
     * The indexed column.
     */
    protected int indexColumn;

    /**
     * The index for this table.
     */
    protected MetaIndex metaIndex;

    /**
     * Create a new metadata table.
     *
     * @param schema the schema
     * @param id the object id
     * @param type the meta table type
     */
    protected MetaTable(Schema schema, int id, int type) {
        // tableName will be set later
        super(schema, id, null, true, true);
        this.type = type;
    }

    protected final void setMetaTableName(String upperName) {
        setObjectName(database.sysIdentifier(upperName));
    }

    protected final Column[] createColumns(String... names) {
        Column[] cols = new Column[names.length];
        TypeInfo defaultType = database.getSettings().caseInsensitiveIdentifiers ? TypeInfo.TYPE_VARCHAR_IGNORECASE
                : TypeInfo.TYPE_VARCHAR;
        Mode mode = database.getMode();
        for (int i = 0; i < names.length; i++) {
            String nameType = names[i];
            int idx = nameType.indexOf(' ');
            TypeInfo dataType;
            String name;
            if (idx < 0) {
                dataType = defaultType;
                name = nameType;
            } else {
                String tName = nameType.substring(idx + 1);
                DataType t = DataType.getTypeByName(tName, mode);
                if (t != null) {
                    dataType = TypeInfo.getTypeInfo(t.type);
                } else {
                    assert tName.endsWith(" ARRAY");
                    dataType = TypeInfo.getTypeInfo(Value.ARRAY, -1L, 0, TypeInfo.getTypeInfo(
                            DataType.getTypeByName(tName.substring(0, tName.length() - 6), mode).type));
                }
                name = nameType.substring(0, idx);
            }
            cols[i] = new Column(database.sysIdentifier(name), dataType);
        }
        return cols;
    }

    @Override
    public final String getCreateSQL() {
        return null;
    }

    @Override
    public final Index addIndex(Session session, String indexName, int indexId,
            IndexColumn[] cols, IndexType indexType, boolean create,
            String indexComment) {
        throw DbException.getUnsupportedException("META");
    }

    @Override
    public final boolean lock(Session session, boolean exclusive, boolean forceLockEvenInMvcc) {
        // nothing to do
        return false;
    }

    @Override
    public final boolean isLockedExclusively() {
        return false;
    }

    protected final String identifier(String s) {
        if (database.getSettings().databaseToLower) {
            s = s == null ? null : StringUtils.toLowerEnglish(s);
        }
        return s;
    }

    /**
     * Get all tables of this database, including local temporary tables for the
     * session.
     *
     * @param session the session
     * @return the array of tables
     */
    protected final ArrayList<Table> getAllTables(Session session) {
        ArrayList<Table> tables = database.getAllTablesAndViews(true);
        ArrayList<Table> tempTables = session.getLocalTempTables();
        tables.addAll(tempTables);
        return tables;
    }

    protected final ArrayList<Table> getTablesByName(Session session, String tableName) {
        ArrayList<Table> tables = database.getTableOrViewByName(tableName);
        Table temp = session.findLocalTempTable(tableName);
        if (temp != null) {
            tables.add(temp);
        }
        return tables;
    }

    protected final boolean checkIndex(Session session, String value, Value indexFrom, Value indexTo) {
        if (value == null || (indexFrom == null && indexTo == null)) {
            return true;
        }
        Value v;
        if (database.getSettings().caseInsensitiveIdentifiers) {
            v = ValueVarcharIgnoreCase.get(value);
        } else {
            v = ValueVarchar.get(value);
        }
        if (indexFrom != null && session.compare(v, indexFrom) < 0) {
            return false;
        }
        if (indexTo != null && session.compare(v, indexTo) > 0) {
            return false;
        }
        return true;
    }

    protected final boolean hideTable(Table table, Session session) {
        return table.isHidden() && session != database.getSystemSession();
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
    public abstract ArrayList<Row> generateRows(Session session, SearchRow first, SearchRow last);

    @Override
    public final void removeRow(Session session, Row row) {
        throw DbException.getUnsupportedException("META");
    }

    @Override
    public final void addRow(Session session, Row row) {
        throw DbException.getUnsupportedException("META");
    }

    @Override
    public final void removeChildrenAndResources(Session session) {
        throw DbException.getUnsupportedException("META");
    }

    @Override
    public final void close(Session session) {
        // nothing to do
    }

    @Override
    public final void unlock(Session s) {
        // nothing to do
    }

    protected final void add(Session session, ArrayList<Row> rows, Object... stringsOrValues) {
        Value[] values = new Value[stringsOrValues.length];
        for (int i = 0; i < stringsOrValues.length; i++) {
            Object s = stringsOrValues[i];
            Value v = s == null ? ValueNull.INSTANCE : s instanceof String ? ValueVarchar.get((String) s) : (Value) s;
            values[i] = columns[i].convert(session, v);
        }
        rows.add(Row.get(values, 1, rows.size()));
    }

    @Override
    public final void checkRename() {
        throw DbException.getUnsupportedException("META");
    }

    @Override
    public final void checkSupportAlter() {
        throw DbException.getUnsupportedException("META");
    }

    @Override
    public final void truncate(Session session) {
        throw DbException.getUnsupportedException("META");
    }

    @Override
    public final long getRowCount(Session session) {
        throw DbException.throwInternalError(toString());
    }

    @Override
    public final boolean canGetRowCount() {
        return false;
    }

    @Override
    public final boolean canDrop() {
        return false;
    }

    @Override
    public final TableType getTableType() {
        return TableType.SYSTEM_TABLE;
    }

    @Override
    public final Index getScanIndex(Session session) {
        return new MetaIndex(this, IndexColumn.wrap(columns), true);
    }

    @Override
    public final ArrayList<Index> getIndexes() {
        ArrayList<Index> list = new ArrayList<>(2);
        if (metaIndex == null) {
            return list;
        }
        list.add(new MetaIndex(this, IndexColumn.wrap(columns), true));
        // TODO re-use the index
        list.add(metaIndex);
        return list;
    }

    @Override
    public final Index getUniqueIndex() {
        return null;
    }

    @Override
    public final long getRowCountApproximation() {
        return ROW_COUNT_APPROXIMATION;
    }

    @Override
    public final long getDiskSpaceUsed() {
        return 0L;
    }

    @Override
    public final boolean isDeterministic() {
        return true;
    }

    @Override
    public final boolean canReference() {
        return false;
    }

}
