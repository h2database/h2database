/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.util.Collections;
import java.util.List;
import org.h2.command.ddl.CreateTableData;
import org.h2.engine.Database;
import org.h2.index.IndexType;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.util.StringUtils;
import org.h2.value.Value;

/**
 * The base class of a regular table, or a user defined table.
 *
 * @author Thomas Mueller
 * @author Sergi Vladykin
 */
public abstract class TableBase extends Table {

    /**
     * The table engine used (null for regular tables).
     */
    private final String tableEngine;
    /** Provided table parameters */
    private final List<String> tableEngineParams;

    private final boolean globalTemporary;

    /**
     * Returns main index column if index is an primary key index and has only
     * one column with _ROWID_ compatible data type.
     *
     * @param indexType type of an index
     * @param cols columns of the index
     * @return main index column or {@link SearchRow#ROWID_INDEX}
     */
    public static int getMainIndexColumn(IndexType indexType, IndexColumn[] cols) {
        if (!indexType.isPrimaryKey() || cols.length != 1) {
            return SearchRow.ROWID_INDEX;
        }
        IndexColumn first = cols[0];
        if ((first.sortType & SortOrder.DESCENDING) != 0) {
            return SearchRow.ROWID_INDEX;
        }
        switch (first.column.getType().getValueType()) {
        case Value.TINYINT:
        case Value.SMALLINT:
        case Value.INTEGER:
        case Value.BIGINT:
            return first.column.getColumnId();
        default:
            return SearchRow.ROWID_INDEX;
        }
    }

    public TableBase(CreateTableData data) {
        super(data.schema, data.id, data.tableName,
                data.persistIndexes, data.persistData);
        this.tableEngine = data.tableEngine;
        this.globalTemporary = data.globalTemporary;
        if (data.tableEngineParams != null) {
            this.tableEngineParams = data.tableEngineParams;
        } else {
            this.tableEngineParams = Collections.emptyList();
        }
        setTemporary(data.temporary);
        setColumns(data.columns.toArray(new Column[0]));
    }

    @Override
    public String getDropSQL() {
        StringBuilder builder = new StringBuilder("DROP TABLE IF EXISTS ");
        getSQL(builder, DEFAULT_SQL_FLAGS).append(" CASCADE");
        return builder.toString();
    }

    @Override
    public String getCreateSQLForMeta() {
        return getCreateSQL(true);
    }

    @Override
    public String getCreateSQL() {
        return getCreateSQL(false);
    }

    private String getCreateSQL(boolean forMeta) {
        Database db = getDatabase();
        if (db == null) {
            // closed
            return null;
        }
        StringBuilder buff = new StringBuilder("CREATE ");
        if (isTemporary()) {
            if (isGlobalTemporary()) {
                buff.append("GLOBAL ");
            } else {
                buff.append("LOCAL ");
            }
            buff.append("TEMPORARY ");
        } else if (isPersistIndexes()) {
            buff.append("CACHED ");
        } else {
            buff.append("MEMORY ");
        }
        buff.append("TABLE ");
        getSQL(buff, DEFAULT_SQL_FLAGS);
        if (comment != null) {
            buff.append(" COMMENT ");
            StringUtils.quoteStringSQL(buff, comment);
        }
        buff.append("(\n    ");
        for (int i = 0, l = columns.length; i < l; i++) {
            if (i > 0) {
                buff.append(",\n    ");
            }
            buff.append(columns[i].getCreateSQL(forMeta));
        }
        buff.append("\n)");
        if (tableEngine != null) {
            String d = db.getSettings().defaultTableEngine;
            if (d == null || !tableEngine.endsWith(d)) {
                buff.append("\nENGINE ");
                StringUtils.quoteIdentifier(buff, tableEngine);
            }
        }
        if (!tableEngineParams.isEmpty()) {
            buff.append("\nWITH ");
            for (int i = 0, l = tableEngineParams.size(); i < l; i++) {
                if (i > 0) {
                    buff.append(", ");
                }
                StringUtils.quoteIdentifier(buff, tableEngineParams.get(i));
            }
        }
        if (!isPersistIndexes() && !isPersistData()) {
            buff.append("\nNOT PERSISTENT");
        }
        return buff.toString();
    }

    @Override
    public boolean isGlobalTemporary() {
        return globalTemporary;
    }

}
