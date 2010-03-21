/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import org.h2.command.ddl.CreateTableData;
import org.h2.util.StatementBuilder;
import org.h2.util.StringUtils;

/**
 * @author Thomas Mueller
 * @author Sergi Vladykin
 */
public abstract class RecreatableTable extends Table {

    protected final String tableEngine;
    private final boolean globalTemporary;

    public RecreatableTable(CreateTableData data) {
        super(data.schema, data.id, data.tableName, data.persistIndexes, data.persistData);
        this.tableEngine = data.tableEngine;
        this.globalTemporary = data.globalTemporary;
        setTemporary(data.temporary);
        Column[] cols = new Column[data.columns.size()];
        data.columns.toArray(cols);
        setColumns(cols);
    }

    @Override
    public String getDropSQL() {
        return "DROP TABLE IF EXISTS " + getSQL();
    }

    @Override
    public String getCreateSQL() {
        StatementBuilder buff = new StatementBuilder("CREATE ");
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
        buff.append("TABLE ").append(getSQL());
        if (comment != null) {
            buff.append(" COMMENT ").append(StringUtils.quoteStringSQL(comment));
        }
        buff.append("(\n    ");
        for (Column column : columns) {
            buff.appendExceptFirst(",\n    ");
            buff.append(column.getCreateSQL());
        }
        buff.append("\n)");
        if (tableEngine != null) {
            buff.append("\nENGINE '");
            buff.append(tableEngine);
            buff.append("'");
        }
        if (!isPersistIndexes() && !isPersistData()) {
            buff.append("\nNOT PERSISTENT");
        }
        return buff.toString();
    }
    
    public boolean isGlobalTemporary() {
        return globalTemporary;
    }

}
