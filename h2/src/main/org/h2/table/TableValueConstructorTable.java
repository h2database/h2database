/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.util.ArrayList;

import org.h2.command.query.TableValueConstructor;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.result.ResultInterface;
import org.h2.result.SimpleResult;
import org.h2.schema.Schema;

/**
 * A table for table value constructor.
 */
public class TableValueConstructorTable extends VirtualConstructedTable {

    private final ArrayList<ArrayList<Expression>> rows;

    public TableValueConstructorTable(Schema schema, SessionLocal session, Column[] columns,
            ArrayList<ArrayList<Expression>> rows) {
        super(schema, 0, "VALUES");
        setColumns(columns);
        this.rows = rows;
    }

    @Override
    public boolean canGetRowCount(SessionLocal session) {
        return true;
    }

    @Override
    public long getRowCount(SessionLocal session) {
        return rows.size();
    }

    @Override
    public long getRowCountApproximation(SessionLocal session) {
        return rows.size();
    }

    @Override
    public ResultInterface getResult(SessionLocal session) {
        SimpleResult simple = new SimpleResult();
        int columnCount = columns.length;
        for (int i = 0; i < columnCount; i++) {
            Column column = columns[i];
            simple.addColumn(column.getName(), column.getType());
        }
        TableValueConstructor.getVisibleResult(session, simple, columns, rows);
        return simple;
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        builder.append('(');
        TableValueConstructor.getValuesSQL(builder, sqlFlags, rows);
        return builder.append(')');
    }

    @Override
    public boolean isDeterministic() {
        return true;
    }

}
