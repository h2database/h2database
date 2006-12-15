/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.sql.SQLException;
import java.util.HashMap;

import org.h2.command.Parser;
import org.h2.command.dml.Select;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.schema.Constant;
import org.h2.schema.Schema;
import org.h2.table.Column;
import org.h2.table.ColumnResolver;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.value.Value;


/**
 * @author Thomas
 */
public class ExpressionColumn extends Expression {
    private Database database;
    private String schemaName;
    private String tableAlias;
    private String columnName;
    private ColumnResolver resolver;
    private int queryLevel;
    private Column column;
    private boolean evaluatable;
    private Select select;

    public ExpressionColumn(Database database, Select select, Column column) {
        this.database = database;
        this.select = select;
        this.column = column;
    }

    public ExpressionColumn(Database database, Select select, String schemaName, String tableAlias, String columnName) {
        this.database = database;
        this.select = select;
        this.tableAlias = tableAlias;
        this.columnName = columnName;
    }

    public String getSQL() {
        String sql;
        if(column != null) {
            sql = column.getSQL();
        } else {
            sql = columnName;
        }
        if(tableAlias != null) {
            sql = Parser.quoteIdentifier(tableAlias) + "." + sql;
        }
        if(schemaName != null) {
            sql = Parser.quoteIdentifier(schemaName) + "." + sql;
        }
        return sql;
    }

    public TableFilter getTableFilter() {
        return resolver == null ? null : resolver.getTableFilter();
    }

    public void mapColumns(ColumnResolver resolver, int level) throws SQLException {
        if (tableAlias != null && !tableAlias.equals(resolver.getTableAlias())) {
            return;
        }
        if(schemaName != null && !schemaName.equals(resolver.getSchemaName())) {
            return;
        }
        Column[] columns = resolver.getColumns();
        for (int i = 0; i < columns.length; i++) {
            Column col = columns[i];
            if (columnName.equals(col.getName())) {
                if(this.resolver == null) {
                    queryLevel = level;
                    column = col;
                    this.resolver = resolver;
                    break;
                } else if(queryLevel==level && this.resolver != resolver) {
                    throw Message.getSQLException(Message.AMBIGUOUS_COLUMN_NAME_1, columnName);
                }
            }
        }
    }

    public Expression optimize(Session session) throws SQLException {
        if (resolver == null) {
            Schema schema = session.getDatabase().findSchema(tableAlias == null ? session.getCurrentSchemaName() : tableAlias);
            if(schema != null) {
                Constant constant = schema.findConstant(columnName);
                if(constant != null) {
                    return constant.getValue();
                }
            }
            String name = columnName;
            if (tableAlias != null) {
                name = tableAlias + "." + name;
                if(schemaName != null) {
                    name = schemaName + "." + name;
                }
            }
            throw Message.getSQLException(Message.COLUMN_NOT_FOUND_1, name);
        }
        return this;
    }

    public void updateAggregate(Session session) throws SQLException {
        Value now = resolver.getValue(column);
        if(select == null) {
            throw Message.getSQLException(Message.MUST_GROUP_BY_COLUMN_1, getSQL());
        }
        HashMap values = select.getCurrentGroup();
        Value v = (Value)values.get(this);
        if(v==null) {
            values.put(this, now);
        } else {
            if(!database.areEqual(now, v)) {
                throw Message.getSQLException(Message.MUST_GROUP_BY_COLUMN_1, getSQL());
            }
        }
    }

    public Value getValue(Session session) throws SQLException {
        // TODO refactor: simplify check if really part of an aggregated value / detection of 
        // usage of non-grouped by columns without aggregate function
        if(select != null) {
            HashMap values = select.getCurrentGroup();
            if(values != null) {
                Value v = (Value)values.get(this);
                if(v!=null) {
                    return v;
                }
            }
        }
        Value value = resolver.getValue(column);
        if(value== null) {
            throw Message.getSQLException(Message.MUST_GROUP_BY_COLUMN_1, getSQL());
        }
        return value;
    }

    public int getType() {
        return column.getType();
    }

    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        if (tableFilter == resolver.getTableFilter()) {
            evaluatable = b;
        }
    }

    public Column getColumn() {
        return column;
    }

    public int getScale() {
        return column.getScale();
    }

    public long getPrecision() {
        return column.getPrecision();
    }

    public String getOriginalColumnName() {
        return columnName;
    }
    
    public String getOriginalAliasName() {
        return tableAlias;
    }

    public String getColumnName() {
        return columnName!=null ? columnName : column.getName();
    }

    public String getSchemaName() {
        Table table = column.getTable();
        return table == null ? null : table.getSchema().getName();
    } 
    
    public String getTableName() {
        Table table = column.getTable();
        return table == null ? null : table.getName();
    }

    public String getAlias() {
        return column.getName();
    }
    
    public boolean isAutoIncrement() {
        return column.getSequence() != null;
    }
    
    public int getNullable() {
        return column.getNullable() ? Column.NULLABLE : Column.NOT_NULLABLE;
    }

    public boolean isEverything(ExpressionVisitor visitor) {
        switch(visitor.type) {
        case ExpressionVisitor.OPTIMIZABLE_MIN_MAX_COUNT_ALL:
            return false;
        case ExpressionVisitor.DETERMINISTIC:
            return true;
        case ExpressionVisitor.INDEPENDENT:
            return this.queryLevel < visitor.queryLevel;
        case ExpressionVisitor.EVALUATABLE:
            // if the current value is known (evalutable set)
            // or if this columns belongs to a 'higher level' query and is therefore just a parameter
            return evaluatable || visitor.queryLevel < this.queryLevel;
        case ExpressionVisitor.SET_MAX_DATA_MODIFICATION_ID:
            visitor.addDataModificationId(column.getTable().getMaxDataModificationId());
            return true;
        default:
            throw Message.getInternalError("type="+visitor.type);
        }
    }    
    
    public int getCost() {
        return 2;
    }
    
}
