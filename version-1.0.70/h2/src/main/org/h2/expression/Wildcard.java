/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.util.StringUtils;
import org.h2.value.Value;

/**
 * A wildcard expression as in SELECT * FROM TEST.
 * This object is only used temporarily during the parsing phase, and later
 * replaced by column expressions.
 */
public class Wildcard extends Expression {
    private String schema;
    private String table;

    public Wildcard(String schema, String table) {
        this.schema = schema;
        this.table = table;
    }

    public boolean isWildcard() {
        return true;
    }

    public Value getValue(Session session) {
        throw Message.getInternalError();
    }

    public int getType() {
        throw Message.getInternalError();
    }

    public void mapColumns(ColumnResolver resolver, int level) throws SQLException {
        throw Message.getSQLException(ErrorCode.SYNTAX_ERROR_1, table);
    }

    public Expression optimize(Session session) throws SQLException {
        throw Message.getSQLException(ErrorCode.SYNTAX_ERROR_1, table);
    }

    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        throw Message.getInternalError();
    }

    public int getScale() {
        throw Message.getInternalError();
    }

    public long getPrecision() {
        throw Message.getInternalError();
    }

    public int getDisplaySize() {
        throw Message.getInternalError();
    }

    public String getSchema() {
        return schema;
    }

    public String getTableAlias() {
        return table;
    }

    public String getSQL() {
        if (table == null) {
            return "*";
        } else {
            return StringUtils.quoteIdentifier(table) + ".*";
        }
    }

    public void updateAggregate(Session session) {
        throw Message.getInternalError();
    }

    public boolean isEverything(ExpressionVisitor visitor) {
        throw Message.getInternalError();
    }

    public int getCost() {
        throw Message.getInternalError();
    }

}
