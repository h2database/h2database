/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
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
        throw Message.throwInternalError();
    }

    public int getType() {
        throw Message.throwInternalError();
    }

    public void mapColumns(ColumnResolver resolver, int level) throws SQLException {
        throw Message.getSQLException(ErrorCode.SYNTAX_ERROR_1, table);
    }

    public Expression optimize(Session session) throws SQLException {
        throw Message.getSQLException(ErrorCode.SYNTAX_ERROR_1, table);
    }

    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        Message.throwInternalError();
    }

    public int getScale() {
        throw Message.throwInternalError();
    }

    public long getPrecision() {
        throw Message.throwInternalError();
    }

    public int getDisplaySize() {
        throw Message.throwInternalError();
    }

    public String getTableAlias() {
        return table;
    }

    public String getSchemaName() {
        return schema;
    }

    public String getSQL() {
        if (table == null) {
            return "*";
        }
        return StringUtils.quoteIdentifier(table) + ".*";
    }

    public void updateAggregate(Session session) {
        Message.throwInternalError();
    }

    public boolean isEverything(ExpressionVisitor visitor) {
        throw Message.throwInternalError();
    }

    public int getCost() {
        throw Message.throwInternalError();
    }

}
