/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.util.ArrayList;

import org.h2.command.QueryScope;
import org.h2.command.query.Query;
import org.h2.engine.SessionLocal;
import org.h2.expression.Parameter;
import org.h2.index.QueryExpressionIndex;
import org.h2.index.RecursiveIndex;
import org.h2.index.RegularQueryExpressionIndex;
import org.h2.result.ResultInterface;
import org.h2.util.ParserUtil;

/**
 * A common table expression.
 */
public final class CTE extends QueryExpressionTable {

    private final String querySQL;
    private final boolean recursive;
    private final QueryScope queryScope;
    private final ArrayList<Parameter> originalParameters;

    private ResultInterface recursiveResult;

    public CTE(String name, Query query, String querySQL, ArrayList<Parameter> params, Column[] columnTemplates,
            SessionLocal session, boolean recursive, QueryScope queryScope) {
        super(session.getDatabase().getMainSchema(), 0, name);
        setTemporary(true);
        this.queryScope = queryScope;
        this.querySQL = querySQL;
        this.recursive = recursive;
        this.originalParameters = params;
        tables = new ArrayList<>(query.getTables());
        setColumns(initColumns(session, columnTemplates, query, false));
        viewQuery = query;
    }

    @Override
    protected QueryExpressionIndex createIndex(SessionLocal session, int[] masks) {
        return recursive ? new RecursiveIndex(this, querySQL, originalParameters, session)
                : new RegularQueryExpressionIndex(this, querySQL, originalParameters, session, masks);
    }

    @Override
    public Query getTopQuery() {
        return null;
    }

    @Override
    public String getCreateSQL() {
        return null;
    }

    @Override
    public boolean canDrop() {
        return false;
    }

    @Override
    public TableType getTableType() {
        return null;
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        return ParserUtil.quoteIdentifier(builder, getName(), sqlFlags);
    }

    public String getQuerySQL() {
        return querySQL;
    }

    @Override
    public QueryScope getQueryScope() {
        return queryScope;
    }

    public boolean isRecursive() {
        return recursive;
    }

    @Override
    public boolean isDeterministic() {
        if (recursive) {
            return false;
        }
        return super.isDeterministic();
    }

    public void setRecursiveResult(ResultInterface value) {
        if (recursiveResult != null) {
            recursiveResult.close();
        }
        this.recursiveResult = value;
    }

    public ResultInterface getRecursiveResult() {
        return recursiveResult;
    }

}
