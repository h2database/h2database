/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.util.ArrayList;

import org.h2.api.ErrorCode;
import org.h2.command.Parser;
import org.h2.command.query.AllColumnsForPlan;
import org.h2.command.query.Query;
import org.h2.command.query.SelectUnion;
import org.h2.engine.SessionLocal;
import org.h2.expression.Parameter;
import org.h2.message.DbException;
import org.h2.result.LocalResult;
import org.h2.result.ResultInterface;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.CTE;
import org.h2.table.QueryExpressionTable;
import org.h2.table.TableFilter;
import org.h2.value.Value;

/**
 * A recursive index.
 */
public final class RecursiveIndex extends QueryExpressionIndex {

    private final SessionLocal createSession;

    /**
     * Creates a new instance of a recursive index.
     *
     * @param table
     *            the query expression table
     * @param querySQL
     *            the query SQL
     * @param originalParameters
     *            the original parameters
     * @param session
     *            the session
     */
    public RecursiveIndex(QueryExpressionTable table, String querySQL, ArrayList<Parameter> originalParameters,
            SessionLocal session) {
        super(table, querySQL, originalParameters);
        this.createSession = session;
    }

    @Override
    public boolean isExpired() {
        return false;
    }

    @Override
    public double getCost(SessionLocal session, int[] masks, TableFilter[] filters, int filter, SortOrder sortOrder,
            AllColumnsForPlan allColumnsSet) {
        return 1000d;
    }

    @Override
    public Cursor find(SessionLocal session, SearchRow first, SearchRow last, boolean reverse) {
        assert !reverse;
        CTE cte = (CTE) table;
        ResultInterface recursiveResult = cte.getRecursiveResult();
        if (recursiveResult != null) {
            recursiveResult.reset();
            return new QueryExpressionCursor(this, recursiveResult, first, last);
        }
        if (query == null) {
            Parser parser = new Parser(createSession);
            parser.setRightsChecked(true);
            parser.setSuppliedParameters(originalParameters);
            parser.setQueryScope(table.getQueryScope());
            query = (Query) parser.prepare(querySQL);
            query.setNeverLazy(true);
        }
        if (!query.isUnion()) {
            throw DbException.get(ErrorCode.SYNTAX_ERROR_2, "recursive queries without UNION");
        }
        SelectUnion union = (SelectUnion) query;
        Query left = union.getLeft();
        left.setNeverLazy(true);
        // to ensure the last result is not closed
        left.disableCache();
        ResultInterface resultInterface = left.query(0);
        LocalResult localResult = union.getEmptyResult();
        // ensure it is not written to disk,
        // because it is not closed normally
        localResult.setMaxMemoryRows(Integer.MAX_VALUE);
        while (resultInterface.next()) {
            Value[] cr = resultInterface.currentRow();
            localResult.addRow(cr);
        }
        Query right = union.getRight();
        right.setNeverLazy(true);
        resultInterface.reset();
        cte.setRecursiveResult(resultInterface);
        // to ensure the last result is not closed
        right.disableCache();
        while (true) {
            resultInterface = right.query(0);
            if (!resultInterface.hasNext()) {
                break;
            }
            while (resultInterface.next()) {
                Value[] cr = resultInterface.currentRow();
                localResult.addRow(cr);
            }
            resultInterface.reset();
            cte.setRecursiveResult(resultInterface);
        }
        cte.setRecursiveResult(null);
        localResult.done();
        return new QueryExpressionCursor(this, localResult, first, last);
    }

}
