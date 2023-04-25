/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.h2.command.CommandInterface;
import org.h2.command.Prepared;
import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.mvstore.db.Store;
import org.h2.result.LocalResult;
import org.h2.result.ResultInterface;
import org.h2.table.Column;
import org.h2.util.HasSQL;
import org.h2.value.TypeInfo;
import org.h2.value.ValueVarchar;

/**
 * This class represents the statement
 * EXPLAIN
 */
public class Explain extends Prepared {

    private Prepared command;
    private LocalResult result;
    private boolean executeCommand;

    public Explain(SessionLocal session) {
        super(session);
    }

    public void setCommand(Prepared command) {
        this.command = command;
    }

    public Prepared getCommand() {
        return command;
    }

    @Override
    public void prepare() {
        command.prepare();
    }

    public void setExecuteCommand(boolean executeCommand) {
        this.executeCommand = executeCommand;
    }

    @Override
    public ResultInterface queryMeta() {
        return query(-1);
    }

    @Override
    protected void checkParameters() {
        // Check params only in case of EXPLAIN ANALYZE
        if (executeCommand) {
            super.checkParameters();
        }
    }

    @Override
    public ResultInterface query(long maxrows) {
        Database db = getDatabase();
        Expression[] expressions = { new ExpressionColumn(db, new Column("PLAN", TypeInfo.TYPE_VARCHAR)) };
        result = new LocalResult(session, expressions, 1, 1);
        int sqlFlags = HasSQL.ADD_PLAN_INFORMATION;
        if (maxrows >= 0) {
            String plan;
            if (executeCommand) {
                Store store = null;
                if (db.isPersistent()) {
                    store = db.getStore();
                    store.statisticsStart();
                }
                if (command.isQuery()) {
                    command.query(maxrows);
                } else {
                    command.update();
                }
                plan = command.getPlanSQL(sqlFlags);
                Map<String, Integer> statistics = null;
                if (store != null) {
                    statistics = store.statisticsEnd();
                }
                if (statistics != null) {
                    int total = 0;
                    for (Integer value : statistics.values()) {
                        total += value;
                    }
                    if (total > 0) {
                        statistics = new TreeMap<>(statistics);
                        StringBuilder buff = new StringBuilder();
                        if (statistics.size() > 1) {
                            buff.append("total: ").append(total).append('\n');
                        }
                        for (Entry<String, Integer> e : statistics.entrySet()) {
                            int value = e.getValue();
                            int percent = (int) (100L * value / total);
                            buff.append(e.getKey()).append(": ").append(value);
                            if (statistics.size() > 1) {
                                buff.append(" (").append(percent).append("%)");
                            }
                            buff.append('\n');
                        }
                        plan += "\n/*\n" + buff + "*/";
                    }
                }
            } else {
                plan = command.getPlanSQL(sqlFlags);
            }
            add(plan);
        }
        result.done();
        return result;
    }

    private void add(String text) {
        result.addRow(ValueVarchar.get(text));
    }

    @Override
    public boolean isQuery() {
        return true;
    }

    @Override
    public boolean isTransactional() {
        return true;
    }

    @Override
    public boolean isReadOnly() {
        return command.isReadOnly();
    }

    @Override
    public int getType() {
        return executeCommand ? CommandInterface.EXPLAIN_ANALYZE : CommandInterface.EXPLAIN;
    }

    @Override
    public void collectDependencies(HashSet<DbObject> dependencies) {
        command.collectDependencies(dependencies);
    }

}
