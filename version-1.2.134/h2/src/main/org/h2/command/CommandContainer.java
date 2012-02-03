/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command;

import java.util.ArrayList;
import org.h2.expression.Parameter;
import org.h2.expression.ParameterInterface;
import org.h2.result.ResultInterface;
import org.h2.value.Value;

/**
 * Represents a single SQL statements.
 * It wraps a prepared statement.
 */
public class CommandContainer extends Command {

    private Prepared prepared;

    CommandContainer(Parser parser, String sql, Prepared prepared) {
        super(parser, sql);
        prepared.setCommand(this);
        this.prepared = prepared;
    }

    public ArrayList< ? extends ParameterInterface> getParameters() {
        return prepared.getParameters();
    }

    public boolean isTransactional() {
        return prepared.isTransactional();
    }

    public boolean isQuery() {
        return prepared.isQuery();
    }

    private void recompileIfRequired() {
        if (prepared.needRecompile()) {
            // TODO test with 'always recompile'
            prepared.setModificationMetaId(0);
            String sql = prepared.getSQL();
            ArrayList<Parameter> oldParams = prepared.getParameters();
            Parser parser = new Parser(session);
            prepared = parser.parseOnly(sql);
            long mod = prepared.getModificationMetaId();
            prepared.setModificationMetaId(0);
            ArrayList<Parameter> newParams = prepared.getParameters();
            for (int i = 0; i < newParams.size(); i++) {
                Parameter old = oldParams.get(i);
                if (old.isValueSet()) {
                    Value v = old.getValue(session);
                    Parameter p = newParams.get(i);
                    p.setValue(v);
                }
            }
            prepared.prepare();
            prepared.setModificationMetaId(mod);
        }
    }

    public int update() {
        recompileIfRequired();
        // TODO query time: should keep lock time separate from running time
        start();
        prepared.checkParameters();
        int updateCount = prepared.update();
        prepared.trace(startTime, updateCount);
        return updateCount;
    }

    public ResultInterface query(int maxrows) {
        recompileIfRequired();
        // TODO query time: should keep lock time separate from running time
        start();
        prepared.checkParameters();
        ResultInterface result = prepared.query(maxrows);
        prepared.trace(startTime, result.getRowCount());
        return result;
    }

    public boolean isReadOnly() {
        return prepared.isReadOnly();
    }

    public ResultInterface queryMeta() {
        return prepared.queryMeta();
    }

}
