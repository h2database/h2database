/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command;

import java.sql.SQLException;

import org.h2.expression.Parameter;
import org.h2.expression.ParameterInterface;
import org.h2.result.LocalResult;
import org.h2.util.ObjectArray;
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

    public ObjectArray< ? extends ParameterInterface> getParameters() {
        return prepared.getParameters();
    }

    public boolean isTransactional() {
        return prepared.isTransactional();
    }

    public boolean isQuery() {
        return prepared.isQuery();
    }

    private void recompileIfRequired() throws SQLException {
        if (prepared.needRecompile()) {
            // TODO test with 'always recompile'
            prepared.setModificationMetaId(0);
            String sql = prepared.getSQL();
            ObjectArray<Parameter> oldParams = prepared.getParameters();
            Parser parser = new Parser(session);
            prepared = parser.parseOnly(sql);
            long mod = prepared.getModificationMetaId();
            prepared.setModificationMetaId(0);
            ObjectArray<Parameter> newParams = prepared.getParameters();
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

    public int update() throws SQLException {
        recompileIfRequired();
        // TODO query time: should keep lock time separate from running time
        start();
        prepared.checkParameters();
        int updateCount = prepared.update();
        prepared.trace(startTime, updateCount);
        return updateCount;
    }

    public LocalResult query(int maxrows) throws SQLException {
        recompileIfRequired();
        // TODO query time: should keep lock time separate from running time
        start();
        prepared.checkParameters();
        LocalResult result = prepared.query(maxrows);
        prepared.trace(startTime, result.getRowCount());
        return result;
    }

    public boolean isReadOnly() {
        return prepared.isReadOnly();
    }

    public LocalResult queryMeta() throws SQLException {
        return prepared.queryMeta();
    }

}
