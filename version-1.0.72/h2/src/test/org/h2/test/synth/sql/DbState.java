/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth.sql;

import java.sql.SQLException;
import java.util.ArrayList;

/**
 * Represents a connection to a simulated database.
 */
public class DbState implements DbInterface {

    private TestSynth config;
    private ArrayList tables = new ArrayList();
    private ArrayList indexes = new ArrayList();
    boolean connected;
    boolean autoCommit;

    DbState(TestSynth config) {
        this.config = config;
    }

    public void reset() throws SQLException {
        tables = new ArrayList();
        indexes = new ArrayList();
    }

    public void connect() throws SQLException {
        connected = true;
    }

    public void disconnect() throws SQLException {
        connected = false;
    }

    public void createTable(Table table) throws SQLException {
        tables.add(table);
    }

    public void dropTable(Table table) throws SQLException {
        tables.remove(table);
    }

    public void createIndex(Index index) throws SQLException {
        indexes.add(index);
    }

    public void dropIndex(Index index) throws SQLException {
        indexes.remove(index);
    }

    public Result insert(Table table, Column[] c, Value[] v) throws SQLException {
        return null;
    }

    public Result select(String sql) throws SQLException {
        return null;
    }

    public Result delete(Table table, String condition) throws SQLException {
        return null;
    }

    public Result update(Table table, Column[] columns, Value[] values, String condition) {
        return null;
    }

    public void setAutoCommit(boolean b) throws SQLException {
        autoCommit = b;
    }

    public void commit() throws SQLException {
    }

    public void rollback() throws SQLException {
    }

    public Table randomTable() {
        if (tables.size() == 0) {
            return null;
        }
        int i = config.random().getInt(tables.size());
        return (Table) tables.get(i);
    }

    public void end() throws SQLException {
    }

}
