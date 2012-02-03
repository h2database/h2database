/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth.sql;

import java.util.ArrayList;
import org.h2.util.New;

/**
 * Represents a connection to a simulated database.
 */
public class DbState implements DbInterface {

    private boolean connected;
    private boolean autoCommit;
    private TestSynth config;
    private ArrayList<Table> tables = New.arrayList();
    private ArrayList<Index> indexes = New.arrayList();

    DbState(TestSynth config) {
        this.config = config;
    }

    public void reset() {
        tables = New.arrayList();
        indexes = New.arrayList();
    }

    public void connect() {
        connected = true;
    }

    public void disconnect() {
        connected = false;
    }

    public void createTable(Table table) {
        tables.add(table);
    }

    public void dropTable(Table table) {
        tables.remove(table);
    }

    public void createIndex(Index index) {
        indexes.add(index);
    }

    public void dropIndex(Index index) {
        indexes.remove(index);
    }

    public Result insert(Table table, Column[] c, Value[] v) {
        return null;
    }

    public Result select(String sql) {
        return null;
    }

    public Result delete(Table table, String condition) {
        return null;
    }

    public Result update(Table table, Column[] columns, Value[] values, String condition) {
        return null;
    }

    public void setAutoCommit(boolean b) {
        autoCommit = b;
    }

    public void commit() {
        // nothing to do
    }

    public void rollback() {
        // nothing to do
    }

    /**
     * Get a random table.
     *
     * @return the table
     */
    Table randomTable() {
        if (tables.size() == 0) {
            return null;
        }
        int i = config.random().getInt(tables.size());
        return tables.get(i);
    }

    public void end() {
        // nothing to do
    }

    public String toString() {
        return "autocommit: " + autoCommit + " connected: " + connected;
    }

}
