/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.bnf;

import java.util.HashMap;
import java.util.HashSet;

import org.h2.server.web.DbTableOrView;

/**
 * A query context object. It contains the list of table and alias objects.
 * Used for autocomplete.
 */
public class Sentence {
    public static final int CONTEXT = 0;
    static final int KEYWORD = 1;
    static final int FUNCTION = 2;
    public String text;
    HashMap next;
    long max;
    private DbTableOrView lastTable;
    private HashSet tables;
    private HashMap aliases;

    boolean stop() {
        return System.currentTimeMillis() > max;
    }

    /**
     * Add a word to the set of next tokens.
     *
     * @param n the token name
     * @param string an example text
     * @param type the type
     */
    public void add(String n, String string, int type) {
        next.put(type+"#"+n, string);
    }

    /**
     * Add an alias name and object
     *
     * @param alias the alias name
     * @param table the alias table
     */
    public void addAlias(String alias, DbTableOrView table) {
        if (aliases == null) {
            aliases = new HashMap();
        }
        aliases.put(alias, table);
    }

    /**
     * Add a table.
     *
     * @param table the table
     */
    public void addTable(DbTableOrView table) {
        lastTable = table;
        if (tables == null) {
            tables = new HashSet();
        }
        tables.add(table);
    }

    /**
     * Get the set of tables.
     *
     * @return the set of tables
     */
    public HashSet getTables() {
        return tables;
    }

    /**
     * Get the alias map.
     *
     * @return the alias map
     */
    public HashMap getAliases() {
        return aliases;
    }

    /**
     * Get the last added table.
     *
     * @return the last table
     */
    public DbTableOrView getLastTable() {
        return lastTable;
    }
}
