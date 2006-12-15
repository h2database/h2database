/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.bnf;

import java.util.HashMap;
import java.util.HashSet;

import org.h2.server.web.DbTableOrView;

public class Sentence {
    public static final int CONTEXT=0, KEYWORD=1;
    public static final int FUNCTION = 2;
    public String text;
    HashMap next;
    long max;
    DbTableOrView lastTable;
    private HashSet tables;
    private HashMap aliases;
    
    public boolean stop() {
        return System.currentTimeMillis() > max;
    }

    public void add(String n, String string, int type) {
        next.put(type+"#"+n, string);
    }
    
    public void addAlias(String alias, DbTableOrView table) {
        if(aliases == null) {
            aliases = new HashMap();
        }
        aliases.put(alias, table);
    }

    public void addTable(DbTableOrView table) {
        lastTable = table;
        if(tables==null) {
            tables = new HashSet();
        }
        tables.add(table);
    }
    
    public HashSet getTables() {
        return tables;
    }
    
    public HashMap getAliases() {
        return aliases;
    }

    public DbTableOrView getLastTable() {
        return lastTable;
    }
}
