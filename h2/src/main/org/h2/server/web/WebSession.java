/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.server.web;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;

import org.h2.bnf.Bnf;
import org.h2.message.TraceSystem;

/**
 * The web session keeps all data of a user session.
 * This class is used by the H2 Console.
 */
public class WebSession {
    
    private static final int MAX_HISTORY = 1000;
    
    long lastAccess;
    HashMap map = new HashMap();
    Locale locale;
    WebServer server;
    Statement executingStatement;
    ResultSet result;

    private ArrayList commandHistory = new ArrayList();

    private Connection conn;
    private DatabaseMetaData meta;
    private DbContents contents = new DbContents();
    private DbContextRule columnRule;
    private DbContextRule newAliasRule;
    private DbContextRule schemaRule;
    private DbContextRule tableRule;
    private DbContextRule aliasRule;
    private DbContextRule columnAliasRule;
    private Bnf bnf;

    WebSession(WebServer server) {
        this.server = server;
    }

    void put(String key, Object value) {
        map.put(key, value);
    }

    Object get(String key) {
        if ("sessions".equals(key)) {
            return server.getSessions();
        }
        return map.get(key);
    }

    void remove(String key) {
        map.remove(key);
    }

    Bnf getBnf() {
        return bnf;
    }

    void loadBnf() {
        try {
            Bnf newBnf = Bnf.getInstance(null);
            columnRule = new DbContextRule(contents, DbContextRule.COLUMN);
            newAliasRule = new DbContextRule(contents, DbContextRule.NEW_TABLE_ALIAS);
            aliasRule = new DbContextRule(contents, DbContextRule.TABLE_ALIAS);
            tableRule = new DbContextRule(contents, DbContextRule.TABLE);
            schemaRule = new DbContextRule(contents, DbContextRule.SCHEMA);
            columnAliasRule = new DbContextRule(contents, DbContextRule.COLUMN_ALIAS);
//            bnf.updateTopic("newTableName", new String[]{"TEST"});
//            String[] schemas;
//            if(contents.isMySQL) {
//                schemas = new String[0];
//            } else {
//                schemas = new String[contents.schemas.length];
//                for(int i=0; i<contents.schemas.length; i++) {
//                    schemas[i] = contents.schemas[i].quotedName + ".";
//                }
//            }
//            bnf.updateTopic("schemaName", schemas);
            newBnf.updateTopic("columnName", columnRule);
            newBnf.updateTopic("newTableAlias", newAliasRule);
            newBnf.updateTopic("tableAlias", aliasRule);
            newBnf.updateTopic("columnAlias", columnAliasRule);
            newBnf.updateTopic("tableName", tableRule);
            newBnf.updateTopic("schemaName", schemaRule);
            // bnf.updateTopic("name", new String[]{""});
            newBnf.linkStatements();
            bnf = newBnf;
        } catch (Exception e) {
            // ok we don't have the bnf
            e.printStackTrace();
        }
    }

    String getCommand(int id) {
        return (String) commandHistory.get(id);
    }

    void addCommand(String sql) {
        if (sql == null) {
            return;
        }
        sql = sql.trim();
        if (sql.length() == 0) {
            return;
        }
        if (commandHistory.size() > MAX_HISTORY) {
            commandHistory.remove(0);
        }
        int idx = commandHistory.indexOf(sql);
        if (idx >= 0) {
            commandHistory.remove(idx);
        }
        commandHistory.add(sql);
    }

    ArrayList getCommands() {
        return commandHistory;
    }

    HashMap getInfo() {
        HashMap m = new HashMap();
        m.putAll(map);
        m.put("lastAccess", new Timestamp(lastAccess).toString());
        try {
            m.put("url", conn == null ? "not connected" : conn.getMetaData().getURL());
            m.put("user", conn == null ? "-" : conn.getMetaData().getUserName());
            m.put("lastQuery", commandHistory.size() == 0 ? "" : commandHistory.get(0));
            m.put("executing", executingStatement == null ? "no" : "yes");
        } catch (SQLException e) {
            TraceSystem.traceThrowable(e);
        }
        return m;
    }

    void setConnection(Connection conn) throws SQLException {
        this.conn = conn;
        if (conn == null) {
            meta = null;
        } else {
            meta = conn.getMetaData();
        }
        contents = new DbContents();
    }

    DatabaseMetaData getMetaData() {
        return meta;
    }

    Connection getConnection() {
        return conn;
    }

    DbContents getContents() {
        return contents;
    }

}
