/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
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
class WebSession {

    private static final int MAX_HISTORY = 1000;

    /**
     * The last time this client sent a request.
     */
    long lastAccess;

    /**
     * The session attribute map.
     */
    HashMap map = new HashMap();

    /**
     * The current locale.
     */
    Locale locale;

    /**
     * The currently executing statement.
     */
    Statement executingStatement;

    /**
     * The current updatable result set.
     */
    ResultSet result;

    private WebServer server;

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
    private boolean shutdownServerOnDisconnect;

    WebSession(WebServer server) {
        this.server = server;
    }

    /**
     * Put an attribute value in the map.
     *
     * @param key the key
     * @param value the new value
     */
    void put(String key, Object value) {
        map.put(key, value);
    }

    /**
     * Get the value for the given key.
     *
     * @param key the key
     * @return the value
     */
    Object get(String key) {
        if ("sessions".equals(key)) {
            return server.getSessions();
        }
        return map.get(key);
    }

    /**
     * Remove a session attribute from the map.
     *
     * @param key the key
     */
    void remove(String key) {
        map.remove(key);
    }

    /**
     * Get the BNF object.
     *
     * @return the BNF object
     */
    Bnf getBnf() {
        return bnf;
    }

    /**
     * Load the SQL grammar BNF.
     */
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

    /**
     * Get the SQL statement from history.
     *
     * @param id the history id
     * @return the SQL statement
     */
    String getCommand(int id) {
        return (String) commandHistory.get(id);
    }

    /**
     * Add a SQL statement to the history.
     *
     * @param sql the SQL statement
     */
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

    /**
     * Get the list of SQL statements in the history.
     *
     * @return the commands
     */
    ArrayList getCommands() {
        return commandHistory;
    }

    /**
     * Update session meta data information and get the information in a map.
     *
     * @return a map containing the session meta data
     */
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

    /**
     * Shutdown the server when disconnecting.
     */
    void setShutdownServerOnDisconnect() {
        this.shutdownServerOnDisconnect = true;
    }

    boolean getShutdownServerOnDisconnect() {
        return shutdownServerOnDisconnect;
    }

}
