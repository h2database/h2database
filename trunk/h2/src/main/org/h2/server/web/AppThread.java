/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.server.web;

import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Map.Entry;

import org.h2.bnf.Bnf;
import org.h2.tools.SimpleResultSet;
import org.h2.util.JdbcUtils;
import org.h2.util.MathUtils;
import org.h2.util.MemoryUtils;
import org.h2.util.NetUtils;
import org.h2.util.ObjectArray;
import org.h2.util.ScriptReader;
import org.h2.util.StringUtils;

/**
 * @author Thomas
 */

public class AppThread extends WebServerThread {

    // TODO web: support online data editing like http://numsum.com/
	
	private boolean allowShutdown;

    AppThread(Socket socket, WebServer server, boolean allowShutdown) {
        super(socket, server);
        setName("H2 Console thread");
        this.allowShutdown = allowShutdown;
    }
    
    AppSession getAppSession() {
        return (AppSession)session;
    }

    String process(String file) {
        server.trace("process " + file);
        while(file.endsWith(".do")) {
            if(file.equals("login.do")) {
                file = login();
            } else if(file.equals("index.do")) {
                file = index();
            } else if(file.equals("logout.do")) {
                file = logout();
            } else if(file.equals("settingRemove.do")) {
                file = settingRemove();
            } else if(file.equals("settingSave.do")) {
                file = settingSave();
            } else if(file.equals("test.do")) {
                file = test();
            } else if(file.equals("query.do")) {
                file = query();
            } else if(file.equals("tables.do")) {
                file = tables();
            } else if(file.equals("editResult.do")) {
                file = editResult();
            } else if(file.equals("getHistory.do")) {
                file = getHistory();
            } else if(file.equals("admin.do")) {
                file = admin();
            } else if(file.equals("adminSave.do")) {
                file = adminSave();
            } else if(file.equals("adminShutdown.do")) {
                file = adminShutdown();
            } else if(file.equals("autoCompleteList.do")) {
                file = autoCompleteList();
            } else {
                file = "error.jsp";
            }
        }
        server.trace("return " + file);
        return file;
    }

    private String autoCompleteList() {
        String query = (String) attributes.get("query");
        boolean lowercase = false;
        if(query.trim().length()>0 && Character.isLowerCase(query.trim().charAt(0))) {
            lowercase = true;
        }
        try {
            String sql = query;
            if(sql.endsWith(";")) {
                sql += " ";
            }
            ScriptReader reader = new ScriptReader(new StringReader(sql));
            reader.setSkipRemarks(true);
            String lastSql = "";
            while(true) {
                String n = reader.readStatement();
                if(n == null) {
                    break;
                }
                lastSql = n;
            }
            String result = "";
            if(reader.isInsideRemark()) {
                if(reader.isBlockRemark()) {
                    result= "1#(End Remark)# */\n" + result;
                } else {
                    result= "1#(Newline)#\n" + result;
                }
            } else {
                sql = lastSql == null ? "" : lastSql;
                while(sql.length() > 0 && sql.charAt(0) <= ' ') {
                    sql = sql.substring(1);
                }
                if(sql.trim().length()>0 && Character.isLowerCase(sql.trim().charAt(0))) {
                    lowercase = true;
                }
                Bnf bnf = getAppSession().getBnf();
                if(bnf == null) {
                    return "autoCompleteList.jsp";
                }
                HashMap map = bnf.getNextTokenList(sql);
                String space = "";
                if(sql.length()>0) {
                    char last = sql.charAt(sql.length()-1);
                    if(!Character.isWhitespace(last) && (last != '.' && last>=' ' && last != '\'' && last != '"')) {
                        space = " ";
                    }
                }
                ArrayList list = new ArrayList(map.size());
                Iterator it = map.entrySet().iterator();
                while(it.hasNext()) {
                    Map.Entry entry = (Entry) it.next();
                    String key = (String) entry.getKey();
                    String type = "" + key.charAt(0);
                    String value = (String) entry.getValue();
                    key = key.substring(2);
                    if(Character.isLetter(key.charAt(0)) && lowercase) {
                        key = StringUtils.toLowerEnglish(key);
                        value = StringUtils.toLowerEnglish(value);
                    }
                    if(key.equals(value) && !value.equals(".")) {
                        value = space + value;
                    }
                    key = StringUtils.urlEncode(key);
                    key = StringUtils.replaceAll(key, "+", " ");
                    value = StringUtils.urlEncode(value);
                    value = StringUtils.replaceAll(value, "+", " ");
                    list.add(type+"#" + key + "#" + value);
                }
                Collections.sort(list);
                StringBuffer buff = new StringBuffer();
                if(query.endsWith("\n") || query.trim().endsWith(";")) {
                    list.add(0, "1#(Newline)#\n");
                }
                for(int i=0; i<list.size(); i++) {
                    if(i>0) {
                        buff.append('|');
                    }
                    buff.append((String) list.get(i));
                }
                result = buff.toString();
            }
            session.put("autoCompleteList", result);
        } catch(Throwable e) {
            e.printStackTrace();
        }
        return "autoCompleteList.jsp";
    }

    private String admin() {
        AppServer app = server.getAppServer();
        session.put("port", ""+app.getPort());
        session.put("allowOthers", ""+app.getAllowOthers());
        session.put("ssl", String.valueOf(app.getSSL()));
        session.put("sessions", server.getSessions());
        return "admin.jsp";
    }

    private String adminSave() {
        AppServer app = server.getAppServer();
        try {
            app.setPort(MathUtils.decodeInt((String)attributes.get("port")));
            app.setAllowOthers(Boolean.valueOf((String)attributes.get("allowOthers")).booleanValue());
            app.setSSL(Boolean.valueOf((String)attributes.get("ssl")).booleanValue());
            app.saveSettings();
        } catch(Exception e) {
            server.trace(e.toString());
        }
        return admin();
    }

    private String adminShutdown() {
    	if(allowShutdown) {
    		System.exit(0);
    	}
        return "admin.jsp";
    }

    private String index() {
        String[][] languageArray = server.getLanguageArray();
        String language = (String) attributes.get("language");
        Locale locale = session.locale;
        if(language != null) {
            if(locale == null || !StringUtils.toLowerEnglish(locale.getLanguage()).equals(language)) {
                locale = new Locale(language, "");
                server.readTranslations(session, locale.getLanguage());
                session.put("language", language);
                session.locale = locale;
            }
        } else {
            language = (String) session.get("language");
        }
        session.put("languageCombo", getComboBox(languageArray, language));
        String[] settingNames = server.getAppServer().getSettingNames();
        String setting = attributes.getProperty("setting");
        if(setting == null && settingNames.length>0) {
            setting = settingNames[0];
        }
        String combobox = getComboBox(settingNames, setting);
        session.put("settingsList", combobox);
        ConnectionInfo info = server.getAppServer().getSetting(setting);
        if(info == null) {
            info = new ConnectionInfo();
        }
        session.put("setting", PageParser.escapeHtml(setting));
        session.put("name", PageParser.escapeHtml(setting));
        session.put("driver", PageParser.escapeHtml(info.driver));
        session.put("url", PageParser.escapeHtml(info.url));
        session.put("user", PageParser.escapeHtml(info.user));
        return "index.jsp";
    }

    private String getHistory() {
        int id = Integer.parseInt(attributes.getProperty("id"));
        String sql = getAppSession().getCommand(id);
        session.put("query", PageParser.escapeHtmlNoBreak(sql));
        return "query.jsp";
    }

    private int addColumns(DbTableOrView table, StringBuffer buff, int treeIndex, boolean showColumnTypes, StringBuffer columnsBuffer) throws SQLException {
        DbColumn[] columns = table.columns;
        for(int i=0; columns != null && i<columns.length; i++) {
            DbColumn column = columns[i];
            if(columnsBuffer.length()>0) {
                columnsBuffer.append(' ');
            }
            columnsBuffer.append(column.name);
            String col = StringUtils.urlEncode(PageParser.escapeJavaScript(column.name));
            buff.append("setNode("+treeIndex+", 1, 1, 'column', '" + PageParser.escapeJavaScript(column.name)+ "', 'javascript:ins(\\'"+col+"\\')');\n");
            treeIndex++;
            if(showColumnTypes) {
                buff.append("setNode("+treeIndex+", 2, 2, 'type', '" + PageParser.escapeJavaScript(column.dataType)+ "', null);\n");
                treeIndex++;
            }
        }
        return treeIndex;
    }

    private static class IndexInfo {
        String name;
        String type;
        String columns;
    }

    private int addIndexes(DatabaseMetaData meta, String table, String schema, StringBuffer buff, int treeIndex) throws SQLException {
        // index reading is very slow for oracle (2 seconds per index), so don't do it
        ResultSet rs = meta.getIndexInfo(null, schema, table, false, false);
        HashMap indexMap = new HashMap();
        while (rs.next()) {
            String name = rs.getString("INDEX_NAME");
            IndexInfo info = (IndexInfo) indexMap.get(name);
            if (info == null) {
                int t = rs.getInt("TYPE");
                String type;
                if (t == DatabaseMetaData.tableIndexClustered) {
                    type = "";
                } else if (t == DatabaseMetaData.tableIndexHashed) {
                    type = " (${text.tree.hashed})";
                } else if (t == DatabaseMetaData.tableIndexOther) {
                    type = "";
                } else {
                    type = null;
                }
                if(name != null && type != null) {
                    info = new IndexInfo();
                    info.name = name;
                    type = (rs.getBoolean("NON_UNIQUE") ? "${text.tree.nonUnique}" : "${text.tree.unique}") + type;
                    info.type = type;
                    info.columns = rs.getString("COLUMN_NAME");
                    indexMap.put(name, info);
                }
            } else {
                info.columns += ", " + rs.getString("COLUMN_NAME");
            }
        }
        rs.close();
        if(indexMap.size() > 0) {
            buff.append("setNode("+treeIndex+", 1, 1, 'index_az', '${text.tree.indexes}', null);\n");
            treeIndex++;
            for (Iterator it = indexMap.values().iterator(); it.hasNext();) {
                IndexInfo info = (IndexInfo) it.next();
                buff.append("setNode("+treeIndex+", 2, 1, 'index', '" + PageParser.escapeJavaScript(info.name)+ "', null);\n");
                treeIndex++;
                buff.append("setNode("+treeIndex+", 3, 2, 'type', '" + info.type+ "', null);\n");
                treeIndex++;
                buff.append("setNode("+treeIndex+", 3, 2, 'type', '" + PageParser.escapeJavaScript(info.columns)+ "', null);\n");
                treeIndex++;
            }
        }
        return treeIndex;
    }

    private int addTablesAndViews(DbSchema schema, boolean mainSchema, StringBuffer buff, int treeIndex) throws SQLException {
        if(schema == null) {
            return treeIndex;
        }
        AppSession app = getAppSession();
        Connection conn = getAppSession().getConnection();
        DatabaseMetaData meta = app.getMetaData();
        int level = mainSchema ? 0 : 1;
        String indentation = ", "+level+", "+(level+1)+", ";
        String indentNode = ", "+(level+1)+", "+(level+1)+", ";
        DbTableOrView[] tables = schema.tables;
        if(tables == null) {
            return treeIndex;
        }
        boolean isOracle = schema.contents.isOracle;
        boolean showColumnTypes = tables.length < 100;
        for(int i=0; i<tables.length; i++) {
            DbTableOrView table = tables[i];
            if(table.isView) {
                continue;
            }
            int tableId = treeIndex;
            String tab = table.quotedName;
            if(!mainSchema) {
                tab =schema.quotedName + "." + tab;
            }
            tab = StringUtils.urlEncode(PageParser.escapeJavaScript(tab));
            buff.append("setNode("+treeIndex+indentation+" 'table', '" + PageParser.escapeJavaScript(table.name)+ "', 'javascript:ins(\\'"+tab+"\\',true)');\n");
            treeIndex++;
            if(mainSchema) {
                StringBuffer columnsBuffer = new StringBuffer();
                treeIndex = addColumns(table, buff, treeIndex, showColumnTypes, columnsBuffer);
                if(!isOracle) {
                    treeIndex = addIndexes(meta, table.name, schema.name, buff, treeIndex);
                }
                buff.append("addTable('"+PageParser.escapeJavaScript(table.name)+"', '"+PageParser.escapeJavaScript(columnsBuffer.toString())+"', "+tableId+");\n");
            }
        }
        tables = schema.tables;
        for(int i=0; i<tables.length; i++) {
            DbTableOrView view = tables[i];
            if(!view.isView) {
                continue;
            }
            int tableId = treeIndex;
            String tab = view.quotedName;
            if(!mainSchema) {
                tab = view.schema.quotedName + "." + tab;
            }
            tab = StringUtils.urlEncode(PageParser.escapeJavaScript(tab));
            buff.append("setNode("+treeIndex+indentation+" 'view', '" + PageParser.escapeJavaScript(view.name)+ "', 'javascript:ins(\\'"+tab+"\\',true)');\n");
            treeIndex++;
            if(mainSchema) {
                StringBuffer columnsBuffer = new StringBuffer();
                treeIndex = addColumns(view, buff, treeIndex, showColumnTypes, columnsBuffer);
                if(schema.contents.isH2) {
                    PreparedStatement prep = null;
                    try {
                        prep = conn.prepareStatement("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME=?");
                        prep.setString(1, view.name);
                        ResultSet rs = prep.executeQuery();
                        if(rs.next()) {
                            String sql = rs.getString("SQL");
                            buff.append("setNode("+treeIndex+ indentNode + " 'type', '" + PageParser.escapeJavaScript(sql)+ "', null);\n");
                            treeIndex++;
                        }
                        rs.close();
                    } finally {
                        JdbcUtils.closeSilently(prep);
                    }
                }
                buff.append("addTable('"+PageParser.escapeJavaScript(view.name)+"', '"+PageParser.escapeJavaScript(columnsBuffer.toString())+"', "+tableId+");\n");
            }
        }
        return treeIndex;
    }

    private String tables() {
        AppSession app = getAppSession();
        DbContents contents = app.getContents();
        try {
            contents.readContents(app.getMetaData());
            app.loadBnf();
            Connection conn = app.getConnection();
            DatabaseMetaData meta = app.getMetaData();
            boolean isH2 = contents.isH2;

            StringBuffer buff = new StringBuffer();
            buff.append("setNode(0, 0, 0, 'database', '" + PageParser.escapeJavaScript((String)session.get("url"))+ "', null);\n");
//            String version = meta.getDatabaseProductName() + " " + meta.getDatabaseProductVersion();
//            buff.append("setNode(1, 0, 0, 'info', '" + PageParser.escapeJavaScript(version)+ "', null);\n");
//
//            int treeIndex = 2;
            int treeIndex = 1;

            DbSchema defaultSchema = contents.defaultSchema;
            treeIndex = addTablesAndViews(defaultSchema, true, buff, treeIndex);
            DbSchema[] schemas = contents.schemas;
            for(int i=0; i<schemas.length; i++) {
                DbSchema schema = schemas[i];
                if(schema == defaultSchema || schema == null) {
                    continue;
                }
                buff.append("setNode("+treeIndex+", 0, 1, 'folder', '" + PageParser.escapeJavaScript(schema.name)+ "', null);\n");
                treeIndex++;
                treeIndex = addTablesAndViews(schema, false, buff, treeIndex);
            }
            if(isH2) {
                Statement stat = null;
                try {
                    stat = conn.createStatement();
                    ResultSet rs = stat.executeQuery("SELECT * FROM INFORMATION_SCHEMA.SEQUENCES ORDER BY SEQUENCE_NAME");
                    for(int i=0; rs.next(); i++) {
                        if(i==0) {
                            buff.append("setNode("+treeIndex+", 0, 1, 'sequences', '${text.tree.sequences}', null);\n");
                            treeIndex++;
                        }
                        String name = rs.getString("SEQUENCE_NAME");
                        String current = rs.getString("CURRENT_VALUE");
                        String increment = rs.getString("INCREMENT");
                        buff.append("setNode("+treeIndex+", 1, 1, 'sequence', '" + PageParser.escapeJavaScript(name)+ "', null);\n");
                        treeIndex++;
                        buff.append("setNode("+treeIndex+", 2, 2, 'type', '${text.tree.current}: " + PageParser.escapeJavaScript(current)+ "', null);\n");
                        treeIndex++;
                        if(!increment.equals("1")) {
                            buff.append("setNode("+treeIndex+", 2, 2, 'type', '${text.tree.increment}: " + PageParser.escapeJavaScript(increment)+ "', null);\n");
                            treeIndex++;
                        }
                    }
                    rs.close();
                    rs = stat.executeQuery("SELECT * FROM INFORMATION_SCHEMA.USERS ORDER BY NAME");
                    for(int i=0; rs.next(); i++) {
                        if(i==0) {
                            buff.append("setNode("+treeIndex+", 0, 1, 'users', '${text.tree.users}', null);\n");
                            treeIndex++;
                        }
                        String name = rs.getString("NAME");
                        String admin = rs.getString("ADMIN");
                        buff.append("setNode("+treeIndex+", 1, 1, 'user', '" + PageParser.escapeJavaScript(name)+ "', null);\n");
                        treeIndex++;
                        if(admin.equalsIgnoreCase("TRUE")) {
                            buff.append("setNode("+treeIndex+", 2, 2, 'type', '${text.tree.admin}', null);\n");
                            treeIndex++;
                        }
                    }
                    rs.close();
                } finally {
                    JdbcUtils.closeSilently(stat);
                }
            }
            String version = meta.getDatabaseProductName() + " " + meta.getDatabaseProductVersion();
            buff.append("setNode("+treeIndex+", 0, 0, 'info', '" + PageParser.escapeJavaScript(version)+ "', null);\n");
            buff.append("refreshQueryTables();");
            session.put("tree", buff.toString());
        } catch(SQLException e) {
            session.put("tree", "");
            session.put("error", getStackTrace(0, e));
        }
        return "tables.jsp";
    }

    private String getStackTrace(int id, Throwable e) {
        StringWriter writer = new StringWriter();
        e.printStackTrace(new PrintWriter(writer));
        String s = writer.toString();
        s = PageParser.escapeHtml(s);
        s = StringUtils.replaceAll(s, "\t", "&nbsp;&nbsp;&nbsp;&nbsp;");
        String message = PageParser.escapeHtml(e.getMessage());
        s = "<a class=\"error\" href=\"#\" onclick=\"var x=document.getElementById('st"+id+"').style;x.display=x.display==''?'none':'';\">" + message + "</a><span style=\"display: none;\" id=\"st"+id+"\"><br />"+ s + "</span>";
        s = formatAsError(s);
        return s;
    }

    private String formatAsError(String s) {
        return "<div class=\"error\">"+s+"</div>";
    }

    private String test() {
        String driver = attributes.getProperty("driver", "");
        String url = attributes.getProperty("url", "");
        String user = attributes.getProperty("user", "");
        String password = attributes.getProperty("password", "");
        session.put("driver", driver);
        session.put("url", url);
        session.put("user", user);
        try {
            Connection conn = server.getAppServer().getConnection(driver, url, user, password);
            JdbcUtils.closeSilently(conn);
            session.put("error", "${text.login.testSuccessful}");
            return "index.jsp";
        } catch(Exception e) {
            session.put("error", getLoginError(e));
            return "index.jsp";
        }
    }

    private String getLoginError(Exception e) {
        if(e instanceof ClassNotFoundException) {
            return "${text.login.driverNotFound}<br />" + getStackTrace(0, e);
        } else {
            return getStackTrace(0, e);
        }
    }

    private String login() {
        String driver = attributes.getProperty("driver", "");
        String url = attributes.getProperty("url", "");
        String user = attributes.getProperty("user", "");
        String password = attributes.getProperty("password", "");
        try {
            Connection conn = server.getAppServer().getConnection(driver, url, user, password);
            AppSession appSession = getAppSession();
            appSession.setConnection(conn);
            session.put("url", url);
            session.put("user", user);
            session.put("autoCommit", "checked");
            session.put("autoComplete", "1");
            session.put("maxrows", "1000");
            session.remove("error");
            settingSave();
            return "frame.jsp";
        } catch(Exception e) {
            session.put("error", getLoginError(e));
            return "index.jsp";
        }
    }

    private String logout() {
        try {
            Connection conn = getAppSession().getConnection();
            getAppSession().setConnection(null);
            session.remove("conn");
            session.remove("result");
            session.remove("tables");
            session.remove("user");
            if(conn != null) {
                conn.close();
            }
        } catch(Exception e) {
            server.trace(e.toString());
        }
        return "index.do";
    }

    private String query() {
        String sql = attributes.getProperty("sql").trim();
        try {
            Connection conn = getAppSession().getConnection();
            String result;
            if(sql.equals("@AUTOCOMMIT TRUE")) {
                conn.setAutoCommit(true);
                result = "${text.result.autoCommitOn}";
            } else if(sql.equals("@AUTOCOMMIT FALSE")) {
                conn.setAutoCommit(false);
                result = "${text.result.autoCommitOff}";
            } else if(sql.startsWith("@TRANSACTION_ISOLATION")) {
                String s = sql.substring("@TRANSACTION_ISOLATION".length()).trim();
                if(s.length()>0) {
                    int level = Integer.parseInt(s);
                    conn.setTransactionIsolation(level);
                }
                result = "Transaction Isolation: " + conn.getTransactionIsolation() + "<br />";
                result += Connection.TRANSACTION_READ_UNCOMMITTED + ": READ_UNCOMMITTED<br />";
                result += Connection.TRANSACTION_READ_COMMITTED + ": READ_COMMITTED<br />";
                result += Connection.TRANSACTION_REPEATABLE_READ + ": REPEATABLE_READ<br />";
                result += Connection.TRANSACTION_SERIALIZABLE + ": SERIALIZABLE";
            } else if(sql.startsWith("@SET MAXROWS ")) {
                int maxrows = Integer.parseInt(sql.substring("@SET MAXROWS ".length()));
                session.put("maxrows", ""+maxrows);
                result = "${text.result.maxrowsSet}";
            } else {
                ScriptReader r = new ScriptReader(new StringReader(sql));
                ObjectArray list = new ObjectArray();
                while(true) {
                    String s = r.readStatement();
                    if(s == null) {
                        break;
                    }
                    list.add(s);
                }
                StringBuffer buff = new StringBuffer();
                for(int i=0; i<list.size(); i++) {
                    String s = (String) list.get(i);
                    if(!s.startsWith("@")) {
                        buff.append(PageParser.escapeHtml(s+";"));
                        buff.append("<br />");
                    }
                    buff.append(getResult(conn, i+1, s, list.size()==1));
                    buff.append("<br />");
                }
                result = buff.toString();
            }
            session.put("result", result);
        } catch(Throwable e) {
            session.put("result", getStackTrace(0, e));
        }
        return "result.jsp";
    }

    private String editResult() {
        ResultSet rs = getAppSession().result;
        int row = Integer.parseInt(attributes.getProperty("row"));
        int op = Integer.parseInt(attributes.getProperty("op"));
        String result = "", error="";
        try {
            if(op==1) {
                boolean insert = row < 0;
                if(insert) {
                    rs.moveToInsertRow();
                } else {
                    rs.absolute(row);
                }
                for(int i=0; i<rs.getMetaData().getColumnCount(); i++) {
                    String x = attributes.getProperty("r"+row+"c"+(i+1));
                    rs.updateString(i+1, x);
                }
                if(insert) {
                    rs.insertRow();
                } else {
                    rs.updateRow();
                }
            } else if(op==2) {
                rs.absolute(row);
                rs.deleteRow();
            } else if(op==3) {
                // cancel
            }
        } catch(Throwable e) {
            result = "<br />"+getStackTrace(0, e);
            error = formatAsError(e.getMessage());
        }
        String sql = "@EDIT " + (String) session.get("resultSetSQL");
        Connection conn = getAppSession().getConnection();
        result = error + getResult(conn, -1, sql, true) + result;
        session.put("result", result);
        return "result.jsp";
    }

    private ResultSet getMetaResultSet(Connection conn, String sql) throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        if(sql.startsWith("@TABLES")) {
            String[] p = split(sql);
            String[] types = p[4] == null ? null : StringUtils.arraySplit(p[4], ',', false);
            return meta.getTables(p[1], p[2], p[3], types);
        } else if(sql.startsWith("@COLUMNS")) {
            String[] p = split(sql);
            return meta.getColumns(p[1], p[2], p[3], p[4]);
        } else if(sql.startsWith("@INDEX_INFO")) {
            String[] p = split(sql);
            boolean unique = p[4] == null ? false : Boolean.valueOf(p[4]).booleanValue();
            boolean approx = p[5] == null ? false : Boolean.valueOf(p[5]).booleanValue();
            return meta.getIndexInfo(p[1], p[2], p[3], unique, approx);
        } else if(sql.startsWith("@PRIMARY_KEYS")) {
            String[] p = split(sql);
            return meta.getPrimaryKeys(p[1], p[2], p[3]);
        } else if(sql.startsWith("@PROCEDURES")) {
            String[] p = split(sql);
            return meta.getProcedures(p[1], p[2], p[3]);
        } else if(sql.startsWith("@PROCEDURE_COLUMNS")) {
            String[] p = split(sql);
            return meta.getProcedureColumns(p[1], p[2], p[3], p[4]);
        } else if(sql.startsWith("@SCHEMAS")) {
            return meta.getSchemas();
        } else if(sql.startsWith("@CATALOG")) {
            SimpleResultSet rs = new SimpleResultSet();
            rs.addColumn("CATALOG", Types.VARCHAR, 0, 0);
            rs.addRow(new String[]{conn.getCatalog()});
            return rs;
        } else if(sql.startsWith("@MEMORY")) {
            SimpleResultSet rs = new SimpleResultSet();
            rs.addColumn("Type", Types.VARCHAR, 0, 0);
            rs.addColumn("Value", Types.VARCHAR, 0, 0);
            rs.addRow(new String[]{"Used Memory", "" + MemoryUtils.getMemoryUsed()});
            rs.addRow(new String[]{"Free Memory", "" + MemoryUtils.getMemoryFree()});
            return rs;
        } else if(sql.startsWith("@INFO")) {
            SimpleResultSet rs = new SimpleResultSet();
            rs.addColumn("KEY", Types.VARCHAR, 0, 0);
            rs.addColumn("VALUE", Types.VARCHAR, 0, 0);
            rs.addRow(new String[]{"conn.getCatalog", conn.getCatalog()});
            rs.addRow(new String[]{"conn.getAutoCommit", ""+conn.getAutoCommit()});
            rs.addRow(new String[]{"conn.getTransactionIsolation", ""+conn.getTransactionIsolation()});
            rs.addRow(new String[]{"conn.getWarnings", ""+conn.getWarnings()});
            String map;
            try {
                map = "" + conn.getTypeMap();
            } catch(SQLException e) {
                map = e.toString();
            }
            rs.addRow(new String[]{"conn.getTypeMap", ""+map});
            rs.addRow(new String[]{"conn.isReadOnly", ""+conn.isReadOnly()});
            rs.addRow(new String[]{"meta.getCatalogSeparator", ""+meta.getCatalogSeparator()});
            rs.addRow(new String[]{"meta.getCatalogTerm", ""+meta.getCatalogTerm()});
            rs.addRow(new String[]{"meta.getDatabaseProductName", ""+meta.getDatabaseProductName()});
            rs.addRow(new String[]{"meta.getDatabaseProductVersion", ""+meta.getDatabaseProductVersion()});
            rs.addRow(new String[]{"meta.getDefaultTransactionIsolation", ""+meta.getDefaultTransactionIsolation()});
            rs.addRow(new String[]{"meta.getDriverMajorVersion", ""+meta.getDriverMajorVersion()});
            rs.addRow(new String[]{"meta.getDriverMinorVersion", ""+meta.getDriverMinorVersion()});
            rs.addRow(new String[]{"meta.getDriverName", ""+meta.getDriverName()});
            rs.addRow(new String[]{"meta.getDriverVersion", ""+meta.getDriverVersion()});
            rs.addRow(new String[]{"meta.getExtraNameCharacters", ""+meta.getExtraNameCharacters()});
            rs.addRow(new String[]{"meta.getIdentifierQuoteString", ""+meta.getIdentifierQuoteString()});
            rs.addRow(new String[]{"meta.getMaxBinaryLiteralLength", ""+meta.getMaxBinaryLiteralLength()});
            rs.addRow(new String[]{"meta.getMaxCatalogNameLength", ""+meta.getMaxCatalogNameLength()});
            rs.addRow(new String[]{"meta.getMaxCharLiteralLength", ""+meta.getMaxCharLiteralLength()});
            rs.addRow(new String[]{"meta.getMaxColumnNameLength", ""+meta.getMaxColumnNameLength()});
            rs.addRow(new String[]{"meta.getMaxColumnsInGroupBy", ""+meta.getMaxColumnsInGroupBy()});
            rs.addRow(new String[]{"meta.getMaxColumnsInIndex", ""+meta.getMaxColumnsInIndex()});
            rs.addRow(new String[]{"meta.getMaxColumnsInOrderBy", ""+meta.getMaxColumnsInOrderBy()});
            rs.addRow(new String[]{"meta.getMaxColumnsInSelect", ""+meta.getMaxColumnsInSelect()});
            rs.addRow(new String[]{"meta.getMaxColumnsInTable", ""+meta.getMaxColumnsInTable()});
            rs.addRow(new String[]{"meta.getMaxConnections", ""+meta.getMaxConnections()});
            rs.addRow(new String[]{"meta.getMaxCursorNameLength", ""+meta.getMaxCursorNameLength()});
            rs.addRow(new String[]{"meta.getMaxIndexLength", ""+meta.getMaxIndexLength()});
            rs.addRow(new String[]{"meta.getMaxProcedureNameLength", ""+meta.getMaxProcedureNameLength()});
            rs.addRow(new String[]{"meta.getMaxRowSize", ""+meta.getMaxRowSize()});
            rs.addRow(new String[]{"meta.getMaxSchemaNameLength", ""+meta.getMaxSchemaNameLength()});
            rs.addRow(new String[]{"meta.getMaxStatementLength", ""+meta.getMaxStatementLength()});
            rs.addRow(new String[]{"meta.getMaxStatements", ""+meta.getMaxStatements()});
            rs.addRow(new String[]{"meta.getMaxTableNameLength", ""+meta.getMaxTableNameLength()});
            rs.addRow(new String[]{"meta.getMaxTablesInSelect", ""+meta.getMaxTablesInSelect()});
            rs.addRow(new String[]{"meta.getMaxUserNameLength", ""+meta.getMaxUserNameLength()});
            rs.addRow(new String[]{"meta.getNumericFunctions", ""+meta.getNumericFunctions()});
            rs.addRow(new String[]{"meta.getProcedureTerm", ""+meta.getProcedureTerm()});
            rs.addRow(new String[]{"meta.getSchemaTerm", ""+meta.getSchemaTerm()});
            rs.addRow(new String[]{"meta.getSearchStringEscape", ""+meta.getSearchStringEscape()});
            rs.addRow(new String[]{"meta.getSQLKeywords", ""+meta.getSQLKeywords()});
            rs.addRow(new String[]{"meta.getStringFunctions", ""+meta.getStringFunctions()});
            rs.addRow(new String[]{"meta.getSystemFunctions", ""+meta.getSystemFunctions()});
            rs.addRow(new String[]{"meta.getTimeDateFunctions", ""+meta.getTimeDateFunctions()});
            rs.addRow(new String[]{"meta.getURL", ""+meta.getURL()});
            rs.addRow(new String[]{"meta.getUserName", ""+meta.getUserName()});
            rs.addRow(new String[]{"meta.isCatalogAtStart", ""+meta.isCatalogAtStart()});
            rs.addRow(new String[]{"meta.isReadOnly", ""+meta.isReadOnly()});
            rs.addRow(new String[]{"meta.allProceduresAreCallable", ""+meta.allProceduresAreCallable()});
            rs.addRow(new String[]{"meta.allTablesAreSelectable", ""+meta.allTablesAreSelectable()});
            rs.addRow(new String[]{"meta.dataDefinitionCausesTransactionCommit", ""+meta.dataDefinitionCausesTransactionCommit()});
            rs.addRow(new String[]{"meta.dataDefinitionIgnoredInTransactions", ""+meta.dataDefinitionIgnoredInTransactions()});
            rs.addRow(new String[]{"meta.doesMaxRowSizeIncludeBlobs", ""+meta.doesMaxRowSizeIncludeBlobs()});
            rs.addRow(new String[]{"meta.nullPlusNonNullIsNull", ""+meta.nullPlusNonNullIsNull()});
            rs.addRow(new String[]{"meta.nullsAreSortedAtEnd", ""+meta.nullsAreSortedAtEnd()});
            rs.addRow(new String[]{"meta.nullsAreSortedAtStart", ""+meta.nullsAreSortedAtStart()});
            rs.addRow(new String[]{"meta.nullsAreSortedHigh", ""+meta.nullsAreSortedHigh()});
            rs.addRow(new String[]{"meta.nullsAreSortedLow", ""+meta.nullsAreSortedLow()});
            rs.addRow(new String[]{"meta.storesLowerCaseIdentifiers", ""+meta.storesLowerCaseIdentifiers()});
            rs.addRow(new String[]{"meta.storesLowerCaseQuotedIdentifiers", ""+meta.storesLowerCaseQuotedIdentifiers()});
            rs.addRow(new String[]{"meta.storesMixedCaseIdentifiers", ""+meta.storesMixedCaseIdentifiers()});
            rs.addRow(new String[]{"meta.storesMixedCaseQuotedIdentifiers", ""+meta.storesMixedCaseQuotedIdentifiers()});
            rs.addRow(new String[]{"meta.storesUpperCaseIdentifiers", ""+meta.storesUpperCaseIdentifiers()});
            rs.addRow(new String[]{"meta.storesUpperCaseQuotedIdentifiers", ""+meta.storesUpperCaseQuotedIdentifiers()});
            rs.addRow(new String[]{"meta.supportsAlterTableWithAddColumn", ""+meta.supportsAlterTableWithAddColumn()});
            rs.addRow(new String[]{"meta.supportsAlterTableWithDropColumn", ""+meta.supportsAlterTableWithDropColumn()});
            rs.addRow(new String[]{"meta.supportsANSI92EntryLevelSQL", ""+meta.supportsANSI92EntryLevelSQL()});
            rs.addRow(new String[]{"meta.supportsANSI92FullSQL", ""+meta.supportsANSI92FullSQL()});
            rs.addRow(new String[]{"meta.supportsANSI92IntermediateSQL", ""+meta.supportsANSI92IntermediateSQL()});
            rs.addRow(new String[]{"meta.supportsBatchUpdates", ""+meta.supportsBatchUpdates()});
            rs.addRow(new String[]{"meta.supportsCatalogsInDataManipulation", ""+meta.supportsCatalogsInDataManipulation()});
            rs.addRow(new String[]{"meta.supportsCatalogsInIndexDefinitions", ""+meta.supportsCatalogsInIndexDefinitions()});
            rs.addRow(new String[]{"meta.supportsCatalogsInPrivilegeDefinitions", ""+meta.supportsCatalogsInPrivilegeDefinitions()});
            rs.addRow(new String[]{"meta.supportsCatalogsInProcedureCalls", ""+meta.supportsCatalogsInProcedureCalls()});
            rs.addRow(new String[]{"meta.supportsCatalogsInTableDefinitions", ""+meta.supportsCatalogsInTableDefinitions()});
            rs.addRow(new String[]{"meta.supportsColumnAliasing", ""+meta.supportsColumnAliasing()});
            rs.addRow(new String[]{"meta.supportsConvert", ""+meta.supportsConvert()});
            rs.addRow(new String[]{"meta.supportsCoreSQLGrammar", ""+meta.supportsCoreSQLGrammar()});
            rs.addRow(new String[]{"meta.supportsCorrelatedSubqueries", ""+meta.supportsCorrelatedSubqueries()});
            rs.addRow(new String[]{"meta.supportsDataDefinitionAndDataManipulationTransactions", ""+meta.supportsDataDefinitionAndDataManipulationTransactions()});
            rs.addRow(new String[]{"meta.supportsDataManipulationTransactionsOnly", ""+meta.supportsDataManipulationTransactionsOnly()});
            rs.addRow(new String[]{"meta.supportsDifferentTableCorrelationNames", ""+meta.supportsDifferentTableCorrelationNames()});
            rs.addRow(new String[]{"meta.supportsExpressionsInOrderBy", ""+meta.supportsExpressionsInOrderBy()});
            rs.addRow(new String[]{"meta.supportsExtendedSQLGrammar", ""+meta.supportsExtendedSQLGrammar()});
            rs.addRow(new String[]{"meta.supportsFullOuterJoins", ""+meta.supportsFullOuterJoins()});
            rs.addRow(new String[]{"meta.supportsGroupBy", ""+meta.supportsGroupBy()});
            // TODO meta data: more supports methods (I'm tired now)
            rs.addRow(new String[]{"meta.usesLocalFilePerTable", ""+meta.usesLocalFilePerTable()});
            rs.addRow(new String[]{"meta.usesLocalFiles", ""+meta.usesLocalFiles()});
//#ifdef JDK14
            rs.addRow(new String[]{"conn.getHoldability", ""+conn.getHoldability()});
            rs.addRow(new String[]{"meta.getDatabaseMajorVersion", ""+meta.getDatabaseMajorVersion()});
            rs.addRow(new String[]{"meta.getDatabaseMinorVersion", ""+meta.getDatabaseMinorVersion()});
            rs.addRow(new String[]{"meta.getJDBCMajorVersion", ""+meta.getJDBCMajorVersion()});
            rs.addRow(new String[]{"meta.getJDBCMinorVersion", ""+meta.getJDBCMinorVersion()});
            rs.addRow(new String[]{"meta.getResultSetHoldability", ""+meta.getResultSetHoldability()});
            rs.addRow(new String[]{"meta.getSQLStateType", ""+meta.getSQLStateType()});
            rs.addRow(new String[]{"meta.supportsGetGeneratedKeys", ""+meta.supportsGetGeneratedKeys()});
            rs.addRow(new String[]{"meta.locatorsUpdateCopy", ""+meta.locatorsUpdateCopy()});
//#endif            
            return rs;
        } else if(sql.startsWith("@CATALOGS")) {
            return meta.getCatalogs();
        } else if(sql.startsWith("@TABLE_TYPES")) {
            return meta.getTableTypes();
        } else if(sql.startsWith("@COLUMN_PRIVILEGES")) {
            String[] p = split(sql);
            return meta.getColumnPrivileges(p[1], p[2], p[3], p[4]);
        } else if(sql.startsWith("@TABLE_PRIVILEGES")) {
            String[] p = split(sql);
            return meta.getTablePrivileges(p[1], p[2], p[3]);
        } else if(sql.startsWith("@BEST_ROW_IDENTIFIER")) {
            String[] p = split(sql);
            int scale = p[4] == null ? 0 : Integer.parseInt(p[4]);
            boolean nullable = p[5] == null ? false : Boolean.valueOf(p[5]).booleanValue();
            return meta.getBestRowIdentifier(p[1], p[2], p[3], scale, nullable);
        } else if(sql.startsWith("@VERSION_COLUMNS")) {
            String[] p = split(sql);
            return meta.getVersionColumns(p[1], p[2], p[3]);
        } else if(sql.startsWith("@IMPORTED_KEYS")) {
            String[] p = split(sql);
            return meta.getImportedKeys(p[1], p[2], p[3]);
        } else if(sql.startsWith("@EXPORTED_KEYS")) {
            String[] p = split(sql);
            return meta.getExportedKeys(p[1], p[2], p[3]);
        } else if(sql.startsWith("@CROSS_REFERENCE")) {
            String[] p = split(sql);
            return meta.getCrossReference(p[1], p[2], p[3], p[4], p[5], p[6]);
        } else if(sql.startsWith("@UDTS")) {
            String[] p = split(sql);
            int[] types;
            if(p[4] == null) {
                types = null;
            } else {
                String[] t = StringUtils.arraySplit(p[4], ',', false);
                types = new int[t.length];
                for(int i=0; i<t.length; i++) {
                    types[i] = Integer.parseInt(t[i]);
                }
            }
            return meta.getUDTs(p[1], p[2], p[3], types);
        } else if(sql.startsWith("@TYPE_INFO")) {
            return meta.getTypeInfo();
//#ifdef JDK14
        } else if(sql.startsWith("@SUPER_TYPES")) {
            String[] p = split(sql);
            return meta.getSuperTypes(p[1], p[2], p[3]);
        } else if(sql.startsWith("@SUPER_TABLES")) {
            String[] p = split(sql);
            return meta.getSuperTables(p[1], p[2], p[3]);
        } else if(sql.startsWith("@ATTRIBUTES")) {
            String[] p = split(sql);
            return meta.getAttributes(p[1], p[2], p[3], p[4]);
//#endif
        }
        return null;
    }

    private String[] split(String s) {
        String[] list = new String[10];
        String[] t = StringUtils.arraySplit(s, ' ', true);
        System.arraycopy(t, 0, list, 0, t.length);
        for(int i=0; i<list.length; i++) {
            if("null".equals(list[i])) {
                list[i] = null;
            }
        }
        return list;
    }

    private int getMaxrows() {
        String r = (String)session.get("maxrows");
        int maxrows = r==null ? 0 : Integer.parseInt(r);
        return maxrows;
    }

    private String getResult(Connection conn, int id, String sql, boolean allowEdit) {
        try {
            sql = sql.trim();
            StringBuffer buff = new StringBuffer();
            String sqlUpper = StringUtils.toUpperEnglish(sql);
            if(sqlUpper.startsWith("CREATE") || sqlUpper.startsWith("DROP") || sqlUpper.startsWith("ALTER") || sqlUpper.startsWith("RUNSCRIPT")) {
                String sessionId = attributes.getProperty("jsessionid");
                buff.append("<script type=\"text/javascript\">top['h2menu'].location='tables.do?jsessionid="+sessionId+"';</script>");
            }
            Statement stat = conn.createStatement();
            ResultSet rs;
            long time = System.currentTimeMillis();
            boolean metadata = false;
            boolean generatedKeys = false;
            boolean edit = false;
            if(sql.equals("@CANCEL")) {
                stat = getAppSession().executingStatement;
                if(stat != null) {
                    stat.cancel();
                    buff.append("${text.result.statementWasCancelled}");
                } else {
                    buff.append("${text.result.noRunningStatement}");
                }
                return buff.toString();
            } else if(sql.startsWith("@META")) {
                metadata = true;
                sql = sql.substring("@META".length()).trim();
            } else if(sql.startsWith("@GENERATED")) {
                generatedKeys = true;
                sql = sql.substring("@GENERATED".length()).trim();
            } else if(sql.startsWith("@LOOP")) {
                metadata = true;
                sql = sql.substring("@LOOP".length()).trim();
                int idx = sql.indexOf(' ');
                int count = MathUtils.decodeInt(sql.substring(0, idx));
                sql = sql.substring(idx).trim();
                return executeLoop(conn, count, sql);
            } else if(sql.startsWith("@EDIT")) {
                edit = true;
                sql = sql.substring("@EDIT".length()).trim();
                session.put("resultSetSQL", sql);
            } else if(sql.equals("@HISTORY")) {
                buff.append(getHistoryString());
                return buff.toString();
            }
            if(sql.startsWith("@")) {
                rs = getMetaResultSet(conn, sql);
                if(rs == null) {
                    buff.append("?: "+sql);
                    return buff.toString();
                }
            } else {
                int maxrows = getMaxrows();
                stat.setMaxRows(maxrows);
                getAppSession().executingStatement = stat;
                boolean isResultSet = stat.execute(sql);
                getAppSession().addCommand(sql);
                if(generatedKeys) {
                    rs = null;
//#ifdef JDK14
                    rs = stat.getGeneratedKeys();
//#endif
                } else {
                    if(!isResultSet) {
                        buff.append("${text.result.updateCount}: "+stat.getUpdateCount());
                        time = System.currentTimeMillis() - time;
                        buff.append("<br />(");
                        buff.append(time);
                        buff.append(" ms)");
                        stat.close();
                        return buff.toString();
                    }
                    rs = stat.getResultSet();
                }
            }
            time = System.currentTimeMillis() - time;
            buff.append(getResultSet(sql, rs, metadata, edit, time, allowEdit));
//            SQLWarning warning = stat.getWarnings();
//            if(warning != null) {
//                buff.append("<br />Warning:<br />");
//                buff.append(getStackTrace(id, warning));
//            }
            if(!edit) {
                stat.close();
            }
            return buff.toString();
        } catch(Exception e) {
            return getStackTrace(id, e);
        } finally {
            getAppSession().executingStatement = null;
        }
    }

    private String executeLoop(Connection conn, int count, String sql) throws SQLException {
        ArrayList params = new ArrayList();
        int idx = 0;
        while(true) {
            idx = sql.indexOf('?', idx);
            if(idx < 0) {
                break;
            }
            if(sql.substring(idx).startsWith("?/*RND*/")) {
                params.add(new Integer(1));
                sql = sql.substring(0, idx) + "?" + sql.substring(idx+"/*RND*/".length()+1);
            } else {
                params.add(new Integer(0));
            }
            idx++;
        }
        int rows = 0;
        boolean prepared;
        Random random = new Random(1);
        long time = System.currentTimeMillis();
        if(sql.startsWith("@STATEMENT")) {
            sql = sql.substring("@STATEMENT".length()).trim();
            prepared = false;
            Statement stat = conn.createStatement();
            for(int i=0; i<count; i++) {
                String s = sql;
                for(int j=0; j<params.size(); j++) {
                    idx = s.indexOf('?');
                    Integer type = (Integer) params.get(j);
                    if(type.intValue() == 1) {
                        s = s.substring(0, idx) + random.nextInt(count) + s.substring(idx+1);
                    } else {
                        s = s.substring(0, idx) + i + s.substring(idx+1);
                    }
                }
                if(stat.execute(s)) {
                    ResultSet rs = stat.getResultSet();
                    while(rs.next()) {
                        rows++;
                        // maybe get the data as well
                    }
                    rs.close();
                    // maybe close result set
                }
            }
        } else {
            prepared = true;
            PreparedStatement prep = conn.prepareStatement(sql);
            for(int i=0; i<count; i++) {
                for(int j=0; j<params.size(); j++) {
                    Integer type = (Integer) params.get(j);
                    if(type.intValue() == 1) {
                        prep.setInt(j + 1,  random.nextInt(count));
                    } else {
                        prep.setInt(j + 1,  i);
                    }
                }
                if(getAppSession().getContents().isSQLite) {
                    // SQLite currently throws an exception on prep.execute()
                    prep.executeUpdate();
                } else {
                    if(prep.execute()) {
                        ResultSet rs = prep.getResultSet();
                        while(rs.next()) {
                            rows++;
                            // maybe get the data as well
                        }
                        rs.close();
                    }
                }
            }
        }
        time = System.currentTimeMillis() - time;
        String result = time + " ms: " + count + " * ";
        if(prepared) {
            result += "(Prepared) ";
        } else {
            result += "(Statement) ";
        }
        result+="(";
        StringBuffer buff = new StringBuffer();
        for(int i=0; i<params.size(); i++) {
            if(i>0) {
                buff.append(", ");
            }
            buff.append(((Integer)params.get(i)).intValue() == 0 ? "i" : "rnd");
        }
        result += buff.toString();
        result+=") " + sql;
        return result;
    }

    private String getHistoryString() {
        StringBuffer buff = new StringBuffer();
        ArrayList history = getAppSession().getCommands();
        buff.append("<table cellspacing=0 cellpadding=0>");
        buff.append("<tr><th></th><th>Command</th></tr>");
        for(int i=history.size()-1; i>=0; i--) {
            String sql = (String) history.get(i);
            buff.append("<tr><td>");
            buff.append("<a href=\"getHistory.do?id=");
            buff.append(i);
            buff.append("&jsessionid=${sessionId}\" target=\"h2query\" ><img width=16 height=16 src=\"ico_write.gif\" onmouseover = \"this.className ='icon_hover'\" onmouseout = \"this.className ='icon'\" class=\"icon\" alt=\"${text.resultEdit.edit}\" title=\"${text.resultEdit.edit}\" border=\"1\"/></a>");
            buff.append("</td><td>");
            buff.append(PageParser.escapeHtml(sql));
            buff.append("</td></tr>");
        }
        buff.append("</t>");
        buff.append("</table>");
        return buff.toString();
    }

    private String getResultSet(String sql, ResultSet rs, boolean metadata, boolean edit, long time, boolean allowEdit) throws SQLException {
        int maxrows = getMaxrows();
        time = System.currentTimeMillis() - time;
        StringBuffer buff = new StringBuffer();
        if(edit) {
            buff.append("<form id=\"editing\" name=\"editing\" method=\"post\" "
                    + "action=\"editResult.do?jsessionid=${sessionId}\" id=\"mainForm\" target=\"h2result\">");
            buff.append("<input type=\"hidden\" name=\"op\" value=\"1\" />");
            buff.append("<input type=\"hidden\" name=\"row\" value=\"\" />");
            buff.append("<table cellspacing=0 cellpadding=0 id=\"editTable\">");
        } else {
            buff.append("<table cellspacing=0 cellpadding=0>");
        }
        ResultSetMetaData meta = rs.getMetaData();
        int columns = meta.getColumnCount();
        int rows=0;
        if(metadata) {
            buff.append("<tr><th>i</th><th>label</th><th>cat</th><th>schem</th>");
            buff.append("<th>tab</th><th>col</th><th>type</th><th>typeName</th><th>class</th>");
            buff.append("<th>prec</th><th>scale</th><th>size</th><th>autoInc</th>");
            buff.append("<th>case</th><th>currency</th><th>null</th><th>ro</th>");
            buff.append("<th>search</th><th>sig</th><th>w</th><th>defW</th></tr>");
            for(int i=1; i<=columns; i++) {
                buff.append("<tr>");
                buff.append("<td>").append(i).append("</td>");
                buff.append("<td>").append(PageParser.escapeHtml(meta.getColumnLabel(i))).append("</td>");
                buff.append("<td>").append(PageParser.escapeHtml(meta.getCatalogName(i))).append("</td>");
                buff.append("<td>").append(PageParser.escapeHtml(meta.getSchemaName(i))).append("</td>");
                buff.append("<td>").append(PageParser.escapeHtml(meta.getTableName(i))).append("</td>");
                buff.append("<td>").append(PageParser.escapeHtml(meta.getColumnName(i))).append("</td>");
                buff.append("<td>").append(meta.getColumnType(i)).append("</td>");
                buff.append("<td>").append(PageParser.escapeHtml(meta.getColumnTypeName(i))).append("</td>");
                buff.append("<td>").append(PageParser.escapeHtml(meta.getColumnClassName(i))).append("</td>");
                buff.append("<td>").append(meta.getPrecision(i)).append("</td>");
                buff.append("<td>").append(meta.getScale(i)).append("</td>");
                buff.append("<td>").append(meta.getColumnDisplaySize(i)).append("</td>");
                buff.append("<td>").append(meta.isAutoIncrement(i)).append("</td>");
                buff.append("<td>").append(meta.isCaseSensitive(i)).append("</td>");
                buff.append("<td>").append(meta.isCurrency(i)).append("</td>");
                buff.append("<td>").append(meta.isNullable(i)).append("</td>");
                buff.append("<td>").append(meta.isReadOnly(i)).append("</td>");
                buff.append("<td>").append(meta.isSearchable(i)).append("</td>");
                buff.append("<td>").append(meta.isSigned(i)).append("</td>");
                buff.append("<td>").append(meta.isWritable(i)).append("</td>");
                buff.append("<td>").append(meta.isDefinitelyWritable(i)).append("</td>");
                buff.append("</tr>");
            }
        } else {
            buff.append("<tr>");
            if(edit) {
                buff.append("<th>Action</th>");
            }
            for(int i=0; i<columns; i++) {
                buff.append("<th>");
                buff.append(PageParser.escapeHtml(meta.getColumnLabel(i+1)));
                buff.append("</th>");
            }
            buff.append("</tr>");
            while(rs.next()) {
                if(maxrows > 0 && rows >= maxrows) {
                    break;
                }
                rows++;
                buff.append("<tr>");
                if(edit) {
                    buff.append("<td>");
                    buff.append("<img onclick=\"javascript:editRow(");
                    buff.append(rs.getRow());
                    buff.append(",'${sessionId}', '${text.resultEdit.save}', '${text.resultEdit.cancel}'");
                    buff.append(")\" width=16 height=16 src=\"ico_write.gif\" onmouseover = \"this.className ='icon_hover'\" onmouseout = \"this.className ='icon'\" class=\"icon\" alt=\"${text.resultEdit.edit}\" title=\"${text.resultEdit.edit}\" border=\"1\"/>");
                    buff.append("<a href=\"editResult.do?op=2&row=");
                    buff.append(rs.getRow());
                    buff.append("&jsessionid=${sessionId}\" target=\"h2result\" ><img width=16 height=16 src=\"ico_remove.gif\" onmouseover = \"this.className ='icon_hover'\" onmouseout = \"this.className ='icon'\" class=\"icon\" alt=\"${text.resultEdit.delete}\" title=\"${text.resultEdit.delete}\" border=\"1\" /></a>");
                    buff.append("</td>");
                }
                for(int i=0; i<columns; i++) {
                    buff.append("<td>");
                    buff.append(PageParser.escapeHtml(rs.getString(i+1)));
                    buff.append("</td>");
                }
                buff.append("</tr>");
            }
        }
        boolean isUpdatable = rs.getConcurrency() == ResultSet.CONCUR_UPDATABLE;
        if(edit) {
            ResultSet old = getAppSession().result;
            if(old != null) {
                old.close();
            }
            getAppSession().result = rs;
        } else {
            rs.close();
        }
        if(edit) {
            buff.append("<tr><td>");
            buff.append("<img onclick=\"javascript:editRow(-1, '${sessionId}', '${text.resultEdit.save}', '${text.resultEdit.cancel}'");
            buff.append(")\" width=16 height=16 src=\"ico_add.gif\" onmouseover = \"this.className ='icon_hover'\" onmouseout = \"this.className ='icon'\" class=\"icon\" alt=\"${text.resultEdit.add}\" title=\"${text.resultEdit.add}\" border=\"1\"/>");
            buff.append("</td>");
            for(int i=0; i<columns; i++) {
                buff.append("<td></td>");
            }
            buff.append("</tr>");
        }
        buff.append("</table>");
        if(edit) {
            buff.append("</form>");
        }
        if(rows == 0) {
            buff.append("(${text.result.noRows}");
        } else if(rows == 1) {
            buff.append("(${text.result.1row}");
        } else {
            buff.append("(");
            buff.append(rows);
            buff.append(" ${text.result.rows}");
        }
        buff.append(", ");
        time = System.currentTimeMillis() - time;
        buff.append(time);
        buff.append(" ms)");
        if(!edit && isUpdatable && allowEdit) {
            buff.append("<br /><br /><form name=\"editResult\" method=\"post\" action=\"query.do?jsessionid=${sessionId}\" target=\"h2result\">");
            buff.append("<input type=\"submit\" class=\"button\" value=\"${text.resultEdit.editResult}\" />");
            buff.append("<input type=\"hidden\" name=\"sql\" value=\"@EDIT " + PageParser.escapeHtml(sql) +"\" />");
            buff.append("</form>");
        }
        return buff.toString();
    }

    private String settingSave() {
        ConnectionInfo info = new ConnectionInfo();
        info.name = attributes.getProperty("name", "");
        info.driver = attributes.getProperty("driver", "");
        info.url = attributes.getProperty("url", "");
        info.user = attributes.getProperty("user", "");
        server.getAppServer().updateSetting(info);
        attributes.put("setting", info.name);
        server.getAppServer().saveSettings();
        return "index.do";
    }

    private String settingRemove() {
        String setting = attributes.getProperty("name", "");
        server.getAppServer().removeSetting(setting);
        ArrayList settings = server.getAppServer().getSettings();
        if(settings.size() > 0) {
            attributes.put("setting", settings.get(0));
        }
        server.getAppServer().saveSettings();
        return "index.do";
    }

    boolean allow() {
        if(server.getAppServer().getAllowOthers()) {
            return true;
        }
        return NetUtils.isLoopbackAddress(socket);
    }

}
