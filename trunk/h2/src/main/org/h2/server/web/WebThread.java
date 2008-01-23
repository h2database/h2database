/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.server.web;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.Socket;
import java.security.SecureClassLoader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.Map.Entry;

import org.h2.api.DatabaseEventListener;
import org.h2.bnf.Bnf;
import org.h2.constant.ErrorCode;
import org.h2.engine.Constants;
import org.h2.jdbc.JdbcSQLException;
import org.h2.message.TraceSystem;
import org.h2.tools.SimpleResultSet;
import org.h2.util.JdbcUtils;
import org.h2.util.MathUtils;
import org.h2.util.MemoryUtils;
import org.h2.util.NetUtils;
import org.h2.util.ObjectArray;
import org.h2.util.ObjectUtils;
import org.h2.util.ScriptReader;
import org.h2.util.StringUtils;

/**
 * For each request, an object of this class is created.
 * Keep-alive is not supported at this time.
 * This class is used by the H2 Console.
 */
class WebThread extends Thread implements DatabaseEventListener {
    private WebServer server;
    private WebSession session;
    private Properties attributes;
    private Socket socket;

    private InputStream input;
    private String ifModifiedSince;
    private String mimeType;
    private boolean cache;
    private int listenerLastState;
    private long listenerLastEvent;
    private boolean stop;

    // TODO web: support online data editing like http://numsum.com/

    WebThread(Socket socket, WebServer server) {
        this.server = server;
        this.socket = socket;
        setName("H2 Console thread");
    }

    void setSession(WebSession session, Properties attributes) {
        this.session = session;
        this.attributes = attributes;
    }

    public void stopNow() {
        this.stop = true;
    }

    private String getAllowedFile(String requestedFile) {
        if (!allow()) {
            return "notAllowed.jsp";
        }
        if (requestedFile.length() == 0) {
            return "index.do";
        }
        return requestedFile;
    }

    public String processRequest(String file, String hostname) {
        int index = file.lastIndexOf('.');
        String suffix;
        if (index >= 0) {
            suffix = file.substring(index + 1);
        } else {
            suffix = "";
        }
        if ("ico".equals(suffix)) {
            mimeType = "image/x-icon";
            cache = true;
        } else if ("gif".equals(suffix)) {
            mimeType = "image/gif";
            cache = true;
        } else if ("css".equals(suffix)) {
            cache = true;
            mimeType = "text/css";
        } else if ("html".equals(suffix) || "do".equals(suffix) || "jsp".equals(suffix)) {
            cache = false;
            mimeType = "text/html";
            if (session == null) {
                session = server.createNewSession(hostname);
                if (!"notAllowed.jsp".equals(file)) {
                    file = "index.do";
                }
            }
        } else if ("js".equals(suffix)) {
            cache = true;
            mimeType = "text/javascript";
        } else {
            cache = false;
            mimeType = "text/html";
            file = "error.jsp";
            server.trace("unknown mime type, file " + file);
        }
        server.trace("mimeType=" + mimeType);
        server.trace(file);
        if (file.endsWith(".do")) {
            file = process(file);
        }
        return file;
    }

    public void run() {
        try {
            input = socket.getInputStream();
            String head = readHeaderLine();
            if (head.startsWith("GET ") || head.startsWith("POST ")) {
                int begin = head.indexOf('/'), end = head.lastIndexOf(' ');
                String file = head.substring(begin + 1, end).trim();
                server.trace(head + ": " + file);
                file = getAllowedFile(file);
                attributes = new Properties();
                int paramIndex = file.indexOf("?");
                session = null;
                if (paramIndex >= 0) {
                    String attrib = file.substring(paramIndex + 1);
                    parseAttributes(attrib);
                    String sessionId = attributes.getProperty("jsessionid");
                    file = file.substring(0, paramIndex);
                    session = server.getSession(sessionId);
                }
                parseHeader();
                String hostname = socket.getInetAddress().getHostName();

                file = processRequest(file, hostname);
                if (file.length() == 0) {
                    // asynchronous request
                    return;
                }

                String message;
                byte[] bytes;
                if (cache && ifModifiedSince != null && ifModifiedSince.equals(server.getStartDateTime())) {
                    bytes = null;
                    message = "HTTP/1.1 304 Not Modified\n";
                } else {
                    bytes = server.getFile(file);
                    if (bytes == null) {
                        message = "HTTP/1.0 404 Not Found\n";
                        bytes = StringUtils.utf8Encode("File not found: " + file);
                    } else {
                        if (session != null && file.endsWith(".jsp")) {
                            String page = StringUtils.utf8Decode(bytes);
                            page = PageParser.parse(server, page, session.map);
                            try {
                                bytes = StringUtils.utf8Encode(page);
                            } catch (SQLException e) {
                                server.traceError(e);
                            }
                        }
                        message = "HTTP/1.1 200 OK\n";
                        message += "Content-Type: " + mimeType + "\n";
                        if (!cache) {
                            message += "Cache-Control: no-cache\n";
                        } else {
                            message += "Cache-Control: max-age=10\n";
                            message += "Last-Modified: " + server.getStartDateTime() + "\n";
                        }
                    }
                }
                message += "\n";
                server.trace(message);
                DataOutputStream output = openOutput(message);
                if (bytes != null) {
                    output.write(bytes);
                }
                closeOutput(output);
                return;
            }
        } catch (Exception e) {
            TraceSystem.traceThrowable(e);
        }
    }

    private DataOutputStream openOutput(String message) throws IOException {
        DataOutputStream output = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
        output.write(message.getBytes());
        return output;
    }

    private void closeOutput(DataOutputStream output) {
        try {
            output.flush();
            output.close();
            socket.close();
        } catch (IOException e) {
            // ignore
        } finally {
            server.remove(this);
        }
    }

    protected String getComboBox(String[] elements, String selected) {
        StringBuffer buff = new StringBuffer();
        for (int i = 0; i < elements.length; i++) {
            String value = elements[i];
            buff.append("<option value=\"");
            buff.append(PageParser.escapeHtmlData(value));
            buff.append("\"");
            if (value.equals(selected)) {
                buff.append(" selected");
            }
            buff.append(">");
            buff.append(PageParser.escapeHtml(value));
            buff.append("</option>");
        }
        return buff.toString();
    }

    protected String getComboBox(String[][] elements, String selected) {
        StringBuffer buff = new StringBuffer();
        for (int i = 0; i < elements.length; i++) {
            String[] n = elements[i];
            buff.append("<option value=\"");
            buff.append(PageParser.escapeHtmlData(n[0]));
            buff.append("\"");
            if (n[0].equals(selected)) {
                buff.append(" selected");
            }
            buff.append(">");
            buff.append(PageParser.escapeHtml(n[1]));
            buff.append("</option>");
        }
        return buff.toString();
    }

    private String readHeaderLine() throws IOException {
        StringBuffer buff = new StringBuffer();
        while (true) {
            int i = input.read();
            if (i == -1) {
                throw new IOException("Unexpected EOF");
            } else if (i == '\r' && input.read() == '\n') {
                return buff.length() > 0 ? buff.toString() : null;
            } else {
                buff.append((char) i);
            }
        }
    }

    private void parseAttributes(String s) throws Exception {
        server.trace("data=" + s);
        while (s != null) {
            int idx = s.indexOf('=');
            if (idx >= 0) {
                String property = s.substring(0, idx);
                s = s.substring(idx + 1);
                idx = s.indexOf('&');
                String value;
                if (idx >= 0) {
                    value = s.substring(0, idx);
                    s = s.substring(idx+1);
                } else {
                    value = s;
                }
                // TODO compatibility problem with JDK 1.3
                //String attr = URLDecoder.decode(value, "UTF-8");
                // String attr = URLDecoder.decode(value);
                String attr = StringUtils.urlDecode(value);
                attributes.put(property, attr);
            } else {
                break;
            }
        }
        server.trace(attributes.toString());
    }

    private void parseHeader() throws Exception {
        server.trace("parseHeader");
        int len = 0;
        ifModifiedSince = null;
        while (true) {
            String line = readHeaderLine();
            if (line == null) {
                break;
            }
            server.trace(" " + line);
            String lower = StringUtils.toLowerEnglish(line);
            if (lower.startsWith("if-modified-since")) {
                ifModifiedSince = line.substring(line.indexOf(':') + 1).trim();
            } else if (lower.startsWith("content-length")) {
                len = Integer.parseInt(line.substring(line.indexOf(':') + 1).trim());
                server.trace("len=" + len);
            } else if (lower.startsWith("accept-language")) {
                if (session != null) {
                    Locale locale = session.locale;
                    if (locale == null) {
                        String languages = line.substring(line.indexOf(':') + 1).trim();
                        StringTokenizer tokenizer = new StringTokenizer(languages, ",;");
                        while (tokenizer.hasMoreTokens()) {
                            String token = tokenizer.nextToken();
                            if (!token.startsWith("q=")) {
                                if (server.supportsLanguage(token)) {
                                    int dash = token.indexOf('-');
                                    if (dash >= 0) {
                                        String language = token.substring(0, dash);
                                        String country = token.substring(dash + 1);
                                        locale = new Locale(language, country);
                                    } else {
                                        locale = new Locale(token, "");
                                    }
                                    session.locale = locale;
                                    String language = locale.getLanguage();
                                    session.put("language", language);
                                    server.readTranslations(session, language);
                                    break;
                                }
                            }
                        }
                    }
                }
            } else if (line.trim().length() == 0) {
                break;
            }
        }
        if (session != null && len > 0) {
            byte[] bytes = new byte[len];
            for (int pos = 0; pos < len;) {
                pos += input.read(bytes, pos, len - pos);
            }
            String s = new String(bytes);
            parseAttributes(s);
        }
    }

    String process(String file) {
        server.trace("process " + file);
        while (file.endsWith(".do")) {
            if ("login.do".equals(file)) {
                file = login();
            } else if ("index.do".equals(file)) {
                file = index();
            } else if ("logout.do".equals(file)) {
                file = logout();
            } else if ("settingRemove.do".equals(file)) {
                file = settingRemove();
            } else if ("settingSave.do".equals(file)) {
                file = settingSave();
            } else if ("test.do".equals(file)) {
                file = test();
            } else if ("query.do".equals(file)) {
                file = query();
            } else if ("tables.do".equals(file)) {
                file = tables();
            } else if ("editResult.do".equals(file)) {
                file = editResult();
            } else if ("getHistory.do".equals(file)) {
                file = getHistory();
            } else if ("admin.do".equals(file)) {
                file = admin();
            } else if ("adminSave.do".equals(file)) {
                file = adminSave();
            } else if ("adminShutdown.do".equals(file)) {
                file = adminShutdown();
            } else if ("autoCompleteList.do".equals(file)) {
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
        if (query.trim().length() > 0 && Character.isLowerCase(query.trim().charAt(0))) {
            lowercase = true;
        }
        try {
            String sql = query;
            if (sql.endsWith(";")) {
                sql += " ";
            }
            ScriptReader reader = new ScriptReader(new StringReader(sql));
            reader.setSkipRemarks(true);
            String lastSql = "";
            while (true) {
                String n = reader.readStatement();
                if (n == null) {
                    break;
                }
                lastSql = n;
            }
            String result = "";
            if (reader.isInsideRemark()) {
                if (reader.isBlockRemark()) {
                    result = "1#(End Remark)# */\n" + result;
                } else {
                    result = "1#(Newline)#\n" + result;
                }
            } else {
                sql = lastSql == null ? "" : lastSql;
                while (sql.length() > 0 && sql.charAt(0) <= ' ') {
                    sql = sql.substring(1);
                }
                if (sql.trim().length() > 0 && Character.isLowerCase(sql.trim().charAt(0))) {
                    lowercase = true;
                }
                Bnf bnf = session.getBnf();
                if (bnf == null) {
                    return "autoCompleteList.jsp";
                }
                HashMap map = bnf.getNextTokenList(sql);
                String space = "";
                if (sql.length() > 0) {
                    char last = sql.charAt(sql.length() - 1);
                    if (!Character.isWhitespace(last) && (last != '.' && last >= ' ' && last != '\'' && last != '"')) {
                        space = " ";
                    }
                }
                ArrayList list = new ArrayList(map.size());
                Iterator it = map.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry entry = (Entry) it.next();
                    String key = (String) entry.getKey();
                    String type = "" + key.charAt(0);
                    String value = (String) entry.getValue();
                    key = key.substring(2);
                    if (Character.isLetter(key.charAt(0)) && lowercase) {
                        key = StringUtils.toLowerEnglish(key);
                        value = StringUtils.toLowerEnglish(value);
                    }
                    if (key.equals(value) && !".".equals(value)) {
                        value = space + value;
                    }
                    key = StringUtils.urlEncode(key);
                    key = StringUtils.replaceAll(key, "+", " ");
                    value = StringUtils.urlEncode(value);
                    value = StringUtils.replaceAll(value, "+", " ");
                    list.add(type + "#" + key + "#" + value);
                }
                Collections.sort(list);
                StringBuffer buff = new StringBuffer();
                if (query.endsWith("\n") || query.trim().endsWith(";")) {
                    list.add(0, "1#(Newline)#\n");
                }
                for (int i = 0; i < list.size(); i++) {
                    if (i > 0) {
                        buff.append('|');
                    }
                    buff.append((String) list.get(i));
                }
                result = buff.toString();
            }
            session.put("autoCompleteList", result);
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return "autoCompleteList.jsp";
    }

    private String admin() {
        session.put("port", ""+server.getPort());
        session.put("allowOthers", ""+server.getAllowOthers());
        session.put("ssl", String.valueOf(server.getSSL()));
        session.put("sessions", server.getSessions());
        return "admin.jsp";
    }

    private String adminSave() {
        try {
            server.setPort(MathUtils.decodeInt((String) attributes.get("port")));
            server.setAllowOthers(Boolean.valueOf((String) attributes.get("allowOthers")).booleanValue());
            server.setSSL(Boolean.valueOf((String) attributes.get("ssl")).booleanValue());
            server.saveSettings();
        } catch (Exception e) {
            server.trace(e.toString());
        }
        return admin();
    }

    private String adminShutdown() {
        server.shutdown();
        return "admin.jsp";
    }

    private String index() {
        String[][] languageArray = server.getLanguageArray();
        String language = (String) attributes.get("language");
        Locale locale = session.locale;
        if (language != null) {
            if (locale == null || !StringUtils.toLowerEnglish(locale.getLanguage()).equals(language)) {
                locale = new Locale(language, "");
                server.readTranslations(session, locale.getLanguage());
                session.put("language", language);
                session.locale = locale;
            }
        } else {
            language = (String) session.get("language");
        }
        session.put("languageCombo", getComboBox(languageArray, language));
        String[] settingNames = server.getSettingNames();
        String setting = attributes.getProperty("setting");
        if (setting == null && settingNames.length > 0) {
            setting = settingNames[0];
        }
        String combobox = getComboBox(settingNames, setting);
        session.put("settingsList", combobox);
        ConnectionInfo info = server.getSetting(setting);
        if (info == null) {
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
        String sql = session.getCommand(id);
        session.put("query", PageParser.escapeHtmlData(sql));
        return "query.jsp";
    }

    private int addColumns(DbTableOrView table, StringBuffer buff, int treeIndex, boolean showColumnTypes,
            StringBuffer columnsBuffer) throws SQLException {
        DbColumn[] columns = table.columns;
        for (int i = 0; columns != null && i < columns.length; i++) {
            DbColumn column = columns[i];
            if (columnsBuffer.length() > 0) {
                columnsBuffer.append(' ');
            }
            columnsBuffer.append(column.name);
            String col = StringUtils.urlEncode(PageParser.escapeJavaScript(column.name));
            buff.append("setNode(" + treeIndex + ", 1, 1, 'column', '" + PageParser.escapeJavaScript(column.name)
                    + "', 'javascript:ins(\\'" + col + "\\')');\n");
            treeIndex++;
            if (showColumnTypes) {
                buff.append("setNode(" + treeIndex + ", 2, 2, 'type', '" + PageParser.escapeJavaScript(column.dataType)
                        + "', null);\n");
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

    private int addIndexes(DatabaseMetaData meta, String table, String schema, StringBuffer buff, int treeIndex)
            throws SQLException {
        // index reading is very slow for oracle (2 seconds per index), so don't
        // do it
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
                if (name != null && type != null) {
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
        if (indexMap.size() > 0) {
            buff.append("setNode(" + treeIndex + ", 1, 1, 'index_az', '${text.tree.indexes}', null);\n");
            treeIndex++;
            for (Iterator it = indexMap.values().iterator(); it.hasNext();) {
                IndexInfo info = (IndexInfo) it.next();
                buff.append("setNode(" + treeIndex + ", 2, 1, 'index', '" + PageParser.escapeJavaScript(info.name)
                        + "', null);\n");
                treeIndex++;
                buff.append("setNode(" + treeIndex + ", 3, 2, 'type', '" + info.type + "', null);\n");
                treeIndex++;
                buff.append("setNode(" + treeIndex + ", 3, 2, 'type', '" + PageParser.escapeJavaScript(info.columns)
                        + "', null);\n");
                treeIndex++;
            }
        }
        return treeIndex;
    }

    private int addTablesAndViews(DbSchema schema, boolean mainSchema, StringBuffer buff, int treeIndex)
            throws SQLException {
        if (schema == null) {
            return treeIndex;
        }
        Connection conn = session.getConnection();
        DatabaseMetaData meta = session.getMetaData();
        int level = mainSchema ? 0 : 1;
        String indentation = ", " + level + ", " + (level + 1) + ", ";
        String indentNode = ", " + (level + 1) + ", " + (level + 1) + ", ";
        DbTableOrView[] tables = schema.tables;
        if (tables == null) {
            return treeIndex;
        }
        boolean isOracle = schema.contents.isOracle;
        boolean notManyTables = tables.length < 100;
        for (int i = 0; i < tables.length; i++) {
            DbTableOrView table = tables[i];
            if (table.isView) {
                continue;
            }
            int tableId = treeIndex;
            String tab = table.quotedName;
            if (!mainSchema) {
                tab = schema.quotedName + "." + tab;
            }
            tab = StringUtils.urlEncode(PageParser.escapeJavaScript(tab));
            buff.append("setNode(" + treeIndex + indentation + " 'table', '" + PageParser.escapeJavaScript(table.name)
                    + "', 'javascript:ins(\\'" + tab + "\\',true)');\n");
            treeIndex++;
            if (mainSchema) {
                StringBuffer columnsBuffer = new StringBuffer();
                treeIndex = addColumns(table, buff, treeIndex, notManyTables, columnsBuffer);
                if (!isOracle && notManyTables) {
                    treeIndex = addIndexes(meta, table.name, schema.name, buff, treeIndex);
                }
                buff.append("addTable('" + PageParser.escapeJavaScript(table.name) + "', '"
                        + PageParser.escapeJavaScript(columnsBuffer.toString()) + "', " + tableId + ");\n");
            }
        }
        tables = schema.tables;
        for (int i = 0; i < tables.length; i++) {
            DbTableOrView view = tables[i];
            if (!view.isView) {
                continue;
            }
            int tableId = treeIndex;
            String tab = view.quotedName;
            if (!mainSchema) {
                tab = view.schema.quotedName + "." + tab;
            }
            tab = StringUtils.urlEncode(PageParser.escapeJavaScript(tab));
            buff.append("setNode(" + treeIndex + indentation + " 'view', '" + PageParser.escapeJavaScript(view.name)
                    + "', 'javascript:ins(\\'" + tab + "\\',true)');\n");
            treeIndex++;
            if (mainSchema) {
                StringBuffer columnsBuffer = new StringBuffer();
                treeIndex = addColumns(view, buff, treeIndex, notManyTables, columnsBuffer);
                if (schema.contents.isH2) {
                    PreparedStatement prep = null;
                    try {
                        prep = conn.prepareStatement("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME=?");
                        prep.setString(1, view.name);
                        ResultSet rs = prep.executeQuery();
                        if (rs.next()) {
                            String sql = rs.getString("SQL");
                            buff.append("setNode(" + treeIndex + indentNode + " 'type', '"
                                    + PageParser.escapeJavaScript(sql) + "', null);\n");
                            treeIndex++;
                        }
                        rs.close();
                    } finally {
                        JdbcUtils.closeSilently(prep);
                    }
                }
                buff.append("addTable('" + PageParser.escapeJavaScript(view.name) + "', '"
                        + PageParser.escapeJavaScript(columnsBuffer.toString()) + "', " + tableId + ");\n");
            }
        }
        return treeIndex;
    }

    private String tables() {
        DbContents contents = session.getContents();
        try {
            contents.readContents(session.getMetaData());
            session.loadBnf();
            Connection conn = session.getConnection();
            DatabaseMetaData meta = session.getMetaData();
            boolean isH2 = contents.isH2;

            StringBuffer buff = new StringBuffer();
            buff.append("setNode(0, 0, 0, 'database', '" + PageParser.escapeJavaScript((String) session.get("url"))
                    + "', null);\n");
            int treeIndex = 1;

            DbSchema defaultSchema = contents.defaultSchema;
            treeIndex = addTablesAndViews(defaultSchema, true, buff, treeIndex);
            DbSchema[] schemas = contents.schemas;
            for (int i = 0; i < schemas.length; i++) {
                DbSchema schema = schemas[i];
                if (schema == defaultSchema || schema == null) {
                    continue;
                }
                buff.append("setNode(" + treeIndex + ", 0, 1, 'folder', '" + PageParser.escapeJavaScript(schema.name)
                        + "', null);\n");
                treeIndex++;
                treeIndex = addTablesAndViews(schema, false, buff, treeIndex);
            }
            if (isH2) {
                Statement stat = null;
                try {
                    stat = conn.createStatement();
                    ResultSet rs = stat
                            .executeQuery("SELECT * FROM INFORMATION_SCHEMA.SEQUENCES ORDER BY SEQUENCE_NAME");
                    for (int i = 0; rs.next(); i++) {
                        if (i == 0) {
                            buff.append("setNode(" + treeIndex
                                    + ", 0, 1, 'sequences', '${text.tree.sequences}', null);\n");
                            treeIndex++;
                        }
                        String name = rs.getString("SEQUENCE_NAME");
                        String current = rs.getString("CURRENT_VALUE");
                        String increment = rs.getString("INCREMENT");
                        buff.append("setNode(" + treeIndex + ", 1, 1, 'sequence', '"
                                + PageParser.escapeJavaScript(name) + "', null);\n");
                        treeIndex++;
                        buff.append("setNode(" + treeIndex + ", 2, 2, 'type', '${text.tree.current}: "
                                + PageParser.escapeJavaScript(current) + "', null);\n");
                        treeIndex++;
                        if (!"1".equals(increment)) {
                            buff.append("setNode(" + treeIndex + ", 2, 2, 'type', '${text.tree.increment}: "
                                    + PageParser.escapeJavaScript(increment) + "', null);\n");
                            treeIndex++;
                        }
                    }
                    rs.close();
                    rs = stat.executeQuery("SELECT * FROM INFORMATION_SCHEMA.USERS ORDER BY NAME");
                    for (int i = 0; rs.next(); i++) {
                        if (i == 0) {
                            buff.append("setNode(" + treeIndex + ", 0, 1, 'users', '${text.tree.users}', null);\n");
                            treeIndex++;
                        }
                        String name = rs.getString("NAME");
                        String admin = rs.getString("ADMIN");
                        buff.append("setNode(" + treeIndex + ", 1, 1, 'user', '" + PageParser.escapeJavaScript(name)
                                + "', null);\n");
                        treeIndex++;
                        if (admin.equalsIgnoreCase("TRUE")) {
                            buff.append("setNode(" + treeIndex + ", 2, 2, 'type', '${text.tree.admin}', null);\n");
                            treeIndex++;
                        }
                    }
                    rs.close();
                } finally {
                    JdbcUtils.closeSilently(stat);
                }
            }
            String version = meta.getDatabaseProductName() + " " + meta.getDatabaseProductVersion();
            buff.append("setNode(" + treeIndex + ", 0, 0, 'info', '" + PageParser.escapeJavaScript(version)
                    + "', null);\n");
            buff.append("refreshQueryTables();");
            session.put("tree", buff.toString());
        } catch (Exception e) {
            session.put("tree", "");
            session.put("error", getStackTrace(0, e));
        }
        return "tables.jsp";
    }

    private String getStackTrace(int id, Throwable e) {
        try {
            StringWriter writer = new StringWriter();
            e.printStackTrace(new PrintWriter(writer));
            String s = writer.toString();
            s = PageParser.escapeHtml(s);
            s = StringUtils.replaceAll(s, "\t", "&nbsp;&nbsp;&nbsp;&nbsp;");
            String message = PageParser.escapeHtml(e.getMessage());
            s = "<a class=\"error\" href=\"#\" onclick=\"var x=document.getElementById('st" + id
                    + "').style;x.display=x.display==''?'none':'';\">" + message
                    + "</a><span style=\"display: none;\" id=\"st" + id + "\"><br />" + s + "</span>";
            s = formatAsError(s);
            return s;
        } catch (OutOfMemoryError e2) {
            e.printStackTrace();
            return e.toString();
        }
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
            Connection conn = server.getConnection(driver, url, user, password, this);
            JdbcUtils.closeSilently(conn);
            session.put("error", "${text.login.testSuccessful}");
            return "login.jsp";
        } catch (Exception e) {
            session.put("error", getLoginError(e));
            return "login.jsp";
        }
    }

    private String getLoginError(Exception e) {
        if (e instanceof JdbcSQLException && ((JdbcSQLException) e).getErrorCode() == ErrorCode.CLASS_NOT_FOUND_1) {
            return "${text.login.driverNotFound}<br />" + getStackTrace(0, e);
        } else {
            return getStackTrace(0, e);
        }
    }

    private String login() {
        final String driver = attributes.getProperty("driver", "");
        final String url = attributes.getProperty("url", "");
        final String user = attributes.getProperty("user", "");
        final String password = attributes.getProperty("password", "");
        session.put("autoCommit", "checked");
        session.put("autoComplete", "1");
        session.put("maxrows", "1000");
        boolean thread = false;
        if (socket != null
                && url.startsWith("jdbc:h2:")
                && !url.startsWith("jdbc:h2:tcp:")
                && !url.startsWith("jdbc:h2:ssl:")
                && !url.startsWith("jdbc:h2:mem:")) {
            thread = true;
        }
        if (!thread) {
            try {
                Connection conn = server.getConnection(driver, url, user, password, this);
                session.setConnection(conn);
                session.put("url", url);
                session.put("user", user);
                session.remove("error");
                settingSave();
                return "frame.jsp";
            } catch (Exception e) {
                session.put("error", getLoginError(e));
                return "login.jsp";
            }
        }
        class LoginTask implements Runnable, DatabaseEventListener {
            private DataOutputStream output;
            private PrintWriter writer;
            SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss.SSS");

            LoginTask() throws IOException {
                String message = "HTTP/1.1 200 OK\n";
                message += "Content-Type: " + mimeType + "\n\n";
                output = openOutput(message);
                writer = new PrintWriter(output);
                writer.println("<html><head><link rel=\"stylesheet\" type=\"text/css\" href=\"stylesheet.css\" /></head>");
                writer.println("<body><h2>Opening Database</h2>URL: " + PageParser.escapeHtml(url) + "<br />");
                writer.println("User: " + PageParser.escapeHtml(user) + "<br />");
                writer.println("Version: " + Constants.getVersion() + "<br /><br />");
                writer.flush();
                log("Start...");
            }

            public void closingDatabase() {
                log("Closing database");
            }

            public void diskSpaceIsLow(long stillAvailable) throws SQLException {
                log("Disk space is low; still available: " + stillAvailable);
            }

            public void exceptionThrown(SQLException e, String sql) {
                log("Exception: " + PageParser.escapeHtml(e.toString()) + " SQL: " + PageParser.escapeHtml(sql));
            }

            public void init(String url) {
                log("Init: " + PageParser.escapeHtml(url));
            }

            public void opened() {
                log("Database was opened");
            }

            public void setProgress(int state, String name, int x, int max) {
                name = PageParser.escapeHtml(name);
                if (state == listenerLastState) {
                    long time = System.currentTimeMillis();
                    if (listenerLastEvent + 500 < time) {
                        return;
                    }
                    listenerLastEvent = time;
                } else {
                    listenerLastState = state;
                }
                switch(state) {
                case DatabaseEventListener.STATE_BACKUP_FILE:
                    log("Backing up " + name + " " + (100L * x / max) + "%");
                    break;
                case DatabaseEventListener.STATE_CREATE_INDEX:
                    log("Creating index " + name + " " + (100L * x / max) + "%");
                    break;
                case DatabaseEventListener.STATE_RECOVER:
                    log("Recovering " + name + " " + (100L * x / max) + "%");
                    break;
                case DatabaseEventListener.STATE_SCAN_FILE:
                    log("Scanning file " + name + " " + (100L * x / max) + "%");
                    break;
                default:
                    log("Unknown state: " + state);
                }
            }

            private synchronized void log(String message) {
                if (output != null) {
                    message = dateFormat.format(new Date()) + ": " + message;
                    writer.println(message + "<br />");
                    writer.flush();
                }
            }

            public void run() {
                String sessionId = (String) session.get("sessionId");
                try {
                    Connection conn = server.getConnection(driver, url, user, password, this);
                    session.setConnection(conn);
                    session.put("url", url);
                    session.put("user", user);
                    session.remove("error");
                    settingSave();
                    log("OK<script type=\"text/javascript\">top.location=\"frame.jsp?jsessionid=" +sessionId+ "\"</script></body></htm>");
                    // return "frame.jsp";
                } catch (Exception e) {
                    session.put("error", getLoginError(e));
                    log("Error<script type=\"text/javascript\">top.location=\"index.jsp?jsessionid=" +sessionId+ "\"</script></body></html>");
                    // return "index.jsp";
                }
                synchronized (this) {
                    closeOutput(output);
                    output = null;
                }
            }
        }
        try {
            LoginTask login = new LoginTask();
            Thread t = new Thread(login);
            t.start();
        } catch (IOException e) {
            // ignore
        }
        return "";
    }

    private String logout() {
        try {
            Connection conn = session.getConnection();
            session.setConnection(null);
            session.remove("conn");
            session.remove("result");
            session.remove("tables");
            session.remove("user");
            if (conn != null) {
                conn.close();
            }
        } catch (Exception e) {
            server.trace(e.toString());
        }
        return "index.do";
    }

    private String query() {
        String sql = attributes.getProperty("sql").trim();
        try {
            Connection conn = session.getConnection();
            String result;
            if (sql.startsWith("@JAVA")) {
                if (server.getAllowScript()) {
                    try {
                        result = executeJava(sql.substring("@JAVA".length()));
                    } catch (Throwable t) {
                        result = getStackTrace(0, t);
                    }
                } else {
                    result = "Executing Java code is not allowed, use command line parameters -webScript true";
                }
            } else if ("@AUTOCOMMIT TRUE".equals(sql)) {
                conn.setAutoCommit(true);
                result = "${text.result.autoCommitOn}";
            } else if ("@AUTOCOMMIT FALSE".equals(sql)) {
                conn.setAutoCommit(false);
                result = "${text.result.autoCommitOff}";
            } else if (sql.startsWith("@TRANSACTION_ISOLATION")) {
                String s = sql.substring("@TRANSACTION_ISOLATION".length()).trim();
                if (s.length() > 0) {
                    int level = Integer.parseInt(s);
                    conn.setTransactionIsolation(level);
                }
                result = "Transaction Isolation: " + conn.getTransactionIsolation() + "<br />";
                result += Connection.TRANSACTION_READ_UNCOMMITTED + ": READ_UNCOMMITTED<br />";
                result += Connection.TRANSACTION_READ_COMMITTED + ": READ_COMMITTED<br />";
                result += Connection.TRANSACTION_REPEATABLE_READ + ": REPEATABLE_READ<br />";
                result += Connection.TRANSACTION_SERIALIZABLE + ": SERIALIZABLE";
            } else if (sql.startsWith("@SET MAXROWS ")) {
                int maxrows = Integer.parseInt(sql.substring("@SET MAXROWS ".length()));
                session.put("maxrows", "" + maxrows);
                result = "${text.result.maxrowsSet}";
            } else {
                ScriptReader r = new ScriptReader(new StringReader(sql));
                ObjectArray list = new ObjectArray();
                while (true) {
                    String s = r.readStatement();
                    if (s == null) {
                        break;
                    }
                    list.add(s);
                }
                StringBuffer buff = new StringBuffer();
                for (int i = 0; i < list.size(); i++) {
                    String s = (String) list.get(i);
                    if (!s.startsWith("@")) {
                        buff.append(PageParser.escapeHtml(s + ";"));
                        buff.append("<br />");
                    }
                    buff.append(getResult(conn, i + 1, s, list.size() == 1, false));
                    buff.append("<br />");
                }
                result = buff.toString();
            }
            session.put("result", result);
        } catch (Throwable e) {
            session.put("result", getStackTrace(0, e));
        }
        return "result.jsp";
    }

    static class DynamicClassLoader extends SecureClassLoader {

        private String name;
        private byte[] data;
        private Class clazz;

        DynamicClassLoader(String name, byte[] data) throws MalformedURLException {
            super(DynamicClassLoader.class.getClassLoader());
            this.name = name;
            this.data = data;
        }

        public Class loadClass(String className) throws ClassNotFoundException {
            return findClass(className);
        }

        public Class findClass(String className) throws ClassNotFoundException {
            if (className.equals(name)) {
                if (clazz == null) {
                    clazz = defineClass(className, data, 0, data.length);
                }
                return clazz;
            }
            try {
                return findSystemClass(className);
            } catch (Exception e) {
            }
            return super.findClass(className);
        }
    }

    private String executeJava(String code) throws Exception {
        File javaFile = new File("Java.java");
        File classFile = new File("Java.class");
        try {
            PrintWriter out = new PrintWriter(new FileWriter(javaFile));
            classFile.delete();
            int endImport = code.indexOf("@CODE");
            String importCode = "import java.util.*; import java.math.*; import java.sql.*;";
            if (endImport >= 0) {
                importCode = code.substring(0, endImport);
                code = code.substring("@CODE".length() + endImport);
            }
            out.println(importCode);
            out.println("public class Java { public static Object run() throws Throwable {" + code + "}}");
            out.close();
            Process p = Runtime.getRuntime().exec("javac Java.java");
            InputStream processIn = p.getInputStream();
            InputStream processErrorIn = p.getErrorStream();
            StringBuffer buff = new StringBuffer();
            while (true) {
                int c = processIn.read();
                if (c == -1) {
                    break;
                }
                buff.append((char) c);
            }
            while (true) {
                int c = processErrorIn.read();
                if (c == -1) {
                    break;
                }
                buff.append((char) c);
            }
            String error = buff.toString().trim();
            if (error.length() > 0) {
                throw new Exception("Error compiling: " + error.toString());
            }
            byte[] data = new byte[(int) classFile.length()];
            DataInputStream in = new DataInputStream(new FileInputStream(classFile));
            in.readFully(data);
            in.close();
            DynamicClassLoader cl = new DynamicClassLoader("Java", data);
            Class clazz = cl.loadClass("Java");
            Method[] methods = clazz.getMethods();
            for (int i = 0; i < methods.length; i++) {
                Method m = methods[i];
                if (m.getName().equals("run")) {
                    return "" + m.invoke(null, new Object[0]);
                }
            }
            return null;
        } finally {
            javaFile.delete();
            classFile.delete();
        }
    }

    private String editResult() {
        ResultSet rs = session.result;
        int row = Integer.parseInt(attributes.getProperty("row"));
        int op = Integer.parseInt(attributes.getProperty("op"));
        String result = "", error = "";
        try {
            if (op == 1) {
                boolean insert = row < 0;
                if (insert) {
                    rs.moveToInsertRow();
                } else {
                    rs.absolute(row);
                }
                for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
                    String x = attributes.getProperty("r" + row + "c" + (i + 1));
                    rs.updateString(i + 1, unescapeData(x));
                }
                if (insert) {
                    rs.insertRow();
                } else {
                    rs.updateRow();
                }
            } else if (op == 2) {
                rs.absolute(row);
                rs.deleteRow();
            } else if (op == 3) {
                // cancel
            }
        } catch (Throwable e) {
            result = "<br />" + getStackTrace(0, e);
            error = formatAsError(e.getMessage());
        }
        String sql = "@EDIT " + (String) session.get("resultSetSQL");
        Connection conn = session.getConnection();
        result = error + getResult(conn, -1, sql, true, true) + result;
        session.put("result", result);
        return "result.jsp";
    }

    private ResultSet getMetaResultSet(Connection conn, String sql) throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        if (sql.startsWith("@TABLES")) {
            String[] p = split(sql);
            String[] types = p[4] == null ? null : StringUtils.arraySplit(p[4], ',', false);
            return meta.getTables(p[1], p[2], p[3], types);
        } else if (sql.startsWith("@COLUMNS")) {
            String[] p = split(sql);
            return meta.getColumns(p[1], p[2], p[3], p[4]);
        } else if (sql.startsWith("@INDEX_INFO")) {
            String[] p = split(sql);
            boolean unique = p[4] == null ? false : Boolean.valueOf(p[4]).booleanValue();
            boolean approx = p[5] == null ? false : Boolean.valueOf(p[5]).booleanValue();
            return meta.getIndexInfo(p[1], p[2], p[3], unique, approx);
        } else if (sql.startsWith("@PRIMARY_KEYS")) {
            String[] p = split(sql);
            return meta.getPrimaryKeys(p[1], p[2], p[3]);
        } else if (sql.startsWith("@PROCEDURES")) {
            String[] p = split(sql);
            return meta.getProcedures(p[1], p[2], p[3]);
        } else if (sql.startsWith("@PROCEDURE_COLUMNS")) {
            String[] p = split(sql);
            return meta.getProcedureColumns(p[1], p[2], p[3], p[4]);
        } else if (sql.startsWith("@SCHEMAS")) {
            return meta.getSchemas();
        } else if (sql.startsWith("@CATALOG")) {
            SimpleResultSet rs = new SimpleResultSet();
            rs.addColumn("CATALOG", Types.VARCHAR, 0, 0);
            rs.addRow(new String[] { conn.getCatalog() });
            return rs;
        } else if (sql.startsWith("@MEMORY")) {
            SimpleResultSet rs = new SimpleResultSet();
            rs.addColumn("Type", Types.VARCHAR, 0, 0);
            rs.addColumn("Value", Types.VARCHAR, 0, 0);
            rs.addRow(new String[] { "Used Memory", "" + MemoryUtils.getMemoryUsed() });
            rs.addRow(new String[] { "Free Memory", "" + MemoryUtils.getMemoryFree() });
            return rs;
        } else if (sql.startsWith("@INFO")) {
            SimpleResultSet rs = new SimpleResultSet();
            rs.addColumn("KEY", Types.VARCHAR, 0, 0);
            rs.addColumn("VALUE", Types.VARCHAR, 0, 0);
            rs.addRow(new String[] { "conn.getCatalog", conn.getCatalog() });
            rs.addRow(new String[] { "conn.getAutoCommit", "" + conn.getAutoCommit() });
            rs.addRow(new String[] { "conn.getTransactionIsolation", "" + conn.getTransactionIsolation() });
            rs.addRow(new String[] { "conn.getWarnings", "" + conn.getWarnings() });
            String map;
            try {
                map = "" + conn.getTypeMap();
            } catch (SQLException e) {
                map = e.toString();
            }
            rs.addRow(new String[] { "conn.getTypeMap", "" + map });
            rs.addRow(new String[] { "conn.isReadOnly", "" + conn.isReadOnly() });
            rs.addRow(new String[] { "meta.getCatalogSeparator", "" + meta.getCatalogSeparator() });
            rs.addRow(new String[] { "meta.getCatalogTerm", "" + meta.getCatalogTerm() });
            rs.addRow(new String[] { "meta.getDatabaseProductName", "" + meta.getDatabaseProductName() });
            rs.addRow(new String[] { "meta.getDatabaseProductVersion", "" + meta.getDatabaseProductVersion() });
            rs.addRow(new String[] { "meta.getDefaultTransactionIsolation", "" + meta.getDefaultTransactionIsolation() });
            rs.addRow(new String[] { "meta.getDriverMajorVersion", "" + meta.getDriverMajorVersion() });
            rs.addRow(new String[] { "meta.getDriverMinorVersion", "" + meta.getDriverMinorVersion() });
            rs.addRow(new String[] { "meta.getDriverName", "" + meta.getDriverName() });
            rs.addRow(new String[] { "meta.getDriverVersion", "" + meta.getDriverVersion() });
            rs.addRow(new String[] { "meta.getExtraNameCharacters", "" + meta.getExtraNameCharacters() });
            rs.addRow(new String[] { "meta.getIdentifierQuoteString", "" + meta.getIdentifierQuoteString() });
            rs.addRow(new String[] { "meta.getMaxBinaryLiteralLength", "" + meta.getMaxBinaryLiteralLength() });
            rs.addRow(new String[] { "meta.getMaxCatalogNameLength", "" + meta.getMaxCatalogNameLength() });
            rs.addRow(new String[] { "meta.getMaxCharLiteralLength", "" + meta.getMaxCharLiteralLength() });
            rs.addRow(new String[] { "meta.getMaxColumnNameLength", "" + meta.getMaxColumnNameLength() });
            rs.addRow(new String[] { "meta.getMaxColumnsInGroupBy", "" + meta.getMaxColumnsInGroupBy() });
            rs.addRow(new String[] { "meta.getMaxColumnsInIndex", "" + meta.getMaxColumnsInIndex() });
            rs.addRow(new String[] { "meta.getMaxColumnsInOrderBy", "" + meta.getMaxColumnsInOrderBy() });
            rs.addRow(new String[] { "meta.getMaxColumnsInSelect", "" + meta.getMaxColumnsInSelect() });
            rs.addRow(new String[] { "meta.getMaxColumnsInTable", "" + meta.getMaxColumnsInTable() });
            rs.addRow(new String[] { "meta.getMaxConnections", "" + meta.getMaxConnections() });
            rs.addRow(new String[] { "meta.getMaxCursorNameLength", "" + meta.getMaxCursorNameLength() });
            rs.addRow(new String[] { "meta.getMaxIndexLength", "" + meta.getMaxIndexLength() });
            rs.addRow(new String[] { "meta.getMaxProcedureNameLength", "" + meta.getMaxProcedureNameLength() });
            rs.addRow(new String[] { "meta.getMaxRowSize", "" + meta.getMaxRowSize() });
            rs.addRow(new String[] { "meta.getMaxSchemaNameLength", "" + meta.getMaxSchemaNameLength() });
            rs.addRow(new String[] { "meta.getMaxStatementLength", "" + meta.getMaxStatementLength() });
            rs.addRow(new String[] { "meta.getMaxStatements", "" + meta.getMaxStatements() });
            rs.addRow(new String[] { "meta.getMaxTableNameLength", "" + meta.getMaxTableNameLength() });
            rs.addRow(new String[] { "meta.getMaxTablesInSelect", "" + meta.getMaxTablesInSelect() });
            rs.addRow(new String[] { "meta.getMaxUserNameLength", "" + meta.getMaxUserNameLength() });
            rs.addRow(new String[] { "meta.getNumericFunctions", "" + meta.getNumericFunctions() });
            rs.addRow(new String[] { "meta.getProcedureTerm", "" + meta.getProcedureTerm() });
            rs.addRow(new String[] { "meta.getSchemaTerm", "" + meta.getSchemaTerm() });
            rs.addRow(new String[] { "meta.getSearchStringEscape", "" + meta.getSearchStringEscape() });
            rs.addRow(new String[] { "meta.getSQLKeywords", "" + meta.getSQLKeywords()});
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
        } else if (sql.startsWith("@CATALOGS")) {
            return meta.getCatalogs();
        } else if (sql.startsWith("@TABLE_TYPES")) {
            return meta.getTableTypes();
        } else if (sql.startsWith("@COLUMN_PRIVILEGES")) {
            String[] p = split(sql);
            return meta.getColumnPrivileges(p[1], p[2], p[3], p[4]);
        } else if (sql.startsWith("@TABLE_PRIVILEGES")) {
            String[] p = split(sql);
            return meta.getTablePrivileges(p[1], p[2], p[3]);
        } else if (sql.startsWith("@BEST_ROW_IDENTIFIER")) {
            String[] p = split(sql);
            int scale = p[4] == null ? 0 : Integer.parseInt(p[4]);
            boolean nullable = p[5] == null ? false : Boolean.valueOf(p[5]).booleanValue();
            return meta.getBestRowIdentifier(p[1], p[2], p[3], scale, nullable);
        } else if (sql.startsWith("@VERSION_COLUMNS")) {
            String[] p = split(sql);
            return meta.getVersionColumns(p[1], p[2], p[3]);
        } else if (sql.startsWith("@IMPORTED_KEYS")) {
            String[] p = split(sql);
            return meta.getImportedKeys(p[1], p[2], p[3]);
        } else if (sql.startsWith("@EXPORTED_KEYS")) {
            String[] p = split(sql);
            return meta.getExportedKeys(p[1], p[2], p[3]);
        } else if (sql.startsWith("@CROSS_REFERENCE")) {
            String[] p = split(sql);
            return meta.getCrossReference(p[1], p[2], p[3], p[4], p[5], p[6]);
        } else if (sql.startsWith("@UDTS")) {
            String[] p = split(sql);
            int[] types;
            if (p[4] == null) {
                types = null;
            } else {
                String[] t = StringUtils.arraySplit(p[4], ',', false);
                types = new int[t.length];
                for (int i = 0; i < t.length; i++) {
                    types[i] = Integer.parseInt(t[i]);
                }
            }
            return meta.getUDTs(p[1], p[2], p[3], types);
        } else if (sql.startsWith("@TYPE_INFO")) {
            return meta.getTypeInfo();
//#ifdef JDK14
        } else if (sql.startsWith("@SUPER_TYPES")) {
            String[] p = split(sql);
            return meta.getSuperTypes(p[1], p[2], p[3]);
        } else if (sql.startsWith("@SUPER_TABLES")) {
            String[] p = split(sql);
            return meta.getSuperTables(p[1], p[2], p[3]);
        } else if (sql.startsWith("@ATTRIBUTES")) {
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
        for (int i = 0; i < list.length; i++) {
            if ("null".equals(list[i])) {
                list[i] = null;
            }
        }
        return list;
    }

    private int getMaxrows() {
        String r = (String) session.get("maxrows");
        int maxrows = r == null ? 0 : Integer.parseInt(r);
        return maxrows;
    }

    private String getResult(Connection conn, int id, String sql, boolean allowEdit, boolean forceEdit) {
        try {
            sql = sql.trim();
            StringBuffer buff = new StringBuffer();
            String sqlUpper = StringUtils.toUpperEnglish(sql);
            if (sqlUpper.indexOf("CREATE") >= 0 || sqlUpper.indexOf("DROP") >= 0 || sqlUpper.indexOf("ALTER") >= 0
                    || sqlUpper.indexOf("RUNSCRIPT") >= 0) {
                String sessionId = attributes.getProperty("jsessionid");
                buff.append("<script type=\"text/javascript\">top['h2menu'].location='tables.do?jsessionid="
                        + sessionId + "';</script>");
            }
            Statement stat;
            DbContents contents = session.getContents();
            if (forceEdit || (allowEdit && contents.isH2)) {
                stat = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            } else {
                stat = conn.createStatement();
            }
            ResultSet rs;
            long time = System.currentTimeMillis();
            boolean metadata = false;
            boolean generatedKeys = false;
            boolean edit = false;
            boolean list = false;
            if ("@CANCEL".equals(sql)) {
                stat = session.executingStatement;
                if (stat != null) {
                    stat.cancel();
                    buff.append("${text.result.statementWasCancelled}");
                } else {
                    buff.append("${text.result.noRunningStatement}");
                }
                return buff.toString();
            } else if (sql.startsWith("@META")) {
                metadata = true;
                sql = sql.substring("@META".length()).trim();
            } else if (sql.startsWith("@LIST")) {
                list = true;
                sql = sql.substring("@LIST".length()).trim();
            } else if (sql.startsWith("@GENERATED")) {
                generatedKeys = true;
                sql = sql.substring("@GENERATED".length()).trim();
            } else if (sql.startsWith("@LOOP")) {
                metadata = true;
                sql = sql.substring("@LOOP".length()).trim();
                int idx = sql.indexOf(' ');
                int count = MathUtils.decodeInt(sql.substring(0, idx));
                sql = sql.substring(idx).trim();
                return executeLoop(conn, count, sql);
            } else if (sql.startsWith("@EDIT")) {
                edit = true;
                sql = sql.substring("@EDIT".length()).trim();
                session.put("resultSetSQL", sql);
            } else if ("@HISTORY".equals(sql)) {
                buff.append(getHistoryString());
                return buff.toString();
            }
            if (sql.startsWith("@")) {
                rs = getMetaResultSet(conn, sql);
                if (rs == null) {
                    buff.append("?: " + sql);
                    return buff.toString();
                }
            } else {
                int maxrows = getMaxrows();
                stat.setMaxRows(maxrows);
                session.executingStatement = stat;
                boolean isResultSet = stat.execute(sql);
                session.addCommand(sql);
                if (generatedKeys) {
                    rs = null;
//#ifdef JDK14
                    rs = stat.getGeneratedKeys();
//#endif
                } else {
                    if (!isResultSet) {
                        buff.append("${text.result.updateCount}: " + stat.getUpdateCount());
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
            buff.append(getResultSet(sql, rs, metadata, list, edit, time, allowEdit));
//            SQLWarning warning = stat.getWarnings();
//            if(warning != null) {
//                buff.append("<br />Warning:<br />");
//                buff.append(getStackTrace(id, warning));
//            }
            if (!edit) {
                stat.close();
            }
            return buff.toString();
        } catch (Throwable e) {
            // throwable: including OutOfMemoryError and so on
            return getStackTrace(id, e);
        } finally {
            session.executingStatement = null;
        }
    }

    private String executeLoop(Connection conn, int count, String sql) throws SQLException {
        ArrayList params = new ArrayList();
        int idx = 0;
        while (!stop) {
            idx = sql.indexOf('?', idx);
            if (idx < 0) {
                break;
            }
            if (sql.substring(idx).startsWith("?/*RND*/")) {
                params.add(ObjectUtils.getInteger(1));
                sql = sql.substring(0, idx) + "?" + sql.substring(idx + "/*RND*/".length() + 1);
            } else {
                params.add(ObjectUtils.getInteger(0));
            }
            idx++;
        }
        int rows = 0;
        boolean prepared;
        Random random = new Random(1);
        long time = System.currentTimeMillis();
        if (sql.startsWith("@STATEMENT")) {
            sql = sql.substring("@STATEMENT".length()).trim();
            prepared = false;
            Statement stat = conn.createStatement();
            for (int i = 0; !stop && i < count; i++) {
                String s = sql;
                for (int j = 0; j < params.size(); j++) {
                    idx = s.indexOf('?');
                    Integer type = (Integer) params.get(j);
                    if (type.intValue() == 1) {
                        s = s.substring(0, idx) + random.nextInt(count) + s.substring(idx + 1);
                    } else {
                        s = s.substring(0, idx) + i + s.substring(idx + 1);
                    }
                }
                if (stat.execute(s)) {
                    ResultSet rs = stat.getResultSet();
                    while (!stop && rs.next()) {
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
            for (int i = 0; !stop && i < count; i++) {
                for (int j = 0; j < params.size(); j++) {
                    Integer type = (Integer) params.get(j);
                    if (type.intValue() == 1) {
                        prep.setInt(j + 1, random.nextInt(count));
                    } else {
                        prep.setInt(j + 1, i);
                    }
                }
                if (session.getContents().isSQLite) {
                    // SQLite currently throws an exception on prep.execute()
                    prep.executeUpdate();
                } else {
                    if (prep.execute()) {
                        ResultSet rs = prep.getResultSet();
                        while (!stop && rs.next()) {
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
        if (prepared) {
            result += "(Prepared) ";
        } else {
            result += "(Statement) ";
        }
        result += "(";
        StringBuffer buff = new StringBuffer();
        for (int i = 0; i < params.size(); i++) {
            if (i > 0) {
                buff.append(", ");
            }
            buff.append(((Integer) params.get(i)).intValue() == 0 ? "i" : "rnd");
        }
        result += buff.toString();
        result += ") " + sql;
        return result;
    }

    private String getHistoryString() {
        StringBuffer buff = new StringBuffer();
        ArrayList history = session.getCommands();
        buff.append("<table cellspacing=0 cellpadding=0>");
        buff.append("<tr><th></th><th>Command</th></tr>");
        for (int i = history.size() - 1; i >= 0; i--) {
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

    private String getResultSet(String sql, ResultSet rs, boolean metadata, boolean list, boolean edit, long time, boolean allowEdit) throws SQLException {
        int maxrows = getMaxrows();
        time = System.currentTimeMillis() - time;
        StringBuffer buff = new StringBuffer();
        if (edit) {
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
        int rows = 0;
        if (metadata) {
            buff.append("<tr><th>i</th><th>label</th><th>cat</th><th>schem</th>");
            buff.append("<th>tab</th><th>col</th><th>type</th><th>typeName</th><th>class</th>");
            buff.append("<th>prec</th><th>scale</th><th>size</th><th>autoInc</th>");
            buff.append("<th>case</th><th>currency</th><th>null</th><th>ro</th>");
            buff.append("<th>search</th><th>sig</th><th>w</th><th>defW</th></tr>");
            for (int i = 1; i <= columns; i++) {
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
        } else if (list) {
            buff.append("<tr><th>Column</th><th>Data</th></tr><tr>");
            while (rs.next()) {
                if (maxrows > 0 && rows >= maxrows) {
                    break;
                }
                rows++;
                buff.append("<tr><td>Row #</td><td>");
                buff.append(rows);
                buff.append("</tr>");
                for (int i = 0; i < columns; i++) {
                    buff.append("<tr><td>");
                    buff.append(PageParser.escapeHtml(meta.getColumnLabel(i + 1)));
                    buff.append("</td>");
                    buff.append("<td>");
                    buff.append(escapeData(rs.getString(i + 1)));
                    buff.append("</td></tr>");
                }
            }
        } else {
            buff.append("<tr>");
            if (edit) {
                buff.append("<th>Action</th>");
            }
            for (int i = 0; i < columns; i++) {
                buff.append("<th>");
                buff.append(PageParser.escapeHtml(meta.getColumnLabel(i + 1)));
                buff.append("</th>");
            }
            buff.append("</tr>");
            while (rs.next()) {
                if (maxrows > 0 && rows >= maxrows) {
                    break;
                }
                rows++;
                buff.append("<tr>");
                if (edit) {
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
                for (int i = 0; i < columns; i++) {
                    buff.append("<td>");
                    buff.append(escapeData(rs.getString(i + 1)));
                    buff.append("</td>");
                }
                buff.append("</tr>");
            }
        }
        boolean isUpdatable = rs.getConcurrency() == ResultSet.CONCUR_UPDATABLE
                && rs.getType() != ResultSet.TYPE_FORWARD_ONLY;
        if (edit) {
            ResultSet old = session.result;
            if (old != null) {
                old.close();
            }
            session.result = rs;
        } else {
            rs.close();
        }
        if (edit) {
            buff.append("<tr><td>");
            buff
                    .append("<img onclick=\"javascript:editRow(-1, '${sessionId}', '${text.resultEdit.save}', '${text.resultEdit.cancel}'");
            buff
                    .append(")\" width=16 height=16 src=\"ico_add.gif\" onmouseover = \"this.className ='icon_hover'\" onmouseout = \"this.className ='icon'\" class=\"icon\" alt=\"${text.resultEdit.add}\" title=\"${text.resultEdit.add}\" border=\"1\"/>");
            buff.append("</td>");
            for (int i = 0; i < columns; i++) {
                buff.append("<td></td>");
            }
            buff.append("</tr>");
        }
        buff.append("</table>");
        if (edit) {
            buff.append("</form>");
        }
        if (rows == 0) {
            buff.append("(${text.result.noRows}");
        } else if (rows == 1) {
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
        if (!edit && isUpdatable && allowEdit) {
            buff
                    .append("<br /><br /><form name=\"editResult\" method=\"post\" action=\"query.do?jsessionid=${sessionId}\" target=\"h2result\">");
            buff.append("<input type=\"submit\" class=\"button\" value=\"${text.resultEdit.editResult}\" />");
            buff.append("<input type=\"hidden\" name=\"sql\" value=\"@EDIT " + PageParser.escapeHtml(sql) + "\" />");
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
        server.updateSetting(info);
        attributes.put("setting", info.name);
        server.saveSettings();
        return "index.do";
    }

    private String escapeData(String d) {
        if (d == null) {
            return "<i>null</i>";
        } else if (d.startsWith("null")) {
            return "<div style='display: none'>=</div>" + PageParser.escapeHtml(d);
        }
        return PageParser.escapeHtml(d);
    }

    private String unescapeData(String d) {
        if (d.endsWith("null")) {
            if (d.equals("null")) {
                return null;
            } else if (d.startsWith("=")) {
                return d.substring(1);
            }
        }
        return d;
    }

    private String settingRemove() {
        String setting = attributes.getProperty("name", "");
        server.removeSetting(setting);
        ArrayList settings = server.getSettings();
        if (settings.size() > 0) {
            attributes.put("setting", settings.get(0));
        }
        server.saveSettings();
        return "index.do";
    }

    boolean allow() {
        if (server.getAllowOthers()) {
            return true;
        }
        return NetUtils.isLoopbackAddress(socket);
    }

    public String getMimeType() {
        return mimeType;
    }

    public boolean getCache() {
        return cache;
    }

    public WebSession getSession() {
        return session;
    }

    public void closingDatabase() {
        log("Closing database");
    }

    public void diskSpaceIsLow(long stillAvailable) throws SQLException {
        log("Disk space is low; still available: " + stillAvailable);
    }

    public void exceptionThrown(SQLException e, String sql) {
        log("Exception: " + e.toString() + " SQL: " + sql);
    }

    public void init(String url) {
        log("Init: " + url);
    }

    public void opened() {
        log("Database was opened");
    }

    public void setProgress(int state, String name, int x, int max) {
        if (state == listenerLastState) {
            long time = System.currentTimeMillis();
            if (listenerLastEvent + 500 < time) {
                return;
            }
            listenerLastEvent = time;
        } else {
            listenerLastState = state;
        }
        switch(state) {
        case DatabaseEventListener.STATE_BACKUP_FILE:
            log("Backing up " + name + " " + (100L * x / max) + "%");
            break;
        case DatabaseEventListener.STATE_CREATE_INDEX:
            log("Creating index " + name + " " + (100L * x / max) + "%");
            break;
        case DatabaseEventListener.STATE_RECOVER:
            log("Recovering " + name + " " + (100L * x / max) + "%");
            break;
        case DatabaseEventListener.STATE_SCAN_FILE:
            log("Scanning file " + name + " " + (100L * x / max) + "%");
            break;
        default:
            log("Unknown state: " + state);
        }
    }

    private void log(String s) {
        // System.out.println(s);
    }

}
