/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.server.web;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.SecureClassLoader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ParameterMetaData;
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
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.StringTokenizer;

import org.h2.api.DatabaseEventListener;
import org.h2.bnf.Bnf;
import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.engine.Constants;
import org.h2.jdbc.JdbcSQLException;
import org.h2.message.Message;
import org.h2.message.TraceSystem;
import org.h2.tools.Backup;
import org.h2.tools.ChangeFileEncryption;
import org.h2.tools.ConvertTraceFile;
import org.h2.tools.CreateCluster;
import org.h2.tools.DeleteDbFiles;
import org.h2.tools.Recover;
import org.h2.tools.Restore;
import org.h2.tools.RunScript;
import org.h2.tools.Script;
import org.h2.tools.SimpleResultSet;
import org.h2.util.IOUtils;
import org.h2.util.JdbcUtils;
import org.h2.util.MathUtils;
import org.h2.util.MemoryUtils;
import org.h2.util.NetUtils;
import org.h2.util.New;
import org.h2.util.ObjectArray;
import org.h2.util.ScriptReader;
import org.h2.util.SortedProperties;
import org.h2.util.StatementBuilder;
import org.h2.util.StringUtils;
import org.h2.util.Tool;

/**
 * For each connection to a session, an object of this class is created.
 * This class is used by the H2 Console.
 */
class WebThread extends Thread implements DatabaseEventListener {

    WebSession session;
    OutputStream output;
    String mimeType;
    long listenerLastEvent;
    int listenerLastState;
    Socket socket;
    WebServer server;

    private Properties attributes;
    private InputStream input;
    private String ifModifiedSince;
    private boolean cache;
    private boolean stop;
    private String headerLanguage;

    WebThread(Socket socket, WebServer server) {
        this.server = server;
        this.socket = socket;
        setName("H2 Console thread");
    }

    /**
     * Set the web session and attributes.
     *
     * @param session the session
     * @param attributes the attributes
     */
    void setSession(WebSession session, Properties attributes) {
        this.session = session;
        this.attributes = attributes;
    }

    /**
     * Close the connection now.
     */
    void stopNow() {
        this.stop = true;
        try {
            socket.close();
        } catch (IOException e) {
            // ignore
        }
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

    /**
     * Process an HTTP request.
     *
     * @param file the file that was requested
     * @param the host address
     * @return the name of the file to return to the client
     */
    String processRequest(String file, String hostAddr) {
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
                session = server.createNewSession(hostAddr);
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
            trace("Unknown mime type, file " + file);
        }
        trace("mimeType=" + mimeType);
        trace(file);
        if (file.endsWith(".do")) {
            file = process(file);
        }
        return file;
    }

    public void run() {
        try {
            input = new BufferedInputStream(socket.getInputStream());
            output = new BufferedOutputStream(socket.getOutputStream());
            while (!stop) {
                if (!process()) {
                    break;
                }
            }
        } catch (IOException e) {
            TraceSystem.traceThrowable(e);
        }
        IOUtils.closeSilently(output);
        IOUtils.closeSilently(input);
        try {
            socket.close();
        } catch (IOException e) {
            // ignore
        } finally {
            server.remove(this);
        }
    }

    private boolean process() throws IOException {
        boolean keepAlive = false;
        String head = readHeaderLine();
        if (head.startsWith("GET ") || head.startsWith("POST ")) {
            int begin = head.indexOf('/'), end = head.lastIndexOf(' ');
            String file;
            if (begin < 0 || end < begin) {
                file = "";
            } else {
                file = head.substring(begin + 1, end).trim();
            }
            trace(head + ": " + file);
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
            keepAlive = parseHeader();
            String hostAddr = socket.getInetAddress().getHostAddress();
            file = processRequest(file, hostAddr);
            if (file.length() == 0) {
                // asynchronous request
                return true;
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
                        page = PageParser.parse(page, session.map);
                        bytes = StringUtils.utf8Encode(page);
                    }
                    message = "HTTP/1.1 200 OK\n";
                    message += "Content-Type: " + mimeType + "\n";
                    if (!cache) {
                        message += "Cache-Control: no-cache\n";
                    } else {
                        message += "Cache-Control: max-age=10\n";
                        message += "Last-Modified: " + server.getStartDateTime() + "\n";
                    }
                    message += "Content-Length: " + bytes.length + "\n";
                }
            }
            message += "\n";
            trace(message);
            output.write(message.getBytes());
            if (bytes != null) {
                output.write(bytes);
            }
            output.flush();
        }
        return keepAlive;
    }

    private String getComboBox(String[] elements, String selected) {
        StringBuilder buff = new StringBuilder();
        for (String value : elements) {
            buff.append("<option value=\"").
                append(PageParser.escapeHtmlData(value)).
                append("\"");
            if (value.equals(selected)) {
                buff.append(" selected");
            }
            buff.append('>').
                append(PageParser.escapeHtml(value)).
                append("</option>");
        }
        return buff.toString();
    }

    private String getComboBox(String[][] elements, String selected) {
        StringBuilder buff = new StringBuilder();
        for (String[] n : elements) {
            buff.append("<option value=\"").
                append(PageParser.escapeHtmlData(n[0])).
                append("\"");
            if (n[0].equals(selected)) {
                buff.append(" selected");
            }
            buff.append('>').
                append(PageParser.escapeHtml(n[1])).
                append("</option>");
        }
        return buff.toString();
    }

    private String readHeaderLine() throws IOException {
        StringBuilder buff = new StringBuilder();
        while (true) {
            int i = input.read();
            if (i == -1) {
                throw new IOException("Unexpected EOF");
            } else if (i == '\r' && input.read() == '\n' || i == '\n') {
                return buff.length() > 0 ? buff.toString() : null;
            } else {
                buff.append((char) i);
            }
        }
    }

    private void parseAttributes(String s) {
        trace("data=" + s);
        while (s != null) {
            int idx = s.indexOf('=');
            if (idx >= 0) {
                String property = s.substring(0, idx);
                s = s.substring(idx + 1);
                idx = s.indexOf('&');
                String value;
                if (idx >= 0) {
                    value = s.substring(0, idx);
                    s = s.substring(idx + 1);
                } else {
                    value = s;
                }
                String attr = StringUtils.urlDecode(value);
                attributes.put(property, attr);
            } else {
                break;
            }
        }
        trace(attributes.toString());
    }

    private boolean parseHeader() throws IOException {
        boolean keepAlive = false;
        trace("parseHeader");
        int len = 0;
        ifModifiedSince = null;
        while (true) {
            String line = readHeaderLine();
            if (line == null) {
                break;
            }
            trace(" " + line);
            String lower = StringUtils.toLowerEnglish(line);
            if (lower.startsWith("if-modified-since")) {
                ifModifiedSince = line.substring(line.indexOf(':') + 1).trim();
            } else if (lower.startsWith("connection")) {
                String conn = line.substring(line.indexOf(':') + 1).trim();
                if ("keep-alive".equals(conn)) {
                    keepAlive = true;
                }
            } else if (lower.startsWith("content-length")) {
                len = Integer.parseInt(line.substring(line.indexOf(':') + 1).trim());
                trace("len=" + len);
            } else if (lower.startsWith("accept-language")) {
                Locale locale = session == null ? null : session.locale;
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
                                headerLanguage = locale.getLanguage();
                                if (session != null) {
                                    session.locale = locale;
                                    session.put("language", headerLanguage);
                                    server.readTranslations(session, headerLanguage);
                                }
                                break;
                            }
                        }
                    }
                }
            } else if (line.trim().length() == 0) {
                break;
            }
        }
        if (session != null && len > 0) {
            byte[] bytes = MemoryUtils.newBytes(len);
            for (int pos = 0; pos < len;) {
                pos += input.read(bytes, pos, len - pos);
            }
            String s = new String(bytes);
            parseAttributes(s);
        }
        return keepAlive;
    }

    private String process(String file) {
        trace("process " + file);
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
            } else if ("adminStartTranslate.do".equals(file)) {
                file = adminStartTranslate();
            } else if ("adminShutdown.do".equals(file)) {
                file = adminShutdown();
            } else if ("autoCompleteList.do".equals(file)) {
                file = autoCompleteList();
            } else if ("tools.do".equals(file)) {
                file = tools();
            } else {
                file = "error.jsp";
            }
        }
        trace("return " + file);
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
                sql = lastSql;
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
                HashMap<String, String> map = bnf.getNextTokenList(sql);
                String space = "";
                if (sql.length() > 0) {
                    char last = sql.charAt(sql.length() - 1);
                    if (!Character.isWhitespace(last) && (last != '.' && last >= ' ' && last != '\'' && last != '"')) {
                        space = " ";
                    }
                }
                ArrayList<String> list = New.arrayList(map.size());
                for (Map.Entry<String, String> entry : map.entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();
                    String type = "" + key.charAt(0);
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
                if (query.endsWith("\n") || query.trim().endsWith(";")) {
                    list.add(0, "1#(Newline)#\n");
                }
                StatementBuilder buff = new StatementBuilder();
                for (String s : list) {
                    buff.appendExceptFirst("|");
                    buff.append(s);
                }
                result = buff.toString();
            }
            session.put("autoCompleteList", result);
        } catch (Throwable e) {
            server.traceError(e);
        }
        return "autoCompleteList.jsp";
    }

    private String admin() {
        session.put("port", "" + server.getPort());
        session.put("allowOthers", "" + server.getAllowOthers());
        session.put("ssl", String.valueOf(server.getSSL()));
        session.put("sessions", server.getSessions());
        return "admin.jsp";
    }

    private String adminSave() {
        try {
            Properties prop = new SortedProperties();
            int port = MathUtils.decodeInt((String) attributes.get("port"));
            prop.setProperty("webPort", String.valueOf(port));
            server.setPort(port);
            boolean allowOthers = Boolean.valueOf((String) attributes.get("allowOthers")).booleanValue();
            prop.setProperty("webAllowOthers", String.valueOf(allowOthers));
            server.setAllowOthers(allowOthers);
            boolean ssl = Boolean.valueOf((String) attributes.get("ssl")).booleanValue();
            prop.setProperty("webSSL", String.valueOf(ssl));
            server.setSSL(ssl);
            server.saveSettings(prop);
        } catch (Exception e) {
            trace(e.toString());
        }
        return admin();
    }

    private String tools() {
        try {
            String toolName = (String) attributes.get("tool");
            session.put("tool", toolName);
            String args = (String) attributes.get("args");
            String[] argList = StringUtils.arraySplit(args, ',', false);
            Tool tool = null;
            if ("Backup".equals(toolName)) {
                tool = new Backup();
            } else if ("Restore".equals(toolName)) {
                tool = new Restore();
            } else if ("Recover".equals(toolName)) {
                tool = new Recover();
            } else if ("DeleteDbFiles".equals(toolName)) {
                tool = new DeleteDbFiles();
            } else if ("ChangeFileEncryption".equals(toolName)) {
                tool = new ChangeFileEncryption();
            } else if ("Script".equals(toolName)) {
                tool = new Script();
            } else if ("RunScript".equals(toolName)) {
                tool = new RunScript();
            } else if ("ConvertTraceFile".equals(toolName)) {
                tool = new ConvertTraceFile();
            } else if ("CreateCluster".equals(toolName)) {
                tool = new CreateCluster();
            } else {
                throw Message.throwInternalError(toolName);
            }
            ByteArrayOutputStream outBuff = new ByteArrayOutputStream();
            PrintStream out = new PrintStream(outBuff, false, "UTF-8");
            tool.setOut(out);
            try {
                tool.run(argList);
                out.flush();
                String o = new String(outBuff.toByteArray(), "UTF-8");
                String result = PageParser.escapeHtml(o);
                session.put("toolResult", result);
            } catch (Exception e) {
                session.put("toolResult", getStackTrace(0, e, true));
            }
        } catch (Exception e) {
            server.traceError(e);
        }
        return "tools.jsp";
    }

    @SuppressWarnings("unchecked")
    private String adminStartTranslate() {
        Map<Object, Object> p = (Map<Object, Object>) session.map.get("text");
        String file = server.startTranslate(p);
        session.put("translationFile", file);
        return "helpTranslate.jsp";
    }

    private String adminShutdown() {
        stopNow();
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
        if (language == null) {
            // if the language is not yet known
            // use the last header
            language = headerLanguage;
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
        session.put("setting", PageParser.escapeHtmlData(setting));
        session.put("name", PageParser.escapeHtmlData(setting));
        session.put("driver", PageParser.escapeHtmlData(info.driver));
        session.put("url", PageParser.escapeHtmlData(info.url));
        session.put("user", PageParser.escapeHtmlData(info.user));
        return "index.jsp";
    }

    private String getHistory() {
        int id = Integer.parseInt(attributes.getProperty("id"));
        String sql = session.getCommand(id);
        session.put("query", PageParser.escapeHtmlData(sql));
        return "query.jsp";
    }

    private int addColumns(boolean mainSchema, DbTableOrView table, StringBuilder buff, int treeIndex, boolean showColumnTypes,
            StringBuilder columnsBuffer) {
        DbColumn[] columns = table.columns;
        for (int i = 0; columns != null && i < columns.length; i++) {
            DbColumn column = columns[i];
            if (columnsBuffer.length() > 0) {
                columnsBuffer.append(' ');
            }
            columnsBuffer.append(column.name);
            String col = StringUtils.urlEncode(PageParser.escapeJavaScript(column.name));
            String level = mainSchema ? ", 1, 1" : ", 2, 2";
            buff.append("setNode(" + treeIndex + level + ", 'column', '" + PageParser.escapeJavaScript(column.name)
                    + "', 'javascript:ins(\\'" + col + "\\')');\n");
            treeIndex++;
            if (mainSchema && showColumnTypes) {
                buff.append("setNode(" + treeIndex + ", 2, 2, 'type', '" + PageParser.escapeJavaScript(column.dataType)
                        + "', null);\n");
                treeIndex++;
            }
        }
        return treeIndex;
    }

    /**
     * This class represents index information for the GUI.
     */
    static class IndexInfo {

        /**
         * The index name.
         */
        String name;

        /**
         * The index type name.
         */
        String type;

        /**
         * The indexed columns.
         */
        String columns;
    }

    private int addIndexes(boolean mainSchema, DatabaseMetaData meta, String table, String schema, StringBuilder buff, int treeIndex)
            throws SQLException {
        ResultSet rs = meta.getIndexInfo(null, schema, table, false, true);
        HashMap<String, IndexInfo> indexMap = New.hashMap();
        while (rs.next()) {
            String name = rs.getString("INDEX_NAME");
            IndexInfo info = indexMap.get(name);
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
            String level = mainSchema ? ", 1, 1" : ", 2, 1";
            String levelIndex = mainSchema ? ", 2, 1" : ", 3, 1";
            String levelColumnType = mainSchema ? ", 3, 2" : ", 4, 2";
            buff.append("setNode(" + treeIndex + level + ", 'index_az', '${text.tree.indexes}', null);\n");
            treeIndex++;
            for (IndexInfo info : indexMap.values()) {
                buff.append("setNode(" + treeIndex + levelIndex + ", 'index', '" + PageParser.escapeJavaScript(info.name)
                        + "', null);\n");
                treeIndex++;
                buff.append("setNode(" + treeIndex + levelColumnType + ", 'type', '" + info.type + "', null);\n");
                treeIndex++;
                buff.append("setNode(" + treeIndex + levelColumnType + ", 'type', '" + PageParser.escapeJavaScript(info.columns)
                        + "', null);\n");
                treeIndex++;
            }
        }
        return treeIndex;
    }

    private int addTablesAndViews(DbSchema schema, boolean mainSchema, StringBuilder buff, int treeIndex)
            throws SQLException {
        if (schema == null) {
            return treeIndex;
        }
        Connection conn = session.getConnection();
        DatabaseMetaData meta = session.getMetaData();
        int level = mainSchema ? 0 : 1;
        boolean showColumns = mainSchema || !schema.isSystem;
        String indentation = ", " + level + ", " + (showColumns ? "1" : "2") + ", ";
        String indentNode = ", " + (level + 1) + ", 2, ";
        DbTableOrView[] tables = schema.tables;
        if (tables == null) {
            return treeIndex;
        }
        boolean isOracle = schema.contents.isOracle;
        boolean notManyTables = tables.length < DbSchema.MAX_TABLES_LIST_INDEXES;
        for (DbTableOrView table : tables) {
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
            if (mainSchema || showColumns) {
                StringBuilder columnsBuffer = new StringBuilder();
                treeIndex = addColumns(mainSchema, table, buff, treeIndex, notManyTables, columnsBuffer);
                if (!isOracle && notManyTables) {
                    treeIndex = addIndexes(mainSchema, meta, table.name, schema.name, buff, treeIndex);
                }
                buff.append("addTable('" + PageParser.escapeJavaScript(table.name) + "', '"
                        + PageParser.escapeJavaScript(columnsBuffer.toString()) + "', " + tableId + ");\n");
            }
        }
        tables = schema.tables;
        for (DbTableOrView view : tables) {
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
                StringBuilder columnsBuffer = new StringBuilder();
                treeIndex = addColumns(mainSchema, view, buff, treeIndex, notManyTables, columnsBuffer);
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
        boolean isH2 = false;
        try {
            contents.readContents(session.getMetaData());
            session.loadBnf();
            Connection conn = session.getConnection();
            DatabaseMetaData meta = session.getMetaData();
            isH2 = contents.isH2;

            StringBuilder buff = new StringBuilder();
            buff.append("setNode(0, 0, 0, 'database', '" + PageParser.escapeJavaScript((String) session.get("url"))
                    + "', null);\n");
            int treeIndex = 1;

            DbSchema defaultSchema = contents.defaultSchema;
            treeIndex = addTablesAndViews(defaultSchema, true, buff, treeIndex);
            DbSchema[] schemas = contents.schemas;
            for (DbSchema schema : schemas) {
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
            session.put("error", getStackTrace(0, e, isH2));
        }
        return "tables.jsp";
    }

    private String getStackTrace(int id, Throwable e, boolean isH2) {
        try {
            StringWriter writer = new StringWriter();
            e.printStackTrace(new PrintWriter(writer));
            String stackTrace = writer.toString();
            stackTrace = PageParser.escapeHtml(stackTrace);
            if (isH2) {
                stackTrace = linkToSource(stackTrace);
            }
            stackTrace = StringUtils.replaceAll(stackTrace, "\t", "&nbsp;&nbsp;&nbsp;&nbsp;");
            String message = PageParser.escapeHtml(e.getMessage());
            String error = "<a class=\"error\" href=\"#\" onclick=\"var x=document.getElementById('st" + id
                    + "').style;x.display=x.display==''?'none':'';\">" + message + "</a>";
            if (e instanceof SQLException) {
                SQLException se = (SQLException) e;
                error += " " + se.getSQLState() + "/" + se.getErrorCode();
                if (isH2) {
                    int code = se.getErrorCode();
                    error += " <a href=\"http://h2database.com/javadoc/org/h2/constant/ErrorCode.html#c" + code
                            + "\">(${text.a.help})</a>";
                }
            }
            error += "<span style=\"display: none;\" id=\"st" + id + "\"><br />" + stackTrace + "</span>";
            error = formatAsError(error);
            return error;
        } catch (OutOfMemoryError e2) {
            server.traceError(e);
            return e.toString();
        }
    }

    private String linkToSource(String s) {
        try {
            StringBuilder result = new StringBuilder(s.length());
            int idx = s.indexOf("<br />");
            result.append(s.substring(0, idx));
            while (true) {
                int start = s.indexOf("org.h2.", idx);
                if (start < 0) {
                    result.append(s.substring(idx));
                    break;
                }
                result.append(s.substring(idx, start));
                int end = s.indexOf(')', start);
                if (end < 0) {
                    result.append(s.substring(idx));
                    break;
                }
                String element = s.substring(start, end);
                int open = element.lastIndexOf('(');
                int dotMethod = element.lastIndexOf('.', open - 1);
                int dotClass = element.lastIndexOf('.', dotMethod - 1);
                String packageName = element.substring(0, dotClass);
                int colon = element.lastIndexOf(':');
                String file = element.substring(open + 1, colon);
                String lineNumber = element.substring(colon + 1, element.length());
                String fullFileName = packageName.replace('.', '/') + "/" + file;
                result.append("<a href=\"http://h2database.com/html/source.html?file=");
                result.append(fullFileName);
                result.append("&line=");
                result.append(lineNumber);
                result.append("&build=");
                result.append(Constants.BUILD_ID);
                result.append("\">");
                result.append(element);
                result.append("</a>");
                idx = end;
            }
            return result.toString();
        } catch (Throwable t) {
            return s;
        }
    }

    private String formatAsError(String s) {
        return "<div class=\"error\">" + s + "</div>";
    }

    private String test() {
        String driver = attributes.getProperty("driver", "");
        String url = attributes.getProperty("url", "");
        String user = attributes.getProperty("user", "");
        String password = attributes.getProperty("password", "");
        session.put("driver", driver);
        session.put("url", url);
        session.put("user", user);
        boolean isH2 = url.startsWith("jdbc:h2:");
        try {
            Connection conn = server.getConnection(driver, url, user, password, this);
            JdbcUtils.closeSilently(conn);
            session.put("error", "${text.login.testSuccessful}");
            return "login.jsp";
        } catch (Exception e) {
            session.put("error", getLoginError(e, isH2));
            return "login.jsp";
        }
    }

    /**
     * Get the formatted login error message.
     *
     * @param e the exception
     * @param isH2 if the current database is a H2 database
     * @return the formatted error message
     */
    String getLoginError(Exception e, boolean isH2) {
        if (e instanceof JdbcSQLException && ((JdbcSQLException) e).getErrorCode() == ErrorCode.CLASS_NOT_FOUND_1) {
            return "${text.login.driverNotFound}<br />" + getStackTrace(0, e, isH2);
        }
        return getStackTrace(0, e, isH2);
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
        if (socket != null && url.startsWith("jdbc:h2:") && !url.startsWith("jdbc:h2:tcp:")
                && !url.startsWith("jdbc:h2:ssl:") && !url.startsWith("jdbc:h2:mem:")) {
            thread = true;
        }
        if (!thread) {
            boolean isH2 = url.startsWith("jdbc:h2:");
            try {
                Connection conn = server.getConnection(driver, url, user, password, this);
                session.setConnection(conn);
                session.put("url", url);
                session.put("user", user);
                session.remove("error");
                settingSave();
                return "frame.jsp";
            } catch (Exception e) {
                session.put("error", getLoginError(e, isH2));
                return "login.jsp";
            }
        }

        /**
         * This class is used for the asynchronous login.
         */
        class LoginTask implements Runnable, DatabaseEventListener {
            private final PrintWriter writer;
            private final SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss.SSS");

            LoginTask() throws IOException {
                String message = "HTTP/1.1 200 OK\n";
                message += "Content-Type: " + mimeType + "\n\n";
                output.write(message.getBytes());
                writer = new PrintWriter(output);
                writer.println("<html><head><link rel=\"stylesheet\" type=\"text/css\" href=\"stylesheet.css\" /></head>");
                writer.println("<body><h2>Opening Database</h2>URL: " + PageParser.escapeHtml(url) + "<br />");
                writer.println("User: " + PageParser.escapeHtml(user) + "<br />");
                writer.println("Version: " + Constants.getFullVersion() + "<br /><br />");
                writer.flush();
                log("Start...");
            }

            public void closingDatabase() {
                log("Closing database");
            }

            public void diskSpaceIsLow(long stillAvailable) {
                log("No more disk space is available");
            }

            public void exceptionThrown(SQLException e, String sql) {
                log("Exception: " + PageParser.escapeHtml(e.toString()) + " SQL: " + PageParser.escapeHtml(sql));
                server.traceError(e);
            }

            public void init(String url) {
                log("Init: " + PageParser.escapeHtml(url));
            }

            public void opened() {
                log("Database was opened");
            }

            public void setProgress(int state, String name, int x, int max) {
                if (state == listenerLastState) {
                    long time = System.currentTimeMillis();
                    if (time < listenerLastEvent + 1000) {
                        return;
                    }
                    listenerLastEvent = time;
                } else {
                    listenerLastState = state;
                }
                name = PageParser.escapeHtml(name);
                switch (state) {
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
                server.trace(message);
            }

            public void run() {
                String sessionId = (String) session.get("sessionId");
                boolean isH2 = url.startsWith("jdbc:h2:");
                try {
                    Connection conn = server.getConnection(driver, url, user, password, this);
                    session.setConnection(conn);
                    session.put("url", url);
                    session.put("user", user);
                    session.remove("error");
                    settingSave();
                    log("OK<script type=\"text/javascript\">top.location=\"frame.jsp?jsessionid=" + sessionId
                            + "\"</script></body></htm>");
                    // return "frame.jsp";
                } catch (Exception e) {
                    session.put("error", getLoginError(e, isH2));
                    log("Error<script type=\"text/javascript\">top.location=\"index.jsp?jsessionid=" + sessionId
                            + "\"</script></body></html>");
                    // return "index.jsp";
                }
                synchronized (this) {
                    IOUtils.closeSilently(output);
                    try {
                        socket.close();
                    } catch (IOException e) {
                        // ignore
                    }
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
            session.remove("tool");
            if (conn != null) {
                if (session.getShutdownServerOnDisconnect()) {
                    server.shutdown();
                } else {
                    conn.close();
                }
            }
        } catch (Exception e) {
            trace(e.toString());
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
                        result = getStackTrace(0, t, false);
                    }
                } else {
                    result = "Executing Java code is not allowed, use command line parameter -webScript";
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
                ObjectArray<String> list = ObjectArray.newInstance();
                while (true) {
                    String s = r.readStatement();
                    if (s == null) {
                        break;
                    }
                    list.add(s);
                }
                StringBuilder buff = new StringBuilder();
                for (int i = 0; i < list.size(); i++) {
                    String s = list.get(i);
                    if (!s.startsWith("@")) {
                        buff.append(PageParser.escapeHtml(s + ";")).append("<br />");
                    }
                    buff.append(getResult(conn, i + 1, s, list.size() == 1, false)).append("<br />");
                }
                result = buff.toString();
            }
            session.put("result", result);
        } catch (Throwable e) {
            session.put("result", getStackTrace(0, e, session.getContents().isH2));
        }
        return "result.jsp";
    }

    /**
     * This class allows to load Java code dynamically.
     */
    static class DynamicClassLoader extends SecureClassLoader {

        private String name;
        private byte[] data;
        private Class< ? > clazz;

        DynamicClassLoader(String name, byte[] data) {
            super(DynamicClassLoader.class.getClassLoader());
            this.name = name;
            this.data = data;
        }

        public Class< ? > loadClass(String className) throws ClassNotFoundException {
            return findClass(className);
        }

        public Class< ? > findClass(String className) throws ClassNotFoundException {
            if (className.equals(name)) {
                if (clazz == null) {
                    clazz = defineClass(className, data, 0, data.length);
                }
                return clazz;
            }
            try {
                return findSystemClass(className);
            } catch (Exception e) {
                // ignore
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
            Class< ? > javacClass = Class.forName("com.sun.tools.javac.Main");
            Method compile = javacClass.getMethod("compile", String[].class);
            Object javac = javacClass.newInstance();
            compile.invoke(javac, new Object[] { new String[]{"Java.java"}});
            byte[] data = new byte[(int) classFile.length()];
            DataInputStream in = new DataInputStream(new FileInputStream(classFile));
            in.readFully(data);
            in.close();
            DynamicClassLoader cl = new DynamicClassLoader("Java", data);
            Class< ? > clazz = cl.loadClass("Java");
            Method[] methods = clazz.getMethods();
            for (Method m : methods) {
                if (m.getName().equals("run")) {
                    return "" + m.invoke(null);
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
                    unescapeData(x, rs, i + 1);
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
            result = "<br />" + getStackTrace(0, e, session.getContents().isH2);
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
            rs.addRow(conn.getCatalog());
            return rs;
        } else if (sql.startsWith("@MEMORY")) {
            SimpleResultSet rs = new SimpleResultSet();
            rs.addColumn("Type", Types.VARCHAR, 0, 0);
            rs.addColumn("Value", Types.VARCHAR, 0, 0);
            rs.addRow("Used Memory", "" + MemoryUtils.getMemoryUsed());
            rs.addRow("Free Memory", "" + MemoryUtils.getMemoryFree());
            return rs;
        } else if (sql.startsWith("@INFO")) {
            SimpleResultSet rs = new SimpleResultSet();
            rs.addColumn("KEY", Types.VARCHAR, 0, 0);
            rs.addColumn("VALUE", Types.VARCHAR, 0, 0);
            rs.addRow("conn.getCatalog", conn.getCatalog());
            rs.addRow("conn.getAutoCommit", "" + conn.getAutoCommit());
            rs.addRow("conn.getTransactionIsolation", "" + conn.getTransactionIsolation());
            rs.addRow("conn.getWarnings", "" + conn.getWarnings());
            String map;
            try {
                map = "" + conn.getTypeMap();
            } catch (SQLException e) {
                map = e.toString();
            }
            rs.addRow("conn.getTypeMap", "" + map);
            rs.addRow("conn.isReadOnly", "" + conn.isReadOnly());
            rs.addRow("meta.getCatalogSeparator", "" + meta.getCatalogSeparator());
            rs.addRow("meta.getCatalogTerm", "" + meta.getCatalogTerm());
            rs.addRow("meta.getDatabaseProductName", "" + meta.getDatabaseProductName());
            rs.addRow("meta.getDatabaseProductVersion", "" + meta.getDatabaseProductVersion());
            rs.addRow("meta.getDefaultTransactionIsolation", "" + meta.getDefaultTransactionIsolation());
            rs.addRow("meta.getDriverMajorVersion", "" + meta.getDriverMajorVersion());
            rs.addRow("meta.getDriverMinorVersion", "" + meta.getDriverMinorVersion());
            rs.addRow("meta.getDriverName", "" + meta.getDriverName());
            rs.addRow("meta.getDriverVersion", "" + meta.getDriverVersion());
            rs.addRow("meta.getExtraNameCharacters", "" + meta.getExtraNameCharacters());
            rs.addRow("meta.getIdentifierQuoteString", "" + meta.getIdentifierQuoteString());
            rs.addRow("meta.getMaxBinaryLiteralLength", "" + meta.getMaxBinaryLiteralLength());
            rs.addRow("meta.getMaxCatalogNameLength", "" + meta.getMaxCatalogNameLength());
            rs.addRow("meta.getMaxCharLiteralLength", "" + meta.getMaxCharLiteralLength());
            rs.addRow("meta.getMaxColumnNameLength", "" + meta.getMaxColumnNameLength());
            rs.addRow("meta.getMaxColumnsInGroupBy", "" + meta.getMaxColumnsInGroupBy());
            rs.addRow("meta.getMaxColumnsInIndex", "" + meta.getMaxColumnsInIndex());
            rs.addRow("meta.getMaxColumnsInOrderBy", "" + meta.getMaxColumnsInOrderBy());
            rs.addRow("meta.getMaxColumnsInSelect", "" + meta.getMaxColumnsInSelect());
            rs.addRow("meta.getMaxColumnsInTable", "" + meta.getMaxColumnsInTable());
            rs.addRow("meta.getMaxConnections", "" + meta.getMaxConnections());
            rs.addRow("meta.getMaxCursorNameLength", "" + meta.getMaxCursorNameLength());
            rs.addRow("meta.getMaxIndexLength", "" + meta.getMaxIndexLength());
            rs.addRow("meta.getMaxProcedureNameLength", "" + meta.getMaxProcedureNameLength());
            rs.addRow("meta.getMaxRowSize", "" + meta.getMaxRowSize());
            rs.addRow("meta.getMaxSchemaNameLength", "" + meta.getMaxSchemaNameLength());
            rs.addRow("meta.getMaxStatementLength", "" + meta.getMaxStatementLength());
            rs.addRow("meta.getMaxStatements", "" + meta.getMaxStatements());
            rs.addRow("meta.getMaxTableNameLength", "" + meta.getMaxTableNameLength());
            rs.addRow("meta.getMaxTablesInSelect", "" + meta.getMaxTablesInSelect());
            rs.addRow("meta.getMaxUserNameLength", "" + meta.getMaxUserNameLength());
            rs.addRow("meta.getNumericFunctions", "" + meta.getNumericFunctions());
            rs.addRow("meta.getProcedureTerm", "" + meta.getProcedureTerm());
            rs.addRow("meta.getSchemaTerm", "" + meta.getSchemaTerm());
            rs.addRow("meta.getSearchStringEscape", "" + meta.getSearchStringEscape());
            rs.addRow("meta.getSQLKeywords", "" + meta.getSQLKeywords());
            rs.addRow("meta.getStringFunctions", "" + meta.getStringFunctions());
            rs.addRow("meta.getSystemFunctions", "" + meta.getSystemFunctions());
            rs.addRow("meta.getTimeDateFunctions", "" + meta.getTimeDateFunctions());
            rs.addRow("meta.getURL", "" + meta.getURL());
            rs.addRow("meta.getUserName", "" + meta.getUserName());
            rs.addRow("meta.isCatalogAtStart", "" + meta.isCatalogAtStart());
            rs.addRow("meta.isReadOnly", "" + meta.isReadOnly());
            rs.addRow("meta.allProceduresAreCallable", "" + meta.allProceduresAreCallable());
            rs.addRow("meta.allTablesAreSelectable", "" + meta.allTablesAreSelectable());
            rs.addRow("meta.dataDefinitionCausesTransactionCommit", "" + meta.dataDefinitionCausesTransactionCommit());
            rs.addRow("meta.dataDefinitionIgnoredInTransactions", "" + meta.dataDefinitionIgnoredInTransactions());
            rs.addRow("meta.doesMaxRowSizeIncludeBlobs", "" + meta.doesMaxRowSizeIncludeBlobs());
            rs.addRow("meta.nullPlusNonNullIsNull", "" + meta.nullPlusNonNullIsNull());
            rs.addRow("meta.nullsAreSortedAtEnd", "" + meta.nullsAreSortedAtEnd());
            rs.addRow("meta.nullsAreSortedAtStart", "" + meta.nullsAreSortedAtStart());
            rs.addRow("meta.nullsAreSortedHigh", "" + meta.nullsAreSortedHigh());
            rs.addRow("meta.nullsAreSortedLow", "" + meta.nullsAreSortedLow());
            rs.addRow("meta.storesLowerCaseIdentifiers", "" + meta.storesLowerCaseIdentifiers());
            rs.addRow("meta.storesLowerCaseQuotedIdentifiers", "" + meta.storesLowerCaseQuotedIdentifiers());
            rs.addRow("meta.storesMixedCaseIdentifiers", "" + meta.storesMixedCaseIdentifiers());
            rs.addRow("meta.storesMixedCaseQuotedIdentifiers", "" + meta.storesMixedCaseQuotedIdentifiers());
            rs.addRow("meta.storesUpperCaseIdentifiers", "" + meta.storesUpperCaseIdentifiers());
            rs.addRow("meta.storesUpperCaseQuotedIdentifiers", "" + meta.storesUpperCaseQuotedIdentifiers());
            rs.addRow("meta.supportsAlterTableWithAddColumn", "" + meta.supportsAlterTableWithAddColumn());
            rs.addRow("meta.supportsAlterTableWithDropColumn", "" + meta.supportsAlterTableWithDropColumn());
            rs.addRow("meta.supportsANSI92EntryLevelSQL", "" + meta.supportsANSI92EntryLevelSQL());
            rs.addRow("meta.supportsANSI92FullSQL", "" + meta.supportsANSI92FullSQL());
            rs.addRow("meta.supportsANSI92IntermediateSQL", "" + meta.supportsANSI92IntermediateSQL());
            rs.addRow("meta.supportsBatchUpdates", "" + meta.supportsBatchUpdates());
            rs.addRow("meta.supportsCatalogsInDataManipulation", "" + meta.supportsCatalogsInDataManipulation());
            rs.addRow("meta.supportsCatalogsInIndexDefinitions", "" + meta.supportsCatalogsInIndexDefinitions());
            rs.addRow("meta.supportsCatalogsInPrivilegeDefinitions", "" + meta.supportsCatalogsInPrivilegeDefinitions());
            rs.addRow("meta.supportsCatalogsInProcedureCalls", "" + meta.supportsCatalogsInProcedureCalls());
            rs.addRow("meta.supportsCatalogsInTableDefinitions", "" + meta.supportsCatalogsInTableDefinitions());
            rs.addRow("meta.supportsColumnAliasing", "" + meta.supportsColumnAliasing());
            rs.addRow("meta.supportsConvert", "" + meta.supportsConvert());
            rs.addRow("meta.supportsCoreSQLGrammar", "" + meta.supportsCoreSQLGrammar());
            rs.addRow("meta.supportsCorrelatedSubqueries", "" + meta.supportsCorrelatedSubqueries());
            rs.addRow("meta.supportsDataDefinitionAndDataManipulationTransactions", "" + meta.supportsDataDefinitionAndDataManipulationTransactions());
            rs.addRow("meta.supportsDataManipulationTransactionsOnly", "" + meta.supportsDataManipulationTransactionsOnly());
            rs.addRow("meta.supportsDifferentTableCorrelationNames", "" + meta.supportsDifferentTableCorrelationNames());
            rs.addRow("meta.supportsExpressionsInOrderBy", "" + meta.supportsExpressionsInOrderBy());
            rs.addRow("meta.supportsExtendedSQLGrammar", "" + meta.supportsExtendedSQLGrammar());
            rs.addRow("meta.supportsFullOuterJoins", "" + meta.supportsFullOuterJoins());
            rs.addRow("meta.supportsGroupBy", "" + meta.supportsGroupBy());
            rs.addRow("meta.usesLocalFilePerTable", "" + meta.usesLocalFilePerTable());
            rs.addRow("meta.usesLocalFiles", "" + meta.usesLocalFiles());
//## Java 1.4 begin ##
            rs.addRow("conn.getHoldability", "" + conn.getHoldability());
            rs.addRow("meta.getDatabaseMajorVersion", "" + meta.getDatabaseMajorVersion());
            rs.addRow("meta.getDatabaseMinorVersion", "" + meta.getDatabaseMinorVersion());
            rs.addRow("meta.getJDBCMajorVersion", "" + meta.getJDBCMajorVersion());
            rs.addRow("meta.getJDBCMinorVersion", "" + meta.getJDBCMinorVersion());
            rs.addRow("meta.getResultSetHoldability", "" + meta.getResultSetHoldability());
            rs.addRow("meta.getSQLStateType", "" + meta.getSQLStateType());
            rs.addRow("meta.supportsGetGeneratedKeys", "" + meta.supportsGetGeneratedKeys());
            rs.addRow("meta.locatorsUpdateCopy", "" + meta.locatorsUpdateCopy());
//## Java 1.4 end ##
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
//## Java 1.4 begin ##
        } else if (sql.startsWith("@SUPER_TYPES")) {
            String[] p = split(sql);
            return meta.getSuperTypes(p[1], p[2], p[3]);
        } else if (sql.startsWith("@SUPER_TABLES")) {
            String[] p = split(sql);
            return meta.getSuperTables(p[1], p[2], p[3]);
        } else if (sql.startsWith("@ATTRIBUTES")) {
            String[] p = split(sql);
            return meta.getAttributes(p[1], p[2], p[3], p[4]);
//## Java 1.4 end ##
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
            StringBuilder buff = new StringBuilder();
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
                    buff.append("${text.result.statementWasCanceled}");
                } else {
                    buff.append("${text.result.noRunningStatement}");
                }
                return buff.toString();
            } else if (sql.startsWith("@PARAMETER_META")) {
                sql = sql.substring("@PARAMETER_META".length()).trim();
                PreparedStatement prep = conn.prepareStatement(sql);
                buff.append(getParameterResultSet(prep.getParameterMetaData()));
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
//## Java 1.4 begin ##
                    rs = stat.getGeneratedKeys();
//## Java 1.4 end ##
                } else {
                    if (!isResultSet) {
                        buff.append("${text.result.updateCount}: " + stat.getUpdateCount());
                        time = System.currentTimeMillis() - time;
                        buff.append("<br />(").append(time).append(" ms)");
                        stat.close();
                        return buff.toString();
                    }
                    rs = stat.getResultSet();
                }
            }
            time = System.currentTimeMillis() - time;
            buff.append(getResultSet(sql, rs, metadata, list, edit, time, allowEdit));
            // SQLWarning warning = stat.getWarnings();
            // if(warning != null) {
            // buff.append("<br />Warning:<br />").
            // append(getStackTrace(id, warning));
            // }
            if (!edit) {
                stat.close();
            }
            return buff.toString();
        } catch (Throwable e) {
            // throwable: including OutOfMemoryError and so on
            return getStackTrace(id, e, session.getContents().isH2);
        } finally {
            session.executingStatement = null;
        }
    }

    private String executeLoop(Connection conn, int count, String sql) throws SQLException {
        ArrayList<Integer> params = New.arrayList();
        int idx = 0;
        while (!stop) {
            idx = sql.indexOf('?', idx);
            if (idx < 0) {
                break;
            }
            if (sql.substring(idx).startsWith("?/*RND*/")) {
                params.add(1);
                sql = sql.substring(0, idx) + "?" + sql.substring(idx + "/*RND*/".length() + 1);
            } else {
                params.add(0);
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
                for (Integer type : params) {
                    idx = s.indexOf('?');
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
                    Integer type = params.get(j);
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
        StatementBuilder buff = new StatementBuilder();
        buff.append(time).append(" ms: ").append(count).append(" * ");
        if (prepared) {
            buff.append("(Prepared) ");
        } else {
            buff.append("(Statement) ");
        }
        buff.append('(');
        for (int p : params) {
            buff.appendExceptFirst(", ");
            buff.append(p == 0 ? "i" : "rnd");
        }
        return buff.append(") ").append(sql).toString();
    }

    private String getHistoryString() {
        StringBuilder buff = new StringBuilder();
        ArrayList<String> history = session.getCommands();
        buff.append("<table cellspacing=0 cellpadding=0><tr><th></th><th>Command</th></tr>");
        for (int i = history.size() - 1; i >= 0; i--) {
            String sql = history.get(i);
            buff.append("<tr><td><a href=\"getHistory.do?id=").
                append(i).
                append("&jsessionid=${sessionId}\" target=\"h2query\" >").
                append("<img width=16 height=16 src=\"ico_write.gif\" onmouseover = \"this.className ='icon_hover'\" ").
                append("onmouseout = \"this.className ='icon'\" class=\"icon\" alt=\"${text.resultEdit.edit}\" ").
                append("title=\"${text.resultEdit.edit}\" border=\"1\"/></a>").
                append("</td><td>").
                append(PageParser.escapeHtml(sql)).
                append("</td></tr>");
        }
        buff.append("</table>");
        return buff.toString();
    }

    private String getParameterResultSet(ParameterMetaData meta) throws SQLException {
        StringBuilder buff = new StringBuilder();
        if (meta == null) {
            return "No parameter meta data";
        }
        buff.append("<table cellspacing=0 cellpadding=0>").
            append("<tr><th>className</th><th>mode</th><th>type</th>").
            append("<th>typeName</th><th>precision</th><th>scale</th></tr>");
        for (int i = 0; i < meta.getParameterCount(); i++) {
            buff.append("</tr><td>").
                append(meta.getParameterClassName(i + 1)).
                append("</td><td>").
                append(meta.getParameterMode(i + 1)).
                append("</td><td>").
                append(meta.getParameterType(i + 1)).
                append("</td><td>").
                append(meta.getParameterTypeName(i + 1)).
                append("</td><td>").
                append(meta.getPrecision(i + 1)).
                append("</td><td>").
                append(meta.getScale(i + 1)).
                append("</td></tr>");
        }
        buff.append("</table>");
        return buff.toString();
    }

    private String getResultSet(String sql, ResultSet rs, boolean metadata, boolean list, boolean edit, long time,
            boolean allowEdit) throws SQLException {
        int maxrows = getMaxrows();
        time = System.currentTimeMillis() - time;
        StringBuilder buff = new StringBuilder();
        if (edit) {
            buff.append("<form id=\"editing\" name=\"editing\" method=\"post\" " +
                    "action=\"editResult.do?jsessionid=${sessionId}\" id=\"mainForm\" target=\"h2result\">" +
                    "<input type=\"hidden\" name=\"op\" value=\"1\" />" +
                    "<input type=\"hidden\" name=\"row\" value=\"\" />" +
                    "<table cellspacing=0 cellpadding=0 id=\"editTable\">");
        } else {
            buff.append("<table cellspacing=0 cellpadding=0>");
        }
        ResultSetMetaData meta = rs.getMetaData();
        int columns = meta.getColumnCount();
        int rows = 0;
        if (metadata) {
            buff.append("<tr><th>i</th><th>label</th><th>cat</th><th>schem</th>" +
                "<th>tab</th><th>col</th><th>type</th><th>typeName</th><th>class</th>" +
                "<th>prec</th><th>scale</th><th>size</th><th>autoInc</th>" +
                "<th>case</th><th>currency</th><th>null</th><th>ro</th>" +
                "<th>search</th><th>sig</th><th>w</th><th>defW</th></tr>");
            for (int i = 1; i <= columns; i++) {
                buff.append("<tr>").
                    append("<td>").append(i).append("</td>").
                    append("<td>").append(PageParser.escapeHtml(meta.getColumnLabel(i))).append("</td>").
                    append("<td>").append(PageParser.escapeHtml(meta.getCatalogName(i))).append("</td>").
                    append("<td>").append(PageParser.escapeHtml(meta.getSchemaName(i))).append("</td>").
                    append("<td>").append(PageParser.escapeHtml(meta.getTableName(i))).append("</td>").
                    append("<td>").append(PageParser.escapeHtml(meta.getColumnName(i))).append("</td>").
                    append("<td>").append(meta.getColumnType(i)).append("</td>").
                    append("<td>").append(PageParser.escapeHtml(meta.getColumnTypeName(i))).append("</td>").
                    append("<td>").append(PageParser.escapeHtml(meta.getColumnClassName(i))).append("</td>").
                    append("<td>").append(meta.getPrecision(i)).append("</td>").
                    append("<td>").append(meta.getScale(i)).append("</td>").
                    append("<td>").append(meta.getColumnDisplaySize(i)).append("</td>").
                    append("<td>").append(meta.isAutoIncrement(i)).append("</td>").
                    append("<td>").append(meta.isCaseSensitive(i)).append("</td>").
                    append("<td>").append(meta.isCurrency(i)).append("</td>").
                    append("<td>").append(meta.isNullable(i)).append("</td>").
                    append("<td>").append(meta.isReadOnly(i)).append("</td>").
                    append("<td>").append(meta.isSearchable(i)).append("</td>").
                    append("<td>").append(meta.isSigned(i)).append("</td>").
                    append("<td>").append(meta.isWritable(i)).append("</td>").
                    append("<td>").append(meta.isDefinitelyWritable(i)).append("</td>").
                    append("</tr>");
            }
        } else if (list) {
            buff.append("<tr><th>Column</th><th>Data</th></tr><tr>");
            while (rs.next()) {
                if (maxrows > 0 && rows >= maxrows) {
                    break;
                }
                rows++;
                buff.append("<tr><td>Row #</td><td>").
                    append(rows).append("</tr>");
                for (int i = 0; i < columns; i++) {
                    buff.append("<tr><td>").
                        append(PageParser.escapeHtml(meta.getColumnLabel(i + 1))).
                        append("</td><td>").
                        append(escapeData(rs, i + 1)).
                        append("</td></tr>");
                }
            }
        } else {
            buff.append("<tr>");
            if (edit) {
                buff.append("<th>Action</th>");
            }
            for (int i = 0; i < columns; i++) {
                buff.append("<th>").
                    append(PageParser.escapeHtml(meta.getColumnLabel(i + 1))).
                    append("</th>");
            }
            buff.append("</tr>");
            while (rs.next()) {
                if (maxrows > 0 && rows >= maxrows) {
                    break;
                }
                rows++;
                buff.append("<tr>");
                if (edit) {
                    buff.append("<td>").
                        append("<img onclick=\"javascript:editRow(").
                        append(rs.getRow()).
                        append(",'${sessionId}', '${text.resultEdit.save}', '${text.resultEdit.cancel}'").
                        append(")\" width=16 height=16 src=\"ico_write.gif\" onmouseover = \"this.className ='icon_hover'\" onmouseout = \"this.className ='icon'\" class=\"icon\" alt=\"${text.resultEdit.edit}\" title=\"${text.resultEdit.edit}\" border=\"1\"/>").
                        append("<a href=\"editResult.do?op=2&row=").
                        append(rs.getRow()).
                        append("&jsessionid=${sessionId}\" target=\"h2result\" ><img width=16 height=16 src=\"ico_remove.gif\" onmouseover = \"this.className ='icon_hover'\" onmouseout = \"this.className ='icon'\" class=\"icon\" alt=\"${text.resultEdit.delete}\" title=\"${text.resultEdit.delete}\" border=\"1\" /></a>").
                        append("</td>");
                }
                for (int i = 0; i < columns; i++) {
                    buff.append("<td>").
                        append(escapeData(rs, i + 1)).
                        append("</td>");
                }
                buff.append("</tr>");
            }
        }
        boolean isUpdatable = false;
        try {
            isUpdatable = rs.getConcurrency() == ResultSet.CONCUR_UPDATABLE
                && rs.getType() != ResultSet.TYPE_FORWARD_ONLY;
        } catch (NullPointerException e) {
            // ignore
            // workaround for a JDBC-ODBC bridge problem
        }
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
            buff.append("<tr><td>").
                append("<img onclick=\"javascript:editRow(-1, '${sessionId}', '${text.resultEdit.save}', '${text.resultEdit.cancel}'").
                append(")\" width=16 height=16 src=\"ico_add.gif\" onmouseover = \"this.className ='icon_hover'\" onmouseout = \"this.className ='icon'\" class=\"icon\" alt=\"${text.resultEdit.add}\" title=\"${text.resultEdit.add}\" border=\"1\"/>").
                append("</td>");
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
            buff.append('(').append(rows).append(" ${text.result.rows}");
        }
        buff.append(", ");
        time = System.currentTimeMillis() - time;
        buff.append(time).append(" ms)");
        if (!edit && isUpdatable && allowEdit) {
            buff.append("<br /><br /><form name=\"editResult\" method=\"post\" action=\"query.do?jsessionid=${sessionId}\" target=\"h2result\">" +
                "<input type=\"submit\" class=\"button\" value=\"${text.resultEdit.editResult}\" />" +
                "<input type=\"hidden\" name=\"sql\" value=\"@EDIT ").
                append(PageParser.escapeHtml(sql)).
                append("\" /></form>");
        }
        return buff.toString();
    }

    /**
     * Save the current connection settings to the properties file.
     *
     * @return the file to open afterwards
     */
    String settingSave() {
        ConnectionInfo info = new ConnectionInfo();
        info.name = attributes.getProperty("name", "");
        info.driver = attributes.getProperty("driver", "");
        info.url = attributes.getProperty("url", "");
        info.user = attributes.getProperty("user", "");
        server.updateSetting(info);
        attributes.put("setting", info.name);
        server.saveSettings(null);
        return "index.do";
    }

    private String escapeData(ResultSet rs, int columnIndex) throws SQLException {
        String d = rs.getString(columnIndex);
        if (d == null) {
            return "<i>null</i>";
        } else if (d.length() > SysProperties.WEB_MAX_VALUE_LENGTH) {
            return "<div style='display: none'>=+</div>" +
                PageParser.escapeHtml(d.substring(0, 100) + "... (" + d.length() + ")");
        } else if (d.equals("null") || d.startsWith("= ") || d.startsWith("=+")) {
            return "<div style='display: none'>= </div>" + PageParser.escapeHtml(d);
        }
        return PageParser.escapeHtml(d);
    }

    private void unescapeData(String d, ResultSet rs, int columnIndex) throws SQLException {
        if (d.equals("null")) {
            rs.updateNull(columnIndex);
        } else if (d.startsWith("=+")) {
            // don't update
        } else if (d.startsWith("= ")) {
            d = d.substring(2);
            rs.updateString(columnIndex, d);
        } else {
            rs.updateString(columnIndex, d);
        }
    }

    private String settingRemove() {
        String setting = attributes.getProperty("name", "");
        server.removeSetting(setting);
        ArrayList<ConnectionInfo> settings = server.getSettings();
        if (settings.size() > 0) {
            attributes.put("setting", settings.get(0));
        }
        server.saveSettings(null);
        return "index.do";
    }

    private boolean allow() {
        if (server.getAllowOthers()) {
            return true;
        }
        try {
            return NetUtils.isLocalAddress(socket);
        } catch (UnknownHostException e) {
            server.traceError(e);
            return false;
        }
    }

    /**
     * Get the current mime type.
     *
     * @return the mime type
     */
    String getMimeType() {
        return mimeType;
    }

    boolean getCache() {
        return cache;
    }

    WebSession getSession() {
        return session;
    }

    public void closingDatabase() {
        trace("Closing database");
    }

    public void diskSpaceIsLow(long stillAvailable) {
        trace("No more disk space is available");
    }

    public void exceptionThrown(SQLException e, String sql) {
        trace("Exception: " + e.toString() + " SQL: " + sql);
    }

    public void init(String url) {
        trace("Init: " + url);
    }

    public void opened() {
        trace("Database was opened");
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
        switch (state) {
        case DatabaseEventListener.STATE_BACKUP_FILE:
            trace("Backing up " + name + " " + (100L * x / max) + "%");
            break;
        case DatabaseEventListener.STATE_CREATE_INDEX:
            trace("Creating index " + name + " " + (100L * x / max) + "%");
            break;
        case DatabaseEventListener.STATE_RECOVER:
            trace("Recovering " + name + " " + (100L * x / max) + "%");
            break;
        case DatabaseEventListener.STATE_SCAN_FILE:
            trace("Scanning file " + name + " " + (100L * x / max) + "%");
            break;
        default:
            trace("Unknown state: " + state);
        }
    }

    private void trace(String s) {
        server.trace(s);
    }

}
