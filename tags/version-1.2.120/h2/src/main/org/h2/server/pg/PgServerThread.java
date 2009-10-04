/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.server.pg;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.net.Socket;
import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.HashSet;
import org.h2.constant.SysProperties;
import org.h2.engine.ConnectionInfo;
import org.h2.jdbc.JdbcConnection;
import org.h2.message.Message;
import org.h2.util.IOUtils;
import org.h2.util.JdbcUtils;
import org.h2.util.MemoryUtils;
import org.h2.util.New;
import org.h2.util.Resources;
import org.h2.util.ScriptReader;

/**
 * One server thread is opened for each client.
 */
public class PgServerThread implements Runnable {
    private static final int TYPE_STRING = Types.VARCHAR;
    private PgServer server;
    private Socket socket;
    private Connection conn;
    private boolean stop;
    private DataInputStream dataInRaw;
    private DataInputStream dataIn;
    private OutputStream out;
    private int messageType;
    private ByteArrayOutputStream outBuffer;
    private DataOutputStream dataOut;
    private Thread thread;
    private boolean initDone;
    private String userName;
    private String databaseName;
    private int processId;
    private String clientEncoding = SysProperties.PG_DEFAULT_CLIENT_ENCODING;
    private String dateStyle = "ISO";
    private HashMap<String, Prepared> prepared = New.hashMap();
    private HashMap<String, Portal> portals = New.hashMap();
    private HashSet<Integer> types = New.hashSet();

    PgServerThread(Socket socket, PgServer server) {
        this.server = server;
        this.socket = socket;
    }

    public void run() {
        try {
            server.trace("Connect");
            InputStream ins = socket.getInputStream();
            out = socket.getOutputStream();
            dataInRaw = new DataInputStream(ins);
            while (!stop) {
                process();
                out.flush();
            }
        } catch (EOFException e) {
            // more or less normal disconnect
        } catch (Exception e) {
            server.traceError(e);
        } finally {
            server.trace("Disconnect");
            close();
        }
    }

    private String readString() throws IOException {
        ByteArrayOutputStream buff = new ByteArrayOutputStream();
        while (true) {
            int x = dataIn.read();
            if (x <= 0) {
                break;
            }
            buff.write(x);
        }
        return new String(buff.toByteArray(), getEncoding());
    }

    private int readInt() throws IOException {
        return dataIn.readInt();
    }

    private int readShort() throws IOException {
        return dataIn.readShort();
    }

    private byte readByte() throws IOException {
        return dataIn.readByte();
    }

    private void readFully(byte[] buff) throws IOException {
        dataIn.readFully(buff);
    }

    private void process() throws IOException {
        int x;
        if (initDone) {
            x = dataInRaw.read();
            if (x < 0) {
                stop = true;
                return;
            }
        } else {
            x = 0;
        }
        int len = dataInRaw.readInt();
        len -= 4;
        byte[] data = MemoryUtils.newBytes(len);
        dataInRaw.readFully(data, 0, len);
        dataIn = new DataInputStream(new ByteArrayInputStream(data, 0, len));
        switch (x) {
        case 0:
            server.trace("Init");
            int version = readInt();
            if (version == 80877102) {
                server.trace("CancelRequest (not supported)");
                server.trace(" pid: " + readInt());
                server.trace(" key: " + readInt());
            } else if (version == 80877103) {
                server.trace("SSLRequest");
                out.write('N');
            } else {
                server.trace("StartupMessage");
                server.trace(" version " + version + " (" + (version >> 16) + "." + (version & 0xff) + ")");
                while (true) {
                    String param = readString();
                    if (param.length() == 0) {
                        break;
                    }
                    String value = readString();
                    if ("user".equals(param)) {
                        this.userName = value;
                    } else if ("database".equals(param)) {
                        this.databaseName = value;
                    } else if ("client_encoding".equals(param)) {
                        clientEncoding = value;
                    } else if ("DateStyle".equals(param)) {
                        dateStyle = value;
                    }
                    // server.log(" param " + param + "=" + value);
                }
                sendAuthenticationCleartextPassword();
                initDone = true;
            }
            break;
        case 'p': {
            server.trace("PasswordMessage");
            String password = readString();
            try {
                ConnectionInfo ci = new ConnectionInfo(databaseName);
                String baseDir = server.getBaseDir();
                if (baseDir == null) {
                    baseDir = SysProperties.getBaseDir();
                }
                if (baseDir != null) {
                    ci.setBaseDir(baseDir);
                }
                if (server.getIfExists()) {
                    ci.setProperty("IFEXISTS", "TRUE");
                }
                ci.setProperty("MODE", "PostgreSQL");
                ci.setOriginalURL("jdbc:h2:" + databaseName + ";MODE=PostgreSQL");
                ci.setUserName(userName);
                ci.setProperty("PASSWORD", password);
                ci.convertPasswords();
                conn = new JdbcConnection(ci, false);
                // can not do this because when called inside
                // DriverManager.getConnection, a deadlock occurs
                // conn = DriverManager.getConnection(url, userName, password);
                initDb();
                sendAuthenticationOk();
            } catch (SQLException e) {
                e.printStackTrace();
                stop = true;
            }
            break;
        }
        case 'P': {
            server.trace("Parse");
            Prepared p = new Prepared();
            p.name = readString();
            p.sql = getSQL(readString());
            int count = readShort();
            p.paramType = new int[count];
            for (int i = 0; i < count; i++) {
                int type = readInt();
                checkType(type);
                p.paramType[i] = type;
            }
            try {
                p.prep = conn.prepareStatement(p.sql);
                prepared.put(p.name, p);
                sendParseComplete();
            } catch (SQLException e) {
                sendErrorResponse(e);
            }
            break;
        }
        case 'B': {
            server.trace("Bind");
            Portal portal = new Portal();
            portal.name = readString();
            String prepName = readString();
            Prepared prep = prepared.get(prepName);
            if (prep == null) {
                sendErrorResponse("Portal not found");
                break;
            }
            portal.sql = prep.sql;
            portal.prep = prep.prep;
            portals.put(portal.name, portal);
            int formatCodeCount = readShort();
            int[] formatCodes = new int[formatCodeCount];
            for (int i = 0; i < formatCodeCount; i++) {
                formatCodes[i] = readShort();
            }
            int paramCount = readShort();
            for (int i = 0; i < paramCount; i++) {
                int paramLen = readInt();
                byte[] d2 = MemoryUtils.newBytes(paramLen);
                readFully(d2);
                try {
                    setParameter(portal.prep, i, d2, formatCodes);
                } catch (SQLException e) {
                    sendErrorResponse(e);
                }
            }
            int resultCodeCount = readShort();
            portal.resultColumnFormat = new int[resultCodeCount];
            for (int i = 0; i < resultCodeCount; i++) {
                portal.resultColumnFormat[i] = readShort();
            }
            sendBindComplete();
            break;
        }
        case 'D': {
            char type = (char) readByte();
            String name = readString();
            server.trace("Describe");
            if (type == 'S') {
                Prepared p = prepared.get(name);
                if (p == null) {
                    sendErrorResponse("Prepared not found: " + name);
                } else {
                    sendParameterDescription(p);
                }
            } else if (type == 'P') {
                Portal p = portals.get(name);
                if (p == null) {
                    sendErrorResponse("Portal not found: " + name);
                } else {
                    PreparedStatement prep = p.prep;
                    try {
                        ResultSetMetaData meta = prep.getMetaData();
                        sendRowDescription(meta);
                    } catch (SQLException e) {
                        sendErrorResponse(e);
                    }
                }
            } else {
                server.trace("expected S or P, got " + type);
                sendErrorResponse("expected S or P");
            }
            break;
        }
        case 'E': {
            String name = readString();
            server.trace("Execute");
            Portal p = portals.get(name);
            if (p == null) {
                sendErrorResponse("Portal not found: " + name);
                break;
            }
            int maxRows = readShort();
            PreparedStatement prep = p.prep;
            server.trace(p.sql);
            try {
                prep.setMaxRows(maxRows);
                boolean result = prep.execute();
                if (result) {
                    try {
                        ResultSet rs = prep.getResultSet();
                        ResultSetMetaData meta = rs.getMetaData();
                        sendRowDescription(meta);
                        while (rs.next()) {
                            sendDataRow(rs);
                        }
                        sendCommandComplete(p.sql, 0);
                    } catch (SQLException e) {
                        sendErrorResponse(e);
                    }
                } else {
                    sendCommandComplete(p.sql, prep.getUpdateCount());
                }
            } catch (SQLException e) {
                sendErrorResponse(e);
            }
            break;
        }
        case 'S': {
            server.trace("Sync");
            sendReadyForQuery();
            break;
        }
        case 'Q': {
            server.trace("Query");
            String query = readString();
            ScriptReader reader = new ScriptReader(new StringReader(query));
            while (true) {
                Statement stat = null;
                try {
                    String s = reader.readStatement();
                    if (s == null) {
                        break;
                    }
                    s = getSQL(s);
                    stat = conn.createStatement();
                    boolean result = stat.execute(s);
                    if (result) {
                        ResultSet rs = stat.getResultSet();
                        ResultSetMetaData meta = rs.getMetaData();
                        sendRowDescription(meta);
                        while (rs.next()) {
                            sendDataRow(rs);
                        }
                        sendCommandComplete(s, 0);
                    } else {
                        sendCommandComplete(s, stat.getUpdateCount());
                    }
                } catch (SQLException e) {
                    sendErrorResponse(e);
                } finally {
                    JdbcUtils.closeSilently(stat);
                }
            }
            sendReadyForQuery();
            break;
        }
        case 'X': {
            server.trace("Terminate");
            close();
            break;
        }
        default:
            server.trace("Unsupported: " + x + " (" + (char) x + ")");
            break;
        }
    }

    private void checkType(int type) {
        if (types.contains(type)) {
            server.trace("Unsupported type: " + type);
        }
    }

    private String getSQL(String s) {
        String lower = s.toLowerCase();
        if (lower.startsWith("show max_identifier_length")) {
            s = "CALL 63";
        } else if (lower.startsWith("set client_encoding to")) {
            s = "set DATESTYLE ISO";
        }
        // s = StringUtils.replaceAll(s, "i.indkey[ia.attnum-1]", "0");
        if (server.getTrace()) {
            server.trace(s + ";");
        }
        return s;
    }

    private void sendCommandComplete(String sql, int updateCount) throws IOException {
        startMessage('C');
        sql = sql.trim().toUpperCase();
        // TODO remove remarks at the beginning
        String tag;
        if (sql.startsWith("INSERT")) {
            tag = "INSERT 0 " + updateCount;
        } else if (sql.startsWith("DELETE")) {
            tag = "DELETE " + updateCount;
        } else if (sql.startsWith("UPDATE")) {
            tag = "UPDATE " + updateCount;
        } else if (sql.startsWith("SELECT") || sql.startsWith("CALL")) {
            tag = "SELECT";
        } else if (sql.startsWith("BEGIN")) {
            tag = "BEGIN";
        } else {
            server.trace("Check command tag: " + sql);
            tag = "UPDATE " + updateCount;
        }
        writeString(tag);
        sendMessage();
    }

    private void sendDataRow(ResultSet rs) throws IOException {
        try {
            int columns = rs.getMetaData().getColumnCount();
            String[] values = new String[columns];
            for (int i = 0; i < columns; i++) {
                values[i] = rs.getString(i + 1);
            }
            startMessage('D');
            writeShort(columns);
            for (String s : values) {
                if (s == null) {
                    writeInt(-1);
                } else {
                    // TODO write Binary data
                    byte[] d2 = s.getBytes(getEncoding());
                    writeInt(d2.length);
                    write(d2);
                }
            }
            sendMessage();
        } catch (SQLException e) {
            sendErrorResponse(e);
        }
    }

    private String getEncoding() {
        if ("UNICODE".equals(clientEncoding)) {
            return "UTF-8";
        }
        return clientEncoding;
    }

    private void setParameter(PreparedStatement prep, int i, byte[] d2, int[] formatCodes) throws SQLException {
        boolean text = (i >= formatCodes.length) || (formatCodes[i] == 0);
        String s;
        try {
            if (text) {
                s = new String(d2, getEncoding());
            } else {
                server.trace("Binary format not supported");
                s = new String(d2, getEncoding());
            }
        } catch (Exception e) {
            server.traceError(e);
            s = null;
        }
        // if(server.getLog()) {
        // server.log(" " + i + ": " + s);
        // }
        prep.setString(i + 1, s);
    }

    private void sendErrorResponse(SQLException e) throws IOException {
        server.traceError(e);
        startMessage('E');
        write('S');
        writeString("ERROR");
        write('C');
        writeString(e.getSQLState());
        write('M');
        writeString(e.getMessage());
        write('D');
        writeString(e.toString());
        write(0);
        sendMessage();
    }

    private void sendParameterDescription(Prepared p) throws IOException {
        try {
            PreparedStatement prep = p.prep;
            ParameterMetaData meta = prep.getParameterMetaData();
            int count = meta.getParameterCount();
            startMessage('t');
            writeShort(count);
            for (int i = 0; i < count; i++) {
                int type;
                if (p.paramType != null && p.paramType[i] != 0) {
                    type = p.paramType[i];
                } else {
                    type = TYPE_STRING;
                }
                checkType(type);
                writeInt(type);
            }
            sendMessage();
        } catch (SQLException e) {
            sendErrorResponse(e);
        }
    }

    private void sendNoData() throws IOException {
        startMessage('n');
        sendMessage();
    }

    private void sendRowDescription(ResultSetMetaData meta) throws IOException {
        try {
            if (meta == null) {
                sendNoData();
            } else {
                int columns = meta.getColumnCount();
                int[] types = new int[columns];
                int[] precision = new int[columns];
                String[] names = new String[columns];
                for (int i = 0; i < columns; i++) {
                    names[i] = meta.getColumnName(i + 1);
                    int type = meta.getColumnType(i + 1);
                    precision[i] = meta.getColumnDisplaySize(i + 1);
                    checkType(type);
                    types[i] = type;
                }
                startMessage('T');
                writeShort(columns);
                for (int i = 0; i < columns; i++) {
                    writeString(names[i].toLowerCase());
                    // object ID
                    writeInt(0);
                    // attribute number of the column
                    writeShort(0);
                    // data type
                    writeInt(types[i]);
                    // pg_type.typlen
                    writeShort(getTypeSize(types[i], precision[i]));
                    // pg_attribute.atttypmod
                    writeInt(-1);
                    // text
                    writeShort(0);
                }
                sendMessage();
            }
        } catch (SQLException e) {
            sendErrorResponse(e);
        }
    }

    private int getTypeSize(int type, int precision) {
        switch (type) {
        case Types.VARCHAR:
            return Math.max(255, precision + 10);
        default:
            return precision + 4;
        }
    }

    private void sendErrorResponse(String message) throws IOException {
        server.trace("Exception: " + message);
        startMessage('E');
        write('S');
        writeString("ERROR");
        write('C');
        // PROTOCOL VIOLATION
        writeString("08P01");
        write('M');
        writeString(message);
        sendMessage();
    }

    private void sendParseComplete() throws IOException {
        startMessage('1');
        sendMessage();
    }

    private void sendBindComplete() throws IOException {
        startMessage('2');
        sendMessage();
    }

    private void initDb() throws SQLException {
        Statement stat = null;
        ResultSet rs = null;
        Reader r = null;
        try {
            rs = conn.getMetaData().getTables(null, "PG_CATALOG", "PG_VERSION", null);
            boolean tableFound = rs.next();
            stat = conn.createStatement();
            if (tableFound) {
                rs = stat.executeQuery("SELECT VERSION FROM PG_CATALOG.PG_VERSION");
                if (rs.next()) {
                    if (rs.getInt(1) == 1) {
                        // already installed
                        stat.execute("set search_path = PUBLIC, pg_catalog");
                        return;
                    }
                }
            }
            try {
                r = new InputStreamReader(new ByteArrayInputStream(Resources.get("/org/h2/server/pg/pg_catalog.sql")));
            } catch (IOException e) {
                throw Message.convertIOException(e, "Can not read pg_catalog resource");
            }
            ScriptReader reader = new ScriptReader(r);
            while (true) {
                String sql = reader.readStatement();
                if (sql == null) {
                    break;
                }
                stat.execute(sql);
            }
            reader.close();

            rs = stat.executeQuery("SELECT OID FROM PG_CATALOG.PG_TYPE");
            while (rs.next()) {
                types.add(rs.getInt(1));
            }
        } finally {
            JdbcUtils.closeSilently(stat);
            JdbcUtils.closeSilently(rs);
            IOUtils.closeSilently(r);
        }
    }

    /**
     * Close this connection.
     */
    void close() {
        try {
            stop = true;
            JdbcUtils.closeSilently(conn);
            if (socket != null) {
                socket.close();
            }
            server.trace("Close");
        } catch (Exception e) {
            server.traceError(e);
        }
        conn = null;
        socket = null;
        server.remove(this);
    }

    private void sendAuthenticationCleartextPassword() throws IOException {
        startMessage('R');
        writeInt(3);
        sendMessage();
    }

    private void sendAuthenticationOk() throws IOException {
        startMessage('R');
        writeInt(0);
        sendMessage();
        sendParameterStatus("client_encoding", clientEncoding);
        sendParameterStatus("DateStyle", dateStyle);
        sendParameterStatus("integer_datetimes", "off");
        sendParameterStatus("is_superuser", "off");
        sendParameterStatus("server_encoding", "SQL_ASCII");
        sendParameterStatus("server_version", "8.1.4");
        sendParameterStatus("session_authorization", userName);
        sendParameterStatus("standard_conforming_strings", "off");
        // TODO PostgreSQL TimeZone
        sendParameterStatus("TimeZone", "CET");
        sendBackendKeyData();
        sendReadyForQuery();
    }

    private void sendReadyForQuery() throws IOException {
        startMessage('Z');
        char c;
        try {
            if (conn.getAutoCommit()) {
                // idle
                c = 'I';
            } else {
                // in a transaction block
                c = 'T';
            }
        } catch (SQLException e) {
            // failed transaction block
            c = 'E';
        }
        write((byte) c);
        sendMessage();
    }

    private void sendBackendKeyData() throws IOException {
        startMessage('K');
        writeInt(processId);
        writeInt(processId);
        sendMessage();
    }

    private void writeString(String s) throws IOException {
        write(s.getBytes(getEncoding()));
        write(0);
    }

    private void writeInt(int i) throws IOException {
        dataOut.writeInt(i);
    }

    private void writeShort(int i) throws IOException {
        dataOut.writeShort(i);
    }

    private void write(byte[] data) throws IOException {
        dataOut.write(data);
    }

    private void write(int b) throws IOException {
        dataOut.write(b);
    }

    private void startMessage(int messageType) {
        this.messageType = messageType;
        outBuffer = new ByteArrayOutputStream();
        dataOut = new DataOutputStream(outBuffer);
    }

    private void sendMessage() throws IOException {
        dataOut.flush();
        byte[] buff = outBuffer.toByteArray();
        int len = buff.length;
        dataOut = new DataOutputStream(out);
        dataOut.write(messageType);
        dataOut.writeInt(len + 4);
        dataOut.write(buff);
        dataOut.flush();
    }

    private void sendParameterStatus(String param, String value) throws IOException {
        startMessage('S');
        writeString(param);
        writeString(value);
        sendMessage();
    }

    void setThread(Thread thread) {
        this.thread = thread;
    }

    Thread getThread() {
        return thread;
    }

    void setProcessId(int id) {
        this.processId = id;
    }

    /**
     * Represents a PostgreSQL Prepared object.
     */
    static class Prepared {

        /**
         * The object name.
         */
        String name;

        /**
         * The SQL statement.
         */
        String sql;

        /**
         * The prepared statement.
         */
        PreparedStatement prep;

        /**
         * The list of parameter types (if set).
         */
        int[] paramType;
    }

    /**
     * Represents a PostgreSQL Portal object.
     */
    static class Portal {

        /**
         * The portal name.
         */
        String name;

        /**
         * The SQL statement.
         */
        String sql;

        /**
         * The format used in the result set columns (if set).
         */
        int[] resultColumnFormat;

        /**
         * The prepared statement.
         */
        PreparedStatement prep;
    }

}
