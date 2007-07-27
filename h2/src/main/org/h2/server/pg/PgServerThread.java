/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.server.pg;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
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
import java.util.Properties;

import org.h2.Driver;
import org.h2.util.JdbcUtils;
import org.h2.util.ScriptReader;

/**
 * This class implements a subset of the PostgreSQL protocol as described here:
 * http://developer.postgresql.org/pgdocs/postgres/protocol.html
 * The PostgreSQL catalog is described here:
 * http://www.postgresql.org/docs/7.4/static/view-pg-user.html 
 * @author Thomas
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
    private String clientEncoding = "UTF-8";
    private String dateStyle = "ISO";
    private HashMap prepared = new HashMap();
    private HashMap portals = new HashMap();

    PgServerThread(Socket socket, PgServer server) {
        this.server = server;
        this.socket = socket;
    }

    public void run() {
        try {
            server.log("Connect");
            InputStream ins = socket.getInputStream();
            out = socket.getOutputStream();
            dataInRaw = new DataInputStream(ins);
            while (!stop) {
                process();
                out.flush();
            }
        } catch (Exception e) {
            error("process", e);
            server.logError(e);
        } finally {
            server.log("Disconnect");
            close();
        }
    }
    
    private String readString() throws IOException {
        StringBuffer buff = new StringBuffer();
        while(true) {
            int x = dataIn.read();
            if(x <= 0) {
                break;
            }
            buff.append((char)x);
        }
        return buff.toString();
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
    
    private void error(String message, Exception e) {

        int todoDocumentLimitations;
/*        
Limitations:
- The database name can not contains the path (~ or directory). 
  Workaround: use -baseDir when starting the server.
- SSL is not supported.
- Statements can not be cancelled.
- Metadata is static.
*/        

        if(e != null) {
            server.logError(e);
        }
    }
    
    private void process() throws IOException {
        int x;
        if(initDone) {
            x = dataInRaw.read();
            if(x < 0) {
                stop = true;
                return;
            }
        } else {
            x = 0;
        }
        int len = dataInRaw.readInt();
        len -= 4;
        byte[] data = new byte[len];
        dataInRaw.readFully(data, 0, len);
        dataIn = new DataInputStream(new ByteArrayInputStream(data, 0, len));
        switch(x) {
        case 0:
            server.log("Init");
            int version = readInt();
            if(version == 80877102) {
                int todoSupport;
                server.log("CancelRequest");
                server.log(" pid: "+readInt());
                server.log(" key: "+readInt());
                error("CancelRequest", null);
            } else if(version == 80877103) {
                server.log("SSLRequest");
                out.write('N');
            } else {
                 server.log("StartupMessage");
                 server.log(" version " + version + " (" + (version >> 16) + "." + (version & 0xff) + ")");
                while(true) {
                    String param = readString();
                    if(param.length() == 0) {
                        break;
                    }
                    String value = readString();
                    if("user".equals(param)) {
                        this.userName = value;
                    } else if("database".equals(param)) {
                        this.databaseName = value;
                    } else if("client_encoding".equals(param)) {
                        clientEncoding = value;
                    } else if("DateStyle".equals(param)) {
                        dateStyle = value;
                    }
                    // server.log(" param " + param + "=" + value);
                }
                sendAuthenticationCleartextPassword();
                initDone = true;
            }
            break;
        case 'p': {
            server.log("PasswordMessage");
            String password = readString();
            try {
                String url = "jdbc:h2:" + databaseName + ";MODE=PostgreSQL";
                // can not do this because when called inside DriverManager.getConnection, a deadlock occurs
                // conn = DriverManager.getConnection(url, userName, password);
                Properties prop = new Properties();
                prop.setProperty("user", userName);
                prop.setProperty("password", password);
                conn = Driver.load().connect(url, prop);
                initDb();
                sendAuthenticationOk();
            } catch(SQLException e) {
                e.printStackTrace();
                stop = true;
            }
            break;
        }
        case 'P': {
            server.log("Parse");
            Prepared p = new Prepared();
            p.name = readString();
            p.sql = getSQL(readString());
            int count = readShort();
            p.paramType = new int[count];
            for(int i=0; i<count; i++) {
                p.paramType[i] = readInt();
            }
            try {
                p.prep = conn.prepareStatement(p.sql);
                prepared.put(p.name, p);
                sendParseComplete();
            } catch(SQLException e) {
                sendErrorResponse(e);
            }
            break;
        }
        case 'B': {
            server.log("Bind");
            Portal portal = new Portal();
            portal.name = readString();
            String prepName = readString();
            Prepared prep = (Prepared) prepared.get(prepName);
            if(prep == null) {
                sendErrorResponse("Portal not found");
                break;
            }
            portal.sql = prep.sql;
            portal.prep = prep.prep;
            portals.put(portal.name, portal);
            int formatCodeCount = readShort();
            int[] formatCodes = new int[formatCodeCount];
            for(int i=0; i<formatCodeCount; i++) {
                formatCodes[i] = readShort();
            }
            int paramCount = readShort();
            for(int i=0; i<paramCount; i++) {
                int paramLen = readInt();
                byte[] d2 = new byte[paramLen];
                readFully(d2);
                try {
                    setParameter(portal.prep, i, d2, formatCodes);
                } catch(SQLException e) {
                    sendErrorResponse(e);
                }
            }
            int resultCodeCount  = readShort();
            portal.resultColumnFormat = new int[resultCodeCount];
            for(int i=0; i<resultCodeCount; i++) {
                portal.resultColumnFormat[i] = readShort();
            }
            sendBindComplete();
            break;
        }
        case 'D': {
            char type = (char) readByte();
            String name = readString();
            server.log("Describe");
            PreparedStatement prep;
            if(type == 'S') {
                Prepared p = (Prepared) prepared.get(name);
                if(p == null) {
                    sendErrorResponse("Prepared not found: " + name);
                }
                prep = p.prep;
                sendParameterDescription(p);
            } else if(type == 'P') {
                Portal p = (Portal) portals.get(name);
                if(p == null) {
                    sendErrorResponse("Portal not found: " + name);
                }
                prep = p.prep;
                try {
                    ResultSetMetaData meta = prep.getMetaData();
                    sendRowDescription(meta);
                } catch(SQLException e) {
                    sendErrorResponse(e);
                }
            } else {
                error("expected S or P, got " + type, null);
                sendErrorResponse("expected S or P");
            }
            break;
        }
        case 'E': {
            String name = readString();
            server.log("Execute");
            Portal p = (Portal) portals.get(name);
            if(p == null) {
                sendErrorResponse("Portal not found: " + name);
                break;
            }
            int maxRows = readShort();
            PreparedStatement prep = p.prep;
            server.log(p.sql);
            try {
                prep.setMaxRows(maxRows);
                boolean result = prep.execute();
                if(result) {
                    try {
                        ResultSet rs = prep.getResultSet();
                        ResultSetMetaData meta = rs.getMetaData();
                        sendRowDescription(meta);
                        while(rs.next()) {
                            sendDataRow(p.resultColumnFormat, rs);
                        }
                        sendCommandComplete(p.sql, 0);
                    } catch(SQLException e) {
                        sendErrorResponse(e);
                    }
                } else {
                    sendCommandComplete(p.sql, prep.getUpdateCount());
                }
            } catch(SQLException e) {
                sendErrorResponse(e);
            }
            break;
        }
        case 'S': {
            server.log("Sync");
            sendReadyForQuery();
            break;
        }
        case 'Q': {
            server.log("Query");
            String query = readString();
            ScriptReader reader = new ScriptReader(new StringReader(query));
            while(true) {
                try {
                    String s = reader.readStatement();
                    if(s == null) {
                        break;
                    }
                    s = getSQL(s);
                    Statement stat = conn.createStatement();
                    boolean result = stat.execute(s);
                    if(result) {
                        ResultSet rs = stat.getResultSet();
                        ResultSetMetaData meta = rs.getMetaData();
                        sendRowDescription(meta);                        
                        while(rs.next()) {
                            sendDataRow(null, rs);
                        }
                        sendCommandComplete(s, 0);
                    } else {
                        sendCommandComplete(s, stat.getUpdateCount());
                    }
                } catch(SQLException e) {
                    sendErrorResponse(e);
                }
            }
            sendReadyForQuery();
            break;
        }
        case 'X': {
            server.log("Terminate");
            close();
            break;
        }
        default:
            error("Unsupported: " + x + " (" + (char)x + ")", null);
            break;
        }
    }
    
    private String getSQL(String s) {
        String lower = s.toLowerCase();
        int todo;
        if(lower.startsWith("show max_identifier_length")) {
            s = "CALL 63";
        } else if(lower.startsWith("set client_encoding to")) {
            s = "set DATESTYLE ISO";
        }
        // s = StringUtils.replaceAll(s, "i.indkey[ia.attnum-1]", "0");
        if(server.getLog()) {
            server.log(s + ";");
        }
        return s;
    }
    
    private void sendCommandComplete(String sql, int updateCount) throws IOException {
        startMessage('C');
        sql = sql.trim().toUpperCase();
        // TODO remove remarks at the beginning
        String tag;
        if(sql.startsWith("INSERT")) {
            tag = "INSERT 0 " + updateCount;
        } else if(sql.startsWith("DELETE")) {
            tag = "DELETE " + updateCount;
        } else if(sql.startsWith("UPDATE")) {
            tag = "UPDATE " + updateCount;
        } else if(sql.startsWith("SELECT") || sql.startsWith("CALL")) {
            tag = "SELECT";
        } else if(sql.startsWith("BEGIN")) {
            tag = "BEGIN";
        } else {
            error("check command tag: " + sql, null);
            tag = "UPDATE " + updateCount;
        }
        writeString(tag);
        sendMessage();
    }

    private void sendDataRow(int[] formatCodes, ResultSet rs) throws IOException {
        try {
            int columns = rs.getMetaData().getColumnCount();
            String[] values = new String[columns];
            for(int i=0; i<columns; i++) {
                values[i] = rs.getString(i + 1);
            }
            startMessage('D');
            writeShort(columns);
            for(int i=0; i<columns; i++) {
                String s = values[i];
                if(s == null) {
                    writeInt(-1);
                } else {
                    // TODO write Binary data
                    byte[] d2 = s.getBytes(getEncoding());
                    writeInt(d2.length);
                    write(d2);
                }
            }
            sendMessage();
        } catch(SQLException e) {
            sendErrorResponse(e);
        }
    }
    
    private String getEncoding() {
        if(clientEncoding.equals("UNICODE")) {
            return "UTF-8";
        }
        return clientEncoding;
    }

    private void setParameter(PreparedStatement prep, int i, byte[] d2, int[] formatCodes) throws SQLException {
        boolean text = (i >= formatCodes.length) || (formatCodes[i] == 0);
        String s;
        try {
            if(text) {
                s = new String(d2, getEncoding());
            } else {
                int testing;
                System.out.println("binary format!");
                s = new String(d2, getEncoding());
            }
        } catch(Exception e) {
            error("conversion error", e);
            s = null;
        }
        // if(server.getLog()) {
        //  server.log(" " + i + ": " + s);
        // }
        prep.setString(i + 1, s);
    }

    private void sendErrorResponse(SQLException e) throws IOException {
        error("SQLException", e);
        startMessage('E');
        write('S');
        writeString("ERROR");
        write('C');
        writeString(e.getSQLState());
        write('M');
        writeString(e.getMessage());
        write('D');
        writeString(e.toString());
        sendMessage();
    }

    private void sendParameterDescription(Prepared p) throws IOException {
        try {
            PreparedStatement prep = p.prep;
            ParameterMetaData meta = prep.getParameterMetaData();
            int count = meta.getParameterCount();
            startMessage('t');
            writeShort(count);
            for(int i=0; i<count; i++) {
                if(p.paramType != null && p.paramType[i] != 0) {
                    writeInt(p.paramType[i]);
                } else {
                    writeInt(TYPE_STRING);
                }
            }
            sendMessage();
        } catch(SQLException e) {
            sendErrorResponse(e);
        }
    }
    
    private void sendNoData() throws IOException {
        startMessage('n');
        sendMessage();        
    }

    private void sendRowDescription(ResultSetMetaData meta) throws IOException {
        try {
            if(meta == null) {
                sendNoData();
            } else {
                int columns = meta.getColumnCount();
                int[] types = new int[columns];
                String[] names = new String[columns];
                for(int i=0; i<columns; i++) {
                    names[i] = meta.getColumnName(i + 1);
                    types[i] = meta.getColumnType(i + 1);
                }
                startMessage('T');
                writeShort(columns);
                for(int i=0; i<columns; i++) {
                    writeString(names[i].toLowerCase());
                    writeInt(0); // object ID
                    writeShort(0); // attribute number of the column
                    writeInt(getType(types[i])); // data type
                    writeShort(getTypeSize(types[i])); // pg_type.typlen
                    writeInt(getModifier(types[i])); // pg_attribute.atttypmod
                    writeShort(0); // text
                }
                sendMessage();        
            }
        } catch(SQLException e) {
            sendErrorResponse(e);
        }
    }
    
    private int getType(int type) {
        switch(type) {
        case Types.VARCHAR:
            return 19;
        }
        return type;
    }
    
    private int getTypeSize(int type) {
        switch(type) {
        case Types.VARCHAR:
            return 255;
        }
        return type;
    }
    
    private int getModifier(int type) {
        return -1;
    }    

    private void sendErrorResponse(String message) throws IOException {
        error("Exception: " + message, null);
        startMessage('E');
        write('S');
        writeString("ERROR");
        write('C');
        writeString("08P01"); // PROTOCOL VIOLATION
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
        Statement stat = conn.createStatement();
        Reader r = new InputStreamReader(getClass().getResourceAsStream("pg_catalog.sql"));
        r = new BufferedReader(r);
        ScriptReader reader = new ScriptReader(r);
        while(true) {
            String sql = reader.readStatement();
            if(sql == null) {
                break;
            }
            stat.execute(sql);
        }
        reader.close();
    }

//    private void sendResultSet(ResultSet rs) throws SQLException, IOException {
//        ResultSetMetaData meta = rs.getMetaData();
//        int columnCount = meta.getColumnCount();
//        // 
//        startMessage('T');
//        writeShort(columnCount);
//        for(int i=0; i<columnCount; i++) {
//            writeString(meta.getColumnName(i + 1));
//            writeInt(0); // table id
//            writeShort(0); // column id
//            writeInt(0); // data type id
//            writeShort(26); // data type size (see pg_type.typlen)
//            writeInt(4); // type modifier (see pg_attribute.atttypmod)
//            writeShort(0); // format code 0=text, 1=binary
//        }
//        sendMessage();
//        while(rs.next()) {
//            // DataRow
//            startMessage('D');
//            writeShort(columnCount);
//            for(int i=0; i<columnCount; i++) {
//                String v = rs.getString(i + 1);
//                if(v == null) {
//                    writeInt(-1);
//                } else {
//                    byte[] data = v.getBytes();
//                    writeInt(data.length);
//                    write(data);
//                }
//            }
//            sendMessage();
//        }
//        
//        // CommandComplete
//        startMessage('C');
//        writeString("SELECT");
//        sendMessage();
//        sendReadyForQuery('I');
//    }

    public void close() {
        try {
            stop = true;
            JdbcUtils.closeSilently(conn);
            if(socket != null) {
                socket.close();
            }
            server.log("Close");
        } catch(Exception e) {
            server.logError(e);
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
        sendParameterStatus("TimeZone", "CET"); // TODO
        sendBackendKeyData();
        sendReadyForQuery();
    }
    
    private void sendReadyForQuery() throws IOException {
        startMessage('Z');
        char c;
        try {
            if(conn.getAutoCommit()) {
                c = 'I'; // idle
            } else {
                c = 'T'; // in a transaction block
            }
        } catch(SQLException e) {
            c = 'E'; // failed transaction block
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
        write(s.getBytes("UTF-8"));
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

    public void setThread(Thread thread) {
        this.thread = thread;
    }

    public Thread getThread() {
        return thread;
    }

    public void setProcessId(int id) {
        this.processId = id;
    }
    
    private static class Prepared {
        String name;
        String sql;
        PreparedStatement prep;
        int[] paramType;
    }
    
    private static class Portal {
        String name;
        String sql;
        int[] resultColumnFormat;
        PreparedStatement prep;
    }

}
