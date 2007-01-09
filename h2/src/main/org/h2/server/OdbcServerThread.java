/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.server;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;

import org.h2.engine.ConnectionInfo;
import org.h2.message.Message;
import org.h2.util.JdbcUtils;
import org.h2.util.StringUtils;

/**
 * @author Thomas
 */

public class OdbcServerThread implements Runnable {
    private OdbcServer server;
    private Socket socket;
    private Connection conn;
    private DatabaseMetaData meta;
    private boolean stop;
    private int cacheId;
    private Object cache;
    private OdbcTransfer transfer;
    private Thread thread;
    private BufferedOutputStream outBuff;
    private HashMap object = new HashMap();
    private int nextId;

    OdbcServerThread(Socket socket, OdbcServer server) {
        this.server = server;
        this.socket = socket;
    }

    private int addObject(Object o) {
        int id = nextId++;
        server.log("addObj "+id+" "+o);
        object.put(new Integer(id), o);
        cacheId = id;
        cache = o;
        return id;
    }

    private void freeObject(int id) {
        if (cacheId == id) {
            cacheId = -1;
            cache = null;
        }
        object.remove(new Integer(id));
    }

    private Object getObject(int id) {
        if (id == cacheId) {
            server.log("getObj "+id+" "+cache);
            return cache;
        }
        server.log("getObj "+id+" "+object.get(new Integer(id)));
        return object.get(new Integer(id));
    }

    public void run() {
        try {
            server.log("Connect");
            InputStream ins = socket.getInputStream();
            OutputStream outs = socket.getOutputStream();
            DataInputStream in;
            in = new DataInputStream(new BufferedInputStream(ins,
                    OdbcTransfer.BUFFER_SIZE));
            outBuff = new BufferedOutputStream(outs, OdbcTransfer.BUFFER_SIZE);
            DataOutputStream out = new DataOutputStream(outBuff);
            transfer = new OdbcTransfer(in, out);
            outBuff.flush();
            while (!stop) {
                process();
                outBuff.flush();
            }
            server.log("Disconnect");
        } catch (Exception e) {
            server.logError(e);
        }
    }

    public void close() {
        try {
            stop = true;
            conn.close();
            socket.close();
            server.log("Close");
        } catch(Exception e) {
            server.logError(e);
        }
        conn = null;
        socket = null;
        server.remove(this);
    }

    private void sendError(Throwable e) throws IOException {
        SQLException s = Message.convert(e);
        server.log("Exception "+s);
        s.printStackTrace();
        transfer.writeByte((byte)'E');
    }

    private void processResultSet(ResultSet rs) throws IOException, SQLException {
        int id = addObject(rs);
        transfer.writeInt(id);
        ResultSetMetaData m = rs.getMetaData();
        int columnCount = m.getColumnCount();
        transfer.writeInt(columnCount);
        for(int i=0; i<columnCount; i++) {
            transfer.writeInt(mapType(m.getColumnType(i+1)));
            transfer.writeString(m.getTableName(i+1));
            transfer.writeString(m.getColumnLabel(i+1));
            transfer.writeInt(m.getPrecision(i+1));
            transfer.writeInt(m.getScale(i+1));
            transfer.writeInt(m.getColumnDisplaySize(i+1));
        }
    }

    private void setParameter(PreparedStatement prep, int index, int type) throws SQLException, IOException {
        switch(type) {
        case Types.NULL: {
            // fake: use Integer data type for now
            prep.setNull(index, Types.INTEGER);
            break;
        }
        case Types.INTEGER: {
            int value = transfer.readInt();
            server.log("  index="+index+" int="+value);
            prep.setInt(index, value);
            break;
        }
        case Types.VARCHAR: {
            String value = transfer.readString();
            server.log("  index="+index+" string="+value);
            prep.setString(index, value);
            break;
        }
        default:
            throw Message.getInternalError("unexpected data type "+type);
        }
    }

    private void setParameters(PreparedStatement prep) throws SQLException, IOException {
        while(true) {
            int x = transfer.readByte();
            if(x == '0') {
                break;
            } else if(x=='1') {
                int index = transfer.readInt();
                int type = transfer.readInt();
                setParameter(prep, index+1, type);
            } else {
                throw Message.getInternalError("unexpected "+x);
            }
        }
    }

    private void processMeta() throws IOException {
        int operation = transfer.readByte();
        server.log("meta op="+(char)operation);
        switch(operation) {
        case 'B': {
            String catalog = transfer.readString();
            String schema = transfer.readString();
            String table = transfer.readString();
            if(table ==null || table.length()==0) {
                table = "%";
            }
            int scope = transfer.readInt();
            boolean nullable = transfer.readBoolean();
            try {
                ResultSet rs = meta.getBestRowIdentifier(catalog, schema, table, scope, nullable);
                processResultSet(rs);
            } catch(Throwable e) {
                sendError(e);
            }
            break;
        }
        case 'C': {
//            String catalog = transfer.readString();
            String schemaPattern = transfer.readString();
            String tableNamePattern = transfer.readString();
            String columnNamePattern = transfer.readString();
            if(tableNamePattern ==null || tableNamePattern.length()==0) {
                tableNamePattern = "%";
            }
            if(columnNamePattern ==null || columnNamePattern.length()==0) {
                columnNamePattern = "%";
            }
            PreparedStatement prep = null;
            try {
                prep = conn.prepareStatement("SELECT "
                        + "TABLE_CATALOG TABLE_CAT, "
                        + "TABLE_SCHEMA TABLE_SCHEM, "
                        + "TABLE_NAME, "
                        + "COLUMN_NAME, "
                        + "DATA_TYPE, "
                        + "TYPE_NAME, "
                        + "CHARACTER_MAXIMUM_LENGTH COLUMN_SIZE, "
                        + "CHARACTER_MAXIMUM_LENGTH BUFFER_LENGTH, "
                        + "CAST(NUMERIC_SCALE AS SMALLINT) DECIMAL_DIGITS, " // different in JDBC
                        + "CAST(10 AS SMALLINT) NUM_PREC_RADIX, " // different in JDBC
                        + "CAST(NULLABLE AS SMALLINT) NULLABLE, "   // different in JDBC
                        + "'' REMARKS, "
                        + "COLUMN_DEFAULT COLUMN_DEF, "
                        + "CAST(DATA_TYPE AS SMALLINT) SQL_DATA_TYPE, "  // different in JDBC
                        + "CAST(0 AS SMALLINT) SQL_DATETIME_SUB, " // different in JDBC
                        + "CHARACTER_OCTET_LENGTH CHAR_OCTET_LENGTH, "
                        + "ORDINAL_POSITION ORDINAL_POSITION, "
                        + "NULLABLE IS_NULLABLE "
                        + "FROM INFORMATION_SCHEMA.COLUMNS "
                        + "WHERE TABLE_SCHEMA LIKE ? "
                        + "AND TABLE_NAME LIKE ? "
                        + "AND COLUMN_NAME LIKE ? "
                        + "ORDER BY TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION");
                prep.setString(1, schemaPattern == null ? "%" : schemaPattern);
                prep.setString(2, tableNamePattern == null ? "%" : tableNamePattern);
                prep.setString(3, columnNamePattern == null ? "%" : columnNamePattern);
                // ResultSet rs = meta.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
                ResultSet rs = prep.executeQuery();
                processResultSet(rs);
            } catch(SQLException e) {
                sendError(e);
            } finally {
                JdbcUtils.closeSilently(prep);
            }
            break;
        }
        case 'D': {
            String where;
            if(transfer.readByte()=='A') {
                where= "";
            } else {
                int type = transfer.readInt();
                where = " WHERE TYPE="+type+" ";
            }
            Statement stat = null;
            try {
                stat = conn.createStatement();
                ResultSet rs = stat.executeQuery("SELECT "
                    + "TYPE_NAME, "
                    + "DATA_TYPE, "
                    + "PRECISION COLUMN_SIZE, "
                    + "PREFIX LITERAL_PREFIX, "
                    + "PREFIX LITERAL_SUFFIX, "
                    + "PARAMS CREATE_PARAMS, "
                    + "CAST(" +DatabaseMetaData.typeNullable + " AS SMALLINT) NULLABLE, "
                    + "CAST(1 AS SMALLINT) CASE_SENSITIVE, " // TODO metadata: check if this is ok
                    + "CAST(1 AS SMALLINT) SEARCHABLE, " // TODO metadata: check if this is ok
                    + "CAST(0 AS SMALLINT) UNSIGNED_ATTRIBUTE, " // TODO metadata: check if this is ok
                    + "CAST(0 AS SMALLINT) FIXED_PREC_SCALE, " // TODO metadata: check if this is ok
                    + "CAST(0 AS SMALLINT) AUTO_UNIQUE_VALUE, " // TODO metadata: check if this is ok
                    + "TYPE_NAME LOCAL_TYPE_NAME, "
                    + "MINIMUM_SCALE, "
                    + "MAXIMUM_SCALE, "
                    + "DATA_TYPE SQL_DATA_TYPE, "
                    + "CAST(1 AS SMALLINT) SQL_DATETIME_SUB, " // TODO metadata: check if this is ok
                    + "RADIX NUM_PREC_RADIX, "
                    + "CAST(0 AS SMALLINT) INTERVAL_PRECISION "
                    + "FROM INFORMATION_SCHEMA.TYPE_INFO "
                    + where
                    + "ORDER BY DATA_TYPE, POS");
                processResultSet(rs);
            } catch(SQLException e) {
                sendError(e);
            } finally {
                JdbcUtils.closeSilently(stat);
            }
            break;
        }
        case 'I': {
//            String catalog = transfer.readString();
            String schemaPattern = transfer.readString();
            String tableNamePattern = transfer.readString();
            if(tableNamePattern==null || tableNamePattern.length()==0) {
                tableNamePattern = "%";
            }
//            boolean unique = transfer.readBoolean();
//            boolean approximate = transfer.readBoolean();
            PreparedStatement prep = null;
            try {
                //ResultSet rs = meta.getIndexInfo(catalog, schemaPattern, tableNamePattern, unique, approximate);

                prep = conn.prepareStatement("SELECT "
                        + "TABLE_CATALOG TABLE_CAT, "
                        + "TABLE_SCHEMA TABLE_SCHEM, "
                        + "TABLE_NAME, "
                        + "CAST(NON_UNIQUE AS SMALLINT) NON_UNIQUE, "
                        + "TABLE_CATALOG INDEX_QUALIFIER, "
                        + "INDEX_NAME, "
                        + "CAST("+DatabaseMetaData.tableIndexOther + " AS SMALLINT) TYPE, "
                        + "ORDINAL_POSITION, "
                        + "COLUMN_NAME, "
                        + "'A' ASC_OR_DESC, "
                        + "CARDINALITY, "
                        + "0 PAGES, "
                        + "'' FILTER_CONDITION "
                        + "FROM INFORMATION_SCHEMA.INDEXES "
                        + "WHERE CATALOG_NAME LIKE ? "
                        + "AND TABLE_NAME LIKE ? "
                        + "ORDER BY NON_UNIQUE, TYPE, TABLE_SCHEM, INDEX_NAME, ORDINAL_POSITION");
                prep.setString(1, schemaPattern);
                prep.setString(2, tableNamePattern);
                ResultSet rs = prep.executeQuery();
                processResultSet(rs);
            } catch(SQLException e) {
                sendError(e);
            } finally {
                JdbcUtils.closeSilently(prep);
            }
            break;
        }
        case 'N': {
            String sql = transfer.readString();
            try {
                sql = conn.nativeSQL(sql);
            } catch(SQLException e) {
                sendError(e);
            }
            transfer.writeString(sql);
            break;
        }
        case 'T': {
            String catalog = transfer.readString();
            String schema = transfer.readString();
            String table = transfer.readString();
            String tableTypes = transfer.readString();
            server.log(" catalog="+catalog+" schema="+schema+" table="+table+" tableTypes="+tableTypes);
            ResultSet rs;
            String[] types = null;
            PreparedStatement prep = null;
            try {
                if(catalog.equals("%") && schema.length()==0 && table.length()==0) {
                    server.log(" allCatalogs");
                    prep = conn.prepareStatement("SELECT "
                            + "CATALOG_NAME TABLE_CAT, "
                            + "NULL TABLE_SCHEM, "
                            + "NULL TABLE_NAME, "
                            + "NULL TABLE_TYPE, "
                            + "'' REMARKS "
                            + "FROM INFORMATION_SCHEMA.CATALOGS");
                    rs = prep.executeQuery();
                } else if(catalog.length()==0 && schema.equals("%") && table.length()==0) {
                    server.log(" allSchemas");
                    prep = conn.prepareStatement("SELECT "
                            + "CATALOG_NAME TABLE_CAT, "
                            + "SCHEMA_NAME TABLE_SCHEM, "
                            + "NULL TABLE_NAME, "
                            + "NULL TABLE_TYPE, "
                            + "'' REMARKS "
                            + "FROM INFORMATION_SCHEMA.SCHEMATA");
                    rs = prep.executeQuery();
                } else if(catalog.length()==0 && schema.length()==0 && table.length()==0 && tableTypes.equals("%")) {
                    server.log(" allTableTypes");
                    prep = conn.prepareStatement("SELECT "
                            + "NULL TABLE_CAT, "
                            + "NULL TABLE_SCHEM, "
                            + "NULL TABLE_NAME, "
                            + "TYPE TABLE_TYPE, "
                            + "'' REMARKS "
                            + "FROM  INFORMATION_SCHEMA.TABLE_TYPES");
                    rs = prep.executeQuery();
                } else {
                    server.log(" getTables");
                    if(tableTypes.equals("%") || tableTypes.length()==0) {
                        types = null;
                    } else {
                        types = StringUtils.arraySplit(tableTypes, ',', false);
                        for(int i=0; i<types.length; i++) {
                            String t = StringUtils.toUpperEnglish(types[i]);
                            if(t.startsWith("\'")) {
                                t = t.substring(1, t.length()-2);
                            }
                            types[i] = t;
                        }
                    }
                    server.log("getTables "+catalog+" "+schema+" "+table);
                    if(table.length() == 0) {
                        table = null;
                    }
                    rs = meta.getTables(catalog, schema, table, types);
                }
                processResultSet(rs);
            } catch(SQLException e) {
                sendError(e);
            } finally {
                JdbcUtils.closeSilently(prep);
            }
            break;
        }
        case 'V': {
            String catalog = transfer.readString();
            String schema = transfer.readString();
            String table = transfer.readString();
            if(table ==null || table.length()==0) {
                table = "%";
            }
            try {
                ResultSet rs = meta.getVersionColumns(catalog, schema, table);
//                PreparedStatement prep = conn.prepareStatement("SELECT "
//                        + "CAST(NULL AS INT) SCOPE, "
//                        + "NULL COLUMN_NAME, "
//                        + "CAST(NULL AS INT) DATA_TYPE, "
//                        + "NULL TYPE_NAME, "
//                        + "CAST(NULL AS INT) COLUMN_SIZE, "
//                        + "CAST(NULL AS INT) BUFFER_LENGTH, "
//                        + "CAST(NULL AS INT) DECIMAL_DIGITS, "
//                        + "CAST(NULL AS INT) PSEUDO_COLUMN "
//                        + "FROM SYSTEM_TABLES "
//                        + "WHERE 1=0");

            //    ResultSet rs = prep.executeQuery();
                processResultSet(rs);
            } catch(SQLException e) {
                sendError(e);
            }
            break;
        }
        default:
            server.log("meta operation? " + (char)operation);
        }
    }

    private void process() throws IOException {
        int operation = transfer.readByte();
        if(operation == -1) {
            stop = true;
            return;
        }
        server.log("op="+(char)operation);
        switch(operation) {
        case 'A': {
            try {
                int op = transfer.readByte();
                switch(op) {
                case '0':
                    server.log("autoCommit false");
                    conn.setAutoCommit(false);
                    break;
                case '1':
                    server.log("autoCommit true");
                    conn.setAutoCommit(true);
                    break;
                case 'C':
                    server.log("commit");
                    conn.commit();
                    break;
                case 'R':
                    server.log("rollback");
                    conn.rollback();
                    break;
                default:
                    server.log("operation? " + (char)operation);
                }
            } catch(SQLException e) {
                sendError(e);
            }
            break;
        }
        case 'C':
            server.log("connect");
            String db = transfer.readString();
            server.log(" db="+db);
            String user = transfer.readString();
            server.log(" user="+user);
            String password = transfer.readString();
            server.log(" password="+password);
            String baseDir = server.getBaseDir();
            ConnectionInfo ci = new ConnectionInfo(db);
            if(baseDir != null) {
                ci.setBaseDir(baseDir);
            }
            if(server.getIfExists()) {
                ci.setProperty("IFEXISTS", "TRUE");
            }
            String dbName = ci.getDatabaseName();
            try {
                conn = DriverManager.getConnection("jdbc:h2:" + dbName, user, password);
                meta = conn.getMetaData();
                transfer.writeByte((byte)'O');
            } catch(SQLException e) {
                sendError(e);
            }
            break;
        case 'E': {
            String sql = transfer.readString();
            server.log("<"+sql+">");
            try {
                int params = getParametersCount(sql);
                if(params > 0) {
                    // it is a prepared statement
                    PreparedStatement prep = conn.prepareStatement(sql);
                    int id = addObject(prep);
                    transfer.writeByte((byte)'O');
                    transfer.writeInt(id);
                    transfer.writeInt(params);
                } else {
                    Statement stat = null;
                    try {
                        stat = conn.createStatement();
                        boolean isResultSet = stat.execute(sql);
                        if(isResultSet) {
                            transfer.writeByte((byte)'R');
                            ResultSet rs = stat.getResultSet();
                            processResultSet(rs);
                        } else {
                            transfer.writeByte((byte)'U');
                            transfer.writeInt(stat.getUpdateCount());
                        }
                    } finally {
                        JdbcUtils.closeSilently(stat);
                    }
                }
            } catch(SQLException e) {
                sendError(e);
            }
            break;
        }
        case 'F': {
            int id = transfer.readInt();
            server.log("free "+id);
            freeObject(id);
            break;
        }
        case 'G': {
            int objectId = transfer.readInt();
            ResultSet rs = (ResultSet)getObject(objectId);
            try {
                boolean hasNext = rs.next();
                if(hasNext) {
                    transfer.writeByte((byte)'1');
                    ResultSetMetaData m = rs.getMetaData();
                    int columnCount = m.getColumnCount();
                    for(int i=0; i<columnCount; i++) {
                        write(m, rs, i);
                    }
                } else {
                    transfer.writeByte((byte)'0');
                }
            } catch(SQLException e) {
                sendError(e);
            }
            break;
        }
        case 'M':
            processMeta();
            break;
        case 'P': {
            String sql = transfer.readString();
            server.log("<"+sql+">");
            try {
                PreparedStatement prep = conn.prepareStatement(sql);
                int id = addObject(prep);
                transfer.writeByte((byte)'O');
                transfer.writeInt(id);
                int params = getParametersCount(sql);
                transfer.writeInt(params);
            } catch(SQLException e) {
                sendError(e);
            }
            break;
        }
        case 'Q': {
            // executePrepared
            int id = transfer.readInt();
            PreparedStatement prep = (PreparedStatement)getObject(id);
            try {
                setParameters(prep);
                boolean isResultSet = prep.execute();
                if(isResultSet) {
                    transfer.writeByte((byte)'R');
                    ResultSet rs = prep.getResultSet();
                    processResultSet(rs);
                } else {
                    transfer.writeByte((byte)'U');
                    transfer.writeInt(prep.getUpdateCount());
                }
            } catch(SQLException e) {
                sendError(e);
            }
            break;
        }
        case 'X':
            stop = true;
            break;
        default:
            server.log("operation? " + (char)operation);
        }
    }

    private void write(ResultSetMetaData m, ResultSet rs, int i) throws IOException {
        try {
            int type = mapType(m.getColumnType(i+1));
            switch(type) {
            case Types.SMALLINT:
            case Types.INTEGER: {
                int value = rs.getInt(i+1);
                if(rs.wasNull()) {
                    transfer.writeBoolean(true);
                } else {
                    transfer.writeBoolean(false);
                    transfer.writeInt(value);
                }
                break;
            }
            case Types.NULL:
                break;
            case Types.VARCHAR:
                transfer.writeString(rs.getString(i+1));
                break;
            default:
                throw Message.getInternalError("unsupported data type "+type);
            }
        } catch(SQLException e) {
            sendError(e);
        }
    }

    int mapType(int sqlType) {
        switch(sqlType) {
        case Types.SMALLINT:
        case Types.INTEGER:
        case Types.NULL:
        case Types.VARCHAR:
            return sqlType;
        case Types.TINYINT:
        case Types.BIT:
        case Types.BOOLEAN:
            return Types.INTEGER;
        case Types.BIGINT:
        case Types.BINARY:
        case Types.BLOB:
        case Types.CHAR:
        case Types.DATE:
        case Types.TIME:
        case Types.TIMESTAMP:
        case Types.DECIMAL:
        case Types.DOUBLE:
        case Types.FLOAT:
        case Types.JAVA_OBJECT:
        case Types.LONGVARBINARY:
        case Types.LONGVARCHAR:
        case Types.NUMERIC:
        case Types.OTHER:
        case Types.REAL:
        case Types.VARBINARY:
            return Types.VARCHAR;
        default:
            throw Message.getInternalError("sqlType "+sqlType);
        }

    }

    int getParametersCount(String sql) throws SQLException {
        if (sql == null || sql.indexOf('?') < 0) {
            return 0;
        }
        int len = sql.length();
        int param = 0;
        for (int i = 0; i < len; i++) {
            try {
                char c = sql.charAt(i);
                switch (c) {
                case '\'': {
                    int j = sql.indexOf('\'', i + 1);
                    if (j < 0) {
                        throw Message.getSyntaxError(sql, i);
                    }
                    i = j;
                    break;
                }
                case '"': {
                    int j = sql.indexOf('"', i + 1);
                    if (j < 0) {
                        throw Message.getSyntaxError(sql, i);
                    }
                    i = j;
                    break;
                }
                case '/': {
                    if (sql.charAt(i + 1) == '*') {
                        // block comment
                        int j = sql.indexOf("*/", i + 2);
                        if (j < 0) {
                            throw Message.getSyntaxError(sql, i);
                        }
                        i = j + 1;
                    } else if (sql.charAt(i + 1) == '/') {
                        // single line comment
                        i += 2;
                        while (i < len && (c = sql.charAt(i)) != '\r'
                                && c != '\n') {
                            i++;
                        }
                    }
                    break;
                }
                case '-':
                    if (sql.charAt(i + 1) == '-') {
                        // single line comment
                        i += 2;
                        while (i < len && (c = sql.charAt(i)) != '\r'
                                && c != '\n') {
                            i++;
                        }
                    }
                    break;
                case '?':
                    param++;
                    break;
                }
            } catch (ArrayIndexOutOfBoundsException e) {
                throw Message.getSyntaxError(sql, i);
            }
        }
        return param;
    }

    public void setThread(Thread thread) {
        this.thread = thread;
    }

    public Thread getThread() {
        return thread;
    }

}
