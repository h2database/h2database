/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.server.pg;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.h2.constant.SysProperties;
import org.h2.engine.Constants;
import org.h2.server.Service;
import org.h2.tools.Server;
import org.h2.util.MathUtils;
import org.h2.util.NetUtils;

/**
 * This class implements a subset of the PostgreSQL protocol as described here:
 * http://developer.postgresql.org/pgdocs/postgres/protocol.html
 * The PostgreSQL catalog is described here:
 * http://www.postgresql.org/docs/7.4/static/catalogs.html
 */
public class PgServer implements Service {

    public static final int DEFAULT_PORT = 5435; // also in the docs

    private int port = PgServer.DEFAULT_PORT;
    private boolean stop;
    private boolean trace;
    private ServerSocket serverSocket;
    private Set running = Collections.synchronizedSet(new HashSet());
    private String baseDir;
    private String url;
    private boolean allowOthers;
    private boolean ifExists;

    public void init(String[] args) throws Exception {
        port = DEFAULT_PORT;
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            if ("-trace".equals(a)) {
                trace = true;
            } else if ("-log".equals(a) && SysProperties.OLD_COMMAND_LINE_OPTIONS) {
                trace = Server.readArgBoolean(args, i) == 1;
                i++;
            } else if ("-pgPort".equals(a)) {
                port = MathUtils.decodeInt(args[++i]);
            } else if ("-baseDir".equals(a)) {
                baseDir = args[++i];
            } else if ("-pgAllowOthers".equals(a)) {
                if (Server.readArgBoolean(args, i) != 0) {
                    allowOthers = Server.readArgBoolean(args, i) == 1;
                    i++;
                } else {
                    allowOthers = true;
                }
            } else if ("-ifExists".equals(a)) {
                if (Server.readArgBoolean(args, i) != 0) {
                    ifExists = Server.readArgBoolean(args, i) == 1;
                    i++;
                } else {
                    ifExists = true;
                }
            }
        }
        org.h2.Driver.load();
        url = "pg://" + NetUtils.getLocalAddress() + ":" + port;

//        int testing;
//        log = true;
    }

    boolean getTrace() {
        return trace;
    }

    void trace(String s) {
        if (trace) {
            System.out.println(s);
        }
    }

    synchronized void remove(PgServerThread t) {
        running.remove(t);
    }

    void traceError(Exception e) {
        if (trace) {
            e.printStackTrace();
        }
    }

    public String getURL() {
        return url;
    }

    boolean allow(Socket socket) {
        if (allowOthers) {
            return true;
        }
        return NetUtils.isLoopbackAddress(socket);
    }

    public void start() throws SQLException {
        serverSocket = NetUtils.createServerSocket(port, false);
    }

    public void listen() {
        String threadName = Thread.currentThread().getName();
        try {
            while (!stop) {
                Socket s = serverSocket.accept();
                if (!allow(s)) {
                    trace("Connection not allowed");
                    s.close();
                } else {
                    PgServerThread c = new PgServerThread(s, this);
                    running.add(c);
                    c.setProcessId(running.size());
                    Thread thread = new Thread(c);
                    thread.setName(threadName+" thread");
                    c.setThread(thread);
                    thread.start();
                }
            }
        } catch (Exception e) {
            if (!stop) {
                e.printStackTrace();
            }
        }
    }

    public void stop() {
        // TODO server: combine with tcp server
        if (!stop) {
            stop = true;
            if (serverSocket != null) {
                try {
                    serverSocket.close();
                } catch (IOException e) {
                    // TODO log exception
                    e.printStackTrace();
                }
                serverSocket = null;
            }
        }
        // TODO server: using a boolean 'now' argument? a timeout?
        ArrayList list = new ArrayList(running);
        for (int i = 0; i < list.size(); i++) {
            PgServerThread c = (PgServerThread) list.get(i);
            c.close();
            try {
                Thread t = c.getThread();
                if (t != null) {
                    t.join(100);
                }
            } catch (Exception e) {
                // TODO log exception
                e.printStackTrace();
            }
        }
    }

    public boolean isRunning(boolean traceError) {
        if (serverSocket == null) {
            return false;
        }
        try {
            Socket s = NetUtils.createLoopbackSocket(serverSocket.getLocalPort(), false);
            s.close();
            return true;
        } catch (Exception e) {
            if (traceError) {
                traceError(e);
            }
            return false;
        }
    }

    public String getBaseDir() {
        return baseDir;
    }

    public boolean getAllowOthers() {
        return allowOthers;
    }

    public String getType() {
        return "PG";
    }

    public String getName() {
        return "H2 PG Server";
    }

    public boolean getIfExists() {
        return ifExists;
    }

    /**
     * The Java implementation of the PostgreSQL function pg_get_indexdef. The
     * method is used to get CREATE INDEX command for an index, or the column
     * definition of one column in the index.
     * 
     * @param conn the connection
     * @param indexId the index id
     * @param ordinalPosition the ordinal position (null if the SQL statement
     *            should be returned)
     * @param pretty this flag is ignored
     * @return the SQL statement or the column name
     */
    public static String getIndexColumn(Connection conn, int indexId, Integer ordinalPosition, Boolean pretty)
            throws SQLException {
        if (ordinalPosition == null || ordinalPosition.intValue() == 0) {
            PreparedStatement prep = conn.prepareStatement("select sql from information_schema.indexes where id=?");
            prep.setInt(1, indexId);
            ResultSet rs = prep.executeQuery();
            if (rs.next()) {
                return rs.getString(1);
            }
            return null;
        } else {
            PreparedStatement prep = conn.prepareStatement("select column_name from information_schema.indexes where id=? and ordinal_position=?");
            prep.setInt(1, indexId);
            prep.setInt(2, ordinalPosition.intValue());
            ResultSet rs = prep.executeQuery();
            if (rs.next()) {
                return rs.getString(1);
            }
            return null;
        }
    }

    public static String getCurrentSchema(Connection conn) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery("call schema()");
        rs.next();
        return rs.getString(1);
    }

    public static String getEncodingName(int code) throws SQLException {
        switch (code) {
        case 0:
            return "SQL_ASCII";
        case 6:
            return "UTF8";
        case 8:
            return "LATIN1";
        default:
            return code < 40 ? "UTF8" : "";
        }
    }

    public static String getVersion() {
        return "PostgreSQL 8.1.4  server protocol using H2 " + Constants.getVersion();
    }

    public static Timestamp getStartTime() {
        return new Timestamp(System.currentTimeMillis());
    }

    public static String getUserById(Connection conn, int id) throws SQLException {
        PreparedStatement prep = conn.prepareStatement("SELECT NAME FROM INFORMATION_SCHEMA.USERS WHERE ID=?");
        prep.setInt(1, id);
        ResultSet rs = prep.executeQuery();
        if (rs.next()) {
            return rs.getString(1);
        }
        return null;
    }

    public static boolean hasDatabasePrivilege(int id, String privilege) {
        return true;
    }

    public static boolean hasTablePrivilege(String table, String privilege) {
        return true;
    }

    public static int getCurrentTid(String table, String id) {
        return 1;
    }

}
