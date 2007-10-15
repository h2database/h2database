/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.h2.Driver;
import org.h2.engine.Constants;
import org.h2.message.TraceSystem;
import org.h2.util.JdbcUtils;
import org.h2.util.MathUtils;
import org.h2.util.NetUtils;

public class TcpServer implements Service {

    // TODO new feature: implement automatic client / server mode if 'socket'
    // file locking is used
    // TODO better exception message if the port is already in use, maybe
    // automatically use the next free port?

    public static final int DEFAULT_PORT = 9092;
    public static final int SHUTDOWN_NORMAL = 0;
    public static final int SHUTDOWN_FORCE = 1;
    public static boolean logInternalErrors;

    private int port;
    private boolean log;
    private boolean ssl;
    private boolean stop;
    private ServerSocket serverSocket;
    private Set running = Collections.synchronizedSet(new HashSet());
    private String baseDir;
    private String url;
    private boolean allowOthers;
    private boolean ifExists;
    private Connection managementDb;
    private String managementPassword = "";
    private static HashMap servers = new HashMap();
    private Thread listenerThread;

    public static String getManagementDbName(int port) {
        return "mem:" + Constants.MANAGEMENT_DB_PREFIX + port;
    }

    private void initManagementDb() throws SQLException {
        Properties prop = new Properties();
        prop.setProperty("user", "sa");
        prop.setProperty("password", managementPassword);
        // avoid using the driver manager
        Connection conn = Driver.load().connect("jdbc:h2:" + getManagementDbName(port), prop);
        managementDb = conn;
        Statement stat = null;
        try {
            stat = conn.createStatement();
            stat.execute("CREATE ALIAS IF NOT EXISTS STOP_SERVER FOR \"" + TcpServer.class.getName() + ".stopServer\"");
        } finally {
            JdbcUtils.closeSilently(stat);
        }
        servers.put("" + port, this);
    }

    private void stopManagementDb() {
        synchronized (TcpServer.class) {
            if (managementDb != null) {
                try {
                    managementDb.close();
                } catch (SQLException e) {
                    TraceSystem.traceThrowable(e);
                }
                managementDb = null;
            }
        }
    }

    public void init(String[] args) throws Exception {
        port = DEFAULT_PORT;
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            if ("-log".equals(a)) {
                log = Boolean.valueOf(args[++i]).booleanValue();
            } else if ("-tcpSSL".equals(a)) {
                ssl = Boolean.valueOf(args[++i]).booleanValue();
            } else if ("-tcpPort".equals(a)) {
                port = MathUtils.decodeInt(args[++i]);
            } else if ("-tcpPassword".equals(a)) {
                managementPassword = args[++i];
            } else if ("-baseDir".equals(a)) {
                baseDir = args[++i];
            } else if ("-tcpAllowOthers".equals(a)) {
                allowOthers = Boolean.valueOf(args[++i]).booleanValue();
            } else if ("-ifExists".equals(a)) {
                ifExists = Boolean.valueOf(args[++i]).booleanValue();
            }
        }
        org.h2.Driver.load();
        url = (ssl ? "ssl" : "tcp") + "://localhost:" + port;
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
        synchronized (TcpServer.class) {
            serverSocket = NetUtils.createServerSocket(port, ssl);
            initManagementDb();
        }
    }

    public void listen() {
        listenerThread = Thread.currentThread();
        String threadName = listenerThread.getName();
        try {
            while (!stop) {
                Socket s = serverSocket.accept();
                TcpServerThread c = new TcpServerThread(s, this);
                running.add(c);
                Thread thread = new Thread(c);
                thread.setName(threadName + " thread");
                c.setThread(thread);
                thread.start();
            }
            serverSocket.close();
        } catch (Exception e) {
            if (!stop) {
                TraceSystem.traceThrowable(e);
            }
        }
        stopManagementDb();
    }

    public boolean isRunning() {
        if (serverSocket == null) {
            return false;
        }
        try {
            Socket s = NetUtils.createLoopbackSocket(port, ssl);
            s.close();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public synchronized void stop() {
        // TODO server: share code between web and tcp servers
        if (!stop) {
            stopManagementDb();
            stop = true;
            if (serverSocket != null) {
                try {
                    serverSocket.close();
                } catch (IOException e) {
                    TraceSystem.traceThrowable(e);
                }
                serverSocket = null;
            }
            if (listenerThread != null) {
                try {
                    listenerThread.join(1000);
                } catch (InterruptedException e) {
                    TraceSystem.traceThrowable(e);
                }
            }
        }
        // TODO server: using a boolean 'now' argument? a timeout?
        ArrayList list = new ArrayList(running);
        for (int i = 0; i < list.size(); i++) {
            TcpServerThread c = (TcpServerThread) list.get(i);
            c.close();
            try {
                c.getThread().join(100);
            } catch (Exception e) {
                TraceSystem.traceThrowable(e);
            }
        }
        servers.remove("" + port);
    }

    public static synchronized void stopServer(int port, String password, int shutdownMode) {
        TcpServer server = (TcpServer) servers.get("" + port);
        if (server == null) {
            return;
        }
        if (!server.managementPassword.equals(password)) {
            return;
        }
        if (shutdownMode == TcpServer.SHUTDOWN_NORMAL) {
            server.stopManagementDb();
            server.stop = true;
            try {
                Socket s = new Socket("localhost", port);
                s.close();
            } catch (Exception e) {
                // try to connect - so that accept returns
            }
        } else if (shutdownMode == TcpServer.SHUTDOWN_FORCE) {
            server.stop();
        }
    }

    synchronized void remove(TcpServerThread t) {
        running.remove(t);
    }

    boolean getLog() {
        return log;
    }

    String getBaseDir() {
        return baseDir;
    }

    void log(String s) {
        // TODO log: need concept for server log
        if (log) {
            System.out.println(s);
        }
    }

    void logError(Throwable e) {
        if (log) {
            e.printStackTrace();
        }
    }

    public boolean getAllowOthers() {
        return allowOthers;
    }

    public String getType() {
        return "TCP";
    }

    public void logInternalError(String string) {
        if (TcpServer.logInternalErrors) {
            System.out.println(string);
            new Error(string).printStackTrace();
        }
    }

    public boolean getIfExists() {
        return ifExists;
    }

}
