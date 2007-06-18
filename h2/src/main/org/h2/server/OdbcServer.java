/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.server;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;

import org.h2.util.MathUtils;
import org.h2.util.NetUtils;

/**
 * @author Thomas
 */
public class OdbcServer implements Service {

    public static final int DEFAULT_PORT = 9083; // also in the docs

    private int port = OdbcServer.DEFAULT_PORT;
    private boolean stop;
    private boolean log;
    private ServerSocket serverSocket;
    private HashSet running = new HashSet();
    private String baseDir;
    private String url;
    private boolean allowOthers;
    private boolean ifExists;

    boolean getLog() {
        return log;
    }

    void log(String s) {
        if (log) {
            System.out.println(s);
        }
    }

    synchronized void remove(OdbcServerThread t) {
        running.remove(t);
    }

    void logError(Exception e) {
        if (log) {
            e.printStackTrace();
        }
    }

    public void init(String[] args) throws Exception {
        port = DEFAULT_PORT;
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            if (a.equals("-log")) {
                log = Boolean.valueOf(args[++i]).booleanValue();
            } else if (a.equals("-odbcPort")) {
                port = MathUtils.decodeInt(args[++i]);
            } else if (a.equals("-baseDir")) {
                baseDir = args[++i];
            } else if (a.equals("-odbcAllowOthers")) {
                allowOthers = Boolean.valueOf(args[++i]).booleanValue();
            } else if (a.equals("-ifExists")) {
                ifExists = Boolean.valueOf(args[++i]).booleanValue();
            }
        }
        org.h2.Driver.load();
        url = "odbc://localhost:" + port;
    }

    public String getURL() {
        return url;
    }

    boolean allow(Socket socket) {
        if(allowOthers) {
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
                if(!allow(s)) {
                    log("Connection not allowed");
                    s.close();
                } else {
                    OdbcServerThread c = new OdbcServerThread(s, this);
                    running.add(c);
                    Thread thread = new Thread(c);
                    thread.setName(threadName+" thread");
                    c.setThread(thread);
                    thread.start();
                }
            }
        } catch (Exception e) {
            if(!stop) {
                e.printStackTrace();
            }
        }
    }

    public void stop() {
        // TODO server: share code between web and tcp servers
        if(!stop) {
            stop = true;
            if(serverSocket != null) {
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
        for(int i=0; i<list.size(); i++) {
            OdbcServerThread c = (OdbcServerThread) list.get(i);
            c.close();
            try {
                c.getThread().join(100);
            } catch(Exception e) {
                // TODO log exception
                e.printStackTrace();
            }
        }
    }

    public boolean isRunning() {
        if(serverSocket == null) {
            return false;
        }
        try {
            Socket s = NetUtils.createSocket(InetAddress.getLocalHost(), serverSocket.getLocalPort(), false);
            s.close();
            return true;
        } catch(Exception e) {
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
        return "ODBC";
    }

    public boolean getIfExists() {
        return ifExists;
    }

}
