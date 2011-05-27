/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.engine.Constants;
import org.h2.message.Message;
import org.h2.message.TraceSystem;
import org.h2.server.Service;
import org.h2.server.ShutdownHandler;
import org.h2.server.TcpServer;
import org.h2.server.ftp.FtpServer;
import org.h2.server.pg.PgServer;
import org.h2.server.web.WebServer;
import org.h2.util.JdbcUtils;
import org.h2.util.MathUtils;
import org.h2.util.StartBrowser;

/**
 * This tool can be used to start various database servers (listeners).
 */
public class Server implements Runnable, ShutdownHandler {
    
    private Service service;
    private static final int EXIT_ERROR = 1;
    private Server web, tcp, pg, ftp;
    private ShutdownHandler shutdownHandler;
    
    private void showUsage(String a, PrintStream out) {
        if (a != null) {
            out.println("Unknown option: " + a);
            out.println();
        }
        out.println("java "+getClass().getName() + " [options]");
        out.println("By default, -tcp, -web, -browser and -pg are started.");
        out.println("Options are case sensitive. Options:");
        out.println();
        out.println("-web (start the Web Server and H2 Console)");
        out.println("-webAllowOthers [true|false}");
        out.println("-webPort <port> (default: " + Constants.DEFAULT_HTTP_PORT+")");
        out.println("-webSSL [true|false}");
        out.println();
        out.println("-browser (start a browser to connect to the H2 Console)");
        out.println();
        out.println("-tcp (start the TCP Server)");
        out.println("-tcpAllowOthers {true|false}");
        out.println("-tcpPort <port> (default: " + TcpServer.DEFAULT_PORT+")");
        out.println("-tcpSSL {true|false}");        
        out.println("-tcpPassword {password} (the password for shutting down a TCP Server)");
        out.println("-tcpShutdown {url} (shutdown the TCP Server, URL example: tcp://localhost:9094)");
        out.println("-tcpShutdownForce {true|false} (don't wait for other connections to close)");
        out.println();
        out.println("-pg (start the PG Server)");
        out.println("-pgAllowOthers {true|false}");        
        out.println("-pgPort <port> (default: " + PgServer.DEFAULT_PORT+")");
        out.println();
        out.println("-ftp (start the FTP Server)");
        out.println("-ftpPort <port> (default: " + Constants.DEFAULT_FTP_PORT+")");
        out.println("-ftpDir <directory> (default: " + FtpServer.DEFAULT_ROOT+", use jdbc:... to access a database)");        
        out.println("-ftpRead <readUserName> (default: " + FtpServer.DEFAULT_READ+")");
        out.println("-ftpWrite <writeUserName> (default: " + FtpServer.DEFAULT_WRITE+")");
        out.println("-ftpWritePassword <password> (default: " + FtpServer.DEFAULT_WRITE_PASSWORD+")");        
        out.println();
        out.println("-log {true|false} (enable or disable logging, for all servers)");
        out.println("-baseDir <directory> (sets the base directory for H2 databases, for all servers)");
        out.println("-ifExists {true|false} (only existing databases may be opened, for all servers)");
    }
    
    public Server() {
    }
    
    /**
     * The command line interface for this tool.
     * The options must be split into strings like this: "-baseDir", "/temp/data",...
     * By default, -tcp, -web, -browser and -pg are started.
     * If there is a problem starting a service, the program terminates with an exit code of 1.
     * Options are case sensitive.
     * The following options are supported:
     * <ul>
     * <li>-help or -? (print the list of options)
     * </li><li>-web (start the Web Server and H2 Console)
     * </li><li>-tcp (start the TCP Server)
     * </li><li>-tcpShutdown {url} (shutdown the running TCP Server, URL example: tcp://localhost:9094)
     * </li><li>-pg (start the PG Server)
     * </li><li>-browser (start a browser and open a page to connect to the Web Server)
     * </li><li>-log {true|false} (enable or disable logging)
     * </li><li>-baseDir {directory} (sets the base directory for database files; not for H2 Console)
     * </li><li>-ifExists {true|false} (only existing databases may be opened)
     * </li><li>-ftp (start the FTP Server)
     * </li></ul>
     * For each Server, additional options are available:
     * <ul>
     * <li>-webPort {port} (the port of Web Server, default: 8082)
     * </li><li>-webSSL {true|false} (if SSL should be used)
     * </li><li>-webAllowOthers {true|false} (enable/disable remote connections)
     * </li><li>-tcpPort {port} (the port of TCP Server, default: 9092)
     * </li><li>-tcpSSL {true|false} (if SSL should be used)
     * </li><li>-tcpAllowOthers {true|false} (enable/disable remote connections)
     * </li><li>-tcpPassword {password} (the password for shutting down a TCP Server)
     * </li><li>-tcpShutdownForce {true|false} (don't wait for other connections to close)
     * </li><li>-pgPort {port} (the port of PG Server, default: 5435)
     * </li><li>-pgAllowOthers {true|false} (enable/disable remote connections)
     * </li><li>-ftpPort {port}
     * </li><li>-ftpDir {directory}
     * </li><li>-ftpRead  {readUserName}
     * </li><li>-ftpWrite {writeUserName}
     * </li><li>-ftpWritePassword {password}
     * </li></ul>
     * 
     * @param args the command line arguments
     * @throws SQLException
     */
    public static void main(String[] args) throws SQLException {
        int exitCode = new Server().run(args, System.out);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    /**
     * INTERNAL
     */
    public int run(String[] args, PrintStream out) throws SQLException {
        boolean tcpStart = false, pgStart = false, webStart = false, ftpStart = false;
        boolean browserStart = false;
        boolean tcpShutdown = false, tcpShutdownForce = false;
        String tcpPassword = "";
        String tcpShutdownServer = "";
        boolean startDefaultServers = true;
        for (int i = 0; args != null && i < args.length; i++) {
            String a = args[i];
            if (a == null) {
                continue;
            } else if ("-?".equals(a) || "-help".equals(a)) {
                showUsage(null, out);
                return EXIT_ERROR;
            } else if (a.startsWith("-web")) {
                if ("-web".equals(a)) {
                    startDefaultServers = false;
                    webStart = true;
                } else if ("-webAllowOthers".equals(a)) {
                    i++;
                } else if ("-webPort".equals(a)) {
                    i++;
                } else if ("-webSSL".equals(a)) {
                    i++;
                } else {
                    showUsage(a, out);
                    return EXIT_ERROR;
                }
            } else if ("-browser".equals(a)) {
                startDefaultServers = false;
                browserStart = true;
            } else if (a.startsWith("-tcp")) {
                if ("-tcp".equals(a)) {
                    startDefaultServers = false;
                    tcpStart = true;
                } else if ("-tcpAllowOthers".equals(a)) {
                    i++;
                } else if ("-tcpPort".equals(a)) {
                    i++;
                } else if ("-tcpSSL".equals(a)) {
                    i++;
                } else if ("-tcpPassword".equals(a)) {
                    tcpPassword = args[++i];
                } else if ("-tcpShutdown".equals(a)) {
                    startDefaultServers = false;
                    tcpShutdown = true;
                    tcpShutdownServer = args[++i];
                } else if ("-tcpShutdownForce".equals(a)) {
                    tcpShutdownForce = Boolean.valueOf(args[++i]).booleanValue();
                } else {
                    showUsage(a, out);
                    return EXIT_ERROR;
                }
            } else if (a.startsWith("-pg")) {
                if ("-pg".equals(a)) {
                    startDefaultServers = false;
                    pgStart = true;
                } else if ("-pgAllowOthers".equals(a)) {
                    i++;
                } else if ("-pgPort".equals(a)) {
                    i++;
                } else {
                    showUsage(a, out);
                    return EXIT_ERROR;
                }
            } else if (a.startsWith("-ftp")) {
                if ("-ftp".equals(a)) {
                    startDefaultServers = false;
                    ftpStart = true;
                } else if ("-ftpPort".equals(a)) {
                    i++;
                } else if ("-ftpDir".equals(a)) {
                    i++;
                } else if ("-ftpRead".equals(a)) {
                    i++;
                } else if ("-ftpWrite".equals(a)) {
                    i++;
                } else if ("-ftpWritePassword".equals(a)) {
                    i++;
                } else if ("-ftpTask".equals(a)) {
                    i++;
                } else {
                    showUsage(a, out);
                    return EXIT_ERROR;
                }
            } else if (a.startsWith("-log")) {
                i++;
            } else if ("-baseDir".equals(a)) {
                i++;
            } else if ("-ifExists".equals(a)) {
                i++;
            } else {
                showUsage(a, out);
                return EXIT_ERROR;
            }
        }
        int exitCode = 0;
        if (startDefaultServers) {
            tcpStart = true;
            pgStart = true;
            webStart = true;
            browserStart = true;
        }
        // TODO server: maybe use one single properties file?
        if (tcpShutdown) {
            out.println("Shutting down TCP Server at " + tcpShutdownServer);
            shutdownTcpServer(tcpShutdownServer, tcpPassword, tcpShutdownForce);
        }
        if (tcpStart) {
            tcp = createTcpServer(args);
            try {
                tcp.start();
            } catch (SQLException e) {
                // ignore (status is displayed)
                e.printStackTrace();
                exitCode = EXIT_ERROR;
            }
            out.println(tcp.getStatus());
        }
        if (pgStart) {
            pg = createPgServer(args);
            try {
                pg.start();
            } catch (SQLException e) {
                // ignore (status is displayed)
                e.printStackTrace();
                exitCode = EXIT_ERROR;
            }
            out.println(pg.getStatus());
        }
        if (webStart) {
            web = createWebServer(args);
            web.setShutdownHandler(this);
            try {
                web.start();
            } catch (SQLException e) {
                // ignore (status is displayed)
                e.printStackTrace();
                exitCode = EXIT_ERROR;
            }       
            out.println(web.getStatus());
            // start browser anyway (even if the server is already running) 
            // because some people don't look at the output, 
            // but are wondering why nothing happens
            if (browserStart) {
                StartBrowser.openURL(web.getURL());
            }
        }
        if (ftpStart) {
            ftp = createFtpServer(args);
            try {
                ftp.start();
            } catch (SQLException e) {
                // ignore (status is displayed)
                e.printStackTrace();
                exitCode = EXIT_ERROR;
            }
            out.println(ftp.getStatus());
        }
        return exitCode;
    }
    
    /**
     * Shutdown a TCP server. If force is set to false, the server will not allow new connections,
     * but not kill existing connections, instead it will stop if the last connection is closed. 
     * If force is set to true, existing connections are killed.
     * After calling the method with force=false, it is not possible to call it again with
     * force=true because new connections are not allowed.
     * Example:
     * <pre>Server.shutdownTcpServer("tcp://localhost:9094", password, true);</pre>
     * 
     * @param url example: tcp://localhost:9094
     * @param password the password to use ("" for no password)
     * @param force the shutdown (don't wait)
     * @throws ClassNotFoundException
     * @throws SQLException 
     */    
    public static void shutdownTcpServer(String url, String password, boolean force) throws SQLException {
        int port = Constants.DEFAULT_SERVER_PORT;
        int idx = url.indexOf(':', "jdbc:h2:".length());
        if (idx >= 0) {
            String p = url.substring(idx + 1);
            idx = p.indexOf('/');
            if (idx >= 0) {
                p = p.substring(0, idx);
            }
            port = MathUtils.decodeInt(p);
        }
        String db = TcpServer.getManagementDbName(port);
        try {
            org.h2.Driver.load();
        } catch (Throwable e) {
            throw Message.convert(e);
        }
        for (int i = 0; i < 2; i++) {
            Connection conn = null;
            PreparedStatement prep = null;
            try {
                conn = DriverManager.getConnection("jdbc:h2:" + url + "/" + db, "sa", password);
                prep = conn.prepareStatement("CALL STOP_SERVER(?, ?, ?)");
                prep.setInt(1, port);
                prep.setString(2, password);
                prep.setInt(3, force ? TcpServer.SHUTDOWN_FORCE : TcpServer.SHUTDOWN_NORMAL);
                try {
                    prep.execute();
                } catch (SQLException e) {
                    if (force) {
                        // ignore
                    } else {
                        throw e;
                    }
                }
                break;
            } catch (SQLException e) {
                if (i == 1) {
                    throw e;
                }
            } finally {
                JdbcUtils.closeSilently(prep);
                JdbcUtils.closeSilently(conn);
            }
        }
    }

    String getStatus() {
        StringBuffer buff = new StringBuffer();
        if (isRunning()) {
            buff.append(service.getType());
            buff.append(" server running on ");
            buff.append(service.getURL());
            buff.append(" (");
            if (service.getAllowOthers()) {
                buff.append("others can connect");
            } else {
                buff.append("only local connections");
            }
            buff.append(")");
        } else {
            buff.append("Port is in use, maybe another " + service.getType() + " server already running on ");
            buff.append(service.getURL());
        }
        return buff.toString();
    }

    /**
     * Create a new web server, but does not start it yet.
     * Example:
     * <pre>Server server = Server.createWebServer(new String[]{"-log", "true"}).start();</pre>
     * 
     * @param args
     * @return the server
     */
    public static Server createWebServer(String[] args) throws SQLException {
        WebServer service = new WebServer();
        Server server = new Server(service, args);
        service.setShutdownHandler(server);
        return server;
    }

    /**
     * Create a new ftp server, but does not start it yet.
     * Example:
     * <pre>Server server = Server.createFtpServer(new String[]{"-log", "true"}).start();</pre>
     * 
     * @param args
     * @return the server
     */
    public static Server createFtpServer(String[] args) throws SQLException {
        return new Server(new FtpServer(), args);
    }

    /**
     * Create a new TCP server, but does not start it yet.
     * Example:
     * <pre>Server server = Server.createTcpServer(new String[]{"-tcpAllowOthers", "true"}).start();</pre>
     * 
     * @param args
     * @return the server
     */
    public static Server createTcpServer(String[] args) throws SQLException {
        return new Server(new TcpServer(), args);
    }
    
    /**
     * Create a new PG server, but does not start it yet.
     * Example:
     * <pre>Server server = Server.createPgServer(new String[]{"-pgAllowOthers", "true"}).start();</pre>
     * 
     * @param args
     * @return the server
     */
    public static Server createPgServer(String[] args) throws SQLException {
        return new Server(new PgServer(), args);
    }
    
    /**
     * Tries to start the server.
     * @return the server if successful
     * @throws SQLException if the server could not be started
     */
    public Server start() throws SQLException {
        service.start();
        Thread t = new Thread(this);
        t.setName(service.getName() + " (" + service.getURL() + ")");
        t.start();
        for (int i = 1; i < 64; i += i) {
            wait(i);
            if (isRunning()) {
                return this;
            }
        }
        throw Message.getSQLException(ErrorCode.CONNECTION_BROKEN);
    }

    private static void wait(int i) {
        try {
            // sleep at most 4096 ms
            long sleep = (long) i * (long) i;
            Thread.sleep(sleep);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    private void stopAll() {
        if (web != null && web.isRunning()) {
            web.stop();
            web = null;
        }
        if (tcp != null && tcp.isRunning()) {
            tcp.stop();
            tcp = null;
        }
        if (pg != null && pg.isRunning()) {
            pg.stop();
            pg = null;
        }
        if (ftp != null && ftp.isRunning()) {
            ftp.stop();
            ftp = null;
        }
    }

    /**
     * Checks if the server is running.
     * 
     * @return if the server is running
     */
    public boolean isRunning() {
        return service.isRunning();
    }

    /**
     * Stops the server.
     */
    public void stop() {
        service.stop();
    }
    
    /**
     * Gets the URL of this server.
     * @return the url
     */
    public String getURL() {
        return service.getURL();
    }

    private Server(Service service, String[] args) throws SQLException {
        this.service = service;
        try {
            service.init(args);
        } catch (Exception e) {
            throw Message.convert(e);
        }
    }

    /**
     * INTERNAL
     */
    public void run() {
        try {
            service.listen();
        } catch (Exception e) {
            TraceSystem.traceThrowable(e);
        }
    }
    
    /**
     * INTERNAL
     */
    public void setShutdownHandler(ShutdownHandler shutdownHandler) {
        this.shutdownHandler = shutdownHandler;
    }

    /**
     * INTERNAL
     */    
    public void shutdown() {
        if (shutdownHandler != null) {
            shutdownHandler.shutdown();
        } else {
            stopAll();
        }
    }
}
