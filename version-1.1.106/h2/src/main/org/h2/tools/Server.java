/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.engine.Constants;
import org.h2.message.Message;
import org.h2.message.TraceSystem;
import org.h2.server.Service;
import org.h2.server.ShutdownHandler;
import org.h2.server.TcpServer;
import org.h2.server.ftp.FtpServer;
import org.h2.server.pg.PgServer;
import org.h2.server.web.WebServer;
import org.h2.util.StartBrowser;
import org.h2.util.Tool;

/**
 * This tool can be used to start various database servers (listeners).
 */
public class Server implements Runnable, ShutdownHandler {

    private static final int EXIT_ERROR = 1;
    private Service service;
    private Server web, tcp, pg, ftp;
    private ShutdownHandler shutdownHandler;

    public Server() {
        // nothing to do
    }

    private Server(Service service, String[] args) throws SQLException {
        this.service = service;
        try {
            service.init(args);
        } catch (Exception e) {
            throw Message.convert(e);
        }
    }

    private void showUsage(String a, PrintStream out) {
        if (a != null) {
            out.println("Unsupported option: " + a);
            out.println();
        }
        out.println("Starts H2 Servers");
        out.println("By default, -tcp, -web, -browser and -pg are started. Options are case sensitive.");
        out.println("java "+getClass().getName());
        out.println("-web                  Start the Web Server and H2 Console");
        out.println("-webAllowOthers       Allow other computers to connect");
        out.println("-webPort <port>       The port (default: " + Constants.DEFAULT_HTTP_PORT+")");
        out.println("-webSSL               Use encrypted HTTPS connections");
        out.println("-browser              Start a browser to connect to the H2 Console");
        out.println("-tcp                  Start the TCP Server");
        out.println("-tcpAllowOthers       Allow other computers to connect");
        out.println("-tcpPort <port>       The port (default: " + TcpServer.DEFAULT_PORT+")");
        out.println("-tcpSSL               Use encrypted SSL connections");
        out.println("-tcpPassword <pass>   The password for shutting down a TCP Server");
        out.println("-tcpShutdown <url>    Shutdown the TCP Server; example: tcp://localhost:9094");
        out.println("-tcpShutdownForce     Don't wait for other connections to close");
        out.println("-pg                   Start the PG Server");
        out.println("-pgAllowOthers        Allow other computers to connect");
        out.println("-pgPort <port>        The port (default: " + PgServer.DEFAULT_PORT+")");
        out.println("-ftp                  Start the FTP Server");
        out.println("-ftpPort <port>       The port (default: " + Constants.DEFAULT_FTP_PORT+")");
        out.println("-ftpDir <dir>         The base directory (default: " + FtpServer.DEFAULT_ROOT + ")");
        out.println("-ftpRead <user>       The user name for reading (default: " + FtpServer.DEFAULT_READ+")");
        out.println("-ftpWrite <user>      The user name for writing (default: " + FtpServer.DEFAULT_WRITE+")");
        out.println("-ftpWritePassword <p> The write password (default: " + FtpServer.DEFAULT_WRITE_PASSWORD+")");
        out.println("-baseDir <dir>        The base directory for H2 databases; for all servers");
        out.println("-ifExists             Only existing databases may be opened; for all servers");
        out.println("-trace                Print additional trace information; for all servers");
        out.println("See also http://h2database.com/javadoc/" + getClass().getName().replace('.', '/') + ".html");
    }

    /**
     * The command line interface for this tool. The options must be split into
     * strings like this: "-baseDir", "/temp/data",... By default, -tcp, -web,
     * -browser and -pg are started. If there is a problem starting a service,
     * the program terminates with an exit code of 1. Options are case
     * sensitive. The following options are supported:
     * <ul>
     * <li>-help or -? (print the list of options) </li>
     * <li>-web (start the Web Server and H2 Console) </li>
     * <li>-browser (start a browser and open a page to connect to the
     *     Web Server) </li>
     * <li>-tcp (start the TCP Server) </li>
     * <li>-tcpShutdown {url} (shutdown the running TCP Server,
     *     URL example: tcp://localhost:9094) </li>
     * <li>-pg (start the PG Server) </li>
     * <li>-ftp (start the FTP Server) </li>
     * <li>-trace (print additional trace information; for all servers) </li>
     * <li>-baseDir {directory} (sets the base directory for H2 databases;
     *     for all servers) </li>
     * <li>-ifExists (only existing databases may be opened;
     *     for all servers) </li>
     * </ul>
     * For each Server, additional options are available:
     * <ul>
     * <li>-webPort {port} (the port of Web Server, default: 8082) </li>
     * <li>-webSSL (HTTPS is to be be used) </li>
     * <li>-webAllowOthers (enable remote connections)
     * </li>
     * <li>-tcpPort {port} (the port of TCP Server, default: 9092) </li>
     * <li>-tcpSSL (SSL is to be used) </li>
     * <li>-tcpAllowOthers (enable remote connections)
     * </li>
     * <li>-tcpPassword {password} (the password for shutting down a TCP
     * Server) </li>
     * <li>-tcpShutdownForce (don't wait for other connections to
     * close) </li>
     * <li>-pgPort {port} (the port of PG Server, default: 5435) </li>
     * <li>-pgAllowOthers (enable remote connections)
     * </li>
     * <li>-ftpPort {port} </li>
     * <li>-ftpDir {directory} </li>
     * <li>-ftpRead {readUserName} </li>
     * <li>-ftpWrite {writeUserName} </li>
     * <li>-ftpWritePassword {password} </li>
     * </ul>
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
            String arg = args[i];
            if (arg == null) {
                continue;
            } else if ("-?".equals(arg) || "-help".equals(arg)) {
                showUsage(null, out);
                return EXIT_ERROR;
            } else if (arg.startsWith("-web")) {
                if ("-web".equals(arg)) {
                    startDefaultServers = false;
                    webStart = true;
                } else if ("-webAllowOthers".equals(arg)) {
                    if (Tool.readArgBoolean(args, i) != 0) {
                        i++;
                    }
                } else if ("-webSSL".equals(arg)) {
                    if (Tool.readArgBoolean(args, i) != 0) {
                        i++;
                    }
                } else if ("-webPort".equals(arg)) {
                    i++;
                } else if ("-webScript".equals(arg)) {
                    i++;
                } else {
                    showUsage(arg, out);
                    return EXIT_ERROR;
                }
            } else if ("-browser".equals(arg)) {
                startDefaultServers = false;
                browserStart = true;
            } else if (arg.startsWith("-tcp")) {
                if ("-tcp".equals(arg)) {
                    startDefaultServers = false;
                    tcpStart = true;
                } else if ("-tcpAllowOthers".equals(arg)) {
                    if (Tool.readArgBoolean(args, i) != 0) {
                        i++;
                    }
                } else if ("-tcpSSL".equals(arg)) {
                    if (Tool.readArgBoolean(args, i) != 0) {
                        i++;
                    }
                } else if ("-tcpPort".equals(arg)) {
                    i++;
                } else if ("-tcpPassword".equals(arg)) {
                    tcpPassword = args[++i];
                } else if ("-tcpShutdown".equals(arg)) {
                    startDefaultServers = false;
                    tcpShutdown = true;
                    tcpShutdownServer = args[++i];
                } else if ("-tcpShutdownForce".equals(arg)) {
                    if (Tool.readArgBoolean(args, i) != 0) {
                        tcpShutdownForce = Tool.readArgBoolean(args, i) == 1;
                        i++;
                    } else {
                        tcpShutdownForce = true;
                    }
                } else {
                    showUsage(arg, out);
                    return EXIT_ERROR;
                }
            } else if (arg.startsWith("-pg")) {
                if ("-pg".equals(arg)) {
                    startDefaultServers = false;
                    pgStart = true;
                } else if ("-pgAllowOthers".equals(arg)) {
                    if (Tool.readArgBoolean(args, i) != 0) {
                        i++;
                    }
                } else if ("-pgPort".equals(arg)) {
                    i++;
                } else {
                    showUsage(arg, out);
                    return EXIT_ERROR;
                }
            } else if (arg.startsWith("-ftp")) {
                if ("-ftp".equals(arg)) {
                    startDefaultServers = false;
                    ftpStart = true;
                } else if ("-ftpPort".equals(arg)) {
                    i++;
                } else if ("-ftpDir".equals(arg)) {
                    i++;
                } else if ("-ftpRead".equals(arg)) {
                    i++;
                } else if ("-ftpWrite".equals(arg)) {
                    i++;
                } else if ("-ftpWritePassword".equals(arg)) {
                    i++;
                } else if ("-ftpTask".equals(arg)) {
                    // no parameters
                } else {
                    showUsage(arg, out);
                    return EXIT_ERROR;
                }
            } else if ("-trace".equals(arg)) {
                // no parameters
            } else if ("-log".equals(arg) && SysProperties.OLD_COMMAND_LINE_OPTIONS) {
                i++;
            } else if ("-ifExists".equals(arg)) {
                if (Tool.readArgBoolean(args, i) != 0) {
                    i++;
                }
            } else if ("-baseDir".equals(arg)) {
                i++;
            } else {
                showUsage(arg, out);
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
     * Shutdown a TCP server. If force is set to false, the server will not
     * allow new connections, but not kill existing connections, instead it will
     * stop if the last connection is closed. If force is set to true, existing
     * connections are killed. After calling the method with force=false, it is
     * not possible to call it again with force=true because new connections are
     * not allowed. Example:
     *
     * <pre>
     * Server.shutdownTcpServer(&quot;tcp://localhost:9094&quot;, password, true);
     * </pre>
     *
     * @param url example: tcp://localhost:9094
     * @param password the password to use ("" for no password)
     * @param force the shutdown (don't wait)
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    public static void shutdownTcpServer(String url, String password, boolean force) throws SQLException {
        TcpServer.shutdown(url, password, force);
    }

    String getStatus() {
        StringBuffer buff = new StringBuffer();
        if (isRunning(false)) {
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
            buff.append("The " + service.getType() + " server could not be started. Possible cause: another server is already running on ");
            buff.append(service.getURL());
        }
        return buff.toString();
    }

    /**
     * Create a new web server, but does not start it yet. Example:
     *
     * <pre>
     * Server server = Server.createWebServer(
     *     new String[] { &quot;-trace&quot; }).start();
     * </pre>
     *
     * @param args the argument list
     * @return the server
     */
    public static Server createWebServer(String[] args) throws SQLException {
        WebServer service = new WebServer();
        Server server = new Server(service, args);
        service.setShutdownHandler(server);
        return server;
    }

    /**
     * Create a new ftp server, but does not start it yet. Example:
     *
     * <pre>
     * Server server = Server.createFtpServer(
     *     new String[] { &quot;-trace&quot; }).start();
     * </pre>
     *
     * @param args the argument list
     * @return the server
     */
    public static Server createFtpServer(String[] args) throws SQLException {
        return new Server(new FtpServer(), args);
    }

    /**
     * Create a new TCP server, but does not start it yet. Example:
     *
     * <pre>
     * Server server = Server.createTcpServer(
     *     new String[] { &quot;-tcpAllowOthers&quot; }).start();
     * </pre>
     *
     * @param args the argument list
     * @return the server
     */
    public static Server createTcpServer(String[] args) throws SQLException {
        return new Server(new TcpServer(), args);
    }

    /**
     * Create a new PG server, but does not start it yet.
     * Example:
     * <pre>
     * Server server =
     *     Server.createPgServer(new String[]{
     *         "-pgAllowOthers"}).start();
     * </pre>
     *
     * @param args the argument list
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
            if (isRunning(false)) {
                return this;
            }
        }
        if (isRunning(true)) {
            return this;
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
        if (web != null && web.isRunning(false)) {
            web.stop();
            web = null;
        }
        if (tcp != null && tcp.isRunning(false)) {
            tcp.stop();
            tcp = null;
        }
        if (pg != null && pg.isRunning(false)) {
            pg.stop();
            pg = null;
        }
        if (ftp != null && ftp.isRunning(false)) {
            ftp.stop();
            ftp = null;
        }
    }

    /**
     * Checks if the server is running.
     *
     * @param traceError if errors should be written
     * @return if the server is running
     */
    public boolean isRunning(boolean traceError) {
        return service.isRunning(traceError);
    }

    /**
     * Stops the server.
     */
    public void stop() {
        service.stop();
    }

    /**
     * Gets the URL of this server.
     *
     * @return the url
     */
    public String getURL() {
        return service.getURL();
    }

    /**
     * Gets the port this server is listening on.
     *
     * @return the port
     */
    public int getPort() {
        return service.getPort();
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

    /**
     * Get the service attached to this server.
     *
     * @return the service
     */
    public Service getService() {
        return service;
    }

    /**
     * Start a web server and a browser that uses the given connection. The
     * current transaction is preserved. This is specially useful to manually
     * inspect the database when debugging.
     *
     * @param conn the database connection (the database must be open)
     */
    public static void startWebServer(Connection conn) throws SQLException {
        final Object waitUntilDisconnected = new Object();
        WebServer webServer = new WebServer();
        Server server = new Server(webServer, new String[] { "-webPort", "0" });
        webServer.setShutdownHandler(new ShutdownHandler() {
            public void shutdown() {
                synchronized (waitUntilDisconnected) {
                    waitUntilDisconnected.notifyAll();
                }
            }
        });
        server.start();
        String url = webServer.addSession(conn);
        StartBrowser.openURL(url);
        synchronized (waitUntilDisconnected) {
            try {
                waitUntilDisconnected.wait();
            } catch (InterruptedException e) {
                // ignore
            }
        }
        webServer.stop();
    }

}
