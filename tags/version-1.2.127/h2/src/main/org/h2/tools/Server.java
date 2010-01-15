/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

import java.sql.Connection;
import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.message.Message;
import org.h2.message.TraceSystem;
import org.h2.server.Service;
import org.h2.server.ShutdownHandler;
import org.h2.server.TcpServer;
import org.h2.server.pg.PgServer;
import org.h2.server.web.WebServer;
import org.h2.util.StartBrowser;
import org.h2.util.Tool;

/**
 * Starts the H2 Console (web-) server, TCP, and PG server.
 * @h2.resource
 */
public class Server extends Tool implements Runnable, ShutdownHandler {

    private Service service;
    private Server web, tcp, pg;
    private ShutdownHandler shutdownHandler;

    public Server() {
        // nothing to do
    }

    /**
     * Create a new server for the given service.
     *
     * @param service the service
     * @param args the command line arguments
     */
    public Server(Service service, String... args) throws SQLException {
        this.service = service;
        try {
            service.init(args);
        } catch (Exception e) {
            throw Message.convert(e);
        }
    }

    /**
     * When running without options, -tcp, -web, -browser and -pg are started.
     * <br />
     * Options are case sensitive. Supported options are:
     * <table>
     * <tr><td>[-help] or [-?]</td>
     * <td>Print the list of options</td></tr>
     * <tr><td>[-web]</td>
     * <td>Start the web server with the H2 Console</td></tr>
     * <tr><td>[-webAllowOthers]</td>
     * <td>Allow other computers to connect - see below</td></tr>
     * <tr><td>[-webPort &lt;port&gt;]</td>
     * <td>The port (default: 8082)</td></tr>
     * <tr><td>[-webSSL]</td>
     * <td>Use encrypted (HTTPS) connections</td></tr>
     * <tr><td>[-browser]</td>
     * <td>Start a browser and open a page to connect to the web server</td></tr>
     * <tr><td>[-tcp]</td>
     * <td>Start the TCP server</td></tr>
     * <tr><td>[-tcpAllowOthers]</td>
     * <td>Allow other computers to connect - see below</td></tr>
     * <tr><td>[-tcpPort &lt;port&gt;]</td>
     * <td>The port (default: 9092)</td></tr>
     * <tr><td>[-tcpSSL]</td>
     * <td>Use encrypted (SSL) connections</td></tr>
     * <tr><td>[-tcpPassword &lt;pwd&gt;]</td>
     * <td>The password for shutting down a TCP server</td></tr>
     * <tr><td>[-tcpShutdown "&lt;url&gt;"]</td>
     * <td>Stop the TCP server; example: tcp://localhost:9094</td></tr>
     * <tr><td>[-tcpShutdownForce]</td>
     * <td>Do not wait until all connections are closed</td></tr>
     * <tr><td>[-pg]</td>
     * <td>Start the PG server</td></tr>
     * <tr><td>[-pgAllowOthers]</td>
     * <td>Allow other computers to connect - see below</td></tr>
     * <tr><td>[-pgPort &lt;port&gt;]</td>
     * <td>The port (default: 5435)</td></tr>
     * <tr><td>[-baseDir &lt;dir&gt;]</td>
     * <td>The base directory for H2 databases; for all servers</td></tr>
     * <tr><td>[-ifExists]</td>
     * <td>Only existing databases may be opened; for all servers</td></tr>
     * <tr><td>[-trace]</td>
     * <td>Print additional trace information; for all servers</td></tr>
     * </table>
     * The options -xAllowOthers are potentially risky.
     * <br />
     * For details, see Advanced Topics / Protection against Remote Access.
     * @h2.resource
     *
     * @param args the command line arguments
     */
    public static void main(String... args) throws SQLException {
        new Server().run(args);
    }

    public void run(String... args) throws SQLException {
        boolean tcpStart = false, pgStart = false, webStart = false;
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
                showUsage();
                return;
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
                    throwUnsupportedOption(arg);
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
                    throwUnsupportedOption(arg);
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
                    throwUnsupportedOption(arg);
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
                throwUnsupportedOption(arg);
            }
        }
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
        if (webStart) {
            web = createWebServer(args);
            web.setShutdownHandler(this);
            SQLException result = null;
            try {
                web.start();
            } catch (SQLException e) {
                result = e;
            }
            out.println(web.getStatus());
            // start browser in any case (even if the server is already running)
            // because some people don't look at the output,
            // but are wondering why nothing happens
            if (browserStart) {
                StartBrowser.openURL(web.getURL());
            }
            if (result != null) {
                throw result;
            }
        }
        if (tcpStart) {
            tcp = createTcpServer(args);
            tcp.start();
            out.println(tcp.getStatus());
        }
        if (pgStart) {
            pg = createPgServer(args);
            pg.start();
            out.println(pg.getStatus());
        }
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

    /**
     * Get the status of this server.
     *
     * @return the status
     */
    public String getStatus() {
        StringBuilder buff = new StringBuilder();
        if (isRunning(false)) {
            buff.append(service.getType()).
                append(" server running on ").
                append(service.getURL()).
                append(" (");
            if (service.getAllowOthers()) {
                buff.append("others can connect");
            } else {
                buff.append("only local connections");
            }
            buff.append(')');
        } else {
            buff.append("The ").
                append(service.getType()).
                append(" server could not be started. Possible cause: another server is already running on ").
                append(service.getURL());
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
    public static Server createWebServer(String... args) throws SQLException {
        WebServer service = new WebServer();
        Server server = new Server(service, args);
        service.setShutdownHandler(server);
        return server;
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
    public static Server createTcpServer(String... args) throws SQLException {
        return new Server(new TcpServer(), args);
    }

    /**
     * Create a new PG server, but does not start it yet.
     * Example:
     * <pre>
     * Server server =
     *     Server.createPgServer("-pgAllowOthers").start();
     * </pre>
     *
     * @param args the argument list
     * @return the server
     */
    public static Server createPgServer(String... args) throws SQLException {
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
        String name = service.getName() + " (" + service.getURL() + ")";
        t.setName(name);
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
        throw Message.getSQLException(ErrorCode.EXCEPTION_OPENING_PORT_2, name, "timeout");
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
