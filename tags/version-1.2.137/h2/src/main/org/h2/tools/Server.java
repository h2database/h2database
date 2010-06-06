/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;
import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.message.DbException;
import org.h2.message.TraceSystem;
import org.h2.server.Service;
import org.h2.server.ShutdownHandler;
import org.h2.server.TcpServer;
import org.h2.server.pg.PgServer;
import org.h2.server.web.WebServer;
import org.h2.util.StringUtils;
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
            throw DbException.toSQLException(e);
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
     * <tr><td>[-webDaemon]</td>
     * <td>Use a daemon thread</td></tr>
     * <tr><td>[-webPort &lt;port&gt;]</td>
     * <td>The port (default: 8082)</td></tr>
     * <tr><td>[-webSSL]</td>
     * <td>Use encrypted (HTTPS) connections</td></tr>
     * <tr><td>[-browser]</td>
     * <td>Start a browser connecting to the web server</td></tr>
     * <tr><td>[-tcp]</td>
     * <td>Start the TCP server</td></tr>
     * <tr><td>[-tcpAllowOthers]</td>
     * <td>Allow other computers to connect - see below</td></tr>
     * <tr><td>[-tcpDaemon]</td>
     * <td>Use a daemon thread</td></tr>
     * <tr><td>[-tcpPort &lt;port&gt;]</td>
     * <td>The port (default: 9092)</td></tr>
     * <tr><td>[-tcpSSL]</td>
     * <td>Use encrypted (SSL) connections</td></tr>
     * <tr><td>[-tcpPassword &lt;pwd&gt;]</td>
     * <td>The password for shutting down a TCP server</td></tr>
     * <tr><td>[-tcpShutdown "&lt;url&gt;"]</td>
     * <td>Stop the TCP server; example: tcp://localhost</td></tr>
     * <tr><td>[-tcpShutdownForce]</td>
     * <td>Do not wait until all connections are closed</td></tr>
     * <tr><td>[-pg]</td>
     * <td>Start the PG server</td></tr>
     * <tr><td>[-pgAllowOthers]</td>
     * <td>Allow other computers to connect - see below</td></tr>
     * <tr><td>[-pgDaemon]</td>
     * <td>Use a daemon thread</td></tr>
     * <tr><td>[-pgPort &lt;port&gt;]</td>
     * <td>The port (default: 5435)</td></tr>
     * <tr><td>[-baseDir &lt;dir&gt;]</td>
     * <td>The base directory for H2 databases (all servers)</td></tr>
     * <tr><td>[-ifExists]</td>
     * <td>Only existing databases may be opened (all servers)</td></tr>
     * <tr><td>[-trace]</td>
     * <td>Print additional trace information (all servers)</td></tr>
     * </table>
     * The options -xAllowOthers are potentially risky.
     * <br />
     * For details, see Advanced Topics / Protection against Remote Access.
     * @h2.resource
     *
     * @param args the command line arguments
     */
    public static void main(String... args) throws SQLException {
        new Server().runTool(args);
    }

    public void runTool(String... args) throws SQLException {
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
                    // no parameters
                } else if ("-webDaemon".equals(arg)) {
                    // no parameters
                } else if ("-webSSL".equals(arg)) {
                    // no parameters
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
                    // no parameters
                } else if ("-tcpDaemon".equals(arg)) {
                    // no parameters
                } else if ("-tcpSSL".equals(arg)) {
                    // no parameters
                } else if ("-tcpPort".equals(arg)) {
                    i++;
                } else if ("-tcpPassword".equals(arg)) {
                    tcpPassword = args[++i];
                } else if ("-tcpShutdown".equals(arg)) {
                    startDefaultServers = false;
                    tcpShutdown = true;
                    tcpShutdownServer = args[++i];
                } else if ("-tcpShutdownForce".equals(arg)) {
                    tcpShutdownForce = true;
                } else {
                    throwUnsupportedOption(arg);
                }
            } else if (arg.startsWith("-pg")) {
                if ("-pg".equals(arg)) {
                    startDefaultServers = false;
                    pgStart = true;
                } else if ("-pgAllowOthers".equals(arg)) {
                    // no parameters
                } else if ("-pgDaemon".equals(arg)) {
                    // no parameters
                } else if ("-pgPort".equals(arg)) {
                    i++;
                } else {
                    throwUnsupportedOption(arg);
                }
            } else if ("-trace".equals(arg)) {
                // no parameters
            } else if ("-ifExists".equals(arg)) {
                // no parameters
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
            shutdownTcpServer(tcpShutdownServer, tcpPassword, tcpShutdownForce, false);
        }
        if (webStart) {
            web = createWebServer(args);
            web.setShutdownHandler(this);
            SQLException result = null;
            try {
                web.start();
            } catch (Exception e) {
                result = DbException.toSQLException(e);
            }
            out.println(web.getStatus());
            // start browser in any case (even if the server is already running)
            // because some people don't look at the output,
            // but are wondering why nothing happens
            if (browserStart) {
                Server.openBrowser(web.getURL());
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
     * Shutdown one or all TCP server. If force is set to false, the server will not
     * allow new connections, but not kill existing connections, instead it will
     * stop if the last connection is closed. If force is set to true, existing
     * connections are killed. After calling the method with force=false, it is
     * not possible to call it again with force=true because new connections are
     * not allowed. Example:
     *
     * <pre>
     * Server.shutdownTcpServer(
     *     &quot;tcp://localhost:9094&quot;, password, true, false);
     * </pre>
     *
     * @param url example: tcp://localhost:9094
     * @param password the password to use ("" for no password)
     * @param force the shutdown (don't wait)
     * @param all whether all TCP servers that are running in the JVM
     *                  should be stopped
     */
    public static void shutdownTcpServer(String url, String password, boolean force, boolean all) throws SQLException {
        TcpServer.shutdown(url, password, force, all);
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
        try {
            service.start();
            Thread t = new Thread(this);
            t.setDaemon(service.isDaemon());
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
            throw DbException.get(ErrorCode.EXCEPTION_OPENING_PORT_2, name, "timeout");
        } catch (DbException e) {
            throw DbException.toSQLException(e);
        }
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
     * Open a new browser tab or window with the given URL.
     *
     * @param url the URL to open
     */
    public static void openBrowser(String url) {
        String osName = SysProperties.getStringSetting("os.name", "linux").toLowerCase();
        Runtime rt = Runtime.getRuntime();
        try {
            String browser = SysProperties.BROWSER;
            if (browser != null) {
                if (browser.indexOf("%url") >= 0) {
                    String[] args = StringUtils.arraySplit(browser, ',', false);
                    for (int i = 0; i < args.length; i++) {
                        args[i] = StringUtils.replaceAll(args[i], "%url", url);
                    }
                    rt.exec(args);
                } else if (osName.indexOf("windows") >= 0) {
                    rt.exec(new String[] { "cmd.exe", "/C",  browser, url });
                } else {
                    rt.exec(new String[] { browser, url });
                }
                return;
            }
            try {
                Class< ? > desktopClass = Class.forName("java.awt.Desktop");
                // Desktop.isDesktopSupported()
                Boolean supported = (Boolean) desktopClass.
                    getMethod("isDesktopSupported").
                    invoke(null, new Object[0]);
                URI uri = new URI(url);
                if (supported) {
                    // Desktop.getDesktop();
                    Object desktop = desktopClass.getMethod("getDesktop").
                        invoke(null, new Object[0]);
                    // desktop.browse(uri);
                    desktopClass.getMethod("browse", URI.class).
                        invoke(desktop, uri);
                    return;
                }
            } catch (Exception e) {
                // ignore
            }
            if (osName.indexOf("windows") >= 0) {
                rt.exec(new String[] { "rundll32", "url.dll,FileProtocolHandler", url });
            } else if (osName.indexOf("mac") >= 0) {
                // Mac OS: to open a page with Safari, use "open -a Safari"
                Runtime.getRuntime().exec(new String[] { "open", url });
            } else {
                String[] browsers = { "firefox", "mozilla-firefox", "mozilla", "konqueror", "netscape", "opera" };
                boolean ok = false;
                for (String b : browsers) {
                    try {
                        rt.exec(new String[] { b, url });
                        ok = true;
                        break;
                    } catch (Exception e) {
                        // ignore and try the next
                    }
                }
                if (!ok) {
                    // No success in detection.
                    System.out.println("Please open a browser and go to " + url);
                }
            }
        } catch (IOException e) {
            System.out.println("Failed to start a browser to open the URL " + url);
            e.printStackTrace();
        }
    }

    /**
     * Start a web server and a browser that uses the given connection. The
     * current transaction is preserved. This is specially useful to manually
     * inspect the database when debugging. This method return as soon as the
     * user has disconnected.
     *
     * @param conn the database connection (the database must be open)
     */
    public static void startWebServer(Connection conn) throws SQLException {
        WebServer webServer = new WebServer();
        Server web = new Server(webServer, new String[] { "-webPort", "0" });
        web.start();
        Server server = new Server();
        server.web = web;
        webServer.setShutdownHandler(server);
        String url = webServer.addSession(conn);
        Server.openBrowser(url);
        while (!webServer.isStopped()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

}
