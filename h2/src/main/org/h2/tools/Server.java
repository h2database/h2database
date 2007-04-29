/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.h2.engine.Constants;
import org.h2.message.Message;
import org.h2.message.TraceSystem;
import org.h2.server.OdbcServer;
import org.h2.server.Service;
import org.h2.server.TcpServer;
import org.h2.server.ftp.FtpServer;
import org.h2.server.web.WebServer;
import org.h2.util.JdbcUtils;
import org.h2.util.MathUtils;
import org.h2.util.StartBrowser;

/**
 * This tool can be used to start various database servers (listeners).
 */
public class Server implements Runnable {
    
    private String name;
    private Service service;
    private static final int EXIT_ERROR = 1;
    
    private void showUsage() {
        System.out.println("java "+getClass().getName() + " [options]");
        System.out.println("By default, -tcp, -web, -browser and -odbc are started");
        System.out.println("-tcp (start the TCP Server)");
        System.out.println("-tcpPort <port> (default: " + TcpServer.DEFAULT_PORT+")");
        System.out.println("-tcpSSL [true|false]");        
        System.out.println("-tcpAllowOthers [true|false]");
        System.out.println("-tcpPassword {password} (the password for shutting down a TCP Server)");
        System.out.println("-tcpShutdown {url} (shutdown the TCP Server, URL example: tcp://localhost:9094)");
        System.out.println("-tcpShutdownForce [true|false] (don't wait for other connections to close)");

        System.out.println("-web (start the Web Server)");
        System.out.println("-webPort <port> (default: " + Constants.DEFAULT_HTTP_PORT+")");
        System.out.println("-webSSL [true|false}");
        System.out.println("-webAllowOthers [true|false}");
        System.out.println("-browser (start a browser)");

        System.out.println("-odbc (start the ODBC Server)");
        System.out.println("-odbcPort <port> (default: " + OdbcServer.DEFAULT_PORT+")");
        System.out.println("-odbcAllowOthers [true|false]");        

        System.out.println("-ftp (start the FTP Server)");
        System.out.println("-ftpPort <port> (default: " + Constants.DEFAULT_FTP_PORT+")");
        System.out.println("-ftpDir <directory> (default: " + FtpServer.DEFAULT_ROOT+", use jdbc:... to access a database)");        
        System.out.println("-ftpRead <readUserName> (default: " + FtpServer.DEFAULT_READ+")");
        System.out.println("-ftpWrite <writeUserName> (default: " + FtpServer.DEFAULT_WRITE+")");
        System.out.println("-ftpWritePassword <password> (default: " + FtpServer.DEFAULT_WRITE_PASSWORD+")");        

        System.out.println("-log [true|false]");
        System.out.println("-baseDir <directory>");
        System.out.println("-ifExists [true|false] (only existing databases may be opened)");
    }
    
    private Server() {
    }
    
    /**
     * The command line interface for this tool.
     * The options must be split into strings like this: "-baseDir", "/temp/data",...
     * By default, -tcp, -web, -browser and -odbc are started.
     * If there is a problem starting a service, the program terminates with an exit code of 1.
     * The following options are supported:
     * <ul>
     * <li>-help or -? (print the list of options)
     * </li><li>-web (start the Web Server / H2 Console application)
     * </li><li>-tcp (start the TCP Server)
     * </li><li>-tcpShutdown {url} (shutdown the running TCP Server, URL example: tcp://localhost:9094)
     * </li><li>-odbc (start the ODBC Server)
     * </li><li>-browser (start a browser and open a page to connect to the Web Server)
     * </li><li>-log [true|false] (enable or disable logging)
     * </li><li>-baseDir {directory} (sets the base directory for database files; not for H2 Console)
     * </li><li>-ifExists [true|false] (only existing databases may be opened)
     * </li><li>-ftp (start the FTP Server)
     * </li></ul>
     * For each Server, there are additional options available:
     * <ul>
     * <li>-webPort {port} (the port of Web Server, default: 8082)
     * </li><li>-webSSL [true|false] (if SSL should be used)
     * </li><li>-webAllowOthers [true|false] (enable/disable remote connections)
     * </li><li>-tcpPort {port} (the port of TCP Server, default: 9092)
     * </li><li>-tcpSSL [true|false] (if SSL should be used)
     * </li><li>-tcpAllowOthers [true|false] (enable/disable remote connections)
     * </li><li>-tcpPassword {password} (the password for shutting down a TCP Server)
     * </li><li>-tcpShutdownForce [true|false] (don't wait for other connections to close)
     * </li><li>-odbcPort {port} (the port of ODBC Server, default: 9083)
     * </li><li>-odbcAllowOthers [true|false] (enable/disable remote connections)
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
        int exitCode = new Server().run(args);
        if(exitCode != 0) {
            System.exit(exitCode);
        }
    }

    private int run(String[] args) throws SQLException {
        boolean tcpStart = false, odbcStart = false, webStart = false, ftpStart = false;
        boolean browserStart = false;
        boolean tcpShutdown = false, tcpShutdownForce = false;
        String tcpPassword = "";
        String tcpShutdownServer = "";
        boolean startDefaultServers = true;
        for(int i=0; args != null && i<args.length; i++) {
            String a = args[i];
            if(a.equals("-?") || a.equals("-help")) {
                showUsage();
                return EXIT_ERROR;
            } else if(a.equals("-web")) {
                startDefaultServers = false;
                webStart = true;
            } else if(a.equals("-odbc")) {
                startDefaultServers = false;
                odbcStart = true;
            } else if(a.equals("-tcp")) {
                startDefaultServers = false;
                tcpStart = true;
            } else if(a.equals("-ftp")) {
                startDefaultServers = false;
                ftpStart = true;
            } else if(a.equals("-tcpShutdown")) {
                startDefaultServers = false;
                tcpShutdown = true;
                tcpShutdownServer = args[++i];
            } else if(a.equals("-tcpPassword")) {
                tcpPassword = args[++i];
            } else if(a.equals("-tcpShutdownForce")) {
                tcpShutdownForce = true;
            } else if(a.equals("-browser")) {
                startDefaultServers = false;
                browserStart = true;
            }
        }
        int exitCode = 0;
        if(startDefaultServers) {
            tcpStart = true;
            odbcStart = true;
            webStart = true;
            browserStart = true;
        }
        // TODO server: maybe use one single properties file?
        if(tcpShutdown) {
            System.out.println("Shutting down TCP Server at " + tcpShutdownServer);
            shutdownTcpServer(tcpShutdownServer, tcpPassword, tcpShutdownForce);
        }
        if(tcpStart) {
            Server tcp = createTcpServer(args);
            try {
                tcp.start();
            } catch(SQLException e) {
                // ignore (status is displayed)
                e.printStackTrace();
                exitCode = EXIT_ERROR;
            }
            System.out.println(tcp.getStatus());
        }
        if(odbcStart) {
            Server odbc = createOdbcServer(args);
            try {
                odbc.start();
            } catch(SQLException e) {
                // ignore (status is displayed)
                e.printStackTrace();
                exitCode = EXIT_ERROR;
            }            
            System.out.println(odbc.getStatus());
        }
        if(webStart) {
            Server web = createWebServer(args);
            try {
                web.start();
            } catch(SQLException e) {
                // ignore (status is displayed)
                e.printStackTrace();
                exitCode = EXIT_ERROR;
            }       
            System.out.println(web.getStatus());
            // start browser anyway (even if the server is already running) 
            // because some people don't look at the output, 
            // but are wondering why nothing happens
            if(browserStart) {
                StartBrowser.openURL(web.getURL());
            }
        }
        if(ftpStart) {
            Server ftp = createFtpServer(args);
            try {
                ftp.start();
            } catch(SQLException e) {
                // ignore (status is displayed)
                e.printStackTrace();
                exitCode = EXIT_ERROR;
            }       
            System.out.println(ftp.getStatus());
        }
        return exitCode;
    }
    
    /**
     * Shutdown a TCP server.
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
        if(idx >= 0) {
            String p = url.substring(idx+1);
            idx = p.indexOf('/');
            if(idx >= 0) {
                p = p.substring(0, idx);
            }
            port = MathUtils.decodeInt(p);
        }
        String db = TcpServer.getManagementDbName(port);
        try {
            org.h2.Driver.load();
        } catch(Throwable e) {
            throw Message.convert(e);
        }
        for(int i=0; i<2; i++) {
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
                } catch(SQLException e) {
                    if(force) {
                        // ignore
                    } else {
                        throw e;
                    }
                }
                break;
            } catch(SQLException e) {
                if(i == 1) {
                    throw e;
                }
            } finally {
                JdbcUtils.closeSilently(prep);
                JdbcUtils.closeSilently(conn);
            }
        }
    }
    
    private String getStatus() {
        StringBuffer buff = new StringBuffer();
        if(isRunning()) {
             buff.append(service.getType());
             buff.append(" server running on ");
             buff.append(service.getURL());
             buff.append(" (");
             if(service.getAllowOthers()) {
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
     * @param args
     * @return the server
     */
    public static Server createWebServer(String[] args) throws SQLException {
        return new Server("H2 Console Server", new WebServer(), args);
    }

    /**
     * Create a new ftp server, but does not start it yet.
     * @param args
     * @return the server
     */
    public static Server createFtpServer(String[] args) throws SQLException {
        return new Server("H2 FTP Server", new FtpServer(), args);
    }

    /**
     * Create a new TCP server, but does not start it yet.
     * @param args
     * @return the server
     */
    public static Server createTcpServer(String[] args) throws SQLException {
        return new Server("H2 TCP Server", new TcpServer(), args);
    }
    
    /**
     * Create a new TCP server, but does not start it yet.
     * @param args
     * @return the server
     */
    public static Server createOdbcServer(String[] args) throws SQLException {
        return new Server("H2 ODBC Server", new OdbcServer(), args);
    }
    
    /**
     * Tries to start the server.
     * @return the server if successful
     * @throws SQLException if the server could not be started
     */
    public Server start() throws SQLException {
        service.start();
        Thread t = new Thread(this);
        t.setName(name);
        t.start();
        for(int i=1; i<64; i+=i) {
            wait(i);
            if(isRunning()) {
                return this;
            }
        }
        throw Message.getSQLException(Message.CONNECTION_BROKEN);
    }
    
    private static void wait(int i) {
        try {
            // sleep at most 4096 ms
            long sleep = (long)i * (long)i;
            Thread.sleep(sleep);
        } catch (InterruptedException e) {
            // ignore
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

    private Server(String name, Service service, String[] args) throws SQLException {
        this.name = name;
        this.service = service;
        try {
            service.init(args);
        } catch(Exception e) {
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
}
