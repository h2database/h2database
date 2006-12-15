/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.server.ftp;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.h2.engine.Constants;
import org.h2.server.Service;
import org.h2.util.MathUtils;
import org.h2.util.NetUtils;

/**
 * Small FTP Server. Intended for ad-hoc networks in a secure environment.
 * See also http://cr.yp.to/ftp.html http://www.ftpguide.com/
 */
public class FtpServer implements Service {
    
    public static final String DEFAULT_ROOT = "ftp";
    public static final String DEFAULT_READ = "guest";
    public static final String DEFAULT_WRITE = "sa";
    public static final String DEFAULT_WRITE_PASSWORD = "sa";
    
    private ServerSocket serverSocket;
    private int port = Constants.DEFAULT_FTP_PORT;
    private int openConnectionCount;
    private int maxConnectionCount = 100;
    
    private SimpleDateFormat dateFormatNew = new SimpleDateFormat("MMM dd HH:mm", Locale.ENGLISH);
    private SimpleDateFormat dateFormatOld = new SimpleDateFormat("MMM dd  yyyy", Locale.ENGLISH);
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
    
    private String root = DEFAULT_ROOT;
    private String writeUserName = DEFAULT_WRITE, writePassword = DEFAULT_WRITE_PASSWORD;
    private String readUserName = DEFAULT_READ;
    
    private FileSystemDatabase db;
    private boolean log;
    
    public void listen() {
        try {
            while (serverSocket != null) {
                Socket s = serverSocket.accept();
                boolean stop;
                synchronized(this) {
                    openConnectionCount++;
                    stop = openConnectionCount > maxConnectionCount;
                }
                FtpControl c = new FtpControl(s, this, stop);
                c.start();
            }
        } catch (Exception e) {
            logError(e);
        }
    }
    
    void closeConnection() {
        synchronized(this) {
            openConnectionCount--;
        }
    }

    public ServerSocket createDataSocket() throws IOException {
        ServerSocket dataSocket = new ServerSocket(0);
        return dataSocket;
    }
    
    void appendFile(StringBuffer buff, FileObject f) {
        buff.append(f.isDirectory() ? 'd' : '-');
        buff.append(f.canRead() ? 'r' : '-');
        buff.append(f.canWrite() ? 'w' : '-');
        buff.append("------- 1 owner group ");
        String size = String.valueOf(f.length());
        for(int i = size.length(); i < 15; i++) {
            buff.append(' ');
        }
        buff.append(size);
        buff.append(' ');
        Date now = new Date(), mod = new Date(f.lastModified());
        if(mod.after(now) || Math.abs((now.getTime() - mod.getTime())/1000/60/60/24) > 180) {
            buff.append(dateFormatOld.format(mod));
        } else {
            buff.append(dateFormatNew.format(mod));
        }
        buff.append(' ');
        buff.append(f.getName());
        buff.append("\r\n");
    }

    String formatLastModified(FileObject file) {
        return dateFormat.format(new Date(file.lastModified()));
    }     
    
    FileObject getFile(String path) {
        if(path.indexOf("..") > 0) {
            path = "/";
        }
        while(path.startsWith("/") && root.endsWith("/")) {
            path = path.substring(1);
        }
        while(path.endsWith("/")) {
            path = path.substring(0, path.length()-1);
        }
        log("file: " + root + path);
        if(db != null) {
            return FileObjectDatabase.get(db, root + path);
        } else {
            return FileObjectNative.get(root + path);
        }
    }
    
    String getDirectoryListing(FileObject directory, boolean listDirectories) {
        FileObject[] list = directory.listFiles();
        StringBuffer buff = new StringBuffer();
        for(int i=0; list != null && i<list.length; i++) {
            FileObject f = list[i];
            if(f.isFile() || (f.isDirectory() && listDirectories)) {
                appendFile(buff, f);
            }
        }
        return buff.toString();
    }
    
    public boolean checkUserPassword(String userName, String param) {
        return userName.equals(this.writeUserName) && writePassword.equals(this.writePassword);
    }

    public boolean checkUserPasswordReadOnly(String userName, String param) {
        return userName.equals(this.readUserName);
    }

    public void init(String[] args) throws Exception {
        for(int i=0; args != null && i<args.length; i++) {
            if("-ftpPort".equals(args[i])) {
                port = MathUtils.decodeInt(args[++i]);
            } else if("-ftpDir".equals(args[i])) {
                root = args[++i];
            } else if("-ftpRead".equals(args[i])) {
                readUserName = args[++i];
            } else if("-ftpWrite".equals(args[i])) {
                writeUserName = args[++i];
            } else if("-ftpWritePassword".equals(args[i])) {
                writePassword = args[++i];
            } else if("-log".equals(args[i])) {
                log = Boolean.valueOf(args[++i]).booleanValue();
            }
        }
        if(root.startsWith("jdbc:")) {
            org.h2.Driver.load();
            Connection conn = DriverManager.getConnection(root);
            db = new FileSystemDatabase(conn, log);
            root = "/";
        }
    }

    public String getURL() {
        return "ftp://localhost:"+port;
    }

    public void start() throws SQLException {
        getFile("").mkdirs();
        serverSocket = NetUtils.createServerSocket(port, false);
    }

    public void stop() {
        try {
            serverSocket.close();
        } catch(IOException e) {
            logError(e);
        }
        serverSocket = null;
    }

    public boolean isRunning() {
        if(serverSocket == null) {
            return false;
        }
        try {
            Socket s = NetUtils.createLoopbackSocket(port, false);
            s.close();
            return true;
        } catch(Exception e) {
            return false;
        }        
    }

    public boolean getAllowOthers() {
        return true;
    }

    public String getType() {
        return "FTP";
    }
    
    void log(String s) {
        if(log) {
            System.out.println(s);
        }
    }
    
    void logError(Throwable e) {
        if (log) {
            e.printStackTrace();
        }
    }    

}
