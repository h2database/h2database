/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.server.ftp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

import org.h2.engine.Constants;
import org.h2.util.StringUtils;

public class FtpControl extends Thread {
    
    private static final String SERVER_NAME = "Small FTP Server";
    
    private FtpServer server;
    private Socket control;
    private FtpData data;
    private PrintWriter output;
    private String userName;
    private boolean connected, readonly;
    private String currentDir = "/";
    private String serverIpAddress;
    private boolean stop;
    private String renameFrom;
    private boolean replied;
    private long restart;

    public FtpControl(Socket control, FtpServer server, boolean stop) {
        this.server = server;
        this.control = control;
        this.stop = stop;
    }
    
    public void run() {
        try {
            output = new PrintWriter(new OutputStreamWriter(control.getOutputStream(), Constants.UTF8));
            if(stop) {
                reply(421, "Too many users");
            } else {
                reply(220, SERVER_NAME);
                // TODO need option to configure the serverIpAddress
                serverIpAddress = control.getLocalAddress().getHostAddress().replace('.', ',');
                BufferedReader input = new BufferedReader(new InputStreamReader(control.getInputStream()));
                while(!stop) {
                    String command = null;
                    try {
                        command = input.readLine();
                    } catch(IOException e) {
                        // ignore
                    }
                    if(command == null) {
                        break;
                    }
                    process(command);
                }
                if(data != null) {
                    data.close();
                }                
            }
        } catch(Throwable t) {
            server.logError(t);
        }
        server.closeConnection();
    }

    private void process(String command) throws IOException {
        int idx = command.indexOf(' ');
        String param = "";
        if(idx >= 0) {
            param = command.substring(idx).trim();
            command = command.substring(0, idx);
        }
        command = StringUtils.toUpperEnglish(command);
        if(command.length() == 0) {
            reply(506, "No command");
            return;
        }
        server.log(">" + command);
        replied = false;
        if(connected) {
            processConnected(command, param);
        }
        if(!replied) {
            if("USER".equals(command)) {
                userName = param;
                reply(331, "Need password");
            } else if("QUIT".equals(command)) {
                reply(221, "Bye");
                stop = true;
            } else if("PASS".equals(command)) {
                if(userName == null) {
                    reply(332, "Need username");
                } else if(server.checkUserPassword(userName, param)) {
                    reply(230, "Ok");
                    readonly = false;
                    connected = true;
                } else if(server.checkUserPasswordReadOnly(userName, param)) {
                    reply(230, "Ok, readonly");
                    readonly = true;
                    connected = true;
                } else {
                    reply(431, "Wrong user/password");
                }
            } else if("REIN".equals(command)) {
                userName = null;
                connected = false;
                currentDir = "/";
                reply(200, "Ok");
            } else if("HELP".equals(command)) {
                reply(214, SERVER_NAME);
            }
        }            
        if(!replied) {            
            reply(506, "Invalid command");
        }
    }
    
    private void processConnected(String command, String param) throws IOException {
        switch(command.charAt(0)) {
        case 'C':
            if("CWD".equals(command)) {
                param = getFileName(param);
                FileObject file = server.getFile(param);
                if(file.exists() && file.isDirectory()) {
                    if(!param.endsWith("/")) {
                        param += "/";
                    }
                    currentDir = param;
                    reply(250, "Ok");
                } else {
                    reply(550, "Failed");
                }
            } else if("CDUP".equals(command)) {
                if(currentDir.length()>1) {
                    int idx = currentDir.lastIndexOf("/", currentDir.length()-2);
                    currentDir = currentDir.substring(0, idx+1);
                    reply(250, "Ok");
                } else {
                    reply(550, "Failed");
                }
            }
            break;
        case 'D':
            if("DELE".equals(command)) {
                FileObject file = server.getFile(getFileName(param));
                if(!readonly && file.exists() && file.isFile() && file.delete()) {
                    reply(250, "Ok");
                } else {
                    reply(500, "Delete failed");                
                }
            }
            break;
        case 'L':
            if("LIST".equals(command)) {
                processList(param, true);
            }
            break;
        case 'M':
            if("MKD".equals(command)) {
                param = getFileName(param);
                FileObject file = server.getFile(param);
                if(!readonly && file.mkdirs()) {
                    reply(257, "\"" +  param + "\" directory"); // TODO quote (" > "")
                } else {
                    reply(500, "Failed");
                }
            } else if("MODE".equals(command)) {
                if("S".equals(StringUtils.toUpperEnglish(param))) {
                    reply(200, "Ok");
                } else {
                    reply(504, "Invalid");
                }
            } else if("MDTM".equals(command)) {
                FileObject file = server.getFile(getFileName(param));
                if(file.exists() && file.isFile()) {
                    reply(213, server.formatLastModified(file));
                } else {
                    reply(550, "Failed");
                }
            }
            break;
        case 'N':
            if("NLST".equals(command)) {
                processList(param, false);
            } else if("NOOP".equals(command)) {
                reply(200, "Ok");
            }
            break;
        case 'P':
            if("PWD".equals(command)) {
                reply(257, "\"" +  currentDir + "\" directory"); // TODO quote (" > "")
            } else if("PASV".equals(command)) {
                ServerSocket dataSocket = server.createDataSocket();
                data = new FtpData(server, control.getInetAddress(), dataSocket);
                data.start();
                int port = dataSocket.getLocalPort();
                reply(227, "Passive Mode (" + serverIpAddress + "," + (port >> 8) + "," + (port & 255) + ")");
                // reply(501, ne.getMessage());
            }
            break;
        case 'R':
            if("RNFR".equals(command)) {
                param = getFileName(param);
                FileObject file = server.getFile(param);
                if(file.exists()) {
                    renameFrom = param;
                    reply(350, "Ok");
                } else {
                    reply(450, "Not found");                
                }
            } else if("RNTO".equals(command)) {
                if (renameFrom == null) {
                    reply(503, "RNFR required");
                } else {
                    FileObject fileOld = server.getFile(renameFrom);
                    FileObject fileNew = server.getFile(getFileName(param));
                    if(!readonly && fileOld.renameTo(fileNew)) {
                        reply(250, "Ok");
                    } else {
                        reply(550, "Failed");
                    }
                }
            } else if("RETR".equals(command)) {
                FileObject file = server.getFile(getFileName(param));
                if(file.exists() && file.isFile()) {
                    reply(150, "Starting transfer");
                    try {
                        data.send(file, restart);
                        reply(226, "Ok");
                    } catch(IOException e) {
                        reply(426, "Failed");
                    }
                    restart = 0;
                } else {
                    processList(param, true); // Firefox compatibility (still not good)
                    // reply(426, "Not a file");
                }
            } else if("RMD".equals(command)) {
                FileObject file = server.getFile(getFileName(param));
                if(!readonly && file.exists() && file.isDirectory() && file.delete()) {
                    reply(250, "Ok");
                } else {
                    reply(500, "Failed");
                }
            } else if("REST".equals(command)) {
                try {
                    restart = Integer.parseInt(param);
                    reply(350, "Ok");
                } catch(NumberFormatException e) {
                    reply(500, "Invalid");
                }
            }
            break;
        case 'S':
            if("SYST".equals(command)) {
                reply(215, "UNIX Type: L8");
            } else if("SITE".equals(command)) {
                reply(500, "Not understood");
            } else if("SIZE".equals(command)) {
                FileObject file = server.getFile(getFileName(param));
                if(file.exists() && file.isFile()) {
                    reply(250, String.valueOf(file.length()));
                } else {
                    reply(500, "Failed");
                }
            } else if("STOR".equals(command)) {
                FileObject file = server.getFile(getFileName(param));
                if(!readonly && !file.exists() || file.isFile()) {
                    reply(150, "Starting transfer");
                    try {
                        data.receive(file);
                        reply(226, "Ok");
                    } catch(IOException e) {
                        reply(426, "Failed");
                    }
                } else {
                    reply(550, "Failed");
                }
            } else if("STRU".equals(command)) {
                if("F".equals(StringUtils.toUpperEnglish(param))) {
                    reply(200, "Ok");
                } else {
                    reply(504, "Invalid");
                }
            }
            break;
        case 'T':
            if("TYPE".equals(command)) {
                param = StringUtils.toUpperEnglish(param);
                if("A".equals(param) || "A N".equals(param)) {
                    reply(200, "Ok");
                } else if("I".equals(param) || "L 8".equals(param)) {
                    reply(200, "Ok");
                } else {
                    reply(500, "Invalid");
                }
            }
            break;
        }
    }
    
    private String getFileName(String file) {
        return file.startsWith("/") ? file : currentDir + file;
    }
    
    private void processList(String param, boolean directories) throws IOException {
        FileObject directory = server.getFile(getFileName(param));
        if(!directory.exists()) {
            reply(450, "Directory does not exist");
            return;
        } else if(!directory.isDirectory()) {
            reply(450, "Not a directory");
            return;
        }
        String list = server.getDirectoryListing(directory, directories);
        reply(150, "Starting transfer");
        server.log(list);
        // need to use the current locale (UTF-8 would be wrong for the Windows Explorer)
        data.send(list.getBytes());
        reply(226, "Done");
    }
    
    private void reply(int code, String message) throws IOException {
        server.log(code + " " + message);
        output.print(code + " " + message + "\r\n");
        output.flush();
        replied = true;
    }
    
}
