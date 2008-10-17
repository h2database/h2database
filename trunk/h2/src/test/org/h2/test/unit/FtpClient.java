/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;

import org.h2.engine.Constants;
import org.h2.util.IOUtils;
import org.h2.util.NetUtils;
import org.h2.util.StringUtils;

/**
 * A simple standalone FTP client.
 */
public class FtpClient {
    private Socket socket;
    private BufferedReader reader;
    private PrintWriter writer;
    private int code;
    private String message;
    private Socket socketData;
    private InputStream inData;
    private OutputStream outData;

    private FtpClient() {
        // don't allow construction
    }

    /**
     * Open an FTP connection.
     * 
     * @param url the FTP URL
     * @return the ftp client object
     */
    static FtpClient open(String url) throws IOException {
        FtpClient client = new FtpClient();
        client.connect(url);
        return client;
    }

    private void connect(String url) throws IOException {
        socket = NetUtils.createSocket(url, 21, false);
        InputStream in = socket.getInputStream();
        OutputStream out = socket.getOutputStream();
        reader = new BufferedReader(new InputStreamReader(in));
        writer = new PrintWriter(new OutputStreamWriter(out, Constants.UTF8));
        readCode(220);
    }

    private void readLine() throws IOException {
        message = reader.readLine();
        if (message != null) {
            int idx = message.indexOf(' ');
            if (idx < 0) {
                code = 0;
            } else {
                code = Integer.parseInt(message.substring(0, idx));
                message = message.substring(idx + 1);
            }
        }
    }

    private void readCode(int expected) throws IOException {
        readLine();
        if (code != expected) {
            throw new IOException("Expected: " + expected + " got: " + message);
        }
    }

    private void send(String command) {
        writer.println(command);
        writer.flush();
    }

    /**
     * Login to this FTP server (USER, PASS, SYST, SITE, STRU F, TYPE I).
     * 
     * @param userName the user name
     * @param password the password
     */
    void login(String userName, String password) throws IOException {
        send("USER " + userName);
        readCode(331);
        send("PASS " + password);
        readCode(230);
        send("SYST");
        readCode(215);
        send("SITE");
        readCode(500);
        send("STRU F");
        readCode(200);
        send("TYPE I");
        readCode(200);
    }

    /**
     * Close the connection (QUIT).
     */
    void close() throws IOException {
        if (socket != null) {
            send("QUIT");
            readCode(221);
            socket.close();
        }
    }

    /**
     * Change the working directory (CWD).
     * 
     * @param dir the new directory
     */
    void changeWorkingDirectory(String dir) throws IOException {
        send("CWD " + dir);
        readCode(250);
    }

    /**
     * Change to the parent directory (CDUP).
     */
    void changeDirectoryUp() throws IOException {
        send("CDUP");
        readCode(250);
    }

    /**
     * Delete a file (DELE).
     * 
     * @param fileName the name of the file to delete
     */
    void delete(String fileName) throws IOException {
        send("DELE " + fileName);
        readCode(250);
    }

    /**
     * Create a directory (MKD).
     * 
     * @param dir the directory to create
     */
    void makeDirectory(String dir) throws IOException {
        send("MKD " + dir);
        readCode(257);
    }

    /**
     * Change the transfer mode (MODE).
     * 
     * @param mode the mode
     */
    void mode(String mode) throws IOException {
        send("MODE " + mode);
        readCode(200);
    }

    /**
     * Change the modified time of a file (MDTM).
     * 
     * @param fileName the file name
     */
    void modificationTime(String fileName) throws IOException {
        send("MDTM " + fileName);
        readCode(213);
    }
    
    /**
     * Issue a no-operation statement (NOOP).
     */
    void noOperation() throws IOException {
        send("NOOP");
        readCode(200);
    }

    /**
     * Print the working directory (PWD).
     */
    String printWorkingDirectory() throws IOException {
        send("PWD");
        readCode(257);
        return removeQuotes();
    }

    private String removeQuotes() {
        int first = message.indexOf('"') + 1;
        int last = message.lastIndexOf('"');
        StringBuffer buff = new StringBuffer();
        for (int i = first; i < last; i++) {
            char ch = message.charAt(i);
            buff.append(ch);
            if (ch == '\"') {
                i++;
            }
        }
        return buff.toString();
    }

    private void passive() throws IOException {
        send("PASV");
        readCode(227);
        int first = message.indexOf('(') + 1;
        int last = message.indexOf(')');
        String[] address = StringUtils.arraySplit(message.substring(first, last), ',', true);
        StringBuffer buff = new StringBuffer();
        for (int i = 0; i < 4; i++) {
            if (i > 0) {
                buff.append('.');
            }
            buff.append(address[i]);
        }
        String ip = buff.toString();
        InetAddress addr = InetAddress.getByName(ip);
        int port = (Integer.parseInt(address[4]) << 8) | Integer.parseInt(address[5]);
        socketData = NetUtils.createSocket(addr, port, false);
        inData = socketData.getInputStream();
        outData = socketData.getOutputStream();
    }

    /**
     * Rename a file (RNFR / RNTO).
     * 
     * @param fromFileName the old file name
     * @param toFileName the new file name
     */
    void rename(String fromFileName, String toFileName) throws IOException {
        send("RNFR " + fromFileName);
        readCode(350);
        send("RNTO " + toFileName);
        readCode(250);
    }

    /**
     * Read a file ([REST] RETR).
     * 
     * @param fileName the file name
     * @param out the output stream
     * @param restartAt restart at the given position (0 if no restart is required).
     */
    void retrieve(String fileName, OutputStream out, long restartAt) throws IOException {
        passive();
        if (restartAt > 0) {
            send("REST " + restartAt);
            readCode(350);
        }
        send("RETR " + fileName);
        IOUtils.copyAndClose(inData, out);
        readCode(226);
    }

    /**
     * Remove a directory (RMD).
     * 
     * @param dir the directory to remove
     */
    void removeDirectory(String dir) throws IOException {
        send("RMD " + dir);
        readCode(250);
    }

    /**
     * Get the size of a file (SIZE).
     * 
     * @param fileName the file name
     * @return the size
     */
    long size(String fileName) throws IOException {
        send("SIZE " + fileName);
        readCode(250);
        long size = Long.parseLong(message);
        return size;
    }

    /**
     * Store a file (STOR).
     * 
     * @param fileName the file name
     * @param in the input stream
     */
    void store(String fileName, InputStream in) throws IOException {
        passive();
        send("STOR " + fileName);
        readCode(150);
        IOUtils.copyAndClose(in, outData);
        readCode(226);
    }

    /**
     * Get the directory listing (NLST).
     * 
     * @param dir the directory
     * @return the listing
     */
    String nameList(String dir) throws IOException {
        passive();
        send("NLST " + dir);
        readCode(150);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IOUtils.copyAndClose(inData, out);
        readCode(226);
        byte[] data = out.toByteArray();
        return new String(data);
    }

    /**
     * Get the directory listing (LIST).
     * 
     * @param dir the directory
     * @return the listing
     */
    String list(String dir) throws IOException {
        passive();
        send("LIST " + dir);
        readCode(150);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IOUtils.copyAndClose(inData, out);
        readCode(226);
        byte[] data = out.toByteArray();
        return new String(data);
    }

}
