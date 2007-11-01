package org.h2.tools.ftp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.sql.SQLException;
import java.util.ArrayList;

import org.h2.engine.Constants;
import org.h2.util.NetUtils;

public class FtpClient {
    private Socket socket;
    private BufferedReader reader;
    private PrintWriter writer;
    private int code;
    private String message;
    
    public static FtpClient open(String url) throws SQLException, IOException {
        FtpClient client = new FtpClient();
        client.connect(url);
        return client;
    }
    
    private FtpClient() {
    }

    private void connect(String url) throws SQLException, IOException {
        socket = NetUtils.createSocket(url, 21, false);
        InputStream in = socket.getInputStream();
        OutputStream out = socket.getOutputStream();
        reader = new BufferedReader(new InputStreamReader(in));
        writer = new PrintWriter(new OutputStreamWriter(out, Constants.UTF8));
        readCode(220);
    }
    
    private void readLine() throws IOException {
        message = reader.readLine();
        int idx = message.indexOf(' ');
        if (idx < 0) {
            code = 0;
        } else {
            code = Integer.parseInt(message.substring(0, idx));
        }
    }
    
    private void readCode(int expected) throws IOException {
        readLine();
        if (code != expected) {
            throw new IOException("Expected: " + expected + " got: " + message);
        }
    }
    
    private void send(String command) throws IOException {
        writer.println(command);
        writer.flush();
    }
    
    public void sendUser(String userName) throws IOException {
        send("USER " + userName);
        readCode(331);
    }
    
    public void sendQuit() throws IOException {
        send("QUIT");
        readCode(221);
    }
    
    public void sendPassword(String password) throws IOException {
        send("PASS " + password);
        readCode(230);
    }
    
    public void sendChangeWorkingDirectory(String dir) throws IOException {
        send("CWD " + dir);
        readCode(250);
    }

    public void sendChangeDirectoryUp() throws IOException {
        send("CDUP");
        readCode(250);
    }

    public void sendDelete(String fileName) throws IOException {
        send("DELE " + fileName);
        readCode(250);
    }

    public void sendMakeDirectory(String dir) throws IOException {
    }

    public void sendMode(String dir) throws IOException {
    }

    public void sendModifiedTime(String dir) throws IOException {
    }

    public void sendNameList(String dir) throws IOException {
    }

    public void sendRenameFrom(String dir) throws IOException {
    }

    public String[] sendList(String dir) throws IOException {
        send("LIST " + dir);
        readCode(250);
        ArrayList list = new ArrayList();
        
        
        String[] result = new String[list.size()];
        list.toArray(result);
        return result;
    }

}
