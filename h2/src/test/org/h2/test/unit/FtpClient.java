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
import java.sql.SQLException;

import org.h2.engine.Constants;
import org.h2.util.IOUtils;
import org.h2.util.NetUtils;
import org.h2.util.StringUtils;

public class FtpClient {
    private Socket socket;
    private BufferedReader reader;
    private PrintWriter writer;
    private int code;
    private String message;
    private Socket socketData;
    private InputStream inData;
    private OutputStream outData;
    
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
            message = message.substring(idx + 1);
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
    
    public void login(String userName, String password) throws IOException {
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
    
    public void close() throws IOException {
        if (socket != null) {
            send("QUIT");
            readCode(221);
            socket.close();
        }
    }
    
    public void changeWorkingDirectory(String dir) throws IOException {
        send("CWD " + dir);
        readCode(250);
    }

    public void changeDirectoryUp() throws IOException {
        send("CDUP");
        readCode(250);
    }

    public void delete(String fileName) throws IOException {
        send("DELE " + fileName);
        readCode(250);
    }

    public void makeDirectory(String dir) throws IOException {
        send("MKD " + dir);
        readCode(257);
    }

    public void mode(String mode) throws IOException {
        send("MODE " + mode);
        readCode(200);
    }

    public void modificationTime(String fileName) throws IOException {
        send("MDTM " + fileName);
        
        readCode(213);
    }

    public void noOperation() throws IOException {
        send("NOOP");
        readCode(200);
    }

    public String printWorkingDirectory() throws IOException {
        send("PWD");
        readCode(257);
        return unquote();
    }
    
    private String unquote() {
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
    
    private void passive() throws IOException, SQLException {
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
    
    public void rename(String fromFileName, String toFileName) throws IOException {
        send("RNFR " + fromFileName);
        readCode(350);
        send("RNTO " + toFileName);
        readCode(250);
    }
    
    public void retrieve(String fileName, OutputStream out, long restartAt) throws IOException, SQLException {
        passive();
        if (restartAt > 0) {
            send("REST " + restartAt);
            readCode(350);
        }
        send("RETR " + fileName);
        IOUtils.copyAndClose(inData, out);
        readCode(226);
    }

    public void removeDirectory(String dir) throws IOException {
        send("RMD " + dir);
        readCode(250);
    }
    
    public long size(String fileName) throws IOException {
        send("SIZE " + fileName);
        readCode(250);
        long size = Long.parseLong(message);
        return size;
    }
    
    public void store(String fileName, InputStream in) throws IOException, SQLException {
        passive();
        send("STOR " + fileName);
        readCode(150);
        IOUtils.copyAndClose(in, outData);
        readCode(226);
    }
    
    public String nameList(String dir) throws IOException, SQLException {
        passive();
        send("NLST " + dir);
        readCode(150);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IOUtils.copyAndClose(inData, out);
        readCode(226);
        byte[] data = out.toByteArray();
        return new String(data);
    }

    public String list(String dir) throws IOException, SQLException {
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
