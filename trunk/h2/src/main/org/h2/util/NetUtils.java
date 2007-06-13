/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.SQLException;

import org.h2.message.Message;
import org.h2.security.SecureSocketFactory;

public class NetUtils {
    
    public static Socket createLoopbackSocket(int port, boolean ssl) throws IOException, SQLException {
        InetAddress address = InetAddress.getByName("127.0.0.1");
        return createSocket(address, port, ssl);
    }
    
    public static Socket createSocket(InetAddress address, int port, boolean ssl) throws IOException, SQLException {
        if (ssl) {
            SecureSocketFactory f = SecureSocketFactory.getInstance();
            return f.createSocket(address, port);
        } else {
            return new Socket(address, port);
        }        
    }
    
    public static ServerSocket createServerSocket(int port, boolean ssl) throws SQLException {
        try {
            return createServerSocketTry(port, ssl);
        } catch(SQLException e) {
            // try again
            return createServerSocketTry(port, ssl);
        }
    }
    
    private static ServerSocket createServerSocketTry(int port, boolean ssl) throws SQLException {
        // TODO server sockets: maybe automatically open the next port if this is in use?
        // TODO server sockets: maybe a parameter to allow anonymous ssl?
        try {
            if(ssl) {
                SecureSocketFactory f = SecureSocketFactory.getInstance();
                return  f.createServerSocket(port);
            } else {
                return new ServerSocket(port);
            }
        } catch(BindException be) {
            throw Message.getSQLException(Message.EXCEPTION_OPENING_PORT_1, new String[]{""+port}, be);
        } catch(IOException e) {
            throw Message.convertIOException(e, "port: " + port + " ssl: " + ssl);
        }
    }

    public static boolean isLoopbackAddress(Socket socket) {
        boolean result = true;
//#ifdef JDK14
        result = socket.getInetAddress().isLoopbackAddress();
//#endif
        return result;
    }

}
