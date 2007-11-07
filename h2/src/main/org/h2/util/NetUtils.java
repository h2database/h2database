/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.message.Message;
import org.h2.security.SecureSocketFactory;

public class NetUtils {
    
    public static Socket createLoopbackSocket(int port, boolean ssl) throws SQLException {
        return createSocket("127.0.0.1", port, ssl);
    }
    
    public static Socket createSocket(String server, int defaultPort, boolean ssl) throws SQLException {
        int port = defaultPort;
        // IPv6: RFC 2732 format is '[a:b:c:d:e:f:g:h]' or
        // '[a:b:c:d:e:f:g:h]:port'
        // RFC 2396 format is 'a.b.c.d' or 'a.b.c.d:port' or 'hostname' or
        // 'hostname:port'
        int startIndex = server.startsWith("[") ? server.indexOf(']') : 0;
        int idx = server.indexOf(':', startIndex);
        if (idx >= 0) {
            port = MathUtils.decodeInt(server.substring(idx + 1));
            server = server.substring(0, idx);
        }
        try {
            InetAddress address = InetAddress.getByName(server);
            return createSocket(address, port, ssl);
        } catch (IOException e) {
            throw Message.convert(e);
        }
    }
    
    public static Socket createSocket(InetAddress address, int port, boolean ssl) throws SQLException {
        try {
            if (ssl) {
                SecureSocketFactory f = SecureSocketFactory.getInstance();
                return f.createSocket(address, port);
            } else {
                return new Socket(address, port);
            }
        } catch (IOException e) {
            throw Message.convert(e);
        }
    }

    public static ServerSocket createServerSocket(int port, boolean ssl) throws SQLException {
        try {
            return createServerSocketTry(port, ssl);
        } catch (SQLException e) {
            // try again
            return createServerSocketTry(port, ssl);
        }
    }
    
    private static ServerSocket createServerSocketTry(int port, boolean ssl) throws SQLException {
        // TODO server sockets: maybe automatically open the next port if this is in use?
        // TODO server sockets: maybe a parameter to allow anonymous ssl?
        try {
            if (ssl) {
                SecureSocketFactory f = SecureSocketFactory.getInstance();
                return f.createServerSocket(port);
            } else {
                return new ServerSocket(port);
            }
        } catch (BindException be) {
            throw Message.getSQLException(ErrorCode.EXCEPTION_OPENING_PORT_2,
                    new String[] { "" + port, be.toString() }, be);
        } catch (IOException e) {
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
    
    public static ServerSocket closeSilently(ServerSocket socket) {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException e) {
                // ignore
            }
        }
        return null;
    }

}
