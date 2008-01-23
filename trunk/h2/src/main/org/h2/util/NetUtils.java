/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.message.Message;
import org.h2.security.SecureSocketFactory;

/**
 * This utility class contains socket helper functions.
 */
public class NetUtils {

    private static InetAddress bindAddress;

    public static Socket createLoopbackSocket(int port, boolean ssl) throws IOException {
        InetAddress address = getBindAddress();
        if (address == null) {
            address = InetAddress.getLocalHost();
        }
        return createSocket(address.getHostAddress(), port, ssl);
    }

    public static Socket createSocket(String server, int defaultPort, boolean ssl) throws IOException {
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
        InetAddress address = InetAddress.getByName(server);
        return createSocket(address, port, ssl);
    }

    public static Socket createSocket(InetAddress address, int port, boolean ssl) throws IOException {
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
        } catch (SQLException e) {
            // try again
            return createServerSocketTry(port, ssl);
        }
    }

    /**
     * Get the bind address if the system property h2.bindAddress is set, or
     * null if not.
     *
     * @return the bind address
     */
    public static InetAddress getBindAddress() throws UnknownHostException {
        String host = SysProperties.BIND_ADDRESS;
        if (host == null || host.length() == 0) {
            return null;
        }
        synchronized (NetUtils.class) {
            if (bindAddress == null) {
                bindAddress = InetAddress.getByName(host);
            }
        }
        return bindAddress;
    }

    private static ServerSocket createServerSocketTry(int port, boolean ssl) throws SQLException {
        try {
            if (ssl) {
                SecureSocketFactory f = SecureSocketFactory.getInstance();
                return f.createServerSocket(port);
            } else {
                InetAddress bindAddress = getBindAddress();
                if (bindAddress == null) {
                    return new ServerSocket(port);
                } else {
                    return new ServerSocket(port, 0, bindAddress);
                }
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

    public static String getLocalAddress() {
        InetAddress bind = null;
        try {
            bind = getBindAddress();
        } catch (UnknownHostException e) {
            // ignore
        }
        return bind == null ? "localhost" : bind.getHostAddress();
    }

}
