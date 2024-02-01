/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.io.IOException;
import java.net.BindException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

import org.h2.api.ErrorCode;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.security.CipherFactory;

/**
 * This utility class contains socket helper functions.
 */
public class NetUtils {

    private static final int CACHE_MILLIS = 1000;
    private static InetAddress cachedBindAddress;
    private static String cachedLocalAddress;
    private static long cachedLocalAddressTime;

    private NetUtils() {
        // utility class
    }

    /**
     * Create a loopback socket (a socket that is connected to localhost) on
     * this port.
     *
     * @param port the port
     * @param ssl if SSL should be used
     * @return the socket
     * @throws IOException on failure
     */
    public static Socket createLoopbackSocket(int port, boolean ssl)
            throws IOException {
        String local = getLocalAddress();
        try {
            return createSocket(local, port, ssl);
        } catch (IOException e) {
            try {
                return createSocket("localhost", port, ssl);
            } catch (IOException e2) {
                // throw the original exception
                throw e;
            }
        }
    }

    /**
     * Create a client socket that is connected to the given address and port.
     *
     * @param server to connect to (including an optional port)
     * @param defaultPort the default port (if not specified in the server
     *            address)
     * @param ssl if SSL should be used
     * @return the socket
     * @throws IOException on failure
     */
    public static Socket createSocket(String server, int defaultPort, boolean ssl) throws IOException {
        return createSocket(server, defaultPort, ssl, 0);
    }

    /**
     * Create a client socket that is connected to the given address and port.
     *
     * @param server to connect to (including an optional port)
     * @param defaultPort the default port (if not specified in the server
     *            address)
     * @param ssl if SSL should be used
     * @param networkTimeout socket so timeout
     * @return the socket
     * @throws IOException on failure
     */
    public static Socket createSocket(String server, int defaultPort,
            boolean ssl, int networkTimeout) throws IOException {
        int port = defaultPort;
        // IPv6: RFC 2732 format is '[a:b:c:d:e:f:g:h]' or
        // '[a:b:c:d:e:f:g:h]:port'
        // RFC 2396 format is 'a.b.c.d' or 'a.b.c.d:port' or 'hostname' or
        // 'hostname:port'
        int startIndex = server.startsWith("[") ? server.indexOf(']') : 0;
        int idx = server.indexOf(':', startIndex);
        if (idx >= 0) {
            port = Integer.decode(server.substring(idx + 1));
            server = server.substring(0, idx);
        }
        InetAddress address = InetAddress.getByName(server);
        return createSocket(address, port, ssl, networkTimeout);
    }

    /**
     * Create a client socket that is connected to the given address and port.
     *
     * @param address the address to connect to
     * @param port the port
     * @param ssl if SSL should be used
     * @return the socket
     * @throws IOException on failure
     */
    public static Socket createSocket(InetAddress address, int port, boolean ssl)
        throws IOException {
        return createSocket(address, port, ssl, 0);
    }
    /**
     * Create a client socket that is connected to the given address and port.
     *
     * @param address the address to connect to
     * @param port the port
     * @param ssl if SSL should be used
     * @param networkTimeout socket so timeout
     * @return the socket
     * @throws IOException on failure
     */
    public static Socket createSocket(InetAddress address, int port, boolean ssl, int networkTimeout)
            throws IOException {
        long start = System.nanoTime();
        for (int i = 0;; i++) {
            try {
                if (ssl) {
                    return CipherFactory.createSocket(address, port);
                }
                Socket socket = new Socket();
                socket.setSoTimeout(networkTimeout);
                socket.connect(new InetSocketAddress(address, port),
                        SysProperties.SOCKET_CONNECT_TIMEOUT);
                return socket;
            } catch (IOException e) {
                if (System.nanoTime() - start >= SysProperties.SOCKET_CONNECT_TIMEOUT * 1_000_000L) {
                    // either it was a connect timeout,
                    // or list of different exceptions
                    throw e;
                }
                if (i >= SysProperties.SOCKET_CONNECT_RETRY) {
                    throw e;
                }
                // wait a bit and retry
                try {
                    // sleep at most 256 ms
                    long sleep = Math.min(256, i * i);
                    Thread.sleep(sleep);
                } catch (InterruptedException e2) {
                    // ignore
                }
            }
        }
    }

    /**
     * Create a server socket. The system property h2.bindAddress is used if
     * set.
     *
     * @param port the port to listen on
     * @param ssl if SSL should be used
     * @return the server socket
     */
    public static ServerSocket createServerSocket(int port, boolean ssl) {
        try {
            return createServerSocketTry(port, ssl);
        } catch (Exception e) {
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
    private static InetAddress getBindAddress() throws UnknownHostException {
        String host = SysProperties.BIND_ADDRESS;
        if (host == null || host.isEmpty()) {
            return null;
        }
        synchronized (NetUtils.class) {
            if (cachedBindAddress == null) {
                cachedBindAddress = InetAddress.getByName(host);
            }
        }
        return cachedBindAddress;
    }

    private static ServerSocket createServerSocketTry(int port, boolean ssl) {
        try {
            InetAddress bindAddress = getBindAddress();
            if (ssl) {
                return CipherFactory.createServerSocket(port, bindAddress);
            }
            if (bindAddress == null) {
                return new ServerSocket(port);
            }
            return new ServerSocket(port, 0, bindAddress);
        } catch (BindException be) {
            throw DbException.get(ErrorCode.EXCEPTION_OPENING_PORT_2,
                    be, Integer.toString(port), be.toString());
        } catch (IOException e) {
            throw DbException.convertIOException(e, "port: " + port + " ssl: " + ssl);
        }
    }

    /**
     * Check if a socket is connected to a local address.
     *
     * @param socket the socket
     * @return true if it is
     * @throws UnknownHostException on failure
     */
    public static boolean isLocalAddress(Socket socket)
            throws UnknownHostException {
        InetAddress test = socket.getInetAddress();
        if (test.isLoopbackAddress()) {
            return true;
        }
        InetAddress localhost = InetAddress.getLocalHost();
        // localhost.getCanonicalHostName() is very slow
        String host = localhost.getHostAddress();
        for (InetAddress addr : InetAddress.getAllByName(host)) {
            if (test.equals(addr)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Close a server socket and ignore any exceptions.
     *
     * @param socket the socket
     * @return null
     */
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

    /**
     * Get the local host address as a string.
     * For performance, the result is cached for one second.
     *
     * @return the local host address
     */
    public static synchronized String getLocalAddress() {
        long now = System.nanoTime();
        if (cachedLocalAddress != null && now - cachedLocalAddressTime < CACHE_MILLIS * 1_000_000L) {
            return cachedLocalAddress;
        }
        InetAddress bind = null;
        boolean useLocalhost = false;
        try {
            bind = getBindAddress();
            if (bind == null) {
                useLocalhost = true;
            }
        } catch (UnknownHostException e) {
            // ignore
        }
        if (useLocalhost) {
            try {
                bind = InetAddress.getLocalHost();
            } catch (UnknownHostException e) {
                throw DbException.convert(e);
            }
        }
        String address;
        if (bind == null) {
            address = "localhost";
        } else {
            address = bind.getHostAddress();
            if (bind instanceof Inet6Address) {
                if (address.indexOf('%') >= 0) {
                    address = "localhost";
                } else if (address.indexOf(':') >= 0 && !address.startsWith("[")) {
                    // adds'[' and ']' if required for
                    // Inet6Address that contain a ':'.
                    address = "[" + address + "]";
                }
            }
        }
        if (address.equals("127.0.0.1")) {
            address = "localhost";
        }
        cachedLocalAddress = address;
        cachedLocalAddressTime = now;
        return address;
    }

    /**
     * Get the host name of a local address, if available.
     *
     * @param localAddress the local address
     * @return the host name, or another text if not available
     */
    public static String getHostName(String localAddress) {
        try {
            InetAddress addr = InetAddress.getByName(localAddress);
            return addr.getHostName();
        } catch (Exception e) {
            return "unknown";
        }
    }

    /**
     * Appends short representation of the specified IP address to the string
     * builder.
     *
     * @param builder
     *            string builder to append to, or {@code null}
     * @param address
     *            IP address
     * @param addBrackets
     *            if ({@code true}, add brackets around IPv6 addresses
     * @return the specified or the new string builder with short representation
     *         of specified address
     */
    public static StringBuilder ipToShortForm(StringBuilder builder, byte[] address, boolean addBrackets) {
        switch (address.length) {
        case 4:
            if (builder == null) {
                builder = new StringBuilder(15);
            }
            builder //
                    .append(address[0] & 0xff).append('.') //
                    .append(address[1] & 0xff).append('.') //
                    .append(address[2] & 0xff).append('.') //
                    .append(address[3] & 0xff);
            break;
        case 16:
            short[] a = new short[8];
            int maxStart = 0, maxLen = 0, currentLen = 0;
            for (int i = 0, offset = 0; i < 8; i++) {
                if ((a[i] = (short) ((address[offset++] & 0xff) << 8 | address[offset++] & 0xff)) == 0) {
                    currentLen++;
                    if (currentLen > maxLen) {
                        maxLen = currentLen;
                        maxStart = i - currentLen + 1;
                    }
                } else {
                    currentLen = 0;
                }
            }
            if (builder == null) {
                builder = new StringBuilder(addBrackets ? 41 : 39);
            }
            if (addBrackets) {
                builder.append('[');
            }
            int start;
            if (maxLen > 1) {
                for (int i = 0; i < maxStart; i++) {
                    builder.append(Integer.toHexString(a[i] & 0xffff)).append(':');
                }
                if (maxStart == 0) {
                    builder.append(':');
                }
                builder.append(':');
                start = maxStart + maxLen;
            } else {
                start = 0;
            }
            for (int i = start; i < 8; i++) {
                builder.append(Integer.toHexString(a[i] & 0xffff));
                if (i < 7) {
                    builder.append(':');
                }
            }
            if (addBrackets) {
                builder.append(']');
            }
            break;
        default:
            StringUtils.convertBytesToHex(builder, address);
        }
        return builder;
    }

}
