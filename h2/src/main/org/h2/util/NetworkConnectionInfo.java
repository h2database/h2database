/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Network connection information.
 */
public final class NetworkConnectionInfo {

    private final String server;

    private final byte[] clientAddr;

    private final int clientPort;

    private final String clientInfo;

    /**
     * Creates new instance of network connection information.
     *
     * @param server
     *            the protocol and port of the server
     * @param clientAddr
     *            the client address
     * @param clientPort
     *            the client port
     * @throws UnknownHostException
     *             if clientAddr cannot be resolved
     */
    public NetworkConnectionInfo(String server, String clientAddr, int clientPort) throws UnknownHostException {
        this(server, InetAddress.getByName(clientAddr).getAddress(), clientPort, null);
    }

    /**
     * Creates new instance of network connection information.
     *
     * @param server
     *            the protocol and port of the server
     * @param clientAddr
     *            the client address
     * @param clientPort
     *            the client port
     * @param clientInfo
     *            additional client information, or {@code null}
     */
    public NetworkConnectionInfo(String server, byte[] clientAddr, int clientPort, String clientInfo) {
        this.server = server;
        this.clientAddr = clientAddr;
        this.clientPort = clientPort;
        this.clientInfo = clientInfo;
    }

    /**
     * Returns the protocol and port of the server.
     *
     * @return the protocol and port of the server
     */
    public String getServer() {
        return server;
    }

    /**
     * Returns the client address.
     *
     * @return the client address
     */
    public byte[] getClientAddr() {
        return clientAddr;
    }

    /**
     * Returns the client port.
     *
     * @return the client port
     */
    public int getClientPort() {
        return clientPort;
    }

    /**
     * Returns additional client information, or {@code null}.
     *
     * @return additional client information, or {@code null}
     */
    public String getClientInfo() {
        return clientInfo;
    }

    /**
     * Returns the client address and port.
     *
     * @return the client address and port
     */
    public String getClient() {
        return NetUtils.ipToShortForm(new StringBuilder(), clientAddr, true).append(':').append(clientPort).toString();
    }

}
