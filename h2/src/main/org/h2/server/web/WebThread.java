/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.server.web;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Locale;
import java.util.Properties;
import java.util.StringTokenizer;

import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.util.IOUtils;
import org.h2.util.NetUtils;
import org.h2.util.NetworkConnectionInfo;
import org.h2.util.StringUtils;
import org.h2.util.Utils;
import org.h2.util.Utils21;

/**
 * For each connection to a session, an object of this class is created.
 * This class is used by the H2 Console.
 */
class WebThread extends WebApp implements Runnable {

    private static final byte[] RN = { '\r', '\n' };

    private static final byte[] RNRN = { '\r', '\n', '\r', '\n' };

    protected OutputStream output;
    protected final Socket socket;
    private final Thread thread;
    private InputStream input;
    private String host;
    private int dataLength;
    private String ifModifiedSince;

    WebThread(Socket socket, WebServer server) {
        super(server);
        this.socket = socket;
        thread = server.virtualThreads ? Utils21.newVirtualThread(this) : new Thread(this);
        thread.setName("H2 Console thread");
    }

    /**
     * Start the thread.
     */
    void start() {
        thread.start();
    }

    /**
     * Wait until the thread is stopped.
     *
     * @param millis the maximum number of milliseconds to wait
     * @throws InterruptedException if interrupted
     */
    void join(int millis) throws InterruptedException {
        thread.join(millis);
    }

    /**
     * Close the connection now.
     */
    void stopNow() {
        this.stop = true;
        try {
            socket.close();
        } catch (IOException e) {
            // ignore
        }
    }

    private String getAllowedFile(String requestedFile) {
        if (!allow()) {
            return "notAllowed.jsp";
        }
        if (requestedFile.length() == 0) {
            return "index.do";
        }
        if (requestedFile.charAt(0) == '?') {
            return "index.do" + requestedFile;
        }
        return requestedFile;
    }

    @Override
    public void run() {
        try {
            input = new BufferedInputStream(socket.getInputStream());
            output = new BufferedOutputStream(socket.getOutputStream());
            while (!stop) {
                if (!process()) {
                    break;
                }
            }
        } catch (Exception e) {
            DbException.traceThrowable(e);
        }
        IOUtils.closeSilently(output);
        IOUtils.closeSilently(input);
        try {
            socket.close();
        } catch (IOException e) {
            // ignore
        } finally {
            server.remove(this);
        }
    }

    @SuppressWarnings("unchecked")
    private boolean process() throws IOException {
        String head = readHeaderLine();
        boolean get = head.startsWith("GET ");
        if ((!get && !head.startsWith("POST ")) || !head.endsWith(" HTTP/1.1")) {
            writeSimple("HTTP/1.1 400 Bad Request", "Bad request");
            return false;
        }
        String file = StringUtils.trimSubstring(head, get ? 4 : 5, head.length() - 9);
        if (file.isEmpty() || file.charAt(0) != '/') {
            writeSimple("HTTP/1.1 400 Bad Request", "Bad request");
            return false;
        }
        attributes = new Properties();
        boolean keepAlive = parseHeader();
        if (!checkHost(host)) {
            return false;
        }
        file = file.substring(1);
        trace(head + ": " + file);
        file = getAllowedFile(file);
        int paramIndex = file.indexOf('?');
        session = null;
        String key = null;
        if (paramIndex >= 0) {
            String attrib = file.substring(paramIndex + 1);
            parseAttributes(attrib);
            String sessionId = attributes.getProperty("jsessionid");
            key = attributes.getProperty("key");
            file = file.substring(0, paramIndex);
            session = server.getSession(sessionId);
        }
        parseBodyAttributes();
        file = processRequest(file,
                new NetworkConnectionInfo(
                        NetUtils.ipToShortForm(new StringBuilder(server.getSSL() ? "https://" : "http://"),
                                socket.getLocalAddress().getAddress(), true) //
                                .append(':').append(socket.getLocalPort()).toString(), //
                        socket.getInetAddress().getAddress(), socket.getPort(), null));
        if (file.length() == 0) {
            // asynchronous request
            return true;
        }
        String message;
        if (cache && ifModifiedSince != null && ifModifiedSince.equals(server.getStartDateTime())) {
            writeSimple("HTTP/1.1 304 Not Modified", (byte[]) null);
            return keepAlive;
        }
        byte[] bytes = server.getFile(file);
        if (bytes == null) {
            writeSimple("HTTP/1.1 404 Not Found", "File not found: " + file);
            return keepAlive;
        }
        if (session != null && file.endsWith(".jsp")) {
            if (key != null) {
                session.put("key", key);
            }
            String page = new String(bytes, StandardCharsets.UTF_8);
            if (SysProperties.CONSOLE_STREAM) {
                Iterator<String> it = (Iterator<String>) session.map.remove("chunks");
                if (it != null) {
                    message = "HTTP/1.1 200 OK\r\n";
                    message += "Content-Type: " + mimeType + "\r\n";
                    message += "Cache-Control: no-cache\r\n";
                    message += "Transfer-Encoding: chunked\r\n";
                    message += "\r\n";
                    trace(message);
                    output.write(message.getBytes(StandardCharsets.ISO_8859_1));
                    while (it.hasNext()) {
                        String s = it.next();
                        s = PageParser.parse(s, session.map);
                        bytes = s.getBytes(StandardCharsets.UTF_8);
                        if (bytes.length == 0) {
                            continue;
                        }
                        output.write(Integer.toHexString(bytes.length).getBytes(StandardCharsets.ISO_8859_1));
                        output.write(RN);
                        output.write(bytes);
                        output.write(RN);
                        output.flush();
                    }
                    output.write('0');
                    output.write(RNRN);
                    output.flush();
                    return keepAlive;
                }
            }
            page = PageParser.parse(page, session.map);
            bytes = page.getBytes(StandardCharsets.UTF_8);
        }
        message = "HTTP/1.1 200 OK\r\n";
        message += "Content-Type: " + mimeType + "\r\n";
        if (!cache) {
            message += "Cache-Control: no-cache\r\n";
        } else {
            message += "Cache-Control: max-age=10\r\n";
            message += "Last-Modified: " + server.getStartDateTime() + "\r\n";
        }
        message += "Content-Length: " + bytes.length + "\r\n";
        message += "\r\n";
        trace(message);
        output.write(message.getBytes(StandardCharsets.ISO_8859_1));
        output.write(bytes);
        output.flush();
        return keepAlive;
    }

    private void writeSimple(String status, String text) throws IOException {
        writeSimple(status, text != null ? text.getBytes(StandardCharsets.UTF_8) : null);
    }

    private void writeSimple(String status, byte[] bytes) throws IOException {
        trace(status);
        output.write(status.getBytes(StandardCharsets.ISO_8859_1));
        if (bytes != null) {
            output.write(RN);
            String contentLength = "Content-Length: " + bytes.length;
            trace(contentLength);
            output.write(contentLength.getBytes(StandardCharsets.ISO_8859_1));
            output.write(RNRN);
            output.write(bytes);
        } else {
            output.write(RNRN);
        }
        output.flush();
    }

    private boolean checkHost(String host) throws IOException {
        if (host == null) {
            writeSimple("HTTP/1.1 400 Bad Request", "Bad request");
            return false;
        }
        int index = host.indexOf(':');
        if (index >= 0) {
            host = host.substring(0, index);
        }
        if (host.isEmpty()) {
            return false;
        }
        host = StringUtils.toLowerEnglish(host);
        if (host.equals(server.getHost()) || host.equals("localhost") || host.equals("127.0.0.1")) {
            return true;
        }
        String externalNames = server.getExternalNames();
        if (externalNames != null && !externalNames.isEmpty()) {
            for (String s : externalNames.split(",")) {
                if (host.equals(s.trim())) {
                    return true;
                }
            }
        }
        writeSimple("HTTP/1.1 404 Not Found", "Host " + host + " not found");
        return false;
    }

    private String readHeaderLine() throws IOException {
        StringBuilder buff = new StringBuilder();
        while (true) {
            int c = input.read();
            if (c == -1) {
                throw new IOException("Unexpected EOF");
            } else if (c == '\r') {
                if (input.read() == '\n') {
                    return buff.length() > 0 ? buff.toString() : null;
                }
            } else if (c == '\n') {
                return buff.length() > 0 ? buff.toString() : null;
            } else {
                buff.append((char) c);
            }
        }
    }

    private void parseBodyAttributes() throws IOException {
        if (dataLength > 0) {
            byte[] bytes = Utils.newBytes(dataLength);
            for (int pos = 0; pos < dataLength;) {
                pos += input.read(bytes, pos, dataLength - pos);
            }
            String s = new String(bytes, StandardCharsets.UTF_8);
            parseAttributes(s);
        }
    }

    private void parseAttributes(String s) {
        trace("data=" + s);
        while (s != null) {
            int idx = s.indexOf('=');
            if (idx >= 0) {
                String property = s.substring(0, idx);
                s = s.substring(idx + 1);
                idx = s.indexOf('&');
                String value;
                if (idx >= 0) {
                    value = s.substring(0, idx);
                    s = s.substring(idx + 1);
                } else {
                    value = s;
                }
                String attr = StringUtils.urlDecode(value);
                attributes.put(property, attr);
            } else {
                break;
            }
        }
        trace(attributes.toString());
    }

    private boolean parseHeader() throws IOException {
        boolean keepAlive = false;
        trace("parseHeader");
        int len = 0;
        host = null;
        ifModifiedSince = null;
        boolean multipart = false;
        for (String line; (line = readHeaderLine()) != null;) {
            trace(" " + line);
            String lower = StringUtils.toLowerEnglish(line);
            if (lower.startsWith("host")) {
                host = getHeaderLineValue(line);
            } else if (lower.startsWith("if-modified-since")) {
                ifModifiedSince = getHeaderLineValue(line);
            } else if (lower.startsWith("connection")) {
                String conn = getHeaderLineValue(line);
                if ("keep-alive".equals(conn)) {
                    keepAlive = true;
                }
            } else if (lower.startsWith("content-type")) {
                String type = getHeaderLineValue(line);
                if (type.startsWith("multipart/form-data")) {
                    multipart = true;
                }
            } else if (lower.startsWith("content-length")) {
                len = Integer.parseInt(getHeaderLineValue(line));
                trace("len=" + len);
            } else if (lower.startsWith("user-agent")) {
                boolean isWebKit = lower.contains("webkit/");
                if (isWebKit && session != null) {
                    // workaround for what seems to be a WebKit bug:
                    // https://bugs.chromium.org/p/chromium/issues/detail?id=6402
                    session.put("frame-border", "1");
                    session.put("frameset-border", "2");
                }
            } else if (lower.startsWith("accept-language")) {
                Locale locale = session == null ? null : session.locale;
                if (locale == null) {
                    String languages = getHeaderLineValue(line);
                    StringTokenizer tokenizer = new StringTokenizer(languages, ",;");
                    while (tokenizer.hasMoreTokens()) {
                        String token = tokenizer.nextToken();
                        if (!token.startsWith("q=")) {
                            if (server.supportsLanguage(token)) {
                                int dash = token.indexOf('-');
                                if (dash >= 0) {
                                    String language = token.substring(0, dash);
                                    String country = token.substring(dash + 1);
                                    locale = new Locale(language, country);
                                } else {
                                    locale = new Locale(token, "");
                                }
                                headerLanguage = locale.getLanguage();
                                if (session != null) {
                                    session.locale = locale;
                                    session.put("language", headerLanguage);
                                    server.readTranslations(session, headerLanguage);
                                }
                                break;
                            }
                        }
                    }
                }
            } else if (StringUtils.isWhitespaceOrEmpty(line)) {
                break;
            }
        }
        dataLength = 0;
        if (multipart) {
            // not supported
        } else if (len > 0) {
            dataLength = len;
        }
        return keepAlive;
    }

    private static String getHeaderLineValue(String line) {
        return StringUtils.trimSubstring(line, line.indexOf(':') + 1);
    }

    @Override
    protected String adminShutdown() {
        stopNow();
        return super.adminShutdown();
    }

    private boolean allow() {
        if (server.getAllowOthers()) {
            return true;
        }
        try {
            return NetUtils.isLocalAddress(socket);
        } catch (UnknownHostException e) {
            server.traceError(e);
            return false;
        }
    }

    private void trace(String s) {
        server.trace(s);
    }
}
