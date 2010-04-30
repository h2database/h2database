/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
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
import java.util.Iterator;
import java.util.Locale;
import java.util.Properties;
import java.util.StringTokenizer;
import org.h2.constant.SysProperties;
import org.h2.message.TraceSystem;
import org.h2.util.Utils;
import org.h2.util.IOUtils;
import org.h2.util.NetUtils;
import org.h2.util.StringUtils;

/**
 * For each connection to a session, an object of this class is created.
 * This class is used by the H2 Console.
 */
class WebThread extends WebApp implements Runnable {

    protected OutputStream output;
    protected Socket socket;
    private Thread thread;
    private InputStream input;
    private String ifModifiedSince;

    WebThread(Socket socket, WebServer server) {
        super(server);
        this.socket = socket;
        thread = new Thread(this, "H2 Console thread");
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
        return requestedFile;
    }

    public void run() {
        try {
            input = new BufferedInputStream(socket.getInputStream());
            output = new BufferedOutputStream(socket.getOutputStream());
            while (!stop) {
                if (!process()) {
                    break;
                }
            }
        } catch (IOException e) {
            TraceSystem.traceThrowable(e);
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
        boolean keepAlive = false;
        String head = readHeaderLine();
        if (head.startsWith("GET ") || head.startsWith("POST ")) {
            int begin = head.indexOf('/'), end = head.lastIndexOf(' ');
            String file;
            if (begin < 0 || end < begin) {
                file = "";
            } else {
                file = head.substring(begin + 1, end).trim();
            }
            trace(head + ": " + file);
            file = getAllowedFile(file);
            attributes = new Properties();
            int paramIndex = file.indexOf("?");
            session = null;
            if (paramIndex >= 0) {
                String attrib = file.substring(paramIndex + 1);
                parseAttributes(attrib);
                String sessionId = attributes.getProperty("jsessionid");
                file = file.substring(0, paramIndex);
                session = server.getSession(sessionId);
            }
            keepAlive = parseHeader();
            String hostAddr = socket.getInetAddress().getHostAddress();
            file = processRequest(file, hostAddr);
            if (file.length() == 0) {
                // asynchronous request
                return true;
            }
            String message;
            byte[] bytes;
            if (cache && ifModifiedSince != null && ifModifiedSince.equals(server.getStartDateTime())) {
                bytes = null;
                message = "HTTP/1.1 304 Not Modified\r\n";
            } else {
                bytes = server.getFile(file);
                if (bytes == null) {
                    message = "HTTP/1.0 404 Not Found\r\n";
                    bytes = StringUtils.utf8Encode("File not found: " + file);
                } else {
                    if (session != null && file.endsWith(".jsp")) {
                        String page = StringUtils.utf8Decode(bytes);
                        if (SysProperties.CONSOLE_STREAM) {
                            Iterator<String> it = (Iterator<String>) session.map.remove("chunks");
                            if (it != null) {
                                message = "HTTP/1.1 200 OK\r\n";
                                message += "Content-Type: " + mimeType + "\r\n";
                                message += "Cache-Control: no-cache\r\n";
                                message += "Transfer-Encoding: chunked\r\n";
                                message += "\r\n";
                                trace(message);
                                output.write(message.getBytes());
                                while (it.hasNext()) {
                                    String s = it.next();
                                    s = PageParser.parse(s, session.map);
                                    if (bytes.length == 0) {
                                        continue;
                                    }
                                    bytes = StringUtils.utf8Encode(s);
                                    output.write(Integer.toHexString(bytes.length).getBytes());
                                    output.write("\r\n".getBytes());
                                    output.write(bytes);
                                    output.write("\r\n".getBytes());
                                    output.flush();
                                }
                                output.write("0\r\n\r\n".getBytes());
                                output.flush();
                                return keepAlive;
                            }
                        }
                        page = PageParser.parse(page, session.map);
                        bytes = StringUtils.utf8Encode(page);
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
                }
            }
            message += "\r\n";
            trace(message);
            output.write(message.getBytes());
            if (bytes != null) {
                output.write(bytes);
            }
            output.flush();
        }
        return keepAlive;
    }

    private String readHeaderLine() throws IOException {
        StringBuilder buff = new StringBuilder();
        while (true) {
            int i = input.read();
            if (i == -1) {
                throw new IOException("Unexpected EOF");
            } else if (i == '\r' && input.read() == '\n' || i == '\n') {
                return buff.length() > 0 ? buff.toString() : null;
            } else {
                buff.append((char) i);
            }
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
        ifModifiedSince = null;
        while (true) {
            String line = readHeaderLine();
            if (line == null) {
                break;
            }
            trace(" " + line);
            String lower = StringUtils.toLowerEnglish(line);
            if (lower.startsWith("if-modified-since")) {
                ifModifiedSince = line.substring(line.indexOf(':') + 1).trim();
            } else if (lower.startsWith("connection")) {
                String conn = line.substring(line.indexOf(':') + 1).trim();
                if ("keep-alive".equals(conn)) {
                    keepAlive = true;
                }
            } else if (lower.startsWith("content-length")) {
                len = Integer.parseInt(line.substring(line.indexOf(':') + 1).trim());
                trace("len=" + len);
            } else if (lower.startsWith("accept-language")) {
                Locale locale = session == null ? null : session.locale;
                if (locale == null) {
                    String languages = line.substring(line.indexOf(':') + 1).trim();
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
            } else if (line.trim().length() == 0) {
                break;
            }
        }
        if (session != null && len > 0) {
            byte[] bytes = Utils.newBytes(len);
            for (int pos = 0; pos < len;) {
                pos += input.read(bytes, pos, len - pos);
            }
            String s = new String(bytes);
            parseAttributes(s);
        }
        return keepAlive;
    }

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
