/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.server.web;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.util.Locale;
import java.util.Properties;
import java.util.StringTokenizer;

import org.h2.message.TraceSystem;
import org.h2.util.StringUtils;

abstract class WebServerThread extends Thread {
    protected WebServer server;
    protected WebServerSession session;
    protected Properties attributes;
    protected Socket socket;
    
    private InputStream input;
    private String ifModifiedSince;

    WebServerThread(Socket socket, WebServer server) {
        this.server = server;
        this.socket = socket;
    }
    
    abstract String process(String file);

    protected String getCombobox(String[] elements, String selected) {
        StringBuffer buff = new StringBuffer();
        for(int i=0; i<elements.length; i++) {
            String value = elements[i];
            buff.append("<option value=\"");
            buff.append(PageParser.escapeHtml(value));
            buff.append("\"");
            if(value.equals(selected)) {
                buff.append(" selected");
            }
            buff.append(">");
            buff.append(PageParser.escapeHtml(value));
            buff.append("</option>");
        }
        return buff.toString();
    }

    protected String getCombobox(String[][] elements, String selected) {
        StringBuffer buff = new StringBuffer();
        for(int i=0; i<elements.length; i++) {
            String[] n = elements[i];
            buff.append("<option value=\"");
            buff.append(PageParser.escapeHtml(n[0]));
            buff.append("\"");
            if(n[0].equals(selected)) {
                buff.append(" selected");
            }
            buff.append(">");
            buff.append(PageParser.escapeHtml(n[1]));
            buff.append("</option>");
        }
        return buff.toString();
    }

    public void run() {
        try {
            input = socket.getInputStream();
            String head = readHeaderLine();
            if(head.startsWith("GET ") || head.startsWith("POST ")) {
                int begin = head.indexOf('/'), end = head.lastIndexOf(' ');
                String file = head.substring(begin+1, end).trim();
                if(file.length() == 0) {
                    file = "index.do";
                }
                if(!allow()) {
                    file = "notAllowed.jsp";
                }
                server.trace(head + " :" + file);
                attributes = new Properties();
                int paramIndex = file.indexOf("?");
                session = null;
                if(paramIndex >= 0) {
                    String attrib = file.substring(paramIndex+1);
                    parseAttributes(attrib);
                    String sessionId = attributes.getProperty("jsessionid");
                    file = file.substring(0, paramIndex);
                    session = server.getSession(sessionId);
                }
                // TODO web: support errors
                String mimeType;
                boolean cache;
                int index = file.lastIndexOf('.');
                String suffix;
                if(index >= 0) {
                    suffix = file.substring(index+1);
                } else {
                    suffix = "";
                }
                if(suffix.equals("ico")) {
                    mimeType = "image/x-icon";
                    cache=true;
                } else if(suffix.equals("gif")) {
                    mimeType = "image/gif";
                    cache=true;
                } else if(suffix.equals("css")) {
                    cache=true;
                    mimeType = "text/css";
                } else if(suffix.equals("html") || suffix.equals("do") || suffix.equals("jsp")) {
                    cache=false;
                    mimeType = "text/html";
                    if (session == null) {
                        session = server.createNewSession(socket);
                        if (!file.equals("notAllowed.jsp")) {
                            file = "index.do";
                        }
                    }
                } else if(suffix.equals("js")) {
                    cache=true;
                    mimeType = "text/javascript";
                } else {
                    cache = false;
                    mimeType = "text/html";
                    file = "error.jsp";
                    server.trace("unknown mime type, file "+file);
                }
                server.trace("mimeType="+mimeType);                
                parseHeader();
                server.trace(file);
                if(file.endsWith(".do")) {
                    file = process(file);
                }
                String message;
                byte[] bytes;
                if(cache && ifModifiedSince!=null && ifModifiedSince.equals(server.getStartDateTime())) {
                    bytes = null;
                    message = "HTTP/1.1 304 Not Modified\n";
                } else {
                    bytes = server.getFile(file);
                    if(bytes == null) {
                        message = "HTTP/1.0 404 Not Found\n";
                        bytes = StringUtils.utf8Encode("File not found: "+file);
                    } else {
                        if(session != null && file.endsWith(".jsp")) {
                            bytes = StringUtils.utf8Encode(fill(StringUtils.utf8Decode(bytes)));
                        }
                        message = "HTTP/1.1 200 OK\n";
                        message += "Content-Type: "+mimeType+"\n";
                        if(!cache) {
                            message += "Cache-Control: no-cache\n";
                        } else {
                            message += "Cache-Control: max-age=10\n";
                            message += "Last-Modified: "+server.getStartDateTime()+"\n";
                        }
                    }
                }
                message += "\n";
                server.trace(message);
                DataOutputStream output;
                output = new DataOutputStream(
                        new BufferedOutputStream(socket.getOutputStream()));
                output.write(message.getBytes());
                if(bytes!=null) {
                    output.write(bytes);
                }
                output.flush();
                output.close();
                output.close();
                socket.close();
                return;
            }
        } catch (Exception e) {
            TraceSystem.traceThrowable(e);
        }
    }

    abstract boolean allow();

    private String readHeaderLine() throws IOException {
        StringBuffer buff=new StringBuffer();
        while (true) {
            int i = input.read();
            if (i == -1) {
                throw new IOException("Unexpected EOF");
            } else if (i == '\r' && input.read()=='\n') {
                return buff.length() > 0 ? buff.toString() : null;
            } else {
                buff.append((char)i);
            }
        }
    }

    private void parseAttributes(String s) throws Exception {
        server.trace("data="+s);
        while(s != null) {
            int idx = s.indexOf('=');
            if(idx>=0) {
                String property = s.substring(0, idx);
                s = s.substring(idx+1);
                idx = s.indexOf('&');
                String value;
                if(idx >= 0) {
                    value = s.substring(0, idx);
                    s = s.substring(idx+1);
                } else {
                    value = s;
                }
                // TODO compatibility problem with JDK 1.3
                //String attr = URLDecoder.decode(value, "UTF-8");
                // String attr = URLDecoder.decode(value);
                String attr = StringUtils.urlDecode(value);
                attributes.put(property, attr);
            } else {
                break;
            }
        }
        server.trace(attributes.toString());
    }

    private void parseHeader() throws Exception {
        server.trace("parseHeader");
        int len = 0;
        ifModifiedSince = null;
        while(true) {
            String line = readHeaderLine();
            if(line == null) {
                break;
            }
            server.trace(" "+line);
            String lower = StringUtils.toLowerEnglish(line);
            if(lower.startsWith("if-modified-since")) {
                ifModifiedSince = line.substring(line.indexOf(':')+1).trim();
            } else if(lower.startsWith("content-length")) {
                len = Integer.parseInt(line.substring(line.indexOf(':')+1).trim());
                server.trace("len="+len);
            } else if(lower.startsWith("accept-language")) {
                if(session != null) {
                    Locale locale = session.locale;
                    if(locale == null) {
                        String languages = line.substring(line.indexOf(':')+1).trim();
                        StringTokenizer tokenizer = new StringTokenizer(languages, ",;");
                        while(tokenizer.hasMoreTokens()) {
                            String token = tokenizer.nextToken();
                            if(!token.startsWith("q=")) {
                                if(server.supportsLanguage(token)) {
                                    int dash = token.indexOf('-');
                                    if(dash >= 0) {
                                        String language = token.substring(0, dash);
                                        String country = token.substring(dash+1);
                                        locale = new Locale(language, country);
                                    } else {
                                        locale = new Locale(token, "");
                                    }
                                    session.locale = locale;
                                    String language = locale.getLanguage();
                                    session.put("language", language);
                                    server.readTranslations(session, language);
                                    break;
                                }
                            }
                        }
                    }
                }
            } else if(line.trim().length()==0) {
                break;
            }
        }
        if(session != null && len > 0) {
            byte[] bytes = new byte[len];
            for (int pos = 0; pos < len;) {
                pos += input.read(bytes, pos, len - pos);
            }
            String s = new String(bytes);
            parseAttributes(s);
        }
    }
    
    private String fill(String page) {
        return PageParser.parse(server, page, session.map);
    }

}
