/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.server.web;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Properties;
import javax.servlet.ServletConfig;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.h2.util.New;
import org.h2.util.StringUtils;

/**
 * This servlet lets the H2 Console be used in a standard servlet container
 * such as Tomcat or Jetty.
 */
public class WebServlet extends HttpServlet {

    private static final long serialVersionUID = 9171446624885086692L;
    private transient WebServer server;

    public void init() {
        ServletConfig config = getServletConfig();
        Enumeration< ? > en = config.getInitParameterNames();
        ArrayList<String> list = New.arrayList();
        while (en.hasMoreElements()) {
            String name = en.nextElement().toString();
            String value = config.getInitParameter(name);
            if (!name.startsWith("-")) {
                name = "-" + name;
            }
            list.add(name);
            if (value.length() > 0) {
                list.add(value);
            }
        }
        String[] args = new String[list.size()];
        list.toArray(args);
        server = new WebServer();
        server.init(args);
    }

    public void destroy() {
        server.stop();
    }

    private boolean allow(HttpServletRequest req) {
        if (server.getAllowOthers()) {
            return true;
        }
        String addr = req.getRemoteAddr();
        try {
            InetAddress address = InetAddress.getByName(addr);
            return address.isLoopbackAddress();
        } catch (UnknownHostException e) {
            return false;
        }
    }

    private String getAllowedFile(HttpServletRequest req, String requestedFile) {
        if (!allow(req)) {
            return "notAllowed.jsp";
        }
        if (requestedFile.length() == 0) {
            return "index.do";
        }
        return requestedFile;
    }

    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        req.setCharacterEncoding("utf-8");
        String file = req.getPathInfo();
        if (file == null) {
            resp.sendRedirect(req.getRequestURI() + "/");
            return;
        } else if (file.startsWith("/")) {
            file = file.substring(1);
        }
        file = getAllowedFile(req, file);
        byte[] bytes = null;
        Properties attributes = new Properties();
        Enumeration< ? > en = req.getAttributeNames();
        while (en.hasMoreElements()) {
            String name = en.nextElement().toString();
            String value = req.getAttribute(name).toString();
            attributes.put(name, value);
        }
        en = req.getParameterNames();
        while (en.hasMoreElements()) {
            String name = en.nextElement().toString();
            String value = req.getParameter(name);
            attributes.put(name, value);
        }
        WebSession session = null;
        String sessionId = attributes.getProperty("jsessionid");
        if (sessionId != null) {
            session = server.getSession(sessionId);
        }
        WebThread app = new WebThread(null, server);
        app.setSession(session, attributes);
        String ifModifiedSince = req.getHeader("if-modified-since");

        String hostAddr = req.getRemoteAddr();
        file = app.processRequest(file, hostAddr);
        session = app.getSession();

        String mimeType = app.getMimeType();
        boolean cache = app.getCache();

        if (cache && server.getStartDateTime().equals(ifModifiedSince)) {
            resp.setStatus(HttpServletResponse.SC_NOT_MODIFIED);
            return;
        }
        bytes = server.getFile(file);
        if (bytes == null) {
            resp.sendError(HttpServletResponse.SC_NOT_FOUND);
            bytes = StringUtils.utf8Encode("File not found: " + file);
        } else {
            if (session != null && file.endsWith(".jsp")) {
                String page = StringUtils.utf8Decode(bytes);
                page = PageParser.parse(page, session.map);
                bytes = StringUtils.utf8Encode(page);
            }
            resp.setContentType(mimeType);
            if (!cache) {
                resp.setHeader("Cache-Control", "no-cache");
            } else {
                resp.setHeader("Cache-Control", "max-age=10");
                resp.setHeader("Last-Modified", server.getStartDateTime());
            }
        }
        if (bytes != null) {
            ServletOutputStream out = resp.getOutputStream();
            out.write(bytes);
        }
    }

    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doGet(req, resp);
    }
}
