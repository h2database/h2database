/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.server.web;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Properties;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.h2.util.StringUtils;

public class WebServlet extends HttpServlet {
    
    private static final long serialVersionUID = 9171446624885086692L;
    private WebServer server;

    private int todoRefactorRemoveDuplicateCode;
    private int todoRemoveSystem_out;
    private int todoTestWithTomcat;
    private int todoTestWithJetty;

    public void init() throws ServletException {
        ServletConfig config = getServletConfig();
        Enumeration en = config.getInitParameterNames();
        ArrayList list = new ArrayList();
        while(en.hasMoreElements()) {
            String name = (String) en.nextElement();
            String value = config.getInitParameter(name);
            if(!name.startsWith("-")) {
                name = "-" + name;
            }
            list.add(name);
            list.add(value);
        }
        String[] args = new String[list.size()];
        list.toArray(args);
        server = new WebServer();
        server.setAllowShutdown(false);
        try {
            server.init(args);
        } catch(Exception e) {
            throw new ServletException("Init failed", e);
        }
    }
    
    public void destroy() {
    }
    
    boolean allow(HttpServletRequest req) {
        if(server.getAllowOthers()) {
            return true;
        }
        String addr = req.getRemoteAddr();
        InetAddress address;
        try {
            address = InetAddress.getByName(addr);
        } catch (UnknownHostException e) {
            return false;
        }
        return address.isLoopbackAddress();
    }
    
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String file = req.getPathInfo();
        if(file == null) {
            resp.sendRedirect(req.getRequestURI() + "/");
            return;
        } else if(file.startsWith("/")) {
            file = file.substring(1);
        }
        if(file.length() == 0) {
            file = "index.do";
        }        
        if(!allow(req)) {
            file = "notAllowed.jsp";
        }
        byte[] bytes = null;
        Properties attributes = new Properties();
        Enumeration en = req.getAttributeNames();
        while(en.hasMoreElements()) {
            String name = (String) en.nextElement();
            String value = (String) req.getAttribute(name);
            attributes.put(name, value);
        }
        en = req.getParameterNames();
        while(en.hasMoreElements()) {
            String name = (String) en.nextElement();
            String value = req.getParameter(name);
            attributes.put(name, value);
        }
        WebSession session = null;
        String sessionId = attributes.getProperty("jsessionid");
        if(sessionId != null) {
            session = server.getSession(sessionId);
        }
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
            cache = false;
            mimeType = "text/html";
            if (session == null) {
                int todoTest;
                String hostname = req.getRemoteHost();
                session = server.createNewSession(hostname);
                if (!file.equals("notAllowed.jsp")) {
                    file = "index.do";
                }
            }
        } else if(suffix.equals("js")) {
            cache = true;
            mimeType = "text/javascript";
        } else {
            cache = false;
            mimeType = "text/html";
            file = "error.jsp";
            server.trace("unknown mime type, file "+file);
        }
        server.trace("mimeType="+mimeType);                
        // parseHeader();
        String ifModifiedSince = req.getHeader("if-modified-since");
        server.trace(file);
        WebThread app = new WebThread(null, server);
        
        if(file.endsWith(".do")) {
            app.setSession(session, attributes);
            file = app.process(file);
        }
        if(cache && server.getStartDateTime().equals(ifModifiedSince)) {
            resp.setStatus(HttpServletResponse.SC_NOT_MODIFIED);
            return;
        } else {
            bytes = server.getFile(file);
        }
        if(bytes == null) {
            resp.sendError(HttpServletResponse.SC_NOT_FOUND);
            try {
                bytes = StringUtils.utf8Encode("File not found: "+file);
            } catch(SQLException e) {
                int todoNotIgnore;
            }
        } else {
            if(session != null && file.endsWith(".jsp")) {
                String page = StringUtils.utf8Decode(bytes);
                page = PageParser.parse(server, page, session.map);
                try {
                    bytes = StringUtils.utf8Encode(page);
                } catch(SQLException e) {
                    int todoNotIgnore;
                }
            }
            resp.setContentType(mimeType);
            if(!cache) {
                resp.setHeader("Cache-Control", "no-cache");
            } else {
                resp.setHeader("Cache-Control", "max-age=10");
                resp.setHeader("Last-Modified", server.getStartDateTime());
            }
        }
        if(bytes != null) {
            ServletOutputStream out = resp.getOutputStream();
            out.write(bytes);
        }
    }

    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doGet(req, resp);
    }
}
