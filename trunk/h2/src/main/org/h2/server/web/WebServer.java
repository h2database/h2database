/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.server.web;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Properties;
import java.util.TimeZone;

import org.h2.message.TraceSystem;
import org.h2.server.Service;
import org.h2.util.ByteUtils;
import org.h2.util.NetUtils;
import org.h2.util.RandomUtils;
import org.h2.util.Resources;

public class WebServer implements Service {
    
    // TODO tool: implement a watchdog for a server
    
    private static final String DEFAULT_LANGUAGE = "en";
    private static final String[][] LANGUAGES = {
        { "en", "English" },
        { "de", "Deutsch" },
        { "fr", "Fran\u00e7ais" },
        { "es", "Espa\u00f1ol" },
        { "zh_CN", "\u4E2D\u6587"},
        { "ja", "\u65e5\u672c\u8a9e"},
        { "hu", "Magyar"},
        { "in", "Indonesia"},
        { "pt_PT", "Portugu\u00eas (Europeu)"},
    };
    
/*
    String lang = new java.util.Locale("hu").getDisplayLanguage(new java.util.Locale("hu"));
        java.util.Locale.CHINESE.getDisplayLanguage(
        java.util.Locale.CHINESE);
       for(int i=0; i<lang.length(); i++)
         System.out.println(Integer.toHexString(lang.charAt(i))+" ");    
*/         
    
    private static final long SESSION_TIMEOUT = 30 * 60 * 1000; // timeout is 30 min
    private long lastTimeoutCheck;
    private HashMap sessions = new HashMap();
    private HashSet languages = new HashSet();
    private String startDateTime;
    private AppServer appServer;
    private ServerSocket serverSocket;
    private boolean ssl;
    private int port;
    private String url;

    byte[] getFile(String file) throws IOException {
        trace("getFile <"+file+">");
        byte[] data = Resources.get("/org/h2/server/web/res/"+file);
        if(data == null) {
            trace(" null");
        } else {
            trace(" size="+data.length);
        }
        return data;
    }

    String getTextFile(String file) throws IOException {
        byte[] bytes = getFile(file);
        return new String(bytes);
    }
    
    private String generateSessionId() {
        byte[] buff = RandomUtils.getSecureBytes(16);
        return ByteUtils.convertBytesToString(buff);
    }

    AppServer getAppServer() {
        return appServer;
    }

    WebServerSession getSession(String sessionId) {
        long now = System.currentTimeMillis();
        if(lastTimeoutCheck + SESSION_TIMEOUT < now) {
            Object[] list = sessions.keySet().toArray();
            for(int i=0; i<list.length; i++) {
                String id = (String) list[i];
                WebServerSession session = (WebServerSession)sessions.get(id);
                Long last = (Long) session.get("lastAccess");
                if(last != null && last.longValue() + SESSION_TIMEOUT < now) {
                    trace("timeout for " + id);
                    sessions.remove(id);
                }
            }
            lastTimeoutCheck = now;
        }
        WebServerSession session = (WebServerSession)sessions.get(sessionId);
        if(session != null) {
            session.lastAccess = System.currentTimeMillis();
        }
        return session;
    }

    WebServerSession createNewSession(Socket socket) {
        String newId;
        do {
            newId = generateSessionId();
        } while(sessions.get(newId) != null);
        WebServerSession session = new AppSession(this);
        session.put("sessionId", newId);
        //session.put("ip", socket.getInetAddress().getCanonicalHostName());
        session.put("ip", socket.getInetAddress().getHostName());
        session.put("language", DEFAULT_LANGUAGE);
        sessions.put(newId, session);
        // always read the english translation, to that untranslated text appears at least in english
        readTranslations(session, DEFAULT_LANGUAGE);
        return getSession(newId);
    }

    String getStartDateTime() {
        return startDateTime;
    }

    public void init(String[] args) throws Exception {
        // TODO web: support using a different properties file
        appServer = new AppServer(args);
        SimpleDateFormat format = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss z", new Locale("en", ""));
        format.setTimeZone(TimeZone.getTimeZone("GMT"));
        startDateTime = format.format(new Date());
        trace(startDateTime);
        for(int i=0; i<LANGUAGES.length; i++) {
            languages.add(LANGUAGES[i][0]);
        }
        port = appServer.getPort();
        ssl = appServer.getSSL();
        url = (ssl?"https":"http") + "://localhost:"+port;
    }
    
    public String getURL() {
        return url;
    }
    
    public void start() throws SQLException {
        serverSocket = NetUtils.createServerSocket(port, ssl);
    }
    
    public void listen() {
        try {
            while (serverSocket != null) {
                Socket s = serverSocket.accept();
                WebServerThread c = new AppThread(s, this);
                c.start();
            }
        } catch (Exception e) {
            trace(e.toString());
        }
    }

    public boolean isRunning() {
        if(serverSocket == null) {
            return false;
        }
        try {
            Socket s = NetUtils.createLoopbackSocket(port, ssl);
            s.close();
            return true;
        } catch(Exception e) {
            return false;
        }        
    }

    public void stop() {
        try {
            serverSocket.close();
        } catch(IOException e) {
            // TODO log exception
        }
        serverSocket = null;
    }

    void trace(String s) {
        // System.out.println(s);
    }

    public boolean supportsLanguage(String language) {
        return languages.contains(language);
    }

    public void readTranslations(WebServerSession session, String language) {
        Properties text = new Properties();
        try {
            trace("translation: "+language);
            byte[] trans = getFile("_text_"+language+".properties");
            trace("  "+new String(trans));
            text.load(new ByteArrayInputStream(trans));
        } catch (IOException e) {
            TraceSystem.traceThrowable(e);
        }
        session.put("text", new HashMap(text));
    }

    public String[][] getLanguageArray() {
        return LANGUAGES;
    }

    public ArrayList getSessions() {
        ArrayList list = new ArrayList(sessions.values());
        for(int i=0; i<list.size(); i++) {
            WebServerSession s = (WebServerSession) list.get(i);
            list.set(i, s.getInfo());
        }
        return list;
    }

    public boolean getAllowOthers() {
        return appServer.getAllowOthers();
    }

    public String getType() {
        return "Web";
    }

}
