/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import org.h2.util.IOUtils;

/**
 * A simple web browser simulator.
 */
public class WebClient {

    private String sessionId;

    /**
     * Open an URL and get the HTML data.
     *
     * @param url the HTTP URL
     * @return the HTML as a string
     */
    String get(String url) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
        connection.setRequestMethod("GET");
        connection.setInstanceFollowRedirects(true);
        connection.connect();
        int code = connection.getResponseCode();
        if (code != HttpURLConnection.HTTP_OK) {
            throw new IOException("Result code: " + code);
        }
        InputStream in = connection.getInputStream();
        String result = IOUtils.readStringAndClose(new InputStreamReader(in), -1);
        connection.disconnect();
        return result;
    }

    /**
     * Read the session ID from a URL.
     *
     * @param url the URL
     */
    void readSessionId(String url) {
        int idx = url.indexOf("jsessionid=");
        String id = url.substring(idx + "jsessionid=".length());
        for (int i = 0; i < url.length(); i++) {
            char ch = id.charAt(i);
            if (!Character.isLetterOrDigit(ch)) {
                id = id.substring(0, i);
                break;
            }
        }
        this.sessionId = id;
    }

    /**
     * Read the specified HTML page.
     *
     * @param url the base URL
     * @param page the page to read
     * @return the HTML page
     */
    String get(String url, String page) throws IOException {
        if (sessionId != null) {
            if (page.indexOf('?') < 0) {
                page += "?";
            } else {
                page += "&";
            }
            page += "jsessionid=" + sessionId;
        }
        if (!url.endsWith("/")) {
            url += "/";
        }
        url += page;
        return get(url);
    }

}
