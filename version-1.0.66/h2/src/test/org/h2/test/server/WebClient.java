/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
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

    void readSessionId(String result) {
        int idx = result.indexOf("jsessionid=");
        String id = result.substring(idx + "jsessionid=".length());
        for (int i = 0; i < result.length(); i++) {
            char ch = id.charAt(i);
            if (!Character.isLetterOrDigit(ch)) {
                id = id.substring(0, i);
                break;
            }
        }
        this.sessionId = id;
    }

    public String get(String url, String page) throws IOException {
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
