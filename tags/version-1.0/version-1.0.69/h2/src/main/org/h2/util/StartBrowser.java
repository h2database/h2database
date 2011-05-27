/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.io.IOException;

import org.h2.constant.SysProperties;

/**
 * This tool starts the browser with a specific URL.
 */
public class StartBrowser {

    public static void openURL(String url) {
        String osName = SysProperties.getStringSetting("os.name", "linux").toLowerCase();
        Runtime rt = Runtime.getRuntime();
        try {
            if (osName.indexOf("windows") >= 0) {
                rt.exec(new String[] { "rundll32", "url.dll,FileProtocolHandler", url });
            } else if (osName.indexOf("mac") >= 0) {
                Runtime.getRuntime().exec(new String[] { "open", url });
            } else {
                String[] browsers = { "firefox", "mozilla-firefox", "mozilla", "konqueror", "netscape", "opera" };
                boolean ok = false;
                for (int i = 0; i < browsers.length; i++) {
                    try {
                        rt.exec(new String[] { browsers[i], url });
                        ok = true;
                        break;
                    } catch (Exception e) {
                        // ignore and try the next
                    }
                }
                if (!ok) {
                    // No success in detection.
                    System.out.println("Please open a browser and go to " + url);
                }
            }
        } catch (IOException e) {
            System.out.println("Failed to start a browser to open the URL " + url);
            e.printStackTrace();
        }
    }

}
