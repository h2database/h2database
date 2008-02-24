/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
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
        String osName = SysProperties.getStringSetting("os.name", "Linux");
        try {
            if (osName.startsWith("Windows")) {
                Runtime.getRuntime().exec("rundll32 url.dll,FileProtocolHandler " + url);
            } else if (osName.startsWith("Mac OS X")) {
                // Runtime.getRuntime().exec("open -a safari " + url);
                // Runtime.getRuntime().exec("open " + url + "/index.html");
                Runtime.getRuntime().exec("open " + url);
            } else {
                try {
                    Runtime.getRuntime().exec("firefox " + url);
                } catch (Exception e) {
                    try {
                        Runtime.getRuntime().exec("mozilla-firefox " + url);
                    } catch (Exception e2) {
                        // No success in detection.
                        System.out.println("Please open a browser and go to " + url);
                    }
                }
            }
        } catch (IOException e) {
            System.out.println("Failed to start a browser to open the URL " + url);
            e.printStackTrace();
        }
    }

}
