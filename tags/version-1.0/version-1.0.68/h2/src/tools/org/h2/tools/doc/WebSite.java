/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0
 * (license2)
 * Initial Developer: H2 Group
 */
package org.h2.tools.doc;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.h2.samples.Newsfeed;
import org.h2.util.IOUtils;
import org.h2.util.StringUtils;

/**
 * Create the web site, mainly by copying the regular docs. A few items are
 * different in the web site, for example it calls web site analytics.
 * Also, the main entry point page is different.
 * The newsfeeds are generated here as well.
 */
public class WebSite {

    String sourceDir = "docs";
    String targetDir = "dataWeb";
    
    private static final String ANALYTICS_TAG = "<!-- analytics -->";
    private static final String ANALYTICS_SCRIPT = 
        "<script src=\"http://www.google-analytics.com/ga.js\" type=\"text/javascript\"></script>\n" +
        "<script type=\"text/javascript\">var pageTracker=_gat._getTracker(\"UA-2351060-1\");pageTracker._initData();pageTracker._trackPageview();</script>";

    public static void main(String[] args) throws Exception {
        new WebSite().run();
    }

    private void run() throws Exception {
        deleteRecursive(new File(targetDir));
        copy(new File(sourceDir), new File(targetDir));
        Newsfeed.main(new String[] {"dataWeb/html"});
    }

    private void deleteRecursive(File dir) {
        if (dir.isDirectory()) {
            File[] list = dir.listFiles();
            for (int i = 0; i < list.length; i++) {
                deleteRecursive(list[i]);
            }
        }
        dir.delete();
    }

    private void copy(File source, File target) throws IOException {
        if (source.isDirectory()) {
            target.mkdirs();
            File[] list = source.listFiles();
            for (int i = 0; i < list.length; i++) {
                copy(list[i], new File(target, list[i].getName()));
            }
        } else {
            String name = source.getName();
            if (name.endsWith("main.html") || name.endsWith("main_ja.html") || name.endsWith("onePage.html")) {
                return;
            }
            FileInputStream in = new FileInputStream(source);
            byte[] bytes = IOUtils.readBytesAndClose(in, 0);
            if (name.endsWith(".html")) {
                String page = new String(bytes, "UTF-8");
                page = StringUtils.replaceAll(page, ANALYTICS_TAG, ANALYTICS_SCRIPT);
                bytes = page.getBytes("UTF-8");
            }
            FileOutputStream out = new FileOutputStream(target);
            out.write(bytes);
            out.close();
            if (name.endsWith("mainWeb.html")) {
                target.renameTo(new File(target.getParentFile(), "main.html"));
            } else if (name.endsWith("mainWeb_ja.html")) {
                target.renameTo(new File(target.getParentFile(), "main_ja.html"));
            }
        }
    }
    
}
