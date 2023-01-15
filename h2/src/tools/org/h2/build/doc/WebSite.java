/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.build.doc;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;

import org.h2.build.BuildBase;
import org.h2.samples.Newsfeed;
import org.h2.util.StringUtils;

/**
 * Create the web site, mainly by copying the regular docs. A few items are
 * different in the web site, for example it calls web site analytics.
 * Also, the main entry point page is different.
 * The newsfeeds are generated here as well.
 */
public class WebSite {

    private static final String ANALYTICS_TAG = "<!-- analytics -->";
    private static final String ANALYTICS_SCRIPT = "";
    private static final String TRANSLATE_START = "<!-- translate";
    private static final String TRANSLATE_END = "translate -->";

    private static final Path SOURCE_DIR = Paths.get("docs");
    private static final Path WEB_DIR = Paths.get("../h2web");
    private final HashMap<String, String> fragments = new HashMap<>();

    /**
     * This method is called when executing this application from the command
     * line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws Exception {
        new WebSite().run();
    }

    private void run() throws Exception {
        // create the web site
        BuildBase.deleteRecursive(WEB_DIR);
        loadFragments();
        copy(SOURCE_DIR, WEB_DIR, true, true);
        Newsfeed.main(WEB_DIR + "/html");

        // create the internal documentation
        copy(SOURCE_DIR, SOURCE_DIR, true, false);
    }

    private void loadFragments() throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(SOURCE_DIR.resolve("html"), "fragments*")) {
            for (Path f : stream) {
                fragments.put(f.getFileName().toString(), new String(Files.readAllBytes(f), StandardCharsets.UTF_8));
            }
        }
    }

    private String replaceFragments(String fileName, String page) {
        if (fragments.size() == 0) {
            return page;
        }
        String language = "";
        int index = fileName.indexOf('_');
        if (index >= 0) {
            int end = fileName.indexOf('.');
            language = fileName.substring(index, end);
        }
        String fragment = fragments.get("fragments" + language + ".html");
        int start = 0;
        while (true) {
            start = fragment.indexOf("<!-- [", start);
            if (start < 0) {
                break;
            }
            int endTag = fragment.indexOf("] { -->", start);
            int endBlock = fragment.indexOf("<!-- } -->", start);
            String tag = fragment.substring(start, endTag);
            String replacement = fragment.substring(start, endBlock);
            int pageStart = 0;
            while (true) {
                pageStart = page.indexOf(tag, pageStart);
                if (pageStart < 0) {
                    break;
                }
                int pageEnd = page.indexOf("<!-- } -->", pageStart);
                page = page.substring(0, pageStart) + replacement + page.substring(pageEnd);
                pageStart += replacement.length();
            }
            start = endBlock;
        }
        return page;
    }

    private void copy(Path source, Path target, boolean replaceFragments, boolean web) throws IOException {
        Files.walkFileTree(source, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                Files.createDirectories(target.resolve(source.relativize(dir)));
                return FileVisitResult.CONTINUE;
            }
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                copyFile(file, target.resolve(source.relativize(file)), replaceFragments, web);
                return super.visitFile(file, attrs);
            }
        });
    }

    /**
     * Copy a file.
     *
     * @param source the source file
     * @param target the target file
     * @param replaceFragments whether to replace fragments
     * @param web whether the target is a public web site (false for local documentation)
     */
    void copyFile(Path source, Path target, boolean replaceFragments, boolean web) throws IOException {
        String name = source.getFileName().toString();
        if (name.endsWith("onePage.html") || name.startsWith("fragments")) {
            return;
        }
        if (web) {
            if (name.endsWith("main.html")) {
                return;
            }
        } else {
            if (name.endsWith("mainWeb.html")) {
                return;
            }
        }
        byte[] bytes = Files.readAllBytes(source);
        if (name.endsWith(".html")) {
            String page = new String(bytes, StandardCharsets.UTF_8);
            if (web) {
                page = StringUtils.replaceAll(page, ANALYTICS_TAG, ANALYTICS_SCRIPT);
            }
            if (replaceFragments) {
                page = replaceFragments(name, page);
                page = StringUtils.replaceAll(page, "<a href=\"frame", "<a href=\"main");
                page = StringUtils.replaceAll(page, "html/frame.html", "html/main.html");
            }
            if (web) {
                page = StringUtils.replaceAll(page, TRANSLATE_START, "");
                page = StringUtils.replaceAll(page, TRANSLATE_END, "");
                page = StringUtils.replaceAll(page, "<pre>", "<pre class=\"notranslate\">");
                page = StringUtils.replaceAll(page, "<code>", "<code class=\"notranslate\">");
            }
            if (name.endsWith("changelog.html")) {
                page = page.replaceAll("Issue\\s+#?(\\d+)",
                        "<a href=\"https://github.com/h2database/h2database/issues/$1\">Issue #$1</a>");
                page = page.replaceAll("PR\\s+#?(\\d+)",
                        "<a href=\"https://github.com/h2database/h2database/pull/$1\">PR #$1</a>");
            }
            bytes = page.getBytes(StandardCharsets.UTF_8);
        }
        Files.write(target, bytes);
        if (web) {
            if (name.endsWith("mainWeb.html")) {
                Files.move(target, target.getParent().resolve("main.html"));
            }
        }
    }

}
