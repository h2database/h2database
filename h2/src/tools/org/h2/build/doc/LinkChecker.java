/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.build.doc;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import org.h2.tools.Server;
import org.h2.util.StringUtils;

/**
 * The link checker makes sure that each link in the documentation
 * points to an existing target.
 */
public class LinkChecker {

    private static final boolean TEST_EXTERNAL_LINKS = false;
    private static final boolean OPEN_EXTERNAL_LINKS = false;
    private static final String[] IGNORE_MISSING_LINKS_TO = {
        "SysProperties", "ErrorCode",
        // TODO check these replacement link too
        "#build_index",
        "#datatypes_index",
        "#faq_index",
        "#commands_index",
        "#grammar_index",
        "#functions_index",
        "#functions_aggregate_index",
        "#functions_window_index",
        "#tutorial_index",
        "docs/javadoc/"
    };

    private static enum TargetKind {
        FILE, ID
    }
    private final HashMap<String, TargetKind> targets = new HashMap<>();
    /**
     * Map of source link (i.e. <a> tag) in the document, to the document path
     */
    private final HashMap<String, String> links = new HashMap<>();

    /**
     * This method is called when executing this application from the command
     * line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws Exception {
        new LinkChecker().run(args);
    }

    private void run(String... args) throws Exception {
        Path dir = Paths.get("docs");
        for (int i = 0; i < args.length; i++) {
            if ("-dir".equals(args[i])) {
                dir = Paths.get(args[++i]);
            }
        }
        process(dir);
        listExternalLinks();
        listBadLinks();
    }

    private void listExternalLinks() {
        for (String link : links.keySet()) {
            if (link.startsWith("http")) {
                if (link.indexOf("//localhost") > 0) {
                    continue;
                }
                if (TEST_EXTERNAL_LINKS) {
                    try {
                        URL url = new URL(link);
                        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                        conn.setConnectTimeout(2000);
                        conn.setRequestMethod("GET");
                        HttpURLConnection.setFollowRedirects(false);
                        conn.connect();
                        int code = conn.getResponseCode();
                        String msg;
                        switch (code) {
                        case 200:
                            msg = "OK";
                            break;
                        case 301:
                            msg = "Moved Permanently";
                            break;
                        case 302:
                            msg = "Found";
                            break;
                        case 403:
                            msg = "Forbidden";
                            break;
                        case 404:
                            msg = "Not Found";
                            break;
                        case 500:
                            msg = "Internal Server Error";
                            break;
                        default:
                            msg = "?";
                        }
                        System.out.println(code + " " + msg + " " + link);
                        conn.getInputStream().close();
                    } catch (IOException e) {
                        System.out.println("link checker error " + e.toString() + " " + link);
                        // ignore
                    }
                }
                if (OPEN_EXTERNAL_LINKS) {
                    System.out.println(link);
                    try {
                        Server.openBrowser(link);
                        Thread.sleep(100);
                    } catch (Exception e) {
                        // ignore
                    }
                }
            }
        }
    }

    private void listBadLinks() throws Exception {
        ArrayList<String> errors = new ArrayList<>();
        for (String link : links.keySet()) {
            if (!link.startsWith("http") && !link.endsWith("h2.pdf")
                    && /* For Javadoc 8 */ !link.startsWith("docs/javadoc")) {
                if (targets.get(link) == null) {
                    errors.add(links.get(link) + ": Link missing " + link);
                }
            }
        }
        for (String link : links.keySet()) {
            if (!link.startsWith("http")) {
                targets.remove(link);
            }
        }
        for (String name : targets.keySet()) {
            if (targets.get(name) == TargetKind.ID) {
                boolean ignore = false;
                for (String to : IGNORE_MISSING_LINKS_TO) {
                    if (name.contains(to)) {
                        ignore = true;
                        break;
                    }
                }
                if (!ignore) {
                    errors.add("No link to " + name);
                }
            }
        }
        Collections.sort(errors);
        for (String error : errors) {
            System.out.println(error);
        }
        if (!errors.isEmpty()) {
            throw new Exception("Problems where found by the Link Checker");
        }
    }

    private void process(Path path) throws Exception {
        Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                processFile(file);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    /**
     * Process a file.
     *
     * @param file the file
     */
    void processFile(Path file) throws IOException {
        String path = file.toString();
        targets.put(path, TargetKind.FILE);
        String fileName = file.getFileName().toString();
        String lower = StringUtils.toLowerEnglish(fileName);
        if (!lower.endsWith(".html") && !lower.endsWith(".htm")) {
            return;
        }
        Path parent = file.getParent();
        final String html = new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
        // find all the target fragments in the document (those elements marked with id attribute)
        int idx = -1;
        while (true) {
            idx = html.indexOf(" id=\"", idx + 1);
            if (idx < 0) {
                break;
            }
            int start = idx + " id=\"".length();
            int end = html.indexOf('"', start);
            if (end < 0) {
                error(fileName, "Expected \" after id= " + html.substring(idx, idx + 100));
            }
            String ref = html.substring(start, end);
            if (!ref.startsWith("_")) {
                targets.put(path + "#" + ref.replaceAll("%3C|&lt;", "<").replaceAll("%3E|&gt;", ">"), //
                        TargetKind.ID);
            }
        }
        // find all the href links in the document
        idx = -1;
        while (true) {
            idx = html.indexOf(" href=\"", idx + 1);
            if (idx < 0) {
                break;
            }
            int start = html.indexOf('"', idx);
            if (start < 0) {
                error(fileName, "Expected \" after href= at " + html.substring(idx, idx + 100));
            }
            int end = html.indexOf('"', start + 1);
            if (end < 0) {
                error(fileName, "Expected \" after href= at " + html.substring(idx, idx + 100));
            }
            String ref = html.substring(start + 1, end);
            if (ref.startsWith("http:") || ref.startsWith("https:")) {
                // ok
            } else if (ref.startsWith("javascript:")) {
                ref = null;
                // ok
            } else if (ref.length() == 0) {
                ref = null;
                // ok
            } else if (ref.startsWith("#")) {
                ref = path + ref;
            } else {
                Path p = parent;
                while (ref.startsWith(".")) {
                    if (ref.startsWith("./")) {
                        ref = ref.substring(2);
                    } else if (ref.startsWith("../")) {
                        ref = ref.substring(3);
                        p = p.getParent();
                    }
                }
                ref = p + File.separator + ref;
            }
            if (ref != null) {
                links.put(ref.replace('/', File.separatorChar) //
                        .replaceAll("%5B", "[").replaceAll("%5D", "]") //
                        .replaceAll("%3C", "<").replaceAll("%3E", ">"), //
                        path);
            }
        }
        idx = -1;
        while (true) {
            idx = html.indexOf("<a ", idx + 1);
            if (idx < 0) {
                break;
            }
            int equals = html.indexOf('=', idx);
            if (equals < 0) {
                error(fileName, "Expected = after <a at " + html.substring(idx, idx + 100));
            }
            String type = html.substring(idx + 2, equals).trim();
            int start = html.indexOf('"', idx);
            if (start < 0) {
                error(fileName, "Expected \" after <a at " + html.substring(idx, idx + 100));
            }
            int end = html.indexOf('"', start + 1);
            if (end < 0) {
                error(fileName, "Expected \" after <a at " + html.substring(idx, idx + 100));
            }
            String ref = html.substring(start + 1, end);
            if (type.equals("href")) {
                // already checked
            } else if (type.equals("id")) {
                // For Javadoc 8
                targets.put(path + "#" + ref, TargetKind.ID);
            } else if (!type.equals("name")) {
                error(fileName, "Unsupported <a ?: " + html.substring(idx, idx + 100));
            }
        }
    }

    private static void error(String fileName, String string) {
        System.out.println("ERROR with " + fileName + ": " + string);
    }

}
