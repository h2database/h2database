/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.build.doc;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Stack;

/**
 * This class checks that the HTML and XML part of the source code
 * is well-formed XML.
 */
public class XMLChecker {

    /**
     * This method is called when executing this application from the command
     * line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws Exception {
        XMLChecker.run(args);
    }

    private static void run(String... args) throws Exception {
        Path dir = Paths.get(".");
        for (int i = 0; i < args.length; i++) {
            if ("-dir".equals(args[i])) {
                dir = Paths.get(args[++i]);
            }
        }
        process(dir.resolve("src"));
        process(dir.resolve("docs"));
    }

    private static void process(Path path) throws Exception {
        Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                // For Javadoc 8
                if (dir.getFileName().toString().equals("javadoc")) {
                    return FileVisitResult.SKIP_SUBTREE;
                }
                return FileVisitResult.CONTINUE;
            }

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
    static void processFile(Path file) throws IOException {
        String fileName = file.getFileName().toString();
        int idx = fileName.lastIndexOf('.');
        if (idx < 0) {
            return;
        }
        String suffix = fileName.substring(idx + 1);
        if (!suffix.equals("html") && !suffix.equals("xml") && !suffix.equals("jsp")) {
            return;
        }
        // System.out.println("Checking file:" + fileName);
        String s = new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
        Exception last = null;
        try {
            checkXML(s, !suffix.equals("xml"));
        } catch (Exception e) {
            last = e;
            System.out.println("ERROR in file " + fileName + " " + e.toString());
        }
        if (last != null) {
            last.printStackTrace();
        }
    }

    private static void checkXML(String xml, boolean html) throws Exception {
        // String lastElement = null;
        // <li>: replace <li>([^\r]*[^<]*) with <li>$1</li>
        // use this for html file, for example if <li> is not closed
        String[] noClose = {"br", "hr", "input", "link", "meta", "wbr"};
        XMLParser parser = new XMLParser(xml);
        Stack<Object[]> stack = new Stack<>();
        boolean rootElement = false;
        loop: for (;;) {
            int event = parser.next();
            if (event == XMLParser.END_DOCUMENT) {
                break;
            } else if (event == XMLParser.START_ELEMENT) {
                if (stack.isEmpty()) {
                    if (rootElement) {
                        throw new Exception("Second root element at " + parser.getRemaining());
                    }
                    rootElement = true;
                }
                String name = parser.getName();
                if (html) {
                    for (String n : noClose) {
                        if (name.equals(n)) {
                            name = null;
                            break;
                        }
                    }
                }
                if (name != null) {
                    stack.add(new Object[] { name, parser.getPos() });
                }
            } else if (event == XMLParser.END_ELEMENT) {
                String name = parser.getName();
                if (html) {
                    for (String n : noClose) {
                        if (name.equals(n)) {
                            continue loop;
                        }
                    }
                }
                while (true) {
                    Object[] pop = stack.pop();
                    String p = (String) pop[0];
                    if (p.equals(name)) {
                        break;
                    }
                    String remaining = xml.substring((Integer) pop[1]);
                    if (remaining.length() > 100) {
                        remaining = remaining.substring(0, 100);
                    }
                    throw new Exception("Unclosed element " + p + " at " + remaining);
                }
            } else if (event == XMLParser.CHARACTERS) {
                // lastElement = parser.getText();
            } else if (event == XMLParser.DTD) {
                // ignore
            } else if (event == XMLParser.COMMENT) {
                // ignore
            } else {
                int eventType = parser.getEventType();
                throw new Exception("Unexpected event " + eventType + " at "
                        + parser.getRemaining());
            }
        }
        if (!stack.isEmpty()) {
            throw new Exception("Unclosed root element");
        }
    }

}
