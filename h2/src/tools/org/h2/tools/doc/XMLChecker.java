/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools.doc;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.util.Stack;

/**
 * This class checks that the HTML and XML part of the source code
 * is well-formed XML.
 */
public class XMLChecker {

    public static void main(String[] args) throws Exception {
        new XMLChecker().run(args);
    }

    private void run(String[] args) throws Exception {
        String dir = ".";
        for (int i = 0; i < args.length; i++) {
            if ("-dir".equals(args[i])) {
                dir = args[++i];
            }
        }
        process(dir + "/src");
        process(dir + "/docs");
    }

    void process(String path) throws Exception {
        if (path.endsWith("/CVS") || path.endsWith("/.svn")) {
            return;
        }
        File file = new File(path);
        if (file.isDirectory()) {
            String[] list = file.list();
            for (int i = 0; i < list.length; i++) {
                process(path + "/" + list[i]);
            }
        } else {
            processFile(path);
        }
    }

    void processFile(String fileName) throws Exception {
        int idx = fileName.lastIndexOf('.');
        if (idx < 0) {
            return;
        }
        String suffix = fileName.substring(idx + 1);
        if (!suffix.equals("html") && !suffix.equals("xml") && !suffix.equals("jsp")) {
            return;
        }
        System.out.println("Checking file:" + fileName);
        FileReader reader = new FileReader(fileName);
        String s = readStringAndClose(reader, -1);
        Exception last = null;
        try {
            checkXML(s, !suffix.equals("xml"));
        } catch (Exception e) {
            last = e;
            System.out.println("ERROR: " + e.toString());
        }
        if (last != null) {
            last.printStackTrace();
        }
    }

    public static String readStringAndClose(Reader in, int length) throws IOException {
        if (length <= 0) {
            length = Integer.MAX_VALUE;
        }
        int block = Math.min(4096, length);
        StringWriter out = new StringWriter(length == Integer.MAX_VALUE ? block : length);
        char[] buff = new char[block];
        while (length > 0) {
            int len = Math.min(block, length);
            len = in.read(buff, 0, len);
            if (len < 0) {
                break;
            }
            out.write(buff, 0, len);
            length -= len;
        }
        in.close();
        return out.toString();
    }

    private static void checkXML(String xml, boolean html) throws Exception {
        // String lastElement = null;
        // <li>: replace <li>([^\r]*[^<]*) with <li>$1</li>
        // use this for html file, for example if <li> is not closed
        String[] noClose = new String[] {};
        XMLParser parser = new XMLParser(xml);
        Stack stack = new Stack();
        boolean rootElement = false;
        while (true) {
            int event = parser.next();
            if (event == XMLParser.END_DOCUMENT) {
                break;
            } else if (event == XMLParser.START_ELEMENT) {
                if (stack.size() == 0) {
                    if (rootElement) {
                        throw new Exception("Second root element at " + parser.getRemaining());
                    }
                    rootElement = true;
                }
                String name = parser.getName();
                for (int i = 0; html && i < noClose.length; i++) {
                    if (name.equals(noClose[i])) {
                        name = null;
                        break;
                    }
                }
                if (name != null) {
                    stack.add(new Object[] { name, new Integer(parser.getPos()) });
                }
            } else if (event == XMLParser.END_ELEMENT) {
                String name = parser.getName();
                for (int i = 0; html && i < noClose.length; i++) {
                    if (name.equals(noClose[i])) {
                        throw new Exception("Unnecessary closing element " + name + " at " + parser.getRemaining());
                    }
                }
                while (true) {
                    Object[] pop = (Object[]) stack.pop();
                    String p = (String) pop[0];
                    if (p.equals(name)) {
                        break;
                    }
                    String remaining = xml.substring(((Integer) pop[1]).intValue());
                    if (remaining.length() > 100) {
                        remaining = remaining.substring(0, 100);
                    }
                    throw new Exception("Unclosed element " + p + " at " + remaining);
                }
            } else if (event == XMLParser.CHARACTERS) {
                // lastElement = parser.getText();
            } else if (event == XMLParser.DTD) {
            } else if (event == XMLParser.COMMENT) {
            } else {
                int eventType = parser.getEventType();
                throw new Exception("Unexpected event " + eventType + " at " + parser.getRemaining());
            }
        }
        if (stack.size() != 0) {
            throw new Exception("Unclosed root element");
        }
    }

}
