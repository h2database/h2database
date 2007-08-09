/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools.i18n;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Stack;
import org.h2.tools.doc.XMLParser;
import org.h2.util.FileUtils;
import org.h2.util.IOUtils;
import org.h2.util.SortedProperties;
import org.h2.util.StringUtils;

public class PrepareTranslation {
    private static final String MAIN_LANGUAGE = "en";
    private static final String DELETED_PREFIX = "~";

    public static void main(String[] args) throws Exception {
        String baseDir = "src/docsrc/textbase";
        String path;
        path = "src/main/org/h2/res";
        prepare(baseDir, path);
        path = "src/main/org/h2/server/web/res";
        prepare(baseDir, path);
        int todoAllowTranslateHtmlFiles;
        extractFromHtml("src/docsrc/html", "src/docsrc/text");
        buildHtml("src/docsrc/html", "src/docsrc/text", "de");
    }

    private static void buildHtml(String htmlDir, String transDir, String language) throws IOException {
        File[] list = new File(transDir).listFiles();
        for(int i=0; i<list.length; i++) {
            String s = list[i].getName();
            int idx = s.indexOf("_" + language + ".");
            if(idx >= 0) {
                String p = list[i].getAbsolutePath();
                String doc = s.substring(0, idx);
                Properties transProp = FileUtils.loadProperties(p);
                Properties origProp = FileUtils.loadProperties(p);
                buildHtml(htmlDir, doc + ".html", doc + "_" + language + ".html", transProp, origProp);
            }
        }
    }

    private static void buildHtml(String htmlDir, String source, String target, Properties transProp, Properties origProp) {
        
    }

    private static void extractFromHtml(String dir, String target) throws Exception {
        File[] list = new File(dir).listFiles();
        for (int i = 0; i < list.length; i++) {
            File f = list[i];
            String name = f.getName();
            if (!name.endsWith(".html")) {
                continue;
            }
            name = name.substring(0, name.length() - 5);
            extract(name, f, target);
        }
    }

    private static boolean isText(String s) {
        if (s.length() < 2) {
            return false;
        }
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (!Character.isDigit(c) && c != '.' && c != '-' && c != '+') {
                return true;
            }
        }
        return false;
    }
    
    private static String getSpace(String s, boolean start) {
        if(start) {
            for(int i=0; i<s.length(); i++) {
                if(!Character.isWhitespace(s.charAt(i))) {
                    if(i==0) {
                        return "";
                    } else {
                        return s.substring(0, i - 1);
                    }
                }
            }
            return s;
        } else {
            for(int i=s.length() - 1; i>=0; i--) {
                if(!Character.isWhitespace(s.charAt(i))) {
                    if(i==s.length() - 1) {
                        return "";
                    } else {
                        return s.substring(i + 1, s.length());
                    }
                }
            }
            // if all spaces, return an empty string to avoid duplicate spaces
            return "";
        }
    }

    private static String extract(String documentName, File f, String target)
            throws Exception {
        String xml = IOUtils.readStringAndClose(new InputStreamReader(
                new FileInputStream(f), "UTF-8"), -1);
        StringBuffer template = new StringBuffer(xml.length());
        int id = 0;
        Properties prop = new SortedProperties();
        XMLParser parser = new XMLParser(xml);
        StringBuffer buff = new StringBuffer();
        Stack stack = new Stack();
        String tag = "";
        boolean ignoreEnd = false;
        String nextKey = "";
        while (true) {
            int event = parser.next();
            if (event == XMLParser.END_DOCUMENT) {
                break;
            } else if (event == XMLParser.CHARACTERS) {
                String s = parser.getText();
                String trim = s.trim();
                if (trim.length() == 0) {
                    template.append(s);
                    continue;
                } else if ("p".equals(tag) || "li".equals(tag)
                        || "a".equals(tag) || "td".equals(tag)
                        || "th".equals(tag) || "h1".equals(tag)
                        || "h2".equals(tag) || "h3".equals(tag)
                        || "h4".equals(tag) || "body".equals(tag)
                        || "b".equals(tag) || "code".equals(tag)
                        || "form".equals(tag) || "span".equals(tag)
                        || "em".equals(tag)) {
                    if(buff.length() == 0) {
                        nextKey = documentName + "_" + (1000 + id++) + "_" + tag;
                        template.append(getSpace(s, true));
                        
                        int todo;
                        template.append(s);
                        // template.append("${" + nextKey + "}");
                        
                        template.append(getSpace(s, false));
                    }
                    buff.append(clean(s));
                } else if ("pre".equals(tag) || "title".equals(tag)) {
                    // ignore, don't translate
                    template.append(s);
                } else {
                    System.out.println(f.getName()
                            + " invalid wrapper tag for text: " + tag
                            + " text: " + s);
                    System.out.println(parser.getRemaining());
                    throw new Exception();
                }
            } else if (event == XMLParser.START_ELEMENT) {
                stack.add(tag);
                String name = parser.getName();
                if ("code".equals(name) || "a".equals(name) || "b".equals(name)
                        || "span".equals(name)) {
                    // keep tags if wrapped, but not if this is the wrapper
                    if (buff.length() > 0) {
                        buff.append(parser.getToken().trim());
                        ignoreEnd = false;
                    } else {
                        ignoreEnd = true;
                        template.append(parser.getToken());
                    }
                } else if ("p".equals(tag) || "li".equals(tag)
                        || "td".equals(tag) || "th".equals(tag)
                        || "h1".equals(tag) || "h2".equals(tag)
                        || "h3".equals(tag) || "h4".equals(tag)
                        || "body".equals(tag) || "form".equals(tag)) {
                    if (buff.length() > 0) {
                        add(prop, nextKey, buff);
                    }
                    template.append(parser.getToken());
                }
                tag = name;
            } else if (event == XMLParser.END_ELEMENT) {
                String name = parser.getName();
                if ("code".equals(name) || "a".equals(name) || "b".equals(name)
                        || "span".equals(name) || "em".equals(name)) {
                    if (!ignoreEnd) {
                        if(buff.length() > 0) {
                            buff.append(parser.getToken());
                        }
                    } else {
                        template.append(parser.getToken());
                    }
                } else {
                    template.append(parser.getToken());
                    if (buff.length() > 0) {
                        add(prop, nextKey, buff);
                    }
                }
                tag = (String) stack.pop();
            } else if (event == XMLParser.DTD) {
            } else if (event == XMLParser.COMMENT) {
            } else {
                int eventType = parser.getEventType();
                throw new Exception("Unexpected event " + eventType + " at "
                        + parser.getRemaining());
            }
        }
        storeProperties(prop, target + "/" + documentName + ".properties");
        return template.toString();
    }

    private static String clean(String text) {
        if (text.indexOf('\r') < 0 && text.indexOf('\n') < 0) {
            return text;
        }
        text = text.replace('\r', ' ');
        text = text.replace('\n', ' ');
        text = StringUtils.replaceAll(text, "  ", " ");
        text = StringUtils.replaceAll(text, "  ", " ");
        return text;
    }

    private static void add(Properties prop, String document, StringBuffer text) {
        String s = text.toString().trim();
        text.setLength(0);
        prop.setProperty(document, s);
    }

    private static void prepare(String baseDir, String path) throws IOException {
        File dir = new File(path);
        File[] list = dir.listFiles();
        File main = null;
        ArrayList translations = new ArrayList();
        for (int i = 0; list != null && i < list.length; i++) {
            File f = list[i];
            if (f.getName().endsWith(".properties")) {
                if (f.getName().endsWith("_" + MAIN_LANGUAGE + ".properties")) {
                    main = f;
                } else {
                    translations.add(f);
                }
            }
        }
        Properties p = FileUtils.loadProperties(main.getAbsolutePath());
        Properties base = FileUtils.loadProperties(baseDir + "/"
                + main.getName());
        storeProperties(p, main.getAbsolutePath());
        for (int i = 0; i < translations.size(); i++) {
            File trans = (File) translations.get(i);
            prepare(p, base, trans);
        }
        storeProperties(p, baseDir + "/" + main.getName());
    }

    private static void prepare(Properties main, Properties base, File trans)
            throws IOException {
        Properties p = FileUtils.loadProperties(trans.getAbsolutePath());
        // add missing keys, using # and the value from the main file
        Iterator it = main.keySet().iterator();
        while (it.hasNext()) {
            String key = (String) it.next();
            String now = main.getProperty(key);
            if (!p.containsKey(key)) {
                System.out
                        .println(trans.getName()
                                + ": key "
                                + key
                                + " not found in translation file; added dummy # 'translation'");
                p.put(key, "#" + now);
            } else {
                String last = base.getProperty(key);
                if (last != null && !last.equals(now)) {
                    // main data changed since the last run: review translation
                    System.out.println(trans.getName() + ": key " + key
                            + " changed; last=" + last + " now=" + now);
                    String old = p.getProperty(key);
                    p.put(key, "#" + now + " #" + old);
                }
            }
        }
        // remove keys that don't exist in the main file (deleted or typo in the
        // key)
        it = new ArrayList(p.keySet()).iterator();
        while (it.hasNext()) {
            String key = (String) it.next();
            if (!main.containsKey(key) && !key.startsWith(DELETED_PREFIX)) {
                String newKey = key;
                while (true) {
                    newKey = DELETED_PREFIX + newKey;
                    if (!p.containsKey(newKey)) {
                        break;
                    }
                }
                System.out.println(trans.getName() + ": key " + key
                        + " not found in main file; renamed to " + newKey);
                p.put(newKey, p.getProperty(key));
                p.remove(key);
            }
        }
        storeProperties(p, trans.getAbsolutePath());
    }

    static void storeProperties(Properties p, String fileName)
            throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        p.store(out, null);
        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        InputStreamReader reader = new InputStreamReader(in, "ISO8859-1");
        LineNumberReader r = new LineNumberReader(reader);
        FileWriter w = new FileWriter(fileName);
        PrintWriter writer = new PrintWriter(new BufferedWriter(w));
        while (true) {
            String line = r.readLine();
            if (line == null) {
                break;
            }
            if (!line.startsWith("#")) {
                writer.println(line);
            }
        }
        writer.close();
    }
}
