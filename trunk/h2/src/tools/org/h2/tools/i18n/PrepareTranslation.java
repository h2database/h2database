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
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
import java.util.Stack;
import org.h2.server.web.PageParser;
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
        extractFromHtml("src/docsrc/html", "src/docsrc/text");
        buildHtml("src/docsrc/text", "docs/html", "ja");
        buildHtml("src/docsrc/text", "docs/html", "de");
        buildHtml("src/docsrc/text", "docs/html", "en");
    }

    private static void buildHtml(String templateDir, String targetDir, String language) throws IOException {
        File[] list = new File(templateDir).listFiles();
        new File(targetDir).mkdirs();
        ArrayList fileNames = new ArrayList();
        for(int i=0; i<list.length; i++) {
            String name = list[i].getName();
            if(!name.endsWith(".jsp")) {
                continue;
            }
            // remove '.jsp'
            name = name.substring(0, name.length()-4);
            fileNames.add(name);
        }
        for(int i=0; i<list.length; i++) {
            String name = list[i].getName();
            if(!name.endsWith(".jsp")) {
                continue;
            }
            // remove '.jsp'
            name = name.substring(0, name.length()-4);
            String propName = templateDir + "/" + MAIN_LANGUAGE + "/" + name + "_" + MAIN_LANGUAGE + ".properties";
            Properties prop = FileUtils.loadProperties(propName);
            propName = templateDir + "/" + language + "/" + name + "_" + language + ".properties";
            if((new File(propName)).exists()) {
                Properties transProp = FileUtils.loadProperties(propName);
                prop.putAll(transProp);
            }
            String template = IOUtils.readStringAndClose(new FileReader(templateDir + "/" + name + ".jsp"), -1);
            String html = PageParser.parse(null, template, prop);
            html = StringUtils.replaceAll(html, "lang=\""+MAIN_LANGUAGE+"\"", "lang=\""+ language + "\"");
            for(int j=0; j<fileNames.size(); j++) {
                String n = (String) fileNames.get(j);
                html = StringUtils.replaceAll(html, n + ".html\"", n + "_" + language + ".html\"");
            }
            OutputStream out = new FileOutputStream(targetDir + "/" + name + "_" + language + ".html");
            OutputStreamWriter writer = new OutputStreamWriter(out, "UTF-8");
            writer.write(html);
            writer.close();
        }
    }

    private static void extractFromHtml(String dir, String target) throws Exception {
        File[] list = new File(dir).listFiles();
        for (int i = 0; i < list.length; i++) {
            File f = list[i];
            String name = f.getName();
            if (!name.endsWith(".html")) {
                continue;
            }
            // remove '.html'
            name = name.substring(0, name.length() - 5);
            if(name.indexOf('_') >= 0) {
                // ignore translated files
                continue;
            }
            String template = extract(name, f, target);
            FileWriter writer = new FileWriter(target + "/" + name + ".jsp");
            writer.write(template);
            writer.close();
        }
    }

//    private static boolean isText(String s) {
//        if (s.length() < 2) {
//            return false;
//        }
//        for (int i = 0; i < s.length(); i++) {
//            char c = s.charAt(i);
//            if (!Character.isDigit(c) && c != '.' && c != '-' && c != '+') {
//                return true;
//            }
//        }
//        return false;
//    }
    
    private static String getSpace(String s, boolean start) {
        if(start) {
            for(int i=0; i<s.length(); i++) {
                if(!Character.isWhitespace(s.charAt(i))) {
                    if(i==0) {
                        return "";
                    } else {
                        return s.substring(0, i);
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
        boolean templateIsCopy = false;
        while (true) {
            int event = parser.next();
            if (event == XMLParser.END_DOCUMENT) {
                break;
            } else if (event == XMLParser.CHARACTERS) {
                String s = parser.getText();
                String trim = s.trim();
                if (trim.length() == 0) {
                    if(buff.length()>0) {
                        buff.append(s);
                    } else {
                        template.append(s);
                    }                    
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
                    } else if(templateIsCopy) {
                        buff.append(getSpace(s, true));
                    }
                    if(templateIsCopy) {
                        buff.append(trim);
                        buff.append(getSpace(s, false));
                    } else {
                        buff.append(clean(trim));
                    }
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
                        if(templateIsCopy) {
                            template.append(buff.toString());
                        } else {
                            template.append("${" + nextKey + "}");
                        }
                        add(prop, nextKey, buff);
                    }
                    template.append(parser.getToken());
                } else {
                    template.append(parser.getToken());
                }
                tag = name;
            } else if (event == XMLParser.END_ELEMENT) {
                String name = parser.getName();
                if ("code".equals(name) || "a".equals(name) || "b".equals(name)
                        || "span".equals(name) || "em".equals(name)) {
                    if (ignoreEnd) {
                        if (buff.length() > 0) {
                            if(templateIsCopy) {
                                template.append(buff.toString());
                            } else {
                                template.append("${" + nextKey + "}");
                            }
                            add(prop, nextKey, buff);
                        }
                        template.append(parser.getToken());
                    } else {
                        if(buff.length() > 0) {
                            buff.append(parser.getToken());
                        }
                    }
                } else {
                    if (buff.length() > 0) {
                        if(templateIsCopy) {
                            template.append(buff.toString());
                        } else {
                            template.append("${" + nextKey + "}");
                        }
                        add(prop, nextKey, buff);
                    }
                    template.append(parser.getToken());
                }
                tag = (String) stack.pop();
            } else if (event == XMLParser.DTD) {
                template.append(parser.getToken());
            } else if (event == XMLParser.COMMENT) {
                template.append(parser.getToken());
            } else {
                int eventType = parser.getEventType();
                throw new Exception("Unexpected event " + eventType + " at "
                        + parser.getRemaining());
            }
//            if(!xml.startsWith(template.toString())) {
//                System.out.println(nextKey);
//                System.out.println(template.substring(template.length()-60) +";");
//                System.out.println(xml.substring(template.length()-60, template.length()));
//                System.out.println(template.substring(template.length()-55) +";");
//                System.out.println(xml.substring(template.length()-55, template.length()));
//                break;
//            }
        }
        new File(target + "/" +  MAIN_LANGUAGE).mkdirs();
        storeProperties(prop, target + "/" +  MAIN_LANGUAGE + "/" + documentName + "_" + MAIN_LANGUAGE + ".properties");
        String t = template.toString();
        if(templateIsCopy && !t.equals(xml)) {
            for(int i=0; i<Math.min(t.length(), xml.length()); i++) {
                if(t.charAt(i) != xml.charAt(i)) {
                    int start = Math.max(0, i - 30), end = Math.min(i + 30, xml.length());
                    t = t.substring(start, end);
                    xml = xml.substring(start, end);
                }
            }
            System.out.println("xml--------------------------------------------------: ");
            System.out.println(xml);
            System.out.println("t---------------------------------------------------: ");
            System.out.println(t);
            System.exit(1);
        }
        return t;
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
