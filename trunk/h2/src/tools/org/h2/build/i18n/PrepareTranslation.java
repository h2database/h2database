/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.build.i18n;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Stack;
import java.util.Map.Entry;

import org.h2.build.doc.XMLParser;
import org.h2.server.web.PageParser;
import org.h2.util.IOUtils;
import org.h2.util.SortedProperties;
import org.h2.util.StringUtils;

/**
 * This class updates the translation source code files by parsing
 * the HTML documentation. It also generates the translated HTML
 * documentation.
 */
public class PrepareTranslation {
    private static final String MAIN_LANGUAGE = "en";
    private static final boolean AUTO_TRANSLATE = false;
    private static final String[] EXCLUDE = { "datatypes.html", "functions.html", "grammar.html" };

    public static void main(String[] args) throws Exception {
        new PrepareTranslation().run();
    }

    private void run() throws Exception {
        String baseDir = "src/docsrc/textbase";
        prepare(baseDir, "src/main/org/h2/res");
        prepare(baseDir, "src/main/org/h2/server/web/res");

        // convert the txt files to properties files
        PropertiesToUTF8.textUTF8ToProperties("src/docsrc/text/_docs_de.utf8.txt",
                "src/docsrc/text/_docs_de.properties");
        PropertiesToUTF8.textUTF8ToProperties("src/docsrc/text/_docs_ja.utf8.txt",
                "src/docsrc/text/_docs_ja.properties");

        // create the .jsp files and extract the text in the main language
        extractFromHtml("docs/html", "src/docsrc/text");

        // add missing translations and create a new baseline
        prepare(baseDir, "src/docsrc/text");

        // create the translated documentation
        buildHtml("src/docsrc/text", "docs/html", "en");
        // buildHtml("src/docsrc/text", "docs/html", "de");
        buildHtml("src/docsrc/text", "docs/html", "ja");

        // convert the properties files back to utf8 text files, including the
        // main language (to be used as a template)
        PropertiesToUTF8.propertiesToTextUTF8("src/docsrc/text/_docs_en.properties",
                "src/docsrc/text/_docs_en.utf8.txt");
        PropertiesToUTF8.propertiesToTextUTF8("src/docsrc/text/_docs_de.properties",
                "src/docsrc/text/_docs_de.utf8.txt");
        PropertiesToUTF8.propertiesToTextUTF8("src/docsrc/text/_docs_ja.properties",
                "src/docsrc/text/_docs_ja.utf8.txt");

        // delete temporary files
        File[] list = new File("src/docsrc/text").listFiles();
        for (int i = 0; i < list.length; i++) {
            if (!list[i].getName().endsWith(".utf8.txt")) {
                list[i].delete();
            }
        }
    }

    private static void buildHtml(String templateDir, String targetDir, String language) throws IOException {
        File[] list = new File(templateDir).listFiles();
        new File(targetDir).mkdirs();
        // load the main 'translation'
        String propName = templateDir + "/_docs_" + MAIN_LANGUAGE + ".properties";
        Properties prop = SortedProperties.loadProperties(propName);
        propName = templateDir + "/_docs_" + language + ".properties";
        if (!(new File(propName)).exists()) {
            throw new IOException("Translation not found: " + propName);
        }
        Properties transProp = SortedProperties.loadProperties(propName);
        for (Iterator it = transProp.keySet().iterator(); it.hasNext();) {
            String key = (String) it.next();
            String t = transProp.getProperty(key);
            // overload with translations, but not the ones starting with #
            if (t.startsWith("##")) {
                prop.put(key, t.substring(2));
            } else if (!t.startsWith("#")) {
                prop.put(key, t);
            }
        }
        // add spaces to each token
        for (Iterator it = prop.keySet().iterator(); it.hasNext();) {
            String key = (String) it.next();
            String t = prop.getProperty(key);
            prop.put(key, " " + t + " ");
        }

        ArrayList fileNames = new ArrayList();
        for (int i = 0; i < list.length; i++) {
            String name = list[i].getName();
            if (!name.endsWith(".jsp")) {
                continue;
            }
            // remove '.jsp'
            name = name.substring(0, name.length() - 4);
            fileNames.add(name);
        }
        for (int i = 0; i < list.length; i++) {
            String name = list[i].getName();
            if (!name.endsWith(".jsp")) {
                continue;
            }
            // remove '.jsp'
            name = name.substring(0, name.length() - 4);
            String template = IOUtils.readStringAndClose(new FileReader(templateDir + "/" + name + ".jsp"), -1);
            String html = PageParser.parse(template, prop);
            html = StringUtils.replaceAll(html, "lang=\"" + MAIN_LANGUAGE + "\"", "lang=\"" + language + "\"");
            for (int j = 0; j < fileNames.size(); j++) {
                String n = (String) fileNames.get(j);
                if ("frame".equals(n)) {
                    // don't translate 'frame.html' to 'frame_ja.html', 
                    // otherwise we can't switch back to English
                    continue;
                }
                html = StringUtils.replaceAll(html, n + ".html\"", n + "_" + language + ".html\"");
            }
            html = StringUtils.replaceAll(html, "_" + MAIN_LANGUAGE + ".html\"", ".html\"");
            String target;
            if (language.equals(MAIN_LANGUAGE)) {
                target = targetDir + "/" + name + ".html";
            } else {
                target = targetDir + "/" + name + "_" + language + ".html";
            }
            OutputStream out = new FileOutputStream(target);
            OutputStreamWriter writer = new OutputStreamWriter(out, "UTF-8");
            writer.write(html);
            writer.close();
        }
    }
    
    private static boolean exclude(String fileName) {
        for (int i = 0; i < EXCLUDE.length; i++) {
            if (fileName.endsWith(EXCLUDE[i])) {
                return true;
            }
        }
        return false;
    }

    private static void extractFromHtml(String dir, String target) throws Exception {
        File[] list = new File(dir).listFiles();
        for (int i = 0; i < list.length; i++) {
            File f = list[i];
            String name = f.getName();
            if (!name.endsWith(".html")) {
                continue;
            }
            if (exclude(name)) {
                continue;
            }
            // remove '.html'
            name = name.substring(0, name.length() - 5);
            if (name.indexOf('_') >= 0) {
                // ignore translated files
                continue;
            }
            String template = extract(name, f, target);
            FileWriter writer = new FileWriter(target + "/" + name + ".jsp");
            writer.write(template);
            writer.close();
        }
    }

    // private static boolean isText(String s) {
    // if (s.length() < 2) {
    // return false;
    // }
    // for (int i = 0; i < s.length(); i++) {
    // char c = s.charAt(i);
    // if (!Character.isDigit(c) && c != '.' && c != '-' && c != '+') {
    // return true;
    // }
    // }
    // return false;
    // }

    private static String getSpace(String s, boolean start) {
        if (start) {
            for (int i = 0; i < s.length(); i++) {
                if (!Character.isWhitespace(s.charAt(i))) {
                    if (i == 0) {
                        return "";
                    }
                    return s.substring(0, i);
                }
            }
            return s;
        }
        for (int i = s.length() - 1; i >= 0; i--) {
            if (!Character.isWhitespace(s.charAt(i))) {
                if (i == s.length() - 1) {
                    return "";
                }
                return s.substring(i + 1, s.length());
            }
        }
        // if all spaces, return an empty string to avoid duplicate spaces
        return "";
    }

    private static String extract(String documentName, File f, String target) throws Exception {
        String xml = IOUtils.readStringAndClose(new InputStreamReader(new FileInputStream(f), "UTF-8"), -1);
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
                    if (buff.length() > 0) {
                        buff.append(s);
                    } else {
                        template.append(s);
                    }
                } else if ("p".equals(tag) || "li".equals(tag) || "a".equals(tag) || "td".equals(tag)
                        || "th".equals(tag) || "h1".equals(tag) || "h2".equals(tag) || "h3".equals(tag)
                        || "h4".equals(tag) || "body".equals(tag) || "b".equals(tag) || "code".equals(tag)
                        || "form".equals(tag) || "span".equals(tag) || "em".equals(tag)) {
                    if (buff.length() == 0) {
                        nextKey = documentName + "_" + (1000 + id++) + "_" + tag;
                        template.append(getSpace(s, true));
                    } else if (templateIsCopy) {
                        buff.append(getSpace(s, true));
                    }
                    if (templateIsCopy) {
                        buff.append(trim);
                        buff.append(getSpace(s, false));
                    } else {
                        buff.append(clean(trim));
                    }
                } else if ("pre".equals(tag) || "title".equals(tag) || "script".equals(tag) || "style".equals(tag)) {
                    // ignore, don't translate
                    template.append(s);
                } else {
                    System.out.println(f.getName() + " invalid wrapper tag for text: " + tag + " text: " + s);
                    System.out.println(parser.getRemaining());
                    throw new Exception();
                }
            } else if (event == XMLParser.START_ELEMENT) {
                stack.add(tag);
                String name = parser.getName();
                if ("code".equals(name) || "a".equals(name) || "b".equals(name) || "span".equals(name)) {
                    // keep tags if wrapped, but not if this is the wrapper
                    if (buff.length() > 0) {
                        buff.append(' ');
                        buff.append(parser.getToken().trim());
                        ignoreEnd = false;
                    } else {
                        ignoreEnd = true;
                        template.append(parser.getToken());
                    }
                } else if ("p".equals(tag) || "li".equals(tag) || "td".equals(tag) || "th".equals(tag)
                        || "h1".equals(tag) || "h2".equals(tag) || "h3".equals(tag) || "h4".equals(tag)
                        || "body".equals(tag) || "form".equals(tag)) {
                    if (buff.length() > 0) {
                        if (templateIsCopy) {
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
                if ("code".equals(name) || "a".equals(name) || "b".equals(name) || "span".equals(name)
                        || "em".equals(name)) {
                    if (ignoreEnd) {
                        if (buff.length() > 0) {
                            if (templateIsCopy) {
                                template.append(buff.toString());
                            } else {
                                template.append("${" + nextKey + "}");
                            }
                            add(prop, nextKey, buff);
                        }
                        template.append(parser.getToken());
                    } else {
                        if (buff.length() > 0) {
                            buff.append(parser.getToken());
                            buff.append(' ');
                        }
                    }
                } else {
                    if (buff.length() > 0) {
                        if (templateIsCopy) {
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
                throw new Exception("Unexpected event " + eventType + " at " + parser.getRemaining());
            }
            // if(!xml.startsWith(template.toString())) {
            // System.out.println(nextKey);
            // System.out.println(template.substring(template.length()-60)
            // +";");
            // System.out.println(xml.substring(template.length()-60,
            // template.length()));
            // System.out.println(template.substring(template.length()-55)
            // +";");
            // System.out.println(xml.substring(template.length()-55,
            // template.length()));
            // break;
            // }
        }
        new File(target).mkdirs();
        String propFileName = target + "/_docs_" + MAIN_LANGUAGE + ".properties";
        Properties old = SortedProperties.loadProperties(propFileName);
        prop.putAll(old);
        PropertiesToUTF8.storeProperties(prop, propFileName);
        String t = template.toString();
        if (templateIsCopy && !t.equals(xml)) {
            for (int i = 0; i < Math.min(t.length(), xml.length()); i++) {
                if (t.charAt(i) != xml.charAt(i)) {
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

    private void prepare(String baseDir, String path) throws IOException {
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
        Properties p = SortedProperties.loadProperties(main.getAbsolutePath());
        Properties base = SortedProperties.loadProperties(baseDir + "/" + main.getName());
        PropertiesToUTF8.storeProperties(p, main.getAbsolutePath());
        for (int i = 0; i < translations.size(); i++) {
            File trans = (File) translations.get(i);
            String language = trans.getName();
            language = language.substring(language.lastIndexOf('_') + 1, language.lastIndexOf('.'));
            prepare(p, base, trans, language);
        }
        PropertiesToUTF8.storeProperties(p, baseDir + "/" + main.getName());
    }

    private void prepare(Properties main, Properties base, File trans, String language) throws IOException {
        Properties p = SortedProperties.loadProperties(trans.getAbsolutePath());
        Properties oldTranslations = new Properties();
        for (Iterator it = base.keySet().iterator(); it.hasNext();) {
            String key = (String) it.next();
            String m = base.getProperty(key);
            String t = p.getProperty(key);
            if (t != null && !t.startsWith("#")) {
                oldTranslations.setProperty(m, t);
            }
        }
        HashSet toTranslate = new HashSet();
        // add missing keys, using # and the value from the main file
        Iterator it = main.keySet().iterator();
        while (it.hasNext()) {
            String key = (String) it.next();
            String now = main.getProperty(key);
            if (!p.containsKey(key)) {
                String t = oldTranslations.getProperty(now);
                if (t == null) {
                    if (AUTO_TRANSLATE) {
                        toTranslate.add(key);
                    } else {
                        System.out.println(trans.getName() + ": key " + key + " not found in translation file; added dummy # 'translation'");
                        t = "#" + now;
                        p.put(key, t);
                    }
                } else {
                    p.put(key, t);
                }
            } else {
                String t = p.getProperty(key);
                String last = base.getProperty(key);
                if (t.startsWith("#") && !t.startsWith("##")) {
                    // not translated before
                    t = oldTranslations.getProperty(now);
                    if (t == null) {
                        t = "#" + now;
                    }
                    p.put(key, t);
                } else if (last != null && !last.equals(now)) {
                    t = oldTranslations.getProperty(now);
                    if (t == null) {
                        // main data changed since the last run: review translation
                        System.out.println(trans.getName() + ": key " + key + " changed, please review; last=" + last
                                + " now=" + now);
                        if (AUTO_TRANSLATE) {
                            toTranslate.add(key);
                        } else {
                            String old = p.getProperty(key);
                            t = "#" + now + " #" + old;
                            p.put(key, t);
                        }
                    } else {
                        p.put(key, t);
                    }
                }
            }
        }
        Map autoTranslated = new HashMap();
        if (AUTO_TRANSLATE) {
            HashSet set = new HashSet();
            for (it = toTranslate.iterator(); it.hasNext();) {
                String key = (String) it.next();
                String now = main.getProperty(key);
                set.add(now);
            }
            if ("de".equals(language)) {
                autoTranslated = autoTranslate(set, "en", language);
            }
        }
        for (it = toTranslate.iterator(); it.hasNext();) {
            String key = (String) it.next();
            String now = main.getProperty(key);
            String t;
            if (AUTO_TRANSLATE) {
                t = "##" + autoTranslated.get(now);
            } else {
                System.out.println(trans.getName() + ": key " + key + " not found in translation file; added dummy # 'translation'");
                t = "#" + now;
            }
            p.put(key, t);
        }
        // remove keys that don't exist in the main file (deleted or typo in the key)
        it = new ArrayList(p.keySet()).iterator();
        while (it.hasNext()) {
            String key = (String) it.next();
            if (!main.containsKey(key)) {
                p.remove(key);
            }
        }
        PropertiesToUTF8.storeProperties(p, trans.getAbsolutePath());
    }

    private Map autoTranslate(Set toTranslate, String  sourceLanguage, String targetLanguage) {
        HashMap results = new HashMap();
        if (toTranslate.size() == 0) {
            return results;
        }
        int maxLength = 1500;
        int minSeparator = 100000;
        HashMap keyMap = new HashMap(toTranslate.size());
        StringBuffer buff = new StringBuffer(maxLength);
        // TODO make sure these numbers don't occur in the original text
        int separator = minSeparator;
        for (Iterator it = toTranslate.iterator(); it.hasNext();) {
            String original = (String) it.next();
            if (original != null) {
                original = original.trim();
                if (buff.length() + original.length() > maxLength) {
                    System.out.println("remaining: " + (toTranslate.size() - separator + minSeparator));
                    translateChunk(buff, separator, sourceLanguage, targetLanguage, keyMap, results);
                }
                keyMap.put(new Integer(separator), original);
                buff.append(separator);
                buff.append(' ');
                buff.append(original);
                buff.append(' ');
                separator++;
            }
        }
        translateChunk(buff, separator, sourceLanguage, targetLanguage, keyMap, results);
        return results;
    }

    private void translateChunk(StringBuffer buff, int separator, String source, String target, HashMap keyMap, HashMap results) {
        buff.append(separator);
        String original = buff.toString();
        String translation = "";
        try {
            translation = translate(original, source, target);
            System.out.println("original: " + original);
            System.out.println("translation: " + translation);
        } catch (Throwable e) {
            System.out.println("Exception translating [" + original + "]: " + e);
            e.printStackTrace();
        }
        for (Iterator it = keyMap.entrySet().iterator(); it.hasNext();) {
            Entry entry = (Entry) it.next();
            separator = ((Integer) entry.getKey()).intValue();
            String o = (String) entry.getValue();
            String startSeparator = String.valueOf(separator);
            int start = translation.indexOf(startSeparator);
            int end = translation.indexOf(String.valueOf(separator + 1));
            if (start < 0 || end < 0) {
                System.out.println("No translation for " + o);
                results.put(o, "#" + o);
            } else {
                String t = translation.substring(start + startSeparator.length(), end);
                t = t.trim();
                results.put(o, t);
            }
        }
        keyMap.clear();
        buff.setLength(0);
    }

    /**
     * Translate the text using Google Translate
     */
    String translate(String text, String sourceLanguage, String targetLanguage) throws Exception {
        Thread.sleep(4000);
        String url = "http://translate.google.com/translate_t?langpair=" + 
                sourceLanguage + "|" + targetLanguage + 
                "&text=" + URLEncoder.encode(text, "UTF-8");
        HttpURLConnection conn = (HttpURLConnection) (new URL(url)).openConnection();
        conn.setRequestProperty("User-Agent", "Mozilla/5.0 (compatible; Java)");
        String result = IOUtils.readStringAndClose(IOUtils.getReader(conn.getInputStream()), -1);
        int start = result.indexOf("<div id=result_box");
        start = result.indexOf('>', start) + 1;
        int end = result.indexOf("</div>", start);
        return result.substring(start, end);
    }

}
