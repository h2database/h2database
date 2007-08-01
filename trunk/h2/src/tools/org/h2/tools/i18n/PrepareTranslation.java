package org.h2.tools.i18n;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import org.h2.tools.doc.XMLParser;
import org.h2.util.FileUtils;
import org.h2.util.IOUtils;

public class PrepareTranslation {
    
    private static final String MAIN_LANGUAGE = "en";
    private static final String DELETED_PREFIX = "~";
    
    public static void main(String[] args) throws Exception {
        String baseDir = "src/tools/org/h2/tools/i18n";
        String path;
        path = "src/main/org/h2/res";
        prepare(baseDir, path);
        path = "src/main/org/h2/server/web/res";
        prepare(baseDir, path);
        int todoAllowTranslateHtmlFiles;
        // extract("src/docsrc/html");
    }
    
    private static void extract(String dir) throws Exception {
        File[] list = new File(dir).listFiles();
        HashSet set = new HashSet();
        for(int i=0; i<list.length; i++) {
            File f = list[i];
            String name = f.getName();
            if(!name.endsWith(".html")) {
                continue;
            }
            name = name.substring(0, name.length() - 5);
            extract(set, name, f);
        }
    }
    
    private static boolean isText(String s) {
        if(s.length() < 2) {
            return false;
        }
        for(int i=0; i<s.length(); i++) {
            char c = s.charAt(i);
            if(!Character.isDigit(c) && c != '.' && c != '-' && c != '+') {
                return true;
            }
        }
        return false;
    }

    private static void extract(HashSet set, String documentName, File f) throws Exception {
        String xml = IOUtils.readStringAndClose(new FileReader(f), -1);
        XMLParser parser = new XMLParser(xml);
        boolean translate = true;
        for(int i=0;;) {
            int event = parser.next();
            if(event == XMLParser.END_DOCUMENT) {
                break;
            } else if(event == XMLParser.CHARACTERS) {
                String text = parser.getText().trim();
                if(translate && !set.contains(text) && isText(text)) {
                    System.out.println(documentName + "_" + i++ );
                    System.out.println(text);
                    System.out.println();
                    set.add(text);
                }
            } else if(event == XMLParser.START_ELEMENT) {
                String name = parser.getName();
                if("code".equals(name) || "pre".equals(name)) {
                    translate = false;
                }
            } else if(event == XMLParser.END_ELEMENT) {
                translate = true;
            } else if(event == XMLParser.DTD) {
            } else if(event == XMLParser.COMMENT) {
            } else {
                int eventType = parser.getEventType();
                throw new Exception("Unexpected event " + eventType + " at " + parser.getRemaining());
            }
        }
    }

    private static void prepare(String baseDir, String path) throws IOException {
        File dir = new File(path);
        File[] list = dir.listFiles();
        File main = null;
        ArrayList translations = new ArrayList();
        for(int i=0; list != null && i<list.length; i++) {
            File f = list[i];
            if(f.getName().endsWith(".properties")) {
                if(f.getName().endsWith("_" + MAIN_LANGUAGE + ".properties")) {
                    main = f;
                } else {
                    translations.add(f);
                }
            }
        }
        Properties p = FileUtils.loadProperties(main.getAbsolutePath());
        Properties base = FileUtils.loadProperties(baseDir + "/" + main.getName());
        storeProperties(p, main.getAbsolutePath());
        for(int i=0; i<translations.size(); i++) {
            File trans = (File) translations.get(i);
            prepare(p, base, trans);
        }
        storeProperties(p, baseDir + "/" + main.getName());
    }

    private static void prepare(Properties main, Properties base, File trans) throws IOException {
        Properties p = FileUtils.loadProperties(trans.getAbsolutePath());
        // add missing keys, using # and the value from the main file
        Iterator it = main.keySet().iterator();
        while(it.hasNext()) {
            String key = (String) it.next();
            String now = main.getProperty(key);
            if(!p.containsKey(key)) {
                System.out.println(trans.getName() + ": key " + key + " not found in translation file; added dummy # 'translation'");
                p.put(key, "#" + now);
            } else {
                String last = base.getProperty(key);
                if(last != null && !last.equals(now)) {
                    // main data changed since the last run: review translatation
                    System.out.println(trans.getName() + ": key " + key + " changed; last=" + last + " now=" + now);
                    String old = p.getProperty(key);
                    p.put(key, "#" + now + " #" + old);
                }
            }
        }
        // remove keys that don't exist in the main file (deleted or typo in the key)
        it = new ArrayList(p.keySet()).iterator();
        while(it.hasNext()) {
            String key = (String) it.next();
            if(!main.containsKey(key) && !key.startsWith(DELETED_PREFIX)) {
                String newKey = key;
                while(true) {
                    newKey = DELETED_PREFIX + newKey;
                    if(!p.containsKey(newKey)) {
                        break;
                    }
                }
                System.out.println(trans.getName() + ": key " + key + " not found in main file; renamed to " + newKey);
                p.put(newKey, p.getProperty(key));
                p.remove(key);
            }
        }
        storeProperties(p, trans.getAbsolutePath());
    }
    
    static void storeProperties(Properties p, String fileName) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        p.store(out, null);
        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        InputStreamReader reader = new InputStreamReader(in, "ISO8859-1");
        LineNumberReader r = new LineNumberReader(reader);
        FileWriter w = new FileWriter(fileName);
        PrintWriter writer = new PrintWriter(new BufferedWriter(w));
        while(true) {
            String line = r.readLine();
            if(line == null) {
                break;
            }
            if(!line.startsWith("#")) {
                writer.println(line);
            }
        }
        writer.close();
    }
}
