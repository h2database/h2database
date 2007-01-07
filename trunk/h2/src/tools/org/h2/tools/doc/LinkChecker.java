/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools.doc;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;

import org.h2.util.IOUtils;
import org.h2.util.StartBrowser;
import org.h2.util.StringUtils;

public class LinkChecker {
    
    private static final boolean OPEN_EXTERNAL_LINKS = false;
    
    public static void main(String[] args) throws Exception {
        new LinkChecker().run(args);
    }
    
    private HashMap targets = new HashMap();
    private HashMap links = new HashMap();

    private void run(String[] args) throws Exception {
        String dir = "src/docsrc";
        for(int i=0; i<args.length; i++) {
            if("-dir".equals(args[i])) {
                dir = args[++i];
            }
        }
        process(dir);
        listExternalLinks();
        listBadLinks();
    }
    
    void listExternalLinks() {
        for(Iterator it = links.keySet().iterator(); it.hasNext(); ) {
            String link = (String) it.next();
            if(link.startsWith("http")) {
                if(link.indexOf("//localhost")>0) {
                    continue;
                }
                if(OPEN_EXTERNAL_LINKS) {
                    StartBrowser.openURL(link);
                }
                System.out.println("External Link: " + link);
            }
        }
    }
    
    void listBadLinks() throws Exception {
        ArrayList errors = new ArrayList();
        for(Iterator it = links.keySet().iterator(); it.hasNext(); ) {
            String link = (String) it.next();
            if(!link.startsWith("http") && !link.endsWith("h2.pdf")) {
                if(targets.get(link) == null) {
                    errors.add(links.get(link) + ": missing link " + link);
                }
            }
        }
        for(Iterator it = links.keySet().iterator(); it.hasNext(); ) {
            String link = (String) it.next();
            if(!link.startsWith("http")) {
                targets.remove(link);
            }
        }
        for(Iterator it = targets.keySet().iterator(); it.hasNext(); ) {
            String name = (String) it.next();
            if(targets.get(name).equals("name")) {
                errors.add("No link to " + name);
            }
        }
        Collections.sort(errors);
        for(int i=0; i<errors.size(); i++) {
            System.out.println(errors.get(i));
        }
        if(errors.size() > 0) {
            throw new Exception("Problems where found by the Link Checker");
        }
    }
    
    void process(String path) throws Exception {
        if(path.endsWith("/CVS") || path.endsWith("/.svn")) {
            return;
        }
        File file = new File(path);
        if(file.isDirectory()) {
            String[] list = file.list();
            for(int i=0; i<list.length; i++) {
                process(path + "/" + list[i]);
            }
        } else {
            processFile(path);
        }
    }
    
    void processFile(String path) throws Exception {
        targets.put(path, "file");
        String lower = StringUtils.toLowerEnglish(path);
        if(!lower.endsWith(".html") && !lower.endsWith(".htm")) {
            return;
        }
        String fileName = new File(path).getName();
        String parent = path.substring(0, path.lastIndexOf('/'));
        String html = IOUtils.readStringAndClose(new FileReader(path), -1);
        int idx = -1;
        while(true) {
            idx = html.indexOf(" id=\"", idx+1);
            if(idx < 0) {
                break;
            }
            int start = idx + 4;
            int end = html.indexOf("\"", start + 1);
            if(end < 0) {
                error(fileName, "expected \" after id= " + html.substring(idx, idx + 100));
            }
            String ref = html.substring(start+1, end);
            targets.put(path + "#" + ref, "id");
        }
        idx = -1;
        while(true) {
            idx = html.indexOf("<a ", idx+1);
            if(idx < 0) {
                break;
            }
            int equals = html.indexOf("=", idx);
            if(equals < 0) {
                error(fileName, "expected = after <a at " + html.substring(idx, idx + 100));
            }
            String type = html.substring(idx+2, equals).trim();
            int start = html.indexOf("\"", idx);
            if(start < 0) {
                error(fileName, "expected \" after <a at " + html.substring(idx, idx + 100));
            }
            int end = html.indexOf("\"", start + 1);
            if(end < 0) {
                error(fileName, "expected \" after <a at " + html.substring(idx, idx + 100));
            }
            String ref = html.substring(start+1, end);
            if(type.equals("href")) {
                if(ref.startsWith("http:") || ref.startsWith("https:")) {
                    // ok
                } else if(ref.startsWith("#")) {
                    ref = path + ref;
                } else {
                    String p = parent;
                    while(ref.startsWith(".")) {
                        if(ref.startsWith("./")) {
                            ref = ref.substring(2);
                        } else if(ref.startsWith("../")) {
                            ref = ref.substring(3);
                            p = p.substring(0, p.lastIndexOf('/'));
                        }
                    }
                    ref = p + "/" + ref;
                }
                links.put(ref, path);
            } else if(type.equals("name")) {
                targets.put(path + "#" + ref, "name");
            } else {
                error(fileName, "unsupported <a xxx: " + html.substring(idx, idx + 100));
            }
        }
    }

    private void error(String fileName, String string) {
        System.out.println("ERROR with " + fileName + ": " + string);
    }
    
}
