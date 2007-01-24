package org.h2.tools.doc;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.h2.util.IOUtils;

public class SpellChecker {

    private HashSet dictionary = new HashSet();
    private HashSet used = new HashSet();
    private HashMap unknown = new HashMap();
    private boolean debug;
    private boolean addToDictionary;
    private static final String[] SUFFIX = new String[]{"java", "sql", "cpp", "txt", "html", "xml", "jsp", "css", "bat", "nsi", "csv", "xml", "js", "def", "dev", "h", "Driver", "properties", "win", "task", "php", "" };
    private static final String[] IGNORE = new String[]{"gif", "png", "odg", "ico", "sxd", "zip", "bz2", "rc", "layout", "res", "dll", "jar"};
    
    public static void main(String[] args) throws IOException {
        String dir = "src";
        new SpellChecker().run("tools/org/h2/tools/doc/dictionary.txt", dir);
    }

    private void run(String dictionary, String dir) throws IOException {
        process(new File(dir + "/" + dictionary));
        process(new File(dir));
        System.out.println("used words");        
        for(Iterator it = used.iterator(); it.hasNext();) {
            String s = (String) it.next();
            System.out.print(s + " ");
        }
        System.out.println();        
        System.out.println("ALL UNKNOWN ----------------------------");        
        for(Iterator it = unknown.keySet().iterator(); it.hasNext();) {
            String s = (String) it.next();
            int count = ((Integer) unknown.get(s)).intValue();
            if(count > 5) {
                System.out.print(s + " ");
            }
        }
        System.out.println();        
        if(unknown.size() > 0) {
            throw new IOException("spell check failed");
        }
    }

    private void process(File file) throws IOException {
        String name = file.getCanonicalPath();
        if(name.endsWith(".svn")) {
            return;
        }
        
        int removeThisLater;
        if(name.indexOf("\\test\\") >= 0) {
            return;
        }
        
        if(file.isDirectory()) {
            File[] list = file.listFiles();
            for(int i=0; i<list.length; i++) {
                process(list[i]);
            }
        } else {
            String fileName = file.getAbsolutePath();
            int idx = fileName.lastIndexOf('.');
            String suffix;
            if(idx < 0) {
                suffix = "";
            } else {
                suffix = fileName.substring(idx + 1);
            }
            boolean ignore = false;
            for(int i=0; i<IGNORE.length; i++) {
                if(IGNORE[i].equals(suffix)) {
                    ignore = true;
                    break;
                }
            }
            if(ignore) {
                return;
            }
            boolean ok = false;
            for(int i=0; i<SUFFIX.length; i++) {
                if(SUFFIX[i].equals(suffix)) {
                    ok = true;
                    break;
                }
            }
            if(!ok) {
                throw new IOException("Unsupported suffix: " + suffix + " for file: " + fileName);
            }
            if("java".equals(suffix) || "xml".equals(suffix) || "txt".equals(suffix)) {
                FileReader reader = null;
                String text = null;
                try {
                    reader = new FileReader(file);
                    text = readStringAndClose(reader, -1);                    
                } finally {
                    IOUtils.closeSilently(reader);
                }
                if(fileName.endsWith("dictionary.txt")) {
                    addToDictionary = true;
                } else {
                    addToDictionary = false;
                }
                scan(fileName, text);
            }
        }
    }
    
    private void scan(String fileName, String text) {
        HashSet notfound = new HashSet();
        StringTokenizer tokenizer = new StringTokenizer(text, "\r\n \t+\"*%&/()='[]{},.-;:_<>\\!?$@#|~^`");
        while(tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            char first = token.charAt(0);
            if(Character.isDigit(first)) {
                continue;
            }
            if(!addToDictionary && debug) {
                System.out.print(token + " ");
            }
            scanCombinedToken(notfound, token);
            if(!addToDictionary && debug) {
                System.out.println();
            }
        }
        if(notfound.isEmpty()) {
            return;
        }
        if(notfound.size() > 0) {
            System.out.println("file: " + fileName);
            for(Iterator it = notfound.iterator(); it.hasNext();) {
                String s = (String) it.next();
                System.out.print(s + " ");
            }
            System.out.println();
        }
    }
    
    private void scanCombinedToken(HashSet notfound, String token) {
        for(int i=1; i<token.length(); i++) {
            char cleft = token.charAt(i-1);
            char cright = token.charAt(i);
            if(Character.isLowerCase(cleft) && Character.isUpperCase(cright)) {
                scanToken(notfound, token.substring(0, i));
                token = token.substring(i);
                i = 1;
            } else if(Character.isUpperCase(cleft) && Character.isLowerCase(cright)) {
                scanToken(notfound, token.substring(0, i - 1));
                token = token.substring(i - 1);
                i = 1;
            }
        }     
        scanToken(notfound, token);
    }
    
    private void scanToken(HashSet notfound, String token) {
        if(token.length() < 3) {
            return;
        }
        while(true) {
            char last = token.charAt(token.length() - 1);
            if(!Character.isDigit(last)) {
                break;
            }
            token = token.substring(0, token.length() - 1);
        }
        if(token.length() < 3) {
            return;
        }
        for(int i=0; i<token.length(); i++) {
            if(Character.isDigit(token.charAt(i))) {
                return;
            }
        }
        token = token.toLowerCase();
        if(!addToDictionary && debug) {
            System.out.print(token + " ");
        }        
        if(addToDictionary) {
            dictionary.add(token);
        } else {
            if(!dictionary.contains(token)) {
                notfound.add(token);
                increment(unknown, token);
            } else {
                used.add(token);
            }
        }
    }
    
    private void increment(HashMap map, String key) {
        Integer value = (Integer) map.get(key);
        value = new Integer(value == null ? 0 : value.intValue() + 1);
        map.put(key, value);
    }
    
    public static String readStringAndClose(Reader in, int length) throws IOException {
        if(length <= 0) {
            length = Integer.MAX_VALUE;
        }
        int block = Math.min(4096, length);
        StringWriter out=new StringWriter(length == Integer.MAX_VALUE ? block : length);
        char[] buff=new char[block];
        while(length > 0) {
            int len = Math.min(block, length);
            len = in.read(buff, 0, len);
            if(len < 0) {
                break;
            }
            out.write(buff, 0, len);
            length -= len;
        }
        in.close();
        return out.toString();
    }
}
