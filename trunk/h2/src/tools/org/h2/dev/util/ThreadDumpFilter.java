package org.h2.dev.util;
import java.io.InputStreamReader;
import java.io.LineNumberReader;

/**
 * Filter full thread dumps from a log file.
 */
public class ThreadDumpFilter {
    
    /**
     * Usage: java ThreadDumpFilter <log.txt >threadDump.txt
     * @param a ignored
     */    
    public static void main(String[] a) throws Exception {
        LineNumberReader in = new LineNumberReader(new InputStreamReader(System.in));
        for (String s; (s = in.readLine()) != null;) {
            if (s.startsWith("Full thread")) {
                do {
                    System.out.println(s);
                    s = in.readLine();
                } while(s != null && (s.length() == 0 || "\t\"".indexOf(s.charAt(0)) >= 0));
            }
        }
    }
}
