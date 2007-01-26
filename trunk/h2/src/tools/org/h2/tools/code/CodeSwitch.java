/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools.code;


import java.io.*;
import java.util.*;

/**
 * @author Thomas
 * TODO codeswitch: replace with ant 'Replace' task is possible
 */
public class CodeSwitch {
    private boolean recurse;
    private ArrayList list = new ArrayList();
    private ArrayList switchOn = new ArrayList();
    private ArrayList switchOff = new ArrayList();
    private ArrayList switches = new ArrayList();
    private byte[] file;
    private String endOfLine;
    private ArrayList lines;
    private boolean changed;

    public static void main(String[] argv) {
        (new CodeSwitch()).run(argv);
    }

    private void run(String[] a) {
        if (a.length == 0) {
            showUsage();
            return;
        }
        boolean path = false;
        recurse = true;
        for (int i = 0; i < a.length; i++) {
            String p = a[i];
            if (p.startsWith("+")) {
                switchOn.add(p.substring(1));
            } else if (p.startsWith("-r+")) {
                // (default)
                recurse = true;
            } else if (p.startsWith("-r-")) {
                recurse = false;
            } else if (p.startsWith("-")) {
                switchOff.add(p.substring(1));
            } else {
                addDir(p, true);
                path = true;
            }
        }
        if (!path) {
            printError("no path specified");
            showUsage();
        }
        process();
        if (switchOff.size() == 0 && switchOn.size() == 0) {
            printSwitches();
        }
    }

    private void showUsage() {
        String className = getClass().getName();
        System.out.println("Usage: java " + className
                + " [-r+] [-r-] paths [+|-][labels]");
        System.out.println("If no labels are specified then all used");
        System.out.println("labels in the source code are shown.");
        System.out.println("-r+ recurse subdirectories (default)");
        System.out.println("-r- do not recurse subdirectories");
        System.out.println("Use +MODE to switch on code labeled MODE");
        System.out.println("Use -MODE to switch off code labeled MODE");
        System.out
                .println("Path: Any number of path or files may be specified.");
        System.out
                .println(" Use . for the current directory (including sub-directories).");
        System.out.println("Example: java " + className + " +JAVA2 .");
        System.out
                .println("This example switches on code labeled JAVA2 in all *.java files");
        System.out.println("in the current directory and all subdirectories.");
    }

    private void process() {
        int len = list.size();
        for (int i = 0; i < len; i++) {
            String fileName = (String) list.get(i);
            if (!processFile(fileName)) {
                System.out.println("in file " + fileName
                        + " - this file is skipped");
            }
        }
    }

    private void printSwitches() {
        System.out.println("Used labels:");
        for (int i = 0; i < switches.size(); i++) {
            System.out.println((String) (switches.get(i)));
        }
    }

    private void addDir(String path, boolean recurseMore) {
        File f = new File(path);
        if (f.isFile() && path.endsWith(".java")) {
            list.add(path);
        } else if (f.isDirectory()) {
            if (recurse || recurseMore) {
                // one recursion at least
                String[] files = f.list();
                for (int i = 0; i < files.length; i++) {
                    addDir(path + File.separatorChar + files[i], false);
                }
            }
        }
    }

    // lines are terminated with \r, \n or \r\n
    private void breakIntoLines() {
        lines = new ArrayList();
        int len = file.length;
        int last = 0;
        int cr = 0, lf = 0, crlf = 0;
        for (int i = 0; i < len; i++) {
            byte c = file[i];
            if (c == '\r' || c == '\n') {
                if (c == '\r') {
                    if (i < len - 1 && file[i + 1] == '\n') {
                        i++;
                        crlf++;
                    } else {
                        cr++;
                    }
                } else {
                    lf++;
                }
                if (i < len) {
                    lines.add(new String(file, last, i - last + 1));
                    last = i + 1;
                }
            }
        }
        if (cr > lf && cr > crlf) {
            endOfLine = "\r";
        } else if (lf > crlf) {
            endOfLine = "\n";
        } else {
            endOfLine = "\r\n";
        }
        lines.add(new String(file, last, len - last));
    }

    private String getLine(int line) {
        return (String) lines.get(line);
    }

    private void insertLine(int line, String s) {
        lines.add(line, s);
        changed = true;
    }

    private void removeLine(int line) {
        lines.remove(line);
        changed = true;
    }

    private boolean processFile(String name) {
        File f = new File(name);
        boolean off = false;
        boolean working = false;
        int state = 0;
        try {
            long rawLen = f.length();
            if (rawLen > Integer.MAX_VALUE) {
                printError("Files bigger than Integer.MAX_VALUE are not supported");
                return false;
            }
            int len = (int) rawLen;
            file = new byte[len];
            RandomAccessFile read = new RandomAccessFile(f, "r");
            read.readFully(file);
            read.close();
            breakIntoLines();
            changed = false;

            for (int i = 0; i < lines.size(); i++) {
                String line = getLine(i);
                String lineTrim = line.trim();
                if (working) {
                    if (lineTrim.startsWith("/*") || lineTrim.startsWith("*/")) {
                        removeLine(i);
                        i--;
                        continue;
                    }
                }
                if (lineTrim.startsWith("//#")) {
                    if (lineTrim.startsWith("//#ifdef ")) {
                        if (state != 0) {
                            printError("//#ifdef not allowed inside " + "//#ifdef");
                            return false;
                        }
                        state = 1;
                        String s = lineTrim.substring(9);
                        boolean switchedOn = false;
                        boolean switchedOff = false;
                        if (switchOn.indexOf(s) != -1) {
                            switchedOn = true;
                        }
                        if (switchOff.indexOf(s) != -1) {
                            switchedOff = true;
                        }
                        if (s.indexOf("&&") != -1) {
                            switchedOn = true;
                            s += "&&";
                            while (s.length() > 0) {
                                int id = s.indexOf("&&");
                                if (id == -1) {
                                    break;
                                }
                                String s1 = s.substring(0, id).trim();
                                s = s.substring(id + 2).trim();
                                if (switches.indexOf(s1) == -1) {
                                    switches.add(s1);
                                    switchedOn = false;
                                }
                                if (switchOn.indexOf(s1) == -1) {
                                    switchedOff = true;
                                    switchedOn = false;
                                }
                                if (switchOff.indexOf(s1) != -1) {
                                    switchedOff = true;
                                    switchedOn = false;
                                }
                            }
                        }
                        if (switchedOn) {
                            working = true;
                            off = false;
                        } else if (switchedOff) {
                            working = true;
                            insertLine(++i, "/*" + endOfLine);
                            off = true;
                        }
                        if (switches.indexOf(s) == -1) {
                            switches.add(s);
                        }
                    } else if (lineTrim.startsWith("//#else")) {
                        if (state != 1) {
                            printError("//#else without " + "//#ifdef");
                            return false;
                        }
                        state = 2;
                        if (working) {
                            if (off) {
                                insertLine(++i, "*/" + endOfLine);
                                off = false;
                            } else {
                                insertLine(++i, "/*" + endOfLine);
                                off = true;
                            }
                        }
                    } else if (lineTrim.startsWith("//#endif")) {
                        if (state == 0) {
                            printError("//#endif without " + "//#ifdef");
                            return false;
                        }
                        state = 0;
                        if (working && off) {
                            insertLine(i++, "*/" + endOfLine);
                        }
                        working = false;
                    }
                }
            }
            if (state != 0) {
                printError("//#endif missing");
                return false;
            }
            if (changed) {
                File fileNew = new File(name + ".new");
                FileWriter write = new FileWriter(fileNew);
                for (int i = 0; i < lines.size(); i++) {
                    write.write(getLine(i));
                }
                write.close();
                File fileBack = new File(name + ".bak");
                fileBack.delete();
                f.renameTo(fileBack);
                File fileCopy = new File(name);
                fileNew.renameTo(fileCopy);
                fileBack.delete();
                System.out.println(name);
            }
            return true;
        } catch (Exception e) {
            printError(e);
            return false;
        }
    }

    private static void printError(Exception e) {
        e.printStackTrace();
    }

    private static void printError(String s) {
        System.out.println("ERROR: " + s);
    }
}

