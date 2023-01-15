/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.build.indexer;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.StringTokenizer;
import org.h2.util.StringUtils;

/**
 * The indexer creates the fulltext index of the HTML documentation.
 * It is used for the built-in HTML javascript search.
 */
public class Indexer {

    private static final int MIN_WORD_SIZE = 3;
    private static final int MAX_RELATIONS = 30;
    private static final String VERY_COMMON =
        ";the;be;to;of;and;a;in;that;have;i;it;for;not;on;with;he;as;you;do;at;" +
        "this;but;his;by;from;they;we;say;her;she;or;an;will;my;one;all;would;" +
        "there;their;what;so;up;out;if;about;who;get;which;go;me;when;make;" +
        "can;like;no;just;him;know;take;into;your;good;some;" +
        "could;them;see;other;than;then;now;look;only;come;its;over;think;" +
        "also;back;after;use;two;how;our;work;first;well;way;even;new;want;" +
        "because;any;these;give;most;us;";

    private final ArrayList<Page> pages = new ArrayList<>();

    /**
     * Lower case word to Word map.
     */
    private final HashMap<String, Word> words = new HashMap<>();
    private final HashSet<String> noIndex = new HashSet<>();
    private ArrayList <Word>wordList;
    private PrintWriter output;
    private Page page;
    private boolean title;
    private boolean heading;
    private String ignored;

    /**
     * This method is called when executing this application from the command
     * line.
     *
     * @param args the command line parameters
     * @throws Exception on failure
     */
    public static void main(String... args) throws Exception {
        new Indexer().run(args);
    }

    private void run(String... args) throws Exception {
        String dir = "docs";
        String destDir = "docs/html";
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-dir")) {
                dir = args[++i];
            } else if (args[i].equals("-destDir")) {
                destDir = args[++i];
            }
        }
        Path directory = Paths.get(dir);
        setNoIndex("index.html", "html/header.html", "html/search.html",
                "html/frame.html", "html/fragments.html",
                "html/sourceError.html", "html/source.html",
                "html/mainWeb.html", "javadoc/index.html",
                "javadoc/classes.html", "javadoc/allclasses-frame.html",
                "javadoc/allclasses-noframe.html",
                "javadoc/constant-values.html", "javadoc/overview-frame.html",
                "javadoc/overview-summary.html", "javadoc/serialized-form.html");
        output = new PrintWriter(Files.newBufferedWriter(Paths.get(destDir + "/index.js")));
        Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                readPages(directory.relativize(file).toString().replace('\\', '/'), file);
                return FileVisitResult.CONTINUE;
            }
        });
        output.println("var pages=new Array();");
        output.println("var ref=new Array();");
        output.println("var ignored='';");
        output.println("function Page(title, file) { ");
        output.println("    this.title=title; this.file=file;");
        output.println("}");
        output.println("function load() {");
        sortWords();
        removeOverflowRelations();
        sortPages();
        listPages();
        listWords();
        output.println("}");
        output.close();
    }

    private void setNoIndex(String... strings) {
        for (String s : strings) {
            noIndex.add(s);
        }
    }

    private void sortWords() {
        for (String name : new ArrayList<>(words.keySet())) {
            if (name.endsWith("s")) {
                String singular = name.substring(0, name.length() - 1);
                if (words.containsKey(singular)) {
                    Word wp = words.get(name);
                    Word ws = words.get(singular);
                    ws.addAll(wp);
                    words.remove(name);
                }
            } else if (name.startsWith("abc")) {
                words.remove(name);
            }
        }
        wordList = new ArrayList<>(words.values());
        // ignored very common words (to shrink the index)
        StringBuilder ignoredBuff = new StringBuilder(";");
        int maxSize = pages.size() / 4;
        for (int i = 0; i < wordList.size(); i++) {
            Word word = wordList.get(i);
            String search = ";" + word.name.toLowerCase() + ";";
            int idxCommon = VERY_COMMON.indexOf(search);
            if (word.pages.size() >= maxSize || idxCommon >= 0) {
                wordList.remove(i);
                ignoredBuff.append(word.name);
                ignoredBuff.append(';');
                i--;
            }
        }
        ignored = ignoredBuff.toString();
        // TODO support A, B, C,... class links in the index file and use them
        // for combined AND searches
        wordList.sort((w0, w1) -> w0.name.compareToIgnoreCase(w1.name));
    }

    private void removeOverflowRelations() {
        for (Word word : wordList) {
            ArrayList<Weight> weights = word.getSortedWeights();
            int max = MAX_RELATIONS;
            if (weights.size() > max) {
                while (max < weights.size()) {
                    Weight weight = weights.get(max);
                    if (weight.value < Weight.HEADER) {
                        break;
                    }
                    max++;
                }
            }
            while (max < weights.size()) {
                Weight weight = weights.get(max);
                weights.remove(max);
                weight.page.relations--;
            }
        }
    }

    private void sortPages() {
        pages.sort((p0, p1) -> Integer.compare(p1.relations, p0.relations));
        for (int i = 0; i < pages.size(); i++) {
            pages.get(i).id = i;
        }
    }

    private void listPages() {
        for (Page p : pages) {
            output.println("pages[" + p.id + "]=new Page('"
                    + convertUTF(p.title) + "', '" + p.fileName                    + "');");
        }
    }

    /**
     * Read the pages of a file.
     *
     * @param fileName the file name
     * @param file the path
     */
    void readPages(String fileName, Path file) throws IOException {
        String lower = StringUtils.toLowerEnglish(fileName);
        if (!lower.endsWith(".html") && !lower.endsWith(".htm")) {
            return;
        }
        if (!noIndex.contains(fileName)) {
            page = new Page(pages.size(), fileName);
            pages.add(page);
            readPage(file);
        }
    }

    private void listWords() {
        output.println("// words: " + wordList.size());
        StringBuilder buff = new StringBuilder();
        String first = "";
        int firstLen = 1;
        int totalRelations = 0;
        for (Word word : wordList) {
            ArrayList<Weight> weights = word.getSortedWeights();
            String lower = StringUtils.toLowerEnglish(word.name);
            if (!first.equals(lower.substring(0, firstLen))) {
                if (buff.length() > 0) {
                    output.println("ref['" + convertUTF(first) + "']='" + buff.toString() + "';");
                    buff = new StringBuilder();
                }
                first = lower.substring(0, firstLen);
            }
            if (buff.length() > 0) {
                buff.append(';');
            }
            buff.append(convertUTF(word.name));
            buff.append('=');
            String weightString = "r";
            totalRelations += weights.size();
            for (int j = 0; j < weights.size(); j++) {
                Weight weight = weights.get(j);
                Page p = weight.page;
                if (j > 0) {
                    buff.append(",");
                }
                String ws;
                if (weight.value >= Weight.TITLE) {
                    ws = "t";
                } else if (weight.value >= Weight.HEADER) {
                    ws = "h";
                } else {
                    ws = "r";
                }
                if (!ws.equals(weightString)) {
                    weightString = ws;
                    buff.append(ws);
                }
                buff.append(p.id);
            }
        }
        output.println("ref['" + convertUTF(first) + "']='" + buff.toString() + "';");
        output.println("// totalRelations: " + totalRelations);
        output.println("ignored='" + ignored.toLowerCase() + "';");
    }

    private void readPage(Path file) throws IOException {
        String text = new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
        StringTokenizer t = new StringTokenizer(text, "<> \r\n", true);
        boolean inTag = false;
        title = false;
        heading = false;
        while (t.hasMoreTokens()) {
            String token = t.nextToken();
            if (token.length() == 1) {
                char c = token.charAt(0);
                switch (c) {
                case '<': {
                    if (inTag) {
                        process("???");
                    }
                    inTag = true;
                    if (!t.hasMoreTokens()) {
                        break;
                    }
                    token = t.nextToken();
                    if (token.startsWith("/")) {
                        title = false;
                        heading = false;
                    } else if (token.equalsIgnoreCase("title")) {
                        title = true;
                    } else if (token.length() == 2 && Character.toLowerCase(token.charAt(0)) == 'h'
                            && Character.isDigit(token.charAt(1))) {
                        heading = true;
                    }
                    // TODO maybe skip script tags?
                    break;
                }
                case '>': {
                    if (!inTag) {
                        process("???");
                    }
                    inTag = false;
                    break;
                }
                case '\r':
                case '\n':
                case ' ':
                    break;
                default:
                    if (!inTag) {
                        process(token);
                    }
                }
            } else {
                if (!inTag) {
                    process(token);
                }
            }
        }

        if (page.title == null || page.title.trim().length() == 0) {
            String title = file.getFileName().toString();
            System.out.println("Error: not title found in " + title);
            page.title = title;
        }
        page.title = page.title.trim();
    }

    private void process(String text) {
        text = HtmlConverter.convertHtmlToString(text);
        if (title) {
            if (page.title == null) {
                page.title = text;
            } else {
                page.title = page.title + " " + text;
            }
        }
        int weight;
        if (title) {
            weight = Weight.TITLE;
        } else if (heading) {
            weight = Weight.HEADER;
        } else {
            weight = Weight.PARAGRAPH;
        }
        // this list of constants needs to be the same in search.js
        // (char) 160: nbsp
        StringTokenizer t = new StringTokenizer(text,
                " \t\r\n\"'.,:;!&/\\?%@`[]{}()+-=<>|*^~#$" + (char) 160, false);
        while (t.hasMoreTokens()) {
            String token = t.nextToken();
            if (token.length() < MIN_WORD_SIZE) {
                continue;
            }
            if (Character.isDigit(token.charAt(0))) {
                continue;
            }
            String lower = StringUtils.toLowerEnglish(token);
            Word word = words.get(lower);
            if (word == null) {
                word = new Word(token);
                words.put(lower, word);
            } else if (!word.name.equals(token)) {
                word.name = token.compareTo(word.name) > 0 ? token : word.name;
            }
            page.totalWeight += weight;
            word.addPage(page, weight);
        }
    }

    private static String convertUTF(String s) {
        s = StringUtils.quoteJavaString(s);
        s = s.substring(1, s.length() - 1);
        return s;
    }

}
