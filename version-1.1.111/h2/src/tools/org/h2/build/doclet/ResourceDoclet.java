/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.build.doclet;

import java.io.IOException;
import org.h2.build.doc.XMLParser;
import org.h2.build.indexer.HtmlConverter;
import org.h2.util.SortedProperties;
import com.sun.javadoc.ClassDoc;
import com.sun.javadoc.Doc;
import com.sun.javadoc.MethodDoc;
import com.sun.javadoc.RootDoc;
import com.sun.javadoc.Tag;

/**
 * This custom doclet generates resources from javadoc comments.
 * Only comments that contain 'at resource' are included.
 * Only class level and method level comments are supported.
 */
public class ResourceDoclet {

    private String destFile = System.getProperty("h2.destDir", "src/main/org/h2/res/javadoc.properties");

    private SortedProperties resources = new SortedProperties();

    /**
     * This method is called by the javadoc framework and is required for all
     * doclets.
     *
     * @param root the root
     * @return true if successful
     */
    public static boolean start(RootDoc root) throws IOException {
        return new ResourceDoclet().startDoc(root);
    }

    private boolean startDoc(RootDoc root) throws IOException {
        ClassDoc[] classes = root.classes();
        String[][] options = root.options();
        for (int i = 0; i < options.length; i++) {
            if (options[i][0].equals("dest")) {
                destFile = options[i][1];
            }
        }
        for (int i = 0; i < classes.length; ++i) {
            ClassDoc clazz = classes[i];
            processClass(clazz);
        }
        resources.store(destFile);
        return true;
    }

    private void processClass(ClassDoc clazz) throws IOException {
        String packageName = clazz.containingPackage().name();
        String className = clazz.name();
        addResource(packageName + "." + className, clazz);

        MethodDoc[] methods = clazz.methods();
        for (int i = 0; i < methods.length; i++) {
            MethodDoc method = methods[i];
            String name = method.name();
            addResource(packageName + "." + className + "." + name, method);
        }
    }


    private void addResource(String key, Doc doc) {
        if (!isResource(doc)) {
            return;
        }
        String xhtml = doc.commentText();
        XMLParser p = new XMLParser(xhtml);
        StringBuffer buff = new StringBuffer();
        int column = 0;
        int firstColumnSize = 0;
        boolean inColumn = false;
        while (p.hasNext()) {
            String s;
            switch(p.next()) {
            case XMLParser.END_ELEMENT:
                s = p.getName();
                if ("p".equals(s) || "tr".equals(s) || "br".equals(s)) {
                    buff.append('\n');
                }
                break;
            case XMLParser.START_ELEMENT:
                s = p.getName();
                if ("table".equals(s)) {
                    buff.append('\n');
                } else if ("tr".equals(s)) {
                    column = 0;
                } else if ("td".equals(s)) {
                    inColumn = true;
                    column++;
                    if (column == 2) {
                        buff.append('\t');
                    }
                }
                break;
            case XMLParser.CHARACTERS:
                s = HtmlConverter.convertHtmlToString(p.getText().trim());
                if (inColumn && column == 1) {
                    firstColumnSize = Math.max(s.length(), firstColumnSize);
                }
                buff.append(s);
                break;
            }
        }
        for (int i = 0; i < buff.length(); i++) {
            if (buff.charAt(i) == '\t') {
                buff.deleteCharAt(i);
                int length = i - buff.lastIndexOf("\n", i - 1);
                for (int k = length; k < firstColumnSize + 3; k++) {
                    buff.insert(i, ' ');
                }
            }
        }
        String text = buff.toString().trim();
        resources.setProperty(key, text);
    }

    private static boolean isResource(Doc doc) {
        Tag[] tags = doc.tags();
        for (int j = 0; j < tags.length; j++) {
            Tag t = tags[j];
            if (t.kind().equals("@h2.resource")) {
                return true;
            }
        }
        return false;
    }

}
