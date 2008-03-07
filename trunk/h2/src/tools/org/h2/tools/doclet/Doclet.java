/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0
 * (license2)
 * Initial Developer: H2 Group
 */
package org.h2.tools.doclet;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Comparator;

import org.h2.util.StringUtils;

import com.sun.javadoc.ClassDoc;
import com.sun.javadoc.FieldDoc;
import com.sun.javadoc.MethodDoc;
import com.sun.javadoc.ParamTag;
import com.sun.javadoc.Parameter;
import com.sun.javadoc.RootDoc;
import com.sun.javadoc.Tag;
import com.sun.javadoc.ThrowsTag;
import com.sun.javadoc.Type;

/**
 * This class is a custom doclet implementation to generate the
 * Javadoc for this product.
 */
public class Doclet {

    private static final boolean INTERFACES_ONLY = Boolean.getBoolean("h2.interfacesOnly");
    private int errorCount;

    public static boolean start(RootDoc root) throws IOException {
        return new Doclet().startDoc(root);
    }
    
    public boolean startDoc(RootDoc root) throws IOException {
        ClassDoc[] classes = root.classes();
        String[][] options = root.options();
        String destDir = System.getProperty("h2.destDir", "docs/javadoc");
        for (int i = 0; i < options.length; i++) {
            if (options[i][0].equals("destdir")) {
                destDir = options[i][1];
            }
        }
        for (int i = 0; i < classes.length; ++i) {
            ClassDoc clazz = classes[i];
            processClass(destDir, clazz);
        }
        if (errorCount > 0) {
            throw new IOException("FAILED: " + errorCount + " errors found");
        }
        return true;
    }

    private static String getClass(String name) {
        if (name.startsWith("Jdbc")) {
            return name.substring(4);
        }
        return name;
    }

    private void processClass(String destDir, ClassDoc clazz) throws IOException {
        String packageName = clazz.containingPackage().name();
        String dir = destDir + "/" + packageName.replace('.', '/');
        (new File(dir)).mkdirs();
        String fileName = dir + "/" + clazz.name() + ".html";
        String className = getClass(clazz.name());
        FileWriter out = new FileWriter(fileName);
        PrintWriter writer = new PrintWriter(new BufferedWriter(out));
        writer.println("<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Strict//EN\" " + 
                "\"http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd\">");
        String language = "en";
        writer.println("<html xmlns=\"http://www.w3.org/1999/xhtml\" " +
                "lang=\"" + language + "\" xml:lang=\"" + language + "\">");
        writer.println("<head><meta http-equiv=\"Content-Type\" content=\"text/html;charset=utf-8\" /><title>");
        writer.println(className);
        writer.println("</title><link rel=\"stylesheet\" type=\"text/css\" href=\"../../../stylesheet.css\" /></head><body>");
        writer.println("<table class=\"content\"><tr class=\"content\"><td class=\"content\"><div class=\"contentDiv\">");
        writer.println("<h1>" + className + "</h1>");
        writer.println(formatText(clazz.commentText()) + "<br /><br />");

        // method overview
        MethodDoc[] methods = clazz.methods();
        Arrays.sort(methods, new Comparator() {
            public int compare(Object a, Object b) {
                return ((MethodDoc) a).name().compareTo(((MethodDoc) b).name());
            }
        });
        boolean hasMethods = false;
        for (int i = 0; i < methods.length; i++) {
            MethodDoc method = methods[i];
            String name = method.name();
            if (skipMethod(method)) {
                continue;
            }
            if (!hasMethods) {
                writer.println("<table><tr><th colspan=\"2\">Methods</th></tr>");
                hasMethods = true;
            }
            String type = getTypeName(method.isStatic(), method.returnType());
            writer.println("<tr><td class=\"return\">" + type + "</td><td class=\"method\">");
            Parameter[] params = method.parameters();
            StringBuffer buff = new StringBuffer();
            buff.append('(');
            for (int j = 0; j < params.length; j++) {
                if (j > 0) {
                    buff.append(", ");
                }
                buff.append(getTypeName(false, params[j].type()));
                buff.append(' ');
                buff.append(params[j].name());
            }
            buff.append(')');
            if (isDeprecated(method)) {
                name = "<span class=\"deprecated\">" + name + "</span>";
            }
            writer.println("<a href=\"#r" + i + "\">" + name + "</a>" + buff.toString());
            String firstSentence = getFirstSentence(method.firstSentenceTags());
            if (firstSentence != null) {
                writer.println("<div class=\"methodText\">" + formatText(firstSentence) + "</div>");
            }
            writer.println("</td></tr>");
        }
        if (hasMethods) {
            writer.println("</table>");
        }

        // field overview
        FieldDoc[] fields = clazz.fields();
        if (clazz.interfaces().length > 0) {
            fields = clazz.interfaces()[0].fields();
        }
        Arrays.sort(fields, new Comparator() {
            public int compare(Object a, Object b) {
                return ((FieldDoc) a).name().compareTo(((FieldDoc) b).name());
            }
        });
        int fieldId = 0;
        for (int i = 0; i < fields.length; i++) {
            FieldDoc field = fields[i];
            if (skipField(clazz, field)) {
                continue;
            }
            String name = field.name();
            String text = field.commentText();
            if (text == null || text.trim().length() == 0) {
                System.out.println("Undocumented field? " + clazz.name() + "." + name + " " + field);
                errorCount++;
            }
            if (text.startsWith("INTERNAL")) {
                continue;
            }
            if (fieldId == 0) {
                writer.println("<br /><table><tr><th colspan=\"2\">Fields</th></tr>");
            }
            String type = getTypeName(true, field.type());
            writer.println("<tr><td class=\"return\">" + type + "</td><td class=\"method\">");
            String constant = field.constantValueExpression();

            // add a link (a name) if there is a <code> tag
            String link = getFieldLink(text, constant, clazz, name);
            writer.print("<a href=\"#" + link + "\">" + name + "</a>");
            if (constant == null) {
                writer.println();
            } else {
                writer.println(" = " + constant);
            }
            writer.println("</td></tr>");
            fieldId++;
        }
        if (fieldId > 0) {
            writer.println("</table>");
        }

        // message details
        for (int i = 0; i < methods.length; i++) {
            MethodDoc method = methods[i];
            String name = method.name();
            if (skipMethod(method)) {
                continue;
            }
            String type = getTypeName(method.isStatic(), method.returnType());
            writer.println("<a name=\"r" + i + "\"></a>");
            Parameter[] params = method.parameters();
            StringBuffer buff = new StringBuffer();
            buff.append('(');
            for (int j = 0; j < params.length; j++) {
                if (j > 0) {
                    buff.append(", ");
                }
                buff.append(getTypeName(false, params[j].type()));
                buff.append(' ');
                buff.append(params[j].name());
            }
            buff.append(')');
            ClassDoc[] exceptions = method.thrownExceptions();
            if (exceptions.length > 0) {
                buff.append(" throws ");
                for (int k = 0; k < exceptions.length; k++) {
                    if (k > 0) {
                        buff.append(", ");
                    }
                    buff.append(exceptions[k].typeName());
                }
            }
            if (isDeprecated(method)) {
                name = "<span class=\"deprecated\">" + name + "</span>";
            }
            writer.println("<h4>" + type + " <span class=\"methodName\">" + name + "</span>" + buff.toString()
                    + "</h4>");
            writer.println(formatText(method.commentText()));
            ParamTag[] paramTags = method.paramTags();
            boolean space = false;
            for (int j = 0; j < paramTags.length; j++) {
                if (!space) {
                    writer.println("<br /><br />");
                    space = true;
                }
                String p = paramTags[j].parameterName() + " - " + paramTags[j].parameterComment();
                if (j == 0) {
                    writer.println("<div class=\"itemTitle\">Parameters:</div>");
                }
                writer.println("<div class=\"item\">" + p + "</div>");
            }
            Tag[] returnTags = method.tags("return");
            if (returnTags != null && returnTags.length > 0) {
                if (!space) {
                    writer.println("<br /><br />");
                    space = true;
                }
                writer.println("<div class=\"itemTitle\">Returns:</div>");
                writer.println("<div class=\"item\">" + returnTags[0].text() + "</div>");
            }
            ThrowsTag[] throwsTags = method.throwsTags();
            if (throwsTags != null && throwsTags.length > 0) {
                if (!space) {
                    writer.println("<br /><br />");
                    space = true;
                }
                writer.println("<div class=\"itemTitle\">Throws:</div>");
                for (int j = 0; j < throwsTags.length; j++) {
                    String p = throwsTags[j].exceptionName();
                    String c = throwsTags[j].exceptionComment();
                    if (c.length() > 0) {
                        p += " - " + c;
                    }
                    writer.println("<div class=\"item\">" + p + "</div>");
                }
            }
            writer.println("<hr />");
        }

        // field details
        Arrays.sort(fields, new Comparator() {
            public int compare(Object a, Object b) {
                FieldDoc fa = (FieldDoc) a;
                FieldDoc fb = (FieldDoc) b;
                String ca = fa.constantValueExpression();
                String cb = fb.constantValueExpression();
                if (ca != null && cb != null) {
                    return ca.compareTo(cb);
                }
                return fa.name().compareTo(fb.name());
            }
        });
        for (int i = 0; i < fields.length; i++) {
            FieldDoc field = fields[i];
            if (skipField(clazz, field)) {
                continue;
            }
            String text = field.commentText();
            if (text.startsWith("INTERNAL")) {
                continue;
            }
            String name = field.name();
            String constant = field.constantValueExpression();
            String link = getFieldLink(text, constant, clazz, name);
            writer.println("<a name=\"" + link + "\"></a>");
            writer.println("<h4><span class=\"methodName\">" + name);
            if (constant == null) {
                writer.println();
            } else {
                writer.println(" = " + constant);
            }
            writer.println("</span></h4>");
            if (text != null) {
                writer.println("<div class=\"item\">" + formatText(text) + "</div>");
            }
            writer.println("<hr />");
        }

        writer.println("</div></td></tr></table></body></html>");
        writer.close();
        out.close();
    }

    private String getFieldLink(String text, String constant, ClassDoc clazz, String name) {
        String link = constant != null ? constant : name.toLowerCase();
        int linkStart = text.indexOf("<code>");
        if (linkStart >= 0) {
            int linkEnd = text.indexOf("</code>", linkStart);
            link = text.substring(linkStart + "<code>".length(), linkEnd);
            if (constant != null && !constant.equals(link)) {
                System.out.println("Wrong code tag? " + clazz.name() + "." + name +
                        " code: " + link + " constant: " + constant);
                errorCount++;
            }
        }
        if (Character.isDigit(link.charAt(0))) {
            link = "c" + link;
        }
        return link;
    }

    private static String formatText(String text) {
        if (text == null) {
            return text;
        }
        text = StringUtils.replaceAll(text, "\n </pre>", "</pre>");
        return text;
    }

    private static boolean skipField(ClassDoc clazz, FieldDoc field) {
        if (!field.isFinal() || !field.isStatic() || !field.isPublic() || field.containingClass() != clazz) {
            return true;
        }
        return false;
    }

    private boolean skipMethod(MethodDoc method) {
        ClassDoc clazz = method.containingClass();
        boolean isInterface = clazz.isInterface() || (clazz.isAbstract() && method.isAbstract());
         if (INTERFACES_ONLY && !isInterface) {
             return true;
         }
        String name = method.name();
        if (!method.isPublic() || name.equals("finalize")) {
            return true;
        }
        if (method.getRawCommentText().trim().startsWith("@deprecated INTERNAL")) {
            return true;
        }
        String firstSentence = getFirstSentence(method.firstSentenceTags());
        String raw = method.getRawCommentText();
        if (firstSentence != null && firstSentence.startsWith("INTERNAL")) {
            return true;
        }        
        if ((firstSentence == null || firstSentence.trim().length() == 0) && raw.indexOf("{@inheritDoc}") < 0) {
            if (!doesOverride(method)) {
                boolean setterOrGetter = name.startsWith("set") && method.parameters().length == 1;
                setterOrGetter |= name.startsWith("get") && method.parameters().length == 0;
                if (isInterface || !setterOrGetter) {
                    System.out.println("Undocumented method? " + " (" + clazz.name() + ".java:" + method.position().line() +") " + name + " " + raw);
                    errorCount++;
                    return true;
                }
            }
        }
        return false;
    }
    
    private boolean doesOverride(MethodDoc method) {
        ClassDoc clazz = method.containingClass();
int test;        
//System.out.println(clazz.name() + ". " + method.name());        
        ClassDoc[] ifs = clazz.interfaces();
        int pc = method.parameters().length;
        String name = method.name();
        for (int i = 0;; i++) {
            ClassDoc c;
            if (i < ifs.length) {
                c = ifs[i];
            } else {
                clazz = clazz.superclass();
                if (clazz == null) {
                    break;
                }
                c = clazz;
            }
            MethodDoc[] ms = c.methods();
            for (int j = 0; j < ms.length; j++) {
                MethodDoc m = ms[j];
//System.out.println("  " + c.name() + ". " + m);        
                if (m.name().equals(name) && m.parameters().length == pc) {
//System.out.println("    true");        
                    return true;
                }
            }
        }
//System.out.println("    false");        
        return false;
    }

    private static String getFirstSentence(Tag[] tags) {
        String firstSentence = null;
        if (tags.length > 0) {
            Tag first = tags[0];
            firstSentence = first.text();
        }
        return firstSentence;
    }

    private static String getTypeName(boolean isStatic, Type type) {
        String s = type.typeName() + type.dimension();
        if (isStatic) {
            s = "static " + s;
        }
        return s;
    }

    private static boolean isDeprecated(MethodDoc method) {
        Tag[] tags = method.tags();
        boolean deprecated = false;
        for (int j = 0; j < tags.length; j++) {
            Tag t = tags[j];
            if (t.kind().equals("@deprecated")) {
                deprecated = true;
            }
        }
        return deprecated;
    }
}
