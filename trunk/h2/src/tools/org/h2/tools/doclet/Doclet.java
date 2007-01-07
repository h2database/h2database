/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
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

import com.sun.javadoc.ClassDoc;
import com.sun.javadoc.FieldDoc;
import com.sun.javadoc.MethodDoc;
import com.sun.javadoc.ParamTag;
import com.sun.javadoc.Parameter;
import com.sun.javadoc.RootDoc;
import com.sun.javadoc.Tag;
import com.sun.javadoc.ThrowsTag;
import com.sun.javadoc.Type;

public class Doclet {
    public static boolean start(RootDoc root) throws IOException {
        ClassDoc[] classes = root.classes();
        String[][] options = root.options();
        String destDir = "docs/javadoc";
        for(int i=0; i<options.length; i++) {
            if(options[i][0].equals("destdir")) {
                destDir = options[i][1];
            }
        }
        for (int i = 0; i < classes.length; ++i) {
            ClassDoc clazz = classes[i];
            processClass(destDir, clazz);
        }
        return true;
    }

    private static String getClass(String name) {
        if(name.startsWith("Jdbc")) {
            return name.substring(4);
        }
        return name;
    }

    private static void processClass(String destDir, ClassDoc clazz) throws IOException {
        String packageName = clazz.containingPackage().name();
        String dir = destDir +"/"+ packageName.replace('.', '/');
        (new File(dir)).mkdirs();
        String fileName = dir + "/" + clazz.name() + ".html";
        String className = getClass(clazz.name());
        FileWriter out = new FileWriter(fileName);
        PrintWriter writer = new PrintWriter(new BufferedWriter(out));
        writer.println("<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\" \"http://www.w3.org/TR/html4/loose.dtd\">");
        writer.println("<html><head><meta http-equiv=\"Content-Type\" content=\"text/html;charset=utf-8\"><title>");
        writer.println(className);
        writer.println("</title><link rel=\"stylesheet\" type=\"text/css\" href=\"../../../stylesheet.css\"></head><body>");
        writer.println("<table class=\"content\"><tr class=\"content\"><td class=\"content\"><div class=\"contentDiv\">");

        writer.println("<h1>"+className+"</h1>");
        writer.println(clazz.commentText()+"<br><br>");

        MethodDoc[] methods = clazz.methods();
        Arrays.sort(methods, new Comparator() {
            public int compare(Object a, Object b) {
                return ((MethodDoc)a).name().compareTo(((MethodDoc)b).name());
            }
        });
        writer.println("<table><tr><th colspan=\"2\">Methods</th></tr>");
        for(int i=0; i<methods.length; i++) {
            MethodDoc method = methods[i];
            String name = method.name();
            if(skipMethod(method)) {
                continue;
            }
            String type = getTypeName(method.isStatic(), method.returnType());
            writer.println("<tr><td class=\"return\">" + type + "</td><td class=\"method\">");
            Parameter[] params = method.parameters();
            StringBuffer buff = new StringBuffer();
            buff.append('(');
            for(int j=0; j<params.length; j++) {
                if(j>0) {
                    buff.append(", ");
                }
                buff.append(getTypeName(false, params[j].type()));
                buff.append(' ');
                buff.append(params[j].name());
            }
            buff.append(')');
            if(isDeprecated(method)) {
                name = "<span class=\"deprecated\">" + name + "</span>";
            }
            writer.println("<a href=\"#r" + i + "\">" + name + "</a>"+buff.toString());
            String firstSentence = getFirstSentence(method.firstSentenceTags());
            if(firstSentence != null) {
                writer.println("<div class=\"methodText\">"+firstSentence+"</div>");
            }
            writer.println("</td></tr>");
        }
        writer.println("</table>");
        FieldDoc[] fields = clazz.fields();
        if(clazz.interfaces().length > 0) {
            fields = clazz.interfaces()[0].fields();
        }
        Arrays.sort(fields, new Comparator() {
            public int compare(Object a, Object b) {
                return ((FieldDoc)a).name().compareTo(((FieldDoc)b).name());
            }
        });
        int fieldId=0;
        for(int i=0; i<fields.length; i++) {
            FieldDoc field = fields[i];
            if(!field.isFinal() || !field.isStatic() || !field.isPublic()) {
                continue;
            }
            if(fieldId==0) {
                writer.println("<br><table><tr><th colspan=\"2\">Fields</th></tr>");
            }
            String name = field.name();
            String type = getTypeName(true, field.type());
            writer.println("<tr><td class=\"return\">" + type + "</td><td class=\"method\">");
            //writer.println("<a href=\"#f" + fieldId + "\">" + name + "</a>");
            writer.println(name + " = " + field.constantValueExpression());
            String firstSentence = getFirstSentence(field.firstSentenceTags());
            if(firstSentence != null) {
                writer.println("<div class=\"methodText\">"+firstSentence+"</div>");
            }
            writer.println("</td></tr>");
            fieldId++;
        }
        if(fieldId > 0) {
            writer.println("</table>");
        }

        for(int i=0; i<methods.length; i++) {
            MethodDoc method = methods[i];
            String name = method.name();
            if(skipMethod(method)) {
                continue;
            }
            String type = getTypeName(method.isStatic(), method.returnType());
            writer.println("<a name=\"r"+i+"\"></a>");
            Parameter[] params = method.parameters();
            StringBuffer buff = new StringBuffer();
            buff.append('(');
            for(int j=0; j<params.length; j++) {
                if(j>0) {
                    buff.append(", ");
                }
                buff.append(getTypeName(false, params[j].type()));
                buff.append(' ');
                buff.append(params[j].name());
            }
            buff.append(')');
            ClassDoc[] exceptions = method.thrownExceptions();
            if(exceptions.length>0) {
                buff.append(" throws ");
                for(int k=0; k<exceptions.length; k++) {
                    if(k>0) {
                        buff.append(", ");
                    }
                    buff.append(exceptions[k].typeName());
                }
            }
            if(isDeprecated(method)) {
                name = "<span class=\"deprecated\">" + name + "</span>";
            }
            writer.println("<h4>"+ type + " <span class=\"methodName\">"+name+"</span>" + buff.toString()+"</h4>");
            writer.println(method.commentText());
            ParamTag[] paramTags = method.paramTags();
            boolean space = false;
            for(int j=0; j<paramTags.length; j++) {
                if(!space) {
                    writer.println("<br><br >");
                    space = true;
                }
                String p = paramTags[j].parameterName() + " - " + paramTags[j].parameterComment();
                if(j==0) {
                    writer.println("<div class=\"itemTitle\">Parameters:</div>");
                }
                writer.println("<div class=\"item\">"+p+"</div>");
            }
            Tag[] returnTags = method.tags("return");
            if(returnTags != null && returnTags.length>0) {
                if(!space) {
                    writer.println("<br><br >");
                    space = true;
                }
                writer.println("<div class=\"itemTitle\">Returns:</div>");
                writer.println("<div class=\"item\">"+returnTags[0].text()+"</div>");
            }
            ThrowsTag[] throwsTags =  method.throwsTags();
            if(throwsTags != null && throwsTags.length > 0) {
                if(!space) {
                    writer.println("<br><br >");
                    space = true;
                }
                writer.println("<div class=\"itemTitle\">Throws:</div>");
                for(int j=0; j<throwsTags.length; j++) {
                    String p = throwsTags[j].exceptionName();
                    String c = throwsTags[j].exceptionComment();
                    if(c.length() > 0) {
                        p += " - " + c;
                    }
                    writer.println("<div class=\"item\">"+p+"</div>");
                }
            }
            writer.println("<hr>");
        }
        writer.println("</div></td></tr></table></body></html>");
        writer.close();
        out.close();
    }

    private static boolean skipMethod(MethodDoc method) {
        String name = method.name();
        if(!method.isPublic() || name.equals("finalize")) {
            return true;
        }
        if(method.getRawCommentText().startsWith("@deprecated INTERNAL")) {
            return true;
        }
        String firstSentence = getFirstSentence(method.firstSentenceTags());
        if(firstSentence==null || firstSentence.trim().length()==0) {
            throw new Error("undocumented method? " + name+ " " + method.containingClass().name()+" "+method.getRawCommentText());
        }
        if(firstSentence.startsWith("INTERNAL")) {
            return true;
        }
        return false;
    }

    private static String getFirstSentence(Tag[] tags) {

        String firstSentence = null;
        if(tags.length>0) {
            Tag first = tags[0];
            firstSentence = first.text();
        }
        return firstSentence;
    }

    private static String getTypeName(boolean isStatic, Type type) {
        String s = type.typeName() + type.dimension();
        if(isStatic) {
            s = "static " + s;
        }
        return s;
    }

    private static boolean isDeprecated(MethodDoc method) {
        Tag[] tags = method.tags();
        boolean deprecated = false;
        for(int j=0; j<tags.length; j++) {
            Tag t = tags[j];
            if(t.kind().equals("@deprecated")) {
                deprecated = true;
            }
        }
        return deprecated;
    }
}
