/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;

import org.h2.command.Parser;
import org.h2.test.TestBase;
import org.h2.util.ParserUtil;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

/**
 * Tests keywords.
 */
public class TestKeywords extends TestBase {

    /**
     * Run just this test.
     *
     * @param a
     *            ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        final HashSet<String> set = new HashSet<>();
        ClassReader r = new ClassReader(Parser.class.getResourceAsStream("Parser.class"));
        r.accept(new ClassVisitor(Opcodes.ASM6) {
            @Override
            public FieldVisitor visitField(int access, String name, String descriptor, String signature, Object value) {
                add(set, value);
                return null;
            }

            @Override
            public MethodVisitor visitMethod(int access, String name, String descriptor, String signature,
                    String[] exceptions) {
                return new MethodVisitor(Opcodes.ASM6) {
                    @Override
                    public void visitLdcInsn(Object value) {
                        add(set, value);
                    }
                };
            }

            void add(HashSet<String> set, Object value) {
                if (!(value instanceof String)) {
                    return;
                }
                String s = (String) value;
                int l = s.length();
                if (l == 0 || ParserUtil.getSaveTokenType(s, false, 0, l, true) != ParserUtil.IDENTIFIER) {
                    return;
                }
                for (int i = 0; i < l; i++) {
                    char ch = s.charAt(i);
                    if ((ch < 'A' || ch > 'Z') && ch != '_') {
                        return;
                    }
                }
                set.add(s);
            }
        }, ClassReader.SKIP_DEBUG | ClassReader.SKIP_FRAMES);
        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:keywords")) {
            Statement stat = conn.createStatement();
            for (String s : set) {
                // _ROWID_ is a special virtual column
                String column = s.equals("_ROWID_") ? "C" : s;
                try {
                    stat.execute("CREATE TABLE " + s + '(' + column + " INT)");
                    stat.execute("INSERT INTO " + s + '(' + column + ") VALUES (10)");
                    try (ResultSet rs = stat.executeQuery("SELECT " + column + " FROM " + s)) {
                        assertTrue(rs.next());
                        assertEquals(10, rs.getInt(1));
                        assertFalse(rs.next());
                    }
                } catch (Throwable t) {
                    throw new AssertionError(s + " cannot be used as identifier.", t);
                }
            }
        }
    }

}
