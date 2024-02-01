/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import org.h2.engine.SessionLocal;
import org.h2.expression.BinaryOperation;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.Operation0;
import org.h2.message.DbException;
import org.h2.test.TestBase;
import org.h2.value.TypeInfo;
import org.h2.value.Value;

/**
 * Test the binary operation.
 */
public class TestBinaryOperation extends TestBase {

    /**
     * Run just this test.
     *
     * @param a
     *            ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().testFromMain();
    }

    @Override
    public void test() throws Exception {
        testPlusMinus(BinaryOperation.OpType.PLUS);
        testPlusMinus(BinaryOperation.OpType.MINUS);
        testMultiply();
        testDivide();
    }

    private void testPlusMinus(BinaryOperation.OpType type) {
        assertPrecisionScale(2, 0, 2, type, 1, 0, 1, 0);
        assertPrecisionScale(3, 1, 2, type, 1, 1, 1, 0);
        assertPrecisionScale(3, 1, 2, type, 1, 0, 1, 1);
    }

    private void testMultiply() {
        assertPrecisionScale(2, 0, 2, BinaryOperation.OpType.MULTIPLY, 1, 0, 1, 0);
        assertPrecisionScale(2, 1, 2, BinaryOperation.OpType.MULTIPLY, 1, 1, 1, 0);
        assertPrecisionScale(2, 1, 2, BinaryOperation.OpType.MULTIPLY, 1, 0, 1, 1);
    }

    private void testDivide() {
        assertPrecisionScale(3, 2, 2, BinaryOperation.OpType.DIVIDE, 1, 0, 1, 0);
        assertPrecisionScale(3, 3, 2, BinaryOperation.OpType.DIVIDE, 1, 1, 1, 0);
        assertPrecisionScale(3, 1, 2, BinaryOperation.OpType.DIVIDE, 1, 0, 1, 1);
        assertPrecisionScale(25, 0, 10, BinaryOperation.OpType.DIVIDE, 1, 3, 9, 27);
    }

    private void assertPrecisionScale(int expectedPrecision, int expectedScale, int expectedDecfloatPrecision,
            BinaryOperation.OpType type, int precision1, int scale1, int precision2, int scale2) {
        TestExpression left = new TestExpression(TypeInfo.getTypeInfo(Value.NUMERIC, precision1, scale1, null));
        TestExpression right = new TestExpression(TypeInfo.getTypeInfo(Value.NUMERIC, precision2, scale2, null));
        TypeInfo typeInfo = new BinaryOperation(type, left, right).optimize(null).getType();
        assertEquals(Value.NUMERIC, typeInfo.getValueType());
        assertEquals(expectedPrecision, typeInfo.getPrecision());
        assertEquals(expectedScale, typeInfo.getScale());
        left = new TestExpression(TypeInfo.getTypeInfo(Value.DECFLOAT, precision1, 0, null));
        right = new TestExpression(TypeInfo.getTypeInfo(Value.DECFLOAT, precision2, 0, null));
        typeInfo = new BinaryOperation(type, left, right).optimize(null).getType();
        assertEquals(Value.DECFLOAT, typeInfo.getValueType());
        assertEquals(expectedDecfloatPrecision, typeInfo.getPrecision());
    }

    private static final class TestExpression extends Operation0 {

        private final TypeInfo type;

        TestExpression(TypeInfo type) {
            this.type = type;
        }

        @Override
        public Value getValue(SessionLocal session) {
            throw DbException.getUnsupportedException("");
        }

        @Override
        public TypeInfo getType() {
            return type;
        }

        @Override
        public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
            throw DbException.getUnsupportedException("");
        }

        @Override
        public boolean isEverything(ExpressionVisitor visitor) {
            return false;
        }

        @Override
        public int getCost() {
            return 0;
        }

    }

}
