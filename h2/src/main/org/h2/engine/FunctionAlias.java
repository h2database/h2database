/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.SQLException;

import org.h2.command.Parser;
import org.h2.constant.ErrorCode;
import org.h2.expression.Expression;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.table.Table;
import org.h2.util.ClassUtils;
import org.h2.util.ObjectArray;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * Represents a user-defined function, or alias.
 * 
 * @author Thomas Mueller
 * @author Gary Tong
 */
public class FunctionAlias extends DbObjectBase {
    
    private String className;
    private String methodName;
    private JavaMethod[] javaMethods; 

    public FunctionAlias(Database db, int id, String name, String javaClassMethod, boolean force) throws SQLException {
        initDbObjectBase(db, id, name, Trace.FUNCTION);
        int paren = javaClassMethod.indexOf('(');
        int lastDot = javaClassMethod.lastIndexOf('.', paren < 0 ? javaClassMethod.length() : paren);
        if (lastDot < 0) {
            throw Message.getSQLException(ErrorCode.SYNTAX_ERROR_1, javaClassMethod);
        }
        className = javaClassMethod.substring(0, lastDot);
        methodName = javaClassMethod.substring(lastDot + 1);
        try {
            // at least try to load the class, otherwise the data type is not
            // initialized if it could be
            load();
        } catch (SQLException e) {
            if (!force) {
                throw e;
            }
        }
    }

    private synchronized void load() throws SQLException {
        if (javaMethods != null) {
            return;
        }
        Class javaClass = ClassUtils.loadUserClass(className);
        Method[] methods = javaClass.getMethods();
        ObjectArray list = new ObjectArray();
        for (int i = 0; i < methods.length; i++) {
            Method m = methods[i];
            if (!Modifier.isStatic(m.getModifiers())) {
                continue;
            }
            if (m.getName().equals(methodName) || getMethodSignature(m).equals(methodName)) {
                JavaMethod javaMethod = new JavaMethod(m);
                for (int j = 0; j < list.size(); j++) {
                    JavaMethod old = (JavaMethod) list.get(j);
                    if (old.paramCount == javaMethod.paramCount) {
                        throw Message.getSQLException(ErrorCode.METHODS_MUST_HAVE_DIFFERENT_PARAMETER_COUNTS_2, 
                                new String[] {
                                    old.method.toString(),
                                    javaMethod.method.toString()
                                }
                        );
                    }
                }
                list.add(javaMethod);
            }
        }
        if (list.size() == 0) {
            throw Message.getSQLException(ErrorCode.METHOD_NOT_FOUND_1, methodName + " (" + className + ")");
        }
        javaMethods = new JavaMethod[list.size()];
        list.toArray(javaMethods);
    }

    private String getMethodSignature(Method m) {
        StringBuffer buff = new StringBuffer();
        buff.append(m.getName());
        buff.append('(');
        Class[] params = m.getParameterTypes();
        for (int i = 0; i < params.length; i++) {
            if (i > 0) {
                buff.append(", ");
            }
            Class p = params[i];
            if (p.isArray()) {
                buff.append(p.getComponentType().getName());
                buff.append("[]");
            } else {
                buff.append(p.getName());
            }
        }
        buff.append(')');
        return buff.toString();
    }

    public String getCreateSQLForCopy(Table table, String quotedName) {
        throw Message.getInternalError();
    }

    public String getDropSQL() {
        return "DROP ALIAS IF EXISTS " + getSQL();
    }

    public String getCreateSQL() {
        StringBuffer buff = new StringBuffer();
        buff.append("CREATE FORCE ALIAS ");
        buff.append(getSQL());
        buff.append(" FOR ");
        buff.append(Parser.quoteIdentifier(className + "." + methodName));
        return buff.toString();
    }

    public int getType() {
        return DbObject.FUNCTION_ALIAS;
    }

    public synchronized void removeChildrenAndResources(Session session) throws SQLException {
        database.removeMeta(session, getId());
        className = null;
        methodName = null;
        javaMethods = null;
        invalidate();
    }

    public void checkRename() throws SQLException {
        throw Message.getUnsupportedException();
    }

    /**
     * Find the Java method that matches the arguments.
     * 
     * @param args the argument list
     * @return the Java method
     * @throws SQLException if no matching method could be found
     */
    public JavaMethod findJavaMethod(Expression[] args) throws SQLException {
        load();
        for (int i = 0; i < javaMethods.length; i++) {
            if (javaMethods[i].paramCount == args.length) {
                return javaMethods[i];
            }
        }
        throw Message.getSQLException(ErrorCode.METHOD_NOT_FOUND_1, methodName + " (" + className + ")");
    }

    public String getJavaClassName() {
        return this.className;
    }

    public String getJavaMethodName() {
        return this.methodName;
    }
    
    /**
     * Get the Java methods mapped by this function.
     * 
     * @return the Java methods.
     */
    public JavaMethod[] getJavaMethods() throws SQLException {
        load();
        return javaMethods;
    }
    
    /**
     * There may be multiple Java methods that match a function name.
     * Each method must have a different number of parameters however.
     * This helper class represents one such method.
     */
    public static class JavaMethod {
        Method method;
        boolean hasConnectionParam;
        int paramCount;
        int dataType;

        JavaMethod(Method method) throws SQLException {
            this.method = method;
            Class[] paramClasses = method.getParameterTypes();
            paramCount = paramClasses.length;
            if (paramCount > 0) {
                Class paramClass = paramClasses[0];
                if (Connection.class.isAssignableFrom(paramClass)) {
                    hasConnectionParam = true;
                    paramCount--;
                }
            }
            Class returnClass = method.getReturnType();
            dataType = DataType.getTypeFromClass(returnClass);
        }
        
        /**
         * Check if this function requires a database connection.
         * 
         * @return if the function requires a connection
         */
        public boolean hasConnectionParam() {
            return this.hasConnectionParam;
        }
        
        /**
         * Call the user-defined function and return the value.
         * 
         * @param session the session
         * @param args the argument list
         * @param columnList true if the function should only return the column list
         * @return the value
         */
        public Value getValue(Session session, Expression[] args, boolean columnList) throws SQLException {
            Class[] paramClasses = method.getParameterTypes();
            Object[] params = new Object[paramClasses.length];
            int p = 0;
            if (hasConnectionParam && params.length > 0) {
                params[p++] = session.createConnection(columnList);
            }
            for (int a = 0; a < args.length && p < params.length; a++, p++) {
                Class paramClass = paramClasses[p];
                int type = DataType.getTypeFromClass(paramClass);
                Value v = args[a].getValue(session);
                v = v.convertTo(type);
                Object o = v.getObject();
                if (o == null) {
                    if (paramClass.isPrimitive()) {
                        if (columnList) {
                            // if the column list is requested, the parameters may
                            // be null
                            // need to set to default value otherwise the function
                            // can't be called at all
                            o = DataType.getDefaultForPrimitiveType(paramClass);
                        } else {
                            // NULL for a java primitive: return NULL
                            return ValueNull.INSTANCE;
                        }
                    }
                } else {
                    if (!paramClass.isAssignableFrom(o.getClass()) && !paramClass.isPrimitive()) {
                        o = DataType.convertTo(session, session.createConnection(false), v, paramClass);
                    }
                }
                params[p] = o;
            }
            boolean old = session.getAutoCommit();
            try {
                session.setAutoCommit(false);
                try {
                    Object returnValue;
                    returnValue = method.invoke(null, params);
                    if (returnValue == null) {
                        return ValueNull.INSTANCE;
                    }
                    Value ret = DataType.convertToValue(session, returnValue, dataType);
                    return ret.convertTo(dataType);
                } catch (Exception e) {
                    throw Message.convert(e);
                }
            } finally {
                session.setAutoCommit(old);
            }
        }
        
        public Class[] getColumnClasses() throws SQLException {
            return method.getParameterTypes();
        }

        public int getDataType() {
            return dataType;
        }
        
        public int getParameterCount() throws SQLException {
            return paramCount;
        }

    }

}
