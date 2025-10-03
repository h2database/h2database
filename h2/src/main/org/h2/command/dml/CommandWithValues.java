/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.util.ArrayList;

import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.mode.ValuesAliasResolver;
import org.h2.util.Utils;

/**
 * Command that supports VALUES clause.
 */
public abstract class CommandWithValues extends DataChangeStatement {

    /**
     * Expression data for the VALUES clause.
     */
    protected final ArrayList<Expression[]> valuesExpressionList = Utils.newSmallArrayList();

    /**
     * VALUES alias for MySQL 8.19+ style INSERT ... VALUES ... AS alias syntax.
     */
    protected String valuesAlias;

    /**
     * VALUES alias resolver for column resolution.
     */
    protected ValuesAliasResolver valuesAliasResolver;

    /**
     * Creates new instance of command with VALUES clause.
     *
     * @param session
     *            the session
     */
    protected CommandWithValues(SessionLocal session) {
        super(session);
    }

    /**
     * Add a row to this command.
     *
     * @param expr
     *            the list of values
     */
    public void addRow(Expression[] expr) {
        valuesExpressionList.add(expr);
    }

    /**
     * Set the VALUES alias.
     *
     * @param alias the alias name
     */
    public void setValuesAlias(String alias) {
        this.valuesAlias = alias;
    }

    /**
     * Get the VALUES alias.
     *
     * @return the alias name, or null if not set
     */
    public String getValuesAlias() {
        return valuesAlias;
    }

    /**
     * Get the VALUES alias resolver.
     *
     * @return the resolver, or null if not created
     */
    public ValuesAliasResolver getValuesAliasResolver() {
        return valuesAliasResolver;
    }

    /**
     * Set the VALUES alias resolver.
     *
     * @param resolver the resolver
     */
    protected void setValuesAliasResolver(ValuesAliasResolver resolver) {
        this.valuesAliasResolver = resolver;
    }

}
