/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.command.CommandInterface;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.message.DbException;
import org.h2.schema.Domain;
import org.h2.schema.Schema;
import org.h2.table.Column;
import org.h2.table.ColumnTemplate;

/**
 * This class represents the statements
 * ALTER DOMAIN SET DEFAULT
 * ALTER DOMAIN DROP DEFAULT
 * ALTER DOMAIN SET ON UPDATE
 * ALTER DOMAIN DROP ON UPDATE
 */
public class AlterDomainExpressions extends AlterDomain {

    private final int type;

    private Expression expression;

    public AlterDomainExpressions(SessionLocal session, Schema schema, int type) {
        super(session, schema);
        this.type = type;
    }

    public void setExpression(Expression expression) {
        this.expression = expression;
    }

    @Override
    long update(Schema schema, Domain domain) {
        switch (type) {
        case CommandInterface.ALTER_DOMAIN_DEFAULT:
            domain.setDefaultExpression(session, expression);
            break;
        case CommandInterface.ALTER_DOMAIN_ON_UPDATE:
            domain.setOnUpdateExpression(session, expression);
            break;
        default:
            throw DbException.getInternalError("type=" + type);
        }
        if (expression != null) {
            forAllDependencies(session, domain, this::copyColumn, this::copyDomain, true);
        }
        getDatabase().updateMeta(session, domain);
        return 0;
    }

    private boolean copyColumn(Domain domain, Column targetColumn) {
        return copyExpressions(session, domain, targetColumn);
    }

    private boolean copyDomain(Domain domain, Domain targetDomain) {
        return copyExpressions(session, domain, targetDomain);
    }

    private boolean copyExpressions(SessionLocal session, Domain domain, ColumnTemplate targetColumn) {
        switch (type) {
        case CommandInterface.ALTER_DOMAIN_DEFAULT: {
            Expression e = domain.getDefaultExpression();
            if (e != null && targetColumn.getDefaultExpression() == null) {
                targetColumn.setDefaultExpression(session, e);
                return true;
            }
            break;
        }
        case CommandInterface.ALTER_DOMAIN_ON_UPDATE: {
            Expression e = domain.getOnUpdateExpression();
            if (e != null && targetColumn.getOnUpdateExpression() == null) {
                targetColumn.setOnUpdateExpression(session, e);
                return true;
            }
        }
        }
        return false;
    }

    @Override
    public int getType() {
        return type;
    }

}
