/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.util.ArrayList;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.constraint.ConstraintActionType;
import org.h2.constraint.ConstraintDomain;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.message.DbException;
import org.h2.schema.Domain;
import org.h2.schema.Schema;
import org.h2.table.Column;
import org.h2.table.ColumnTemplate;
import org.h2.table.Table;

/**
 * This class represents the statement DROP DOMAIN
 */
public class DropDomain extends AlterDomain {

    private ConstraintActionType dropAction;

    public DropDomain(SessionLocal session, Schema schema) {
        super(session, schema);
        dropAction = getDatabase().getSettings().dropRestrict ? ConstraintActionType.RESTRICT
                : ConstraintActionType.CASCADE;
    }

    public void setDropAction(ConstraintActionType dropAction) {
        this.dropAction = dropAction;
    }

    @Override
    long update(Schema schema, Domain domain) {
        forAllDependencies(session, domain, this::copyColumn, this::copyDomain, true);
        getDatabase().removeSchemaObject(session, domain);
        return 0;
    }

    private boolean copyColumn(Domain domain, Column targetColumn) {
        Table targetTable = targetColumn.getTable();
        if (dropAction == ConstraintActionType.RESTRICT) {
            throw DbException.get(ErrorCode.CANNOT_DROP_2, domainName, targetTable.getCreateSQL());
        }
        String columnName = targetColumn.getName();
        ArrayList<ConstraintDomain> constraints = domain.getConstraints();
        if (constraints != null && !constraints.isEmpty()) {
            for (ConstraintDomain constraint : constraints) {
                Expression checkCondition = constraint.getCheckConstraint(session, columnName);
                AlterTableAddConstraint check = new AlterTableAddConstraint(session, targetTable.getSchema(),
                        CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_CHECK, false);
                check.setTableName(targetTable.getName());
                check.setCheckExpression(checkCondition);
                check.update();
            }
        }
        copyExpressions(session, domain, targetColumn);
        return true;
    }

    private boolean copyDomain(Domain domain, Domain targetDomain) {
        if (dropAction == ConstraintActionType.RESTRICT) {
            throw DbException.get(ErrorCode.CANNOT_DROP_2, domainName, targetDomain.getTraceSQL());
        }
        ArrayList<ConstraintDomain> constraints = domain.getConstraints();
        if (constraints != null && !constraints.isEmpty()) {
            for (ConstraintDomain constraint : constraints) {
                Expression checkCondition = constraint.getCheckConstraint(session, null);
                AlterDomainAddConstraint check = new AlterDomainAddConstraint(session, targetDomain.getSchema(), //
                        false);
                check.setDomainName(targetDomain.getName());
                check.setCheckExpression(checkCondition);
                check.update();
            }
        }
        copyExpressions(session, domain, targetDomain);
        return true;
    }

    private static boolean copyExpressions(SessionLocal session, Domain domain, ColumnTemplate targetColumn) {
        targetColumn.setDomain(domain.getDomain());
        Expression e = domain.getDefaultExpression();
        boolean modified = false;
        if (e != null && targetColumn.getDefaultExpression() == null) {
            targetColumn.setDefaultExpression(session, e);
            modified = true;
        }
        e = domain.getOnUpdateExpression();
        if (e != null && targetColumn.getOnUpdateExpression() == null) {
            targetColumn.setOnUpdateExpression(session, e);
            modified = true;
        }
        return modified;
    }

    @Override
    public int getType() {
        return CommandInterface.DROP_DOMAIN;
    }

}
